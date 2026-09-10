package kc2g

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/RealDougEubanks/solarham/internal/metric"
	"github.com/RealDougEubanks/solarham/internal/redact"
)

// stationTimeLayout is what the API emits: an ISO-like timestamp with no zone
// suffix. The site serves UTC throughout, so it is parsed as UTC. Reading it as
// local time would shift every reading by the container's offset and, worse,
// would make the freshness filter reject or accept the wrong stations.
const stationTimeLayout = "2006-01-02T15:04:05"

const (
	// confidenceManual is the score for an ionogram scaled by hand. It is a
	// sentinel rather than a percentage and means the best possible quality, so
	// it passes any threshold and publishes as a full 100.
	confidenceManual = 999

	// confidenceBest is what a manually scaled station publishes as, keeping the
	// series inside the 0 to 100 range its help text promises.
	confidenceBest = 100
)

// stationRecord is one ionosonde's entry in stations.json.
//
// Every measurement is a pointer so that an absent field is distinguishable
// from a reported zero. Both produce no sample, but for different reasons, and
// conflating them at the parse step would hide which is happening.
type stationRecord struct {
	Time string   `json:"time"`
	FoF2 *float64 `json:"fof2"`
	MUFD *float64 `json:"mufd"`
	MD   number   `json:"md"`
	HmF2 *float64 `json:"hmf2"`
	CS   *float64 `json:"cs"`

	// FoEs and FbEs are the measured sporadic-E frequencies, and they are the
	// most operationally useful fields in this document.
	//
	// foEs is the Es layer's critical frequency: the thing every sporadic-E
	// prediction is trying to estimate, here measured directly by an
	// ionosonde. fbEs is the blanketing frequency, below which the Es layer
	// screens the F region entirely — so a high fbEs does not mean good
	// conditions, it means normal F2 paths have stopped working.
	//
	// Roughly 45 of the 101 stations report foEs at any given time; the rest
	// send null, which is absent rather than zero.
	FoEs *float64 `json:"foes"`
	FbEs *float64 `json:"fbes"`

	// FoE is the ordinary E layer critical frequency.
	FoE *float64 `json:"foe"`

	// TEC is derived from the ionosonde's own electron density profile, which
	// makes it independent of the GNSS-derived TEC that GloTEC publishes.
	// Where both exist for a location, disagreement between them is
	// informative rather than an error.
	TEC *float64 `json:"tec"`

	// Source names the network the measurement came from: giro,
	// giro_fastchar, ingv, noaa or aus-sws. Worth publishing because it is the
	// only way to see, from the metrics, that most of this data originates
	// from GIRO — which is why polling GIRO directly as well would be
	// duplicating a feed we already have.
	Source string `json:"source"`

	Station struct {
		Code string `json:"code"`
		Name string `json:"name"`
	} `json:"station"`
}

// number accepts a JSON number or a JSON string holding a number.
//
// The md field arrives quoted — "3.352" — while every other measurement in the
// same object arrives bare. A non-numeric string is recorded as absent rather
// than failing the decode, because one station with a corrupt field must not
// cost every other station in the document.
type number struct {
	Value float64
	Valid bool
}

// UnmarshalJSON implements json.Unmarshaler.
func (n *number) UnmarshalJSON(data []byte) error {
	text := strings.TrimSpace(string(data))
	if text == "null" || text == "" {
		return nil
	}
	if len(text) > 1 && text[0] == '"' {
		var s string
		if err := json.Unmarshal(data, &s); err != nil {
			return nil
		}
		text = strings.TrimSpace(s)
	}
	v, err := strconv.ParseFloat(text, 64)
	if err != nil {
		return nil
	}
	n.Value, n.Valid = v, true
	return nil
}

// dropReason names why a station produced nothing, for the summary log line.
type dropCounts struct {
	stale        int
	confidence   int
	notSelected  int
	unparsedTime int
	noCode       int
}

// pollStations fetches and converts stations.json.
func (s *Source) pollStations(ctx context.Context) ([]metric.Sample, error) {
	endpoint := s.baseURL + pathStations

	body, err := s.fetch(ctx, endpoint)
	if err != nil {
		return nil, err
	}

	var records []stationRecord
	if err := json.Unmarshal(body, &records); err != nil {
		return nil, fmt.Errorf("kc2g: parsing %s: %w", redact.URL(endpoint), err)
	}

	return s.samplesFromStations(records), nil
}

// samplesFromStations applies the filters and converts what survives.
func (s *Source) samplesFromStations(records []stationRecord) []metric.Sample {
	now := s.now().UTC()
	samples := make([]metric.Sample, 0, len(records)*4)
	var drops dropCounts
	published := 0

	for _, rec := range records {
		code := strings.ToUpper(strings.TrimSpace(rec.Station.Code))
		if code == "" {
			// Without a URSI code there is no usable label value, and an empty
			// label fails validation.
			drops.noCode++
			continue
		}
		if !s.selected(code) {
			drops.notSelected++
			continue
		}

		observed, err := parseStationTime(rec.Time)
		if err != nil {
			drops.unparsedTime++
			s.log.Debug("kc2g station has an unusable timestamp",
				"station", code, "time", rec.Time, "error", err)
			continue
		}
		if age := now.Sub(observed); age > s.maxAge {
			// The document is fresh; this station inside it is not. Publishing
			// a months-old foF2 as the current one is worse than publishing
			// nothing at all.
			drops.stale++
			s.log.Debug("kc2g station dropped as stale",
				"station", code, "observed", observed, "age", age.Round(time.Minute))
			continue
		}

		confidence, ok := confidenceOf(rec.CS)
		if !ok || confidence < s.minConfidence {
			drops.confidence++
			continue
		}

		name := strings.TrimSpace(rec.Station.Name)
		if name == "" {
			// An empty label value fails Sample.Validate, so the code stands in
			// for the missing name rather than the sample being lost.
			name = code
		}
		labels := []string{code, name}

		samples = s.emitStationValue(samples, metric.FoF2, labels, rec.FoF2, observed)
		samples = s.emitStationValue(samples, metric.MUF, labels, rec.MUFD, observed)
		if rec.MD.Valid && rec.MD.Value != 0 {
			samples = s.emit(samples, metric.Sample{
				Desc: metric.MUFFactor, Labels: labels, Value: rec.MD.Value, Time: observed,
			})
		}
		samples = s.emitStationValue(samples, metric.HmF2, labels, rec.HmF2, observed)

		// Sporadic E and the E layer. These fields were present in this
		// document from the start and simply were not being decoded, which
		// meant the exporter was fetching measured sporadic-E data every poll
		// and throwing it away.
		samples = s.emitStationValue(samples, metric.FoEs, labels, rec.FoEs, observed)
		samples = s.emitStationValue(samples, metric.FbEs, labels, rec.FbEs, observed)
		samples = s.emitStationValue(samples, metric.FoE, labels, rec.FoE, observed)
		samples = s.emitStationValue(samples, metric.StationTEC, labels, rec.TEC, observed)

		samples = s.emit(samples, metric.Sample{
			Desc:   metric.StationConfidence,
			Labels: []string{code},
			Value:  confidence,
			Time:   observed,
		})

		// Which upstream network the reading came from. Published as an info
		// metric so an operator can see at a glance how much of this data
		// originates from GIRO.
		if rec.Source != "" {
			samples = s.emit(samples, metric.Sample{
				Desc:   metric.StationSourceInfo,
				Labels: []string{code, rec.Source},
				Value:  1,
				Time:   observed,
			})
		}

		// Per-station age, so the freshness filtering below is visible rather
		// than silent. An operator seeing no foF2 needs to know whether the
		// filter or the upstream is responsible.
		//
		// This one is stamped at poll time, not at the observation time, and
		// that is deliberate. "How old is this reading" is a fact about now,
		// not about when the reading was taken — stamping it at `observed`
		// would assert that the data was 295 seconds old at the very moment it
		// was measured.
		samples = s.emit(samples, metric.Sample{
			Desc:   metric.StationDataAge,
			Labels: []string{Name, code},
			Value:  now.Sub(observed).Seconds(),
			Time:   now,
		})

		published++
	}

	// The filter counts are published, not only logged.
	//
	// Most of this document is routinely discarded — well over half the
	// stations in it are last-known values from years ago — and an operator
	// looking at an empty foF2 panel cannot otherwise tell whether the filter
	// or the upstream is responsible. A debug log answers that only for
	// someone already SSHed into the host.
	for reason, count := range map[string]int{
		"stale":           drops.stale,
		"low_confidence":  drops.confidence,
		"not_selected":    drops.notSelected,
		"bad_timestamp":   drops.unparsedTime,
		"no_station_code": drops.noCode,
	} {
		samples = s.emit(samples, metric.Sample{
			Desc:   metric.StationsFiltered,
			Labels: []string{Name, reason},
			Value:  float64(count),
			Time:   now,
		})
	}

	s.log.Debug("kc2g stations processed",
		"read", len(records),
		"published", published,
		"samples", len(samples),
		"dropped_stale", drops.stale,
		"dropped_low_confidence", drops.confidence,
		"dropped_not_selected", drops.notSelected,
		"dropped_bad_time", drops.unparsedTime,
		"dropped_no_code", drops.noCode,
		"max_age", s.maxAge,
		"min_confidence", s.minConfidence)

	return samples
}

// emitStationValue appends one measurement, if it was reported at all.
func (s *Source) emitStationValue(out []metric.Sample, desc *metric.Descriptor,
	labels []string, value *float64, observed time.Time) []metric.Sample {

	if value == nil || *value == 0 {
		return out
	}
	return s.emit(out, metric.Sample{
		Desc: desc, Labels: labels, Value: *value, Time: observed,
	})
}

// selected reports whether the allow-list admits this station. An empty
// allow-list admits everything.
func (s *Source) selected(code string) bool {
	if len(s.stations) == 0 {
		return true
	}
	_, ok := s.stations[code]
	return ok
}

// confidenceOf normalises the autoscaling confidence score.
//
// The field carries two sentinels alongside its 0 to 100 range: 999 means the
// ionogram was scaled by hand, which is the best quality available, and -1 means
// the scaler did not report a score at all. An unknown score fails the filter,
// because "we do not know whether this reading is any good" is not a reason to
// publish it.
func confidenceOf(cs *float64) (float64, bool) {
	if cs == nil {
		return 0, false
	}
	switch {
	case *cs == confidenceManual:
		return confidenceBest, true
	case *cs < 0:
		return 0, false
	default:
		return *cs, true
	}
}

// parseStationTime reads the zone-less timestamp as UTC, tolerating the zone
// suffix the API does not currently send but might.
func parseStationTime(value string) (time.Time, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}, fmt.Errorf("no timestamp")
	}
	if t, err := time.ParseInLocation(stationTimeLayout, value, time.UTC); err == nil {
		return t.UTC(), nil
	}
	t, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("unrecognised timestamp %q", value)
	}
	return t.UTC(), nil
}
