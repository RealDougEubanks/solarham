// Package fmi polls the Finnish Meteorological Institute's regional auroral
// activity products: the R-index at eleven Fennoscandian magnetometer stations,
// and the RX nowcast, which is a peak-to-peak field range over the previous
// hour.
//
// # What is fetched, and what deliberately is not
//
// Two documents, both small and both CC BY 4.0:
//
//	/MIRACLE/RWC/data/r_index_latest_en.json   ~3.2 KB, R-index per station
//	/MIRACLE/RWC/data/RX_latest_en.json        ~2.8 KB, RX/RXmin/RXmax
//
// FMI also serves the raw realtime magnetometer traces behind these indices,
// at /image/realtime/UT/<STN>/<STN>data_01.txt. Those files are NOT fetched,
// and that is the reason this package is as narrow as it is. They carry a
// different and much more restrictive statement than the JSON does: "The
// real-time data files must not be distributed on any other servers" and "Any
// commercial use is forbidden without a written permission by FMI." An
// exporter's entire job is to redistribute what it fetches onto another
// server, so fetching those files would put every operator of this exporter in
// breach by design. The distinction between the two licences is easy to miss
// because both live under space.fmi.fi, so it is written down here rather than
// left to be rediscovered.
//
// The index JSON, by contrast, states plainly: "Copyright Finnish
// Meteorological Institute. Licensed Under Creative Commons BY 4.0". That is
// fine to republish with attribution, which is why this source is not in the
// restricted tier.
//
// # The stale-content trap
//
// This is the single most important thing in this package.
//
// FMI's server rewrites both files on a timer whether or not the content
// behind them changed. Verified twice against the live service: Last-Modified
// read 2026-09-09 15:40:57 GMT while every station's payload Time read
// 2026-09-07 11:55 — the file was freshly written and the observations inside
// it were two days old, despite the document itself claiming "Updates every
// five minutes".
//
// So Last-Modified is worthless here as a freshness signal, and a conditional
// GET will almost never answer 304 even when nothing has changed. Every
// station's own Time field is parsed and published as StationDataAge, and the
// newest of them as SourceDataAge. The values are still published, with their
// true age attached: a two-day-old R-index is a real measurement of two days
// ago, and dropping it silently would leave an operator with an empty
// dashboard and no explanation. A rising StationDataAge with no polling
// failures is the alarm.
//
// A station with "R-index": null carries "Probability of auroras": "Missing
// last observation". No index or probability sample is emitted for it — a
// missing observation is not a zero R-index — and it is counted under
// StationsFiltered so the gap is visible rather than merely absent.
//
// # RX and the field-range descriptor
//
// RX is published as GeomagneticFieldRange rather than as its own metric. FMI
// documents RX as the peak-to-peak variation of the field over the previous
// hour, in nanotesla, which is exactly what that descriptor means: the same
// quantity USGS observatories contribute under the same name. The label is
// "observatory" rather than "station" because that is the descriptor's label,
// and FMI's magnetometer sites are observatories. RXmin and RXmax are the
// predicted bounds for the *next* hour and are forecasts rather than
// measurements, so they are not published here.
package fmi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/RealDougEubanks/solarham/internal/config"
	"github.com/RealDougEubanks/solarham/internal/httpx"
	"github.com/RealDougEubanks/solarham/internal/metric"
	"github.com/RealDougEubanks/solarham/internal/redact"
	"github.com/RealDougEubanks/solarham/internal/source"
)

// Name identifies this source in logs, metrics and health output.
const Name = "fmi"

const (
	// defaultBaseURL is the public service.
	defaultBaseURL = "https://space.fmi.fi"

	// pathRIndex carries the R-index per station.
	pathRIndex = "/MIRACLE/RWC/data/r_index_latest_en.json"

	// pathRX carries the hourly field-range nowcast.
	pathRX = "/MIRACLE/RWC/data/RX_latest_en.json"

	// defaultInterval matches the cadence the document claims for itself. It is
	// a claim rather than a guarantee — see the package comment — but there is
	// no reason to poll faster than a publisher says it writes.
	defaultInterval = 5 * time.Minute

	// defaultTimeout bounds one request.
	defaultTimeout = 30 * time.Second

	// maxBodyBytes bounds a response. The two documents are about 3 KB each.
	maxBodyBytes = 1 << 20

	// missingObservation is the wording FMI uses in place of a probability when
	// a station's last observation is absent.
	missingObservation = "Missing last observation"
)

// Filter reasons published under StationsFiltered. They are emitted every poll,
// including as zero, because an operator seeing no data for a station needs to
// distinguish "we dropped it" from "the upstream never mentioned it".
const (
	reasonMissingObservation = "missing_observation"
	reasonUnparseableTime    = "unparseable_timestamp"
)

// Timestamp layouts. The two documents disagree with each other, which is
// itself worth writing down: the R-index file writes
// "2026-09-07 11:55:00+00:00" and the RX file writes
// "2026-09-07T11:40:08+0000". Neither is RFC 3339, so neither decodes into a
// time.Time without help.
const (
	rIndexTimeLayout = "2006-01-02 15:04:05Z07:00"
	rxTimeLayout     = "2006-01-02T15:04:05-0700"
)

// Source polls the FMI regional auroral index.
type Source struct {
	baseURL  string
	client   *httpx.Client
	interval time.Duration
	timeout  time.Duration
	retries  int
	log      *slog.Logger

	// now is injectable because the age calculation is the whole point of this
	// source and has to be tested against a fixture with a fixed timestamp.
	now func() time.Time
}

var _ source.Source = (*Source)(nil)

// New builds a source from validated configuration.
//
// The HTTP client is the shared one so that per-host spacing for space.fmi.fi
// is shared with anything else that ever fetches from it. Its timeout is
// process-wide, so cfg.Timeout is applied as a per-request context deadline.
func New(cfg config.FMI, client *httpx.Client, log *slog.Logger) (*Source, error) {
	if client == nil {
		return nil, errors.New("fmi: nil HTTP client")
	}
	if log == nil {
		log = slog.Default()
	}

	base := strings.TrimRight(strings.TrimSpace(cfg.BaseURL), "/")
	if base == "" {
		base = defaultBaseURL
	}
	u, err := url.Parse(base)
	if err != nil {
		return nil, fmt.Errorf("fmi: base URL %q is not a URL: %w", redact.URL(base), err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("fmi: base URL must be http or https, got %q", u.Scheme)
	}
	if u.Host == "" {
		return nil, errors.New("fmi: base URL has no host")
	}

	interval := cfg.Interval
	if interval <= 0 {
		interval = defaultInterval
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	retries := cfg.Retries
	if retries < 0 {
		retries = 0
	}

	return &Source{
		baseURL:  base,
		client:   client,
		interval: interval,
		timeout:  timeout,
		retries:  retries,
		log:      log.With("source", Name),
		now:      time.Now,
	}, nil
}

// Name identifies the source.
func (s *Source) Name() string { return Name }

// Interval is the nominal spacing between polls.
func (s *Source) Interval() time.Duration { return s.interval }

// Schedule polls on a fixed interval matching the documents' claimed cadence.
func (s *Source) Schedule() source.Schedule { return source.Every(s.interval) }

// Poll fetches both documents and converts what they return into samples.
//
// The two are independent products and are fetched independently: RX being
// absent should not cost the R-index, and the R-index having a bad afternoon
// should not cost RX.
func (s *Source) Poll(ctx context.Context) (metric.Batch, error) {
	now := s.now().UTC()
	batch := metric.Batch{Source: Name, Fetched: now}

	if err := ctx.Err(); err != nil {
		return batch, fmt.Errorf("fmi: poll cancelled: %w", err)
	}

	rBody, rErr := s.fetch(ctx, s.baseURL+pathRIndex)
	rxBody, rxErr := s.fetch(ctx, s.baseURL+pathRX)

	// Both unchanged is a real, successful, uneventful poll. It should be rare
	// here, because FMI rewrites these files on a timer and the validators move
	// even when the observations do not.
	if errors.Is(rErr, httpx.ErrNotModified) && errors.Is(rxErr, httpx.ErrNotModified) {
		return batch, source.ErrNotModified
	}

	var (
		samples  []metric.Sample
		newest   time.Time
		filtered = map[string]int{
			reasonMissingObservation: 0,
			reasonUnparseableTime:    0,
		}
		errs       []error
		fetchedAny bool
	)

	switch {
	case rErr == nil:
		fetchedAny = true
		var out []metric.Sample
		out, newest = s.parseRIndex(rBody, now, filtered)
		samples = append(samples, out...)
	case errors.Is(rErr, httpx.ErrNotModified):
	default:
		errs = append(errs, rErr)
	}

	switch {
	case rxErr == nil:
		fetchedAny = true
		out, rxNewest := s.parseRX(rxBody, now, filtered)
		samples = append(samples, out...)
		if rxNewest.After(newest) {
			newest = rxNewest
		}
	case errors.Is(rxErr, httpx.ErrNotModified):
	default:
		errs = append(errs, rxErr)
	}

	// Filter counts are published whenever a document was read, including as
	// zero, so a station disappearing from the output has a number attached to
	// it rather than just a gap. They are not published when nothing was read
	// at all: a zero count from a failed fetch would assert that nothing was
	// dropped, which is not something a failed fetch can know.
	if fetchedAny {
		reasons := make([]string, 0, len(filtered))
		for reason := range filtered {
			reasons = append(reasons, reason)
		}
		sort.Strings(reasons)
		for _, reason := range reasons {
			samples = s.emit(samples, metric.Sample{
				Desc:   metric.StationsFiltered,
				Labels: []string{Name, reason},
				Value:  float64(filtered[reason]),
				Time:   now,
			})
		}
	}

	// SourceDataAge is measured from the newest payload timestamp, never from
	// Last-Modified. That choice is the entire freshness guard of this package.
	if !newest.IsZero() {
		samples = s.emit(samples, metric.Sample{
			Desc:   metric.SourceDataAge,
			Labels: []string{Name},
			Value:  now.Sub(newest).Seconds(),
			Time:   now,
		})
	}
	batch.Samples = samples

	joined := errors.Join(errs...)
	if joined == nil {
		return batch, nil
	}
	if errors.Is(joined, httpx.ErrRateLimited) {
		return metric.Batch{Source: Name, Fetched: now},
			fmt.Errorf("%w: %v", source.ErrRateLimited, joined)
	}
	if len(batch.Samples) > 0 {
		s.log.Warn("fmi partially succeeded; publishing what was fetched",
			"samples", len(batch.Samples), "error", joined)
		return batch, nil
	}
	return metric.Batch{Source: Name, Fetched: now}, fmt.Errorf("fmi: %w", joined)
}

// parseRIndex converts the R-index document into samples and reports the newest
// payload timestamp it contained.
func (s *Source) parseRIndex(body []byte, now time.Time, filtered map[string]int) ([]metric.Sample, time.Time) {
	var doc rIndexDocument
	if err := json.Unmarshal(body, &doc); err != nil {
		s.log.Warn("fmi R-index document was unparseable", "error", err)
		return nil, time.Time{}
	}

	var (
		out    []metric.Sample
		newest time.Time
	)
	for _, code := range sortedKeys(doc.Data) {
		station := doc.Data[code]

		observed, err := time.Parse(rIndexTimeLayout, strings.TrimSpace(station.Time))
		if err != nil {
			// Without a payload timestamp there is no honest age to attach, and
			// an age is the only thing that makes this data trustworthy.
			filtered[reasonUnparseableTime]++
			s.log.Warn("fmi station had an unparseable timestamp",
				"station", code, "time", station.Time)
			continue
		}
		observed = observed.UTC()
		if observed.After(newest) {
			newest = observed
		}

		// Age is published for every station with a readable timestamp,
		// including one whose observation is missing. A station that has gone
		// quiet needs its age to keep climbing; removing the series would make
		// the failure invisible, which is exactly the trap this package exists
		// to avoid.
		out = s.emit(out, metric.Sample{
			Desc:   metric.StationDataAge,
			Labels: []string{Name, code},
			Value:  now.Sub(observed).Seconds(),
			Time:   now,
		})

		if station.RIndex == nil || !usable(*station.RIndex) {
			filtered[reasonMissingObservation]++
			s.log.Debug("fmi station has no R-index for its last interval",
				"station", code, "probability", station.Probability)
			continue
		}

		out = s.emit(out, metric.Sample{
			Desc:   metric.AuroraRegionalIndex,
			Labels: []string{code},
			Value:  *station.RIndex,
			Time:   observed,
		})

		// The worded probability is only meaningful alongside a value. When the
		// observation is missing FMI puts "Missing last observation" in this
		// field, which is a status rather than a likelihood and would read as a
		// forecast category if published as one.
		probability := strings.TrimSpace(station.Probability)
		if probability != "" && probability != missingObservation {
			out = s.emit(out, metric.Sample{
				Desc:   metric.AuroraProbabilityInfo,
				Labels: []string{code, probability},
				Value:  1,
				Time:   observed,
			})
		}
	}
	return out, newest
}

// parseRX converts the RX nowcast into field-range samples.
func (s *Source) parseRX(body []byte, now time.Time, filtered map[string]int) ([]metric.Sample, time.Time) {
	var doc rxDocument
	if err := json.Unmarshal(body, &doc); err != nil {
		s.log.Warn("fmi RX document was unparseable", "error", err)
		return nil, time.Time{}
	}

	var (
		out    []metric.Sample
		newest time.Time
	)
	for _, code := range sortedKeys(doc.Data) {
		station := doc.Data[code]

		observed, err := time.Parse(rxTimeLayout, strings.TrimSpace(station.Time))
		if err != nil {
			filtered[reasonUnparseableTime]++
			s.log.Warn("fmi RX station had an unparseable timestamp",
				"station", code, "time", station.Time)
			continue
		}
		observed = observed.UTC()
		if observed.After(newest) {
			newest = observed
		}

		// A null RX carries the literal string "Null" as its activity level.
		if station.RX.Value == nil || !usable(*station.RX.Value) {
			filtered[reasonMissingObservation]++
			s.log.Debug("fmi station has no RX for the previous hour", "station", code)
			continue
		}

		out = s.emit(out, metric.Sample{
			Desc:   metric.GeomagneticFieldRange,
			Labels: []string{code},
			Value:  *station.RX.Value,
			Time:   observed,
		})
	}
	return out, newest
}

// fetch performs one conditional GET through the shared client.
//
// Conditional is on because it costs a header and, on the days FMI's writer is
// idle, saves a body. It is emphatically not a freshness signal: see the
// package comment on Last-Modified moving while the content does not.
func (s *Source) fetch(ctx context.Context, endpoint string) ([]byte, error) {
	reqCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	resp, err := s.client.Get(reqCtx, httpx.Request{
		URL:         endpoint,
		Conditional: true,
		Retries:     s.retries,
		MaxBody:     maxBodyBytes,
	})
	if err != nil {
		return nil, err
	}
	if len(resp.Body) == 0 {
		return nil, fmt.Errorf("fmi: %s returned an empty body", redact.URL(endpoint))
	}
	return resp.Body, nil
}

// emit appends a sample after checking it against its descriptor.
func (s *Source) emit(out []metric.Sample, sample metric.Sample) []metric.Sample {
	if err := sample.Validate(); err != nil {
		s.log.Warn("fmi discarded an invalid sample", "error", err)
		return out
	}
	return append(out, sample)
}

// sortedKeys orders station codes so a batch is deterministic. Go map order is
// randomised, and a batch whose sample order changes every poll makes test
// failures and log diffs harder to read for no gain.
func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		if strings.TrimSpace(k) != "" {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

// usable reports whether a decoded number is real. JSON permits neither NaN nor
// infinity, but a value arriving as 1e400 decodes to +Inf without error.
func usable(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }

// ---------------------------------------------------------------------------
// Response shapes
// ---------------------------------------------------------------------------

// rIndexDocument is r_index_latest_en.json.
//
// Field names are capitalised and spaced in the payload ("R-index",
// "Probability of auroras"), and RIndex is a pointer because null is how a
// missing observation arrives.
type rIndexDocument struct {
	Info struct {
		Description string `json:"description"`
		Copyright   string `json:"copyright"`
		Encoding    string `json:"encoding"`
	} `json:"info"`
	Data map[string]rIndexStation `json:"data"`
}

type rIndexStation struct {
	Time        string   `json:"Time"`
	RIndex      *float64 `json:"R-index"`
	Latitude    float64  `json:"Geographic latitude"`
	Longitude   float64  `json:"Geographic longitude"`
	LimitLower  float64  `json:"Limit value lower"`
	LimitHigher float64  `json:"Limit value higher"`
	Probability string   `json:"Probability of auroras"`
	Station     string   `json:"Station"`
}

// rxDocument is RX_latest_en.json. Note the lower-case "time" and "station"
// keys, where the R-index document capitalises both.
type rxDocument struct {
	Info struct {
		Description string `json:"description"`
		Rights      string `json:"Rights"`
		Issued      string `json:"issued"`
	} `json:"info"`
	Data map[string]rxStation `json:"data"`
}

type rxStation struct {
	Time    string  `json:"time"`
	RX      rxValue `json:"RX"`
	RXMin   rxValue `json:"RXmin"`
	RXMax   rxValue `json:"RXmax"`
	Station string  `json:"station"`
}

type rxValue struct {
	Value *float64 `json:"value"`
	Level string   `json:"activity level"`
}
