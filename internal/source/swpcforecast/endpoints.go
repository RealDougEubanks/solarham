package swpcforecast

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/RealDougEubanks/solarham/internal/metric"
)

// Document paths. These are the published product URLs; they are constants
// rather than literals so the endpoint table reads as a list of documents.
const (
	pathProbabilities  = "/json/solar_probabilities.json"
	pathRegions        = "/json/solar_regions.json"
	pathKpForecast     = "/products/noaa-planetary-k-index-forecast.json"
	pathFluxForecast   = "/json/predicted_f107cm_flux.json"
	pathAIndexForecast = "/json/predicted_fredericksburg_a_index.json"
)

// Every numeric field in these documents is decoded through *float64 rather
// than float64.
//
// The distinction is load-bearing: SWPC writes JSON null for a field it has no
// value for, and several of them — a region's proton_probability, a spot count
// for a region with no spots resolved — are routinely null. Decoded into a bare
// float64 a null becomes 0, and a zero flare probability published as a fact is
// a different claim from no forecast being issued. A nil pointer produces no
// sample.

// probabilityRecord is one day of the solar probabilities document.
type probabilityRecord struct {
	Date string `json:"date"`

	CClass1Day *float64 `json:"c_class_1_day"`
	CClass2Day *float64 `json:"c_class_2_day"`
	CClass3Day *float64 `json:"c_class_3_day"`

	MClass1Day *float64 `json:"m_class_1_day"`
	MClass2Day *float64 `json:"m_class_2_day"`
	MClass3Day *float64 `json:"m_class_3_day"`

	XClass1Day *float64 `json:"x_class_1_day"`
	XClass2Day *float64 `json:"x_class_2_day"`
	XClass3Day *float64 `json:"x_class_3_day"`

	// The wire names begin with a digit. Go field names cannot, which is
	// exactly the kind of mismatch a struct tag exists for.
	Protons1Day *float64 `json:"10mev_protons_1_day"`
	Protons2Day *float64 `json:"10mev_protons_2_day"`
	Protons3Day *float64 `json:"10mev_protons_3_day"`

	PolarCapAbsorption string `json:"polar_cap_absorption"`
}

// parseProbabilities publishes the flare and proton-event probabilities and the
// polar cap absorption status.
//
// The document carries thirty-one records, newest first. Only the newest is
// published: the rest are the same forecast issued on previous days, which is
// history a time-series database is already storing.
//
// The order is not trusted. It has been newest-first every time it has been
// observed, but "the file happens to be sorted" is not a contract, and reading
// the wrong record here would publish a month-old forecast with today's
// timestamp — a failure that looks exactly like working software.
func parseProbabilities(_ *Source, body []byte) ([]metric.Sample, time.Time, error) {
	var records []probabilityRecord
	if err := json.Unmarshal(body, &records); err != nil {
		return nil, time.Time{}, fmt.Errorf("solar probabilities: %w", err)
	}
	if len(records) == 0 {
		return nil, time.Time{}, fmt.Errorf("solar probabilities: document is empty")
	}

	var (
		newest   probabilityRecord
		issued   time.Time
		haveDate bool
	)
	for _, r := range records {
		when, err := parseDate(r.Date)
		if err != nil {
			continue
		}
		if !haveDate || when.After(issued) {
			newest, issued, haveDate = r, when, true
		}
	}
	if !haveDate {
		return nil, time.Time{}, fmt.Errorf("solar probabilities: no record carried a readable date")
	}

	samples := make([]metric.Sample, 0, 13)
	add := func(desc *metric.Descriptor, v *float64, labels ...string) {
		if v == nil {
			return
		}
		samples = append(samples, metric.Sample{
			Desc: desc, Labels: labels, Value: *v, Time: issued,
		})
	}

	// Nine flare series: three classes by three horizons.
	byClass := map[string][3]*float64{
		"C": {newest.CClass1Day, newest.CClass2Day, newest.CClass3Day},
		"M": {newest.MClass1Day, newest.MClass2Day, newest.MClass3Day},
		"X": {newest.XClass1Day, newest.XClass2Day, newest.XClass3Day},
	}
	// Iterated in a fixed order rather than over the map, so a batch is
	// deterministic.
	for _, class := range []string{"C", "M", "X"} {
		values := byClass[class]
		for i, horizon := range horizons {
			add(metric.FlareProbability, values[i], class, horizon)
		}
	}

	protons := [3]*float64{newest.Protons1Day, newest.Protons2Day, newest.Protons3Day}
	for i, horizon := range horizons {
		add(metric.ProtonEventProbability, protons[i], horizon)
	}

	// Polar cap absorption is a colour word — green, yellow, red — and there is
	// no honest numeric equivalent, so it is published as an info sample with
	// the wording preserved. An operator recognises "red"; they do not
	// recognise an ordinal somebody invented.
	if status := strings.ToLower(strings.TrimSpace(newest.PolarCapAbsorption)); status != "" {
		samples = append(samples, metric.Sample{
			Desc: metric.PolarCapAbsorptionInfo, Labels: []string{status},
			Value: 1, Time: issued,
		})
	}

	return samples, issued, nil
}

// regionRecord is one active region on one day.
type regionRecord struct {
	ObservedDate string `json:"observed_date"`
	Region       *int   `json:"region"`

	Area        *float64 `json:"area"`
	NumberSpots *float64 `json:"number_spots"`
	SpotClass   string   `json:"spot_class"`
	MagClass    string   `json:"mag_class"`

	CFlareProbability *float64 `json:"c_flare_probability"`
	MFlareProbability *float64 `json:"m_flare_probability"`
	XFlareProbability *float64 `json:"x_flare_probability"`
	ProtonProbability *float64 `json:"proton_probability"`

	Latitude  *float64 `json:"latitude"`
	Longitude *float64 `json:"longitude"`
}

// parseRegions publishes aggregates over the newest day's active regions, and
// per-region series when they are enabled.
//
// The document is a rolling history: 232 records covering about a month, with
// each day's regions repeated. Filtering to the newest observed_date is
// mandatory, not a nicety — summing the whole file would report a month's worth
// of regions as being on the disc simultaneously.
//
// Aggregates are the default and per-region series are opt-in because region
// numbers churn. A region visible today has rotated off the disc within a
// fortnight, taking its label value with it, so the per-region series set is
// not stable over weeks and cannot be graphed as one.
func parseRegions(s *Source, body []byte) ([]metric.Sample, time.Time, error) {
	var records []regionRecord
	if err := json.Unmarshal(body, &records); err != nil {
		return nil, time.Time{}, fmt.Errorf("solar regions: %w", err)
	}
	if len(records) == 0 {
		return nil, time.Time{}, fmt.Errorf("solar regions: document is empty")
	}

	var (
		observed time.Time
		haveDate bool
	)
	for _, r := range records {
		when, err := parseDate(r.ObservedDate)
		if err != nil {
			continue
		}
		if !haveDate || when.After(observed) {
			observed, haveDate = when, true
		}
	}
	if !haveDate {
		return nil, time.Time{}, fmt.Errorf("solar regions: no record carried a readable observed_date")
	}

	newest := make([]regionRecord, 0, 16)
	for _, r := range records {
		when, err := parseDate(r.ObservedDate)
		if err != nil || !when.Equal(observed) {
			continue
		}
		newest = append(newest, r)
	}

	samples := make([]metric.Sample, 0, 8)
	add := func(desc *metric.Descriptor, value float64, labels ...string) {
		samples = append(samples, metric.Sample{
			Desc: desc, Labels: labels, Value: value, Time: observed,
		})
	}

	// The count is published even when it is zero. A spotless disc is a real
	// and reportable state — it is what most of a solar minimum looks like —
	// and suppressing the sample would leave the last non-zero count standing
	// on a dashboard indefinitely.
	add(metric.ActiveRegionCount, float64(len(newest)))

	var spots, area float64
	for _, r := range newest {
		if r.NumberSpots != nil {
			spots += *r.NumberSpots
		}
		if r.Area != nil {
			area += *r.Area
		}
	}
	add(metric.ActiveRegionSpotTotal, spots)
	add(metric.ActiveRegionAreaTotal, area)

	// The per-class maximum answers the operational question directly — is any
	// region on the disc likely to produce one — without needing the
	// per-region series set to do it.
	classes := []struct {
		label string
		pick  func(regionRecord) *float64
	}{
		{"C", func(r regionRecord) *float64 { return r.CFlareProbability }},
		{"M", func(r regionRecord) *float64 { return r.MFlareProbability }},
		{"X", func(r regionRecord) *float64 { return r.XFlareProbability }},
	}
	for _, c := range classes {
		maximum := math.Inf(-1)
		for _, r := range newest {
			if v := c.pick(r); v != nil && *v > maximum {
				maximum = *v
			}
		}
		if !math.IsInf(maximum, -1) {
			add(metric.ActiveRegionMaxFlareProbability, maximum, c.label)
		}
	}

	if s != nil && s.regions {
		for _, r := range newest {
			if r.Region == nil || *r.Region <= 0 {
				continue
			}
			label := fmt.Sprint(*r.Region)
			for _, c := range classes {
				if v := c.pick(r); v != nil {
					add(metric.ActiveRegionFlareProbability, *v, label, c.label)
				}
			}
		}
	}

	return samples, observed, nil
}

// kpRow is one three-hourly slot of the planetary K-index forecast document.
type kpRow struct {
	TimeTag string   `json:"time_tag"`
	Kp      *float64 `json:"kp"`

	// Observed is the row's state: "observed" for a measured value,
	// "estimated" for the near-term nowcast, "predicted" for the forecast
	// proper.
	Observed string `json:"observed"`
}

// Kp forecast horizon buckets, as offsets from the newest observed row.
var kpBuckets = []struct {
	label string
	upTo  time.Duration
}{
	{"3h", 3 * time.Hour},
	{"6h", 6 * time.Hour},
	{"1d", 24 * time.Hour},
	{"2d", 48 * time.Hour},
	{"3d", 72 * time.Hour},
}

// parseKpForecast publishes the forecast planetary K-index at five horizons.
//
// # The bucketing
//
// The document is a flat list of three-hourly slots spanning about a week: some
// already observed, some forward-looking. The horizon of a forward-looking slot
// is its time_tag minus the newest observed row's time_tag — that is, how far
// ahead of the last real measurement the prediction reaches. Offsets are
// assigned to the first bucket they fit in:
//
//	<= 3h  -> "3h"
//	<= 6h  -> "6h"
//	<= 24h -> "1d"
//	<= 48h -> "2d"
//	<= 72h -> "3d"
//
// Anything beyond 72 hours is dropped; SWPC's Kp forecast is a three-day
// product and the tail of the file is padding.
//
// Several slots fall into the wider buckets — eight three-hour slots land in
// "1d" alone — so a bucket takes the maximum Kp of its slots rather than the
// first or the mean. The operational question a K-index forecast answers is
// "how bad does it get in the next day", and a mean would average a
// three-hour G2 storm down into a quiet day.
//
// # A deviation, deliberately
//
// The brief said to use rows where observed == "predicted". The live document
// has three states, not two: the slots between the last measurement and the
// next UTC day are labelled "estimated", and only the days beyond that are
// "predicted". Using "predicted" alone therefore leaves the 3h and 6h buckets
// permanently empty — the near-term horizons a radio operator most wants — so
// any row that is not "observed" is treated as forward-looking. "estimated"
// is a nowcast, which is still a statement about a time that has not been
// measured.
func parseKpForecast(_ *Source, body []byte) ([]metric.Sample, time.Time, error) {
	var rows []kpRow
	if err := json.Unmarshal(body, &rows); err != nil {
		return nil, time.Time{}, fmt.Errorf("kp forecast: %w", err)
	}
	if len(rows) == 0 {
		return nil, time.Time{}, fmt.Errorf("kp forecast: document is empty")
	}

	type slot struct {
		when time.Time
		kp   float64
	}

	var (
		issued   time.Time
		haveObs  bool
		forecast []slot
	)
	for _, r := range rows {
		when, err := parseDate(r.TimeTag)
		if err != nil {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(r.Observed), "observed") {
			if !haveObs || when.After(issued) {
				issued, haveObs = when, true
			}
			continue
		}
		if r.Kp == nil {
			continue
		}
		forecast = append(forecast, slot{when: when, kp: *r.Kp})
	}

	if !haveObs {
		// Without a last-observed row there is no anchor for the horizons, and
		// substituting wall-clock time would silently shift every bucket
		// whenever the exporter's clock and SWPC's publication drifted apart.
		return nil, time.Time{}, fmt.Errorf("kp forecast: no observed row to measure horizons from")
	}

	maxima := make(map[string]float64, len(kpBuckets))
	for _, f := range forecast {
		offset := f.when.Sub(issued)
		if offset <= 0 {
			// A forward-looking row older than the last measurement is a
			// revision of history, not a forecast.
			continue
		}
		for _, b := range kpBuckets {
			if offset <= b.upTo {
				if v, seen := maxima[b.label]; !seen || f.kp > v {
					maxima[b.label] = f.kp
				}
				break
			}
		}
	}

	samples := make([]metric.Sample, 0, len(kpBuckets))
	for _, b := range kpBuckets {
		if v, ok := maxima[b.label]; ok {
			samples = append(samples, metric.Sample{
				Desc: metric.KIndexForecast, Labels: []string{b.label},
				Value: v,
				// The forecast was issued no earlier than its newest
				// measurement, so that is the honest issue time.
				Time: issued,
			})
		}
	}

	return samples, issued, nil
}

// threeDayRecord covers the two prediction documents, which share a shape:
// one record per issue date carrying a 1, 2 and 3 day value under
// document-specific field names.
type threeDayRecord struct {
	Date string `json:"date"`

	TenCM1Day *float64 `json:"tencmfcst_1_day"`
	TenCM2Day *float64 `json:"tencmfcst_2_day"`
	TenCM3Day *float64 `json:"tencmfcst_3_day"`

	AFred1Day *float64 `json:"afred_1_day"`
	AFred2Day *float64 `json:"afred_2_day"`
	AFred3Day *float64 `json:"afred_3_day"`
}

// parseFluxForecast publishes the predicted 10.7 cm radio flux.
func parseFluxForecast(_ *Source, body []byte) ([]metric.Sample, time.Time, error) {
	return parseThreeDay(body, "f10.7 forecast", metric.FluxForecastSFU,
		func(r threeDayRecord) [3]*float64 {
			return [3]*float64{r.TenCM1Day, r.TenCM2Day, r.TenCM3Day}
		})
}

// parseAIndexForecast publishes the predicted Fredericksburg A-index.
func parseAIndexForecast(_ *Source, body []byte) ([]metric.Sample, time.Time, error) {
	return parseThreeDay(body, "a-index forecast", metric.AIndexForecast,
		func(r threeDayRecord) [3]*float64 {
			return [3]*float64{r.AFred1Day, r.AFred2Day, r.AFred3Day}
		})
}

// parseThreeDay reads whichever of the two prediction documents pick describes.
//
// As with the probabilities, the newest record is selected by date rather than
// by position. These files are also observed to lag: the flux document's newest
// record has been seen dated a day behind the A-index document's, which is why
// SourceDataAge is computed from the newest timestamp across all five documents
// rather than per document.
func parseThreeDay(body []byte, what string, desc *metric.Descriptor,
	pick func(threeDayRecord) [3]*float64,
) ([]metric.Sample, time.Time, error) {
	var records []threeDayRecord
	if err := json.Unmarshal(body, &records); err != nil {
		return nil, time.Time{}, fmt.Errorf("%s: %w", what, err)
	}
	if len(records) == 0 {
		return nil, time.Time{}, fmt.Errorf("%s: document is empty", what)
	}

	var (
		newest   threeDayRecord
		issued   time.Time
		haveDate bool
	)
	for _, r := range records {
		when, err := parseDate(r.Date)
		if err != nil {
			continue
		}
		if !haveDate || when.After(issued) {
			newest, issued, haveDate = r, when, true
		}
	}
	if !haveDate {
		return nil, time.Time{}, fmt.Errorf("%s: no record carried a readable date", what)
	}

	values := pick(newest)
	samples := make([]metric.Sample, 0, 3)
	for i, horizon := range horizons {
		if values[i] == nil {
			continue
		}
		samples = append(samples, metric.Sample{
			Desc: desc, Labels: []string{horizon}, Value: *values[i], Time: issued,
		})
	}
	if len(samples) == 0 {
		return nil, time.Time{}, fmt.Errorf("%s: newest record carried no values", what)
	}
	return samples, issued, nil
}

// dateLayouts are the timestamp shapes these five documents use.
//
// They are inconsistent with each other, which is why this is a list. The
// probabilities and Kp documents write a naive date-time with no zone; the
// region summary and the two prediction documents write a bare date; and the
// GeoJSON-adjacent products elsewhere on the same host write RFC 3339 with a
// Z. All of it is UTC — SWPC publishes nothing in local time — so a naive value
// is read as UTC rather than as the exporter's local zone, which would shift
// every forecast by the host's offset.
var dateLayouts = []string{
	time.RFC3339,
	"2006-01-02T15:04:05",
	"2006-01-02 15:04:05",
	"2006-01-02",
}

// parseDate reads any of the timestamp shapes above as UTC.
func parseDate(raw string) (time.Time, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, fmt.Errorf("empty timestamp")
	}
	for _, layout := range dateLayouts {
		if t, err := time.Parse(layout, raw); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("timestamp %q matches no known layout", raw)
}
