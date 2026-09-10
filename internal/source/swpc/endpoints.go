package swpc

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/RealDougEubanks/solarham/internal/metric"
)

// The endpoint paths, every one of them verified live against
// services.swpc.noaa.gov. They are constants rather than strings inlined at
// their use sites because SWPC retires paths — the entire
// /products/solar-wind/ directory is now 404 — and when one goes, the list of
// what to check should be in one place.
const (
	pathWindSpeed     = "/products/summary/solar-wind-speed.json"
	pathWindMagField  = "/products/summary/solar-wind-mag-field.json"
	pathWindPlasma    = "/text/ace-swepam.txt"
	pathKpOneMinute   = "/json/planetary_k_index_1m.json"
	pathXRayFlares    = "/json/goes/primary/xray-flares-latest.json"
	pathNOAAScales    = "/products/noaa-scales.json"
	pathAlerts        = "/products/alerts.json"
	pathProtons       = "/json/goes/primary/integral-protons-1-day.json"
	pathElectrons     = "/json/goes/primary/integral-electrons-1-day.json"
	pathHemisphericPw = "/text/aurora-nowcast-hemi-power.txt"
	pathDRAP          = "/text/drap_global_frequencies.txt"
	pathF107          = "/json/f107_cm_flux.json"
	pathPlanetaryK    = "/products/noaa-planetary-k-index.json"
	pathDailyIndices  = "/text/daily-solar-indices.txt"
	pathKyotoDst      = "/products/kyoto-dst.json"
)

// xrayBand names the X-ray channel flare classes are derived from. GOES also
// publishes 0.05-0.4 nm, which is not what a class letter means, so the band is
// a label rather than being left implicit.
const xrayBand = "0.1-0.8nm"

// alertMaxAge bounds how far back an alert is still considered active, and
// maxAlerts bounds how many are published at once.
//
// SWPC's alerts document is a rolling archive, not a list of what is currently
// in force, so without a window every alert of the last week would be published
// as active. The cap exists because alert_active is labelled with a serial
// number: during a severe storm SWPC issues them continuously, and an unbounded
// label value is how a metric with three series becomes a metric with three
// thousand.
const (
	alertMaxAge = 24 * time.Hour
	maxAlerts   = 50
)

// endpoint is one document within a tier and the function that turns it into
// samples.
type endpoint struct {
	path  string
	parse func(t *tier, body []byte, fetched time.Time) ([]metric.Sample, error)
}

var fastTierEndpoints = []endpoint{
	{pathWindSpeed, parseWindSpeed},
	{pathWindMagField, parseWindMagField},
	{pathWindPlasma, parseWindPlasma},
	{pathKpOneMinute, parseKpOneMinute},
	{pathXRayFlares, parseXRayFlares},
	{pathNOAAScales, parseNOAAScales},
	{pathAlerts, parseAlerts},
}

// The OVATION grid at /text/ovation_latest_aurora_n.txt is deliberately absent.
// See the note on aurora_equatorward_boundary_degrees in parseHemisphericPower.
var mediumTierEndpoints = []endpoint{
	{pathProtons, parseProtons},
	{pathElectrons, parseElectrons},
	{pathHemisphericPw, parseHemisphericPower},
}

var slowTierEndpoints = []endpoint{
	{pathF107, parseF107},
	{pathPlanetaryK, parsePlanetaryK},
	{pathDailyIndices, parseDailyIndices},
	{pathKyotoDst, parseKyotoDst},
}

// drapEndpoint is appended to the medium tier only when D-RAP is enabled.
var drapEndpoint = endpoint{pathDRAP, parseDRAP}

// firstRecord decodes a summary product's single record.
//
// The summary products are documented and widely described as objects, and are
// in fact served as a one-element array:
//
//	[{"proton_speed": 401, "time_tag": "2026-09-09T03:46:00Z"}]
//
// Both are accepted. Handling only the documented shape would mean an exporter
// that parses nothing at all against the live service.
func firstRecord(body []byte) (row, error) {
	trimmed := strings.TrimLeft(string(body), " \t\r\n")
	if strings.HasPrefix(trimmed, "[") {
		rows, err := decodeRows(body)
		if err != nil {
			return nil, err
		}
		if len(rows) == 0 {
			return nil, fmt.Errorf("no records")
		}
		return rows[0], nil
	}
	var m row
	if err := json.Unmarshal(body, &m); err != nil {
		return nil, fmt.Errorf("decoding object: %w", err)
	}
	return m, nil
}

// latestRow returns the row with the newest parseable value in the named time
// field.
//
// Taking the maximum rather than the last element is deliberate: SWPC orders
// some products oldest-first and others newest-first — f107_cm_flux.json is
// newest-first while noaa-planetary-k-index.json is oldest-first — and an
// exporter that publishes the oldest row in the file as the current value is
// wrong in a way nobody notices for months.
func latestRow(rows []row, field string) (row, time.Time, bool) {
	var (
		best  row
		bestT time.Time
		found bool
	)
	for _, r := range rows {
		t, ok := r.timeField(field)
		if !ok {
			continue
		}
		if !found || t.After(bestT) {
			best, bestT, found = r, t, true
		}
	}
	return best, bestT, found
}

// parseWindSpeed publishes the L1 solar wind bulk speed.
func parseWindSpeed(_ *tier, body []byte, _ time.Time) ([]metric.Sample, error) {
	rec, err := firstRecord(body)
	if err != nil {
		return nil, err
	}
	when, ok := rec.timeField("time_tag")
	if !ok {
		return nil, fmt.Errorf("no usable time_tag")
	}
	speed, ok := rec.numberField("proton_speed")
	if !ok {
		return nil, nil
	}
	return []metric.Sample{{Desc: metric.WindSpeed, Value: speed, Time: when}}, nil
}

// parseWindPlasma publishes the L1 solar wind proton density.
//
// # Why the legacy text product rather than the JSON one
//
// Density is not in /products/summary/, which carries only speed and the
// magnetic field. The obvious modern source is /json/rtsw/rtsw_wind_1m.json,
// and it is the wrong choice here: it returns roughly 2.5 MB of 3,478 records
// covering a full day, every minute, to yield one number. That is about 3.6 GB
// of transfer a day against a public service, for a single gauge.
//
// /text/ace-swepam.txt carries the same quantity at the same one-minute
// cadence in about 9.5 KB — 265 times smaller. The trade is that it is ACE
// only, where the RTSW feed fails over between ACE, DSCOVR and IMAP, so
// density will go stale if ACE alone stops reporting while the others carry on.
// The sentinel handling below turns that into an absent series rather than a
// wrong one, which is the outcome that matters.
//
// # Why only density
//
// The file also carries a bulk speed, and it is deliberately ignored.
// parseWindSpeed already publishes speed from the summary product, and the two
// disagree: they are different spacecraft sampled at different instants. Two
// sources writing one series would make the value flap for no physical reason.
func parseWindPlasma(_ *tier, body []byte, _ time.Time) ([]metric.Sample, error) {
	// Columns, per the file's own header:
	//   YR MO DA HHMM  MJD  SecondsOfDay  S  ProtonDensity  BulkSpeed  IonTemp
	// S is a status flag where 0 means good data.
	const (
		colStatus  = 6
		colDensity = 7
		colCount   = 9
	)

	lines := textLines(body)
	for i := len(lines) - 1; i >= 0; i-- {
		fields := strings.Fields(lines[i])
		if len(fields) < colCount {
			continue
		}

		// A non-zero status flag marks the row as unusable. Reading its density
		// anyway is how a -9999.9 ends up on a dashboard as a real number.
		if status, err := strconv.Atoi(fields[colStatus]); err != nil || status != 0 {
			continue
		}

		density, err := strconv.ParseFloat(fields[colDensity], 64)
		if err != nil || isSentinel(density) {
			continue
		}

		when, err := parseSwepamTime(fields)
		if err != nil {
			continue
		}

		return []metric.Sample{{Desc: metric.WindDensity, Value: density, Time: when}}, nil
	}

	// Every row was flagged, sentinel-valued or malformed. The upstream is
	// reachable and simply has nothing usable, which is a successful poll with
	// no samples rather than a failure.
	return nil, nil
}

// parseSwepamTime builds a timestamp from the leading date columns of an
// ace-swepam row: year, month, day, then HHMM as a single four-digit field.
func parseSwepamTime(fields []string) (time.Time, error) {
	if len(fields) < 4 {
		return time.Time{}, fmt.Errorf("too few date fields")
	}
	if len(fields[3]) != 4 {
		return time.Time{}, fmt.Errorf("malformed HHMM field %q", fields[3])
	}
	stamp := fmt.Sprintf("%s-%s-%sT%s:%s:00", fields[0], fields[1], fields[2],
		fields[3][:2], fields[3][2:])
	return parseTime(stamp)
}

// parseWindMagField publishes the interplanetary magnetic field total and its
// north-south component.
//
// Bz is the operationally interesting one — a sustained southward Bz is what
// couples solar wind energy into the magnetosphere — so it is published under
// the same metric as Bt with a component label rather than being folded into a
// single number that would hide it.
func parseWindMagField(_ *tier, body []byte, _ time.Time) ([]metric.Sample, error) {
	rec, err := firstRecord(body)
	if err != nil {
		return nil, err
	}
	when, ok := rec.timeField("time_tag")
	if !ok {
		return nil, fmt.Errorf("no usable time_tag")
	}

	var samples []metric.Sample
	for _, f := range []struct {
		field     string
		component string
	}{
		{"bt", "bt"},
		{"bz_gsm", "bz"},
	} {
		v, ok := rec.numberField(f.field)
		if !ok {
			continue
		}
		samples = append(samples, metric.Sample{
			Desc:   metric.WindMagneticField,
			Labels: []string{f.component},
			Value:  v,
			Time:   when,
		})
	}
	return samples, nil
}

// parseKpOneMinute publishes the minute-cadence estimated planetary K-index.
func parseKpOneMinute(_ *tier, body []byte, _ time.Time) ([]metric.Sample, error) {
	rows, err := decodeRows(body)
	if err != nil {
		return nil, err
	}
	rec, when, ok := latestRow(rows, "time_tag")
	if !ok {
		return nil, fmt.Errorf("no usable time_tag in %d rows", len(rows))
	}
	kp, ok := rec.numberField("estimated_kp")
	if !ok {
		return nil, nil
	}
	return []metric.Sample{{Desc: metric.KIndexEstimated, Value: kp, Time: when}}, nil
}

// parseXRayFlares publishes the current GOES flare class both as it is written
// and as the flux it denotes.
func parseXRayFlares(t *tier, body []byte, _ time.Time) ([]metric.Sample, error) {
	rec, err := firstRecord(body)
	if err != nil {
		return nil, err
	}
	when, ok := rec.timeField("time_tag")
	if !ok {
		return nil, fmt.Errorf("no usable time_tag")
	}
	class, _ := rec["current_class"].(string)
	class = strings.TrimSpace(class)
	if class == "" {
		return nil, nil
	}

	samples := []metric.Sample{{
		Desc:   metric.XRayClassInfo,
		Labels: []string{class},
		Value:  1,
		Time:   when,
	}}

	flux, err := decodeFlareClass(class)
	if err != nil {
		// The class string is still publishable; only the derived flux is not.
		t.log.Warn("swpc could not decode a flare class to flux", "class", class, "error", err)
		return samples, nil
	}
	samples = append(samples, metric.Sample{
		Desc:   metric.XRayFlux,
		Labels: []string{xrayBand},
		Value:  flux,
		Time:   when,
	})
	return samples, nil
}

// scaleEntry is one day's NOAA scales.
//
// Scale arrives as a string ("0") and is null where the scale does not apply,
// which for the current entry means none — G0, R0, S0 — rather than unknown.
// That distinction is the whole reason this decodes through the number type
// instead of float64.
type scaleEntry struct {
	DateStamp string `json:"DateStamp"`
	TimeStamp string `json:"TimeStamp"`
	R         struct {
		Scale number `json:"Scale"`
	} `json:"R"`
	S struct {
		Scale number `json:"Scale"`
	} `json:"S"`
	G struct {
		Scale number `json:"Scale"`
	} `json:"G"`
}

// currentScalesKey is the key holding the current observation. The document is
// keyed by day offset: "-1" is yesterday, "0" is now, and "1" upwards are
// forecasts.
const currentScalesKey = "0"

// parseNOAAScales publishes the current NOAA G, S and R scale levels.
func parseNOAAScales(_ *tier, body []byte, _ time.Time) ([]metric.Sample, error) {
	var doc map[string]scaleEntry
	if err := json.Unmarshal(body, &doc); err != nil {
		return nil, fmt.Errorf("decoding noaa scales: %w", err)
	}
	entry, ok := doc[currentScalesKey]
	if !ok {
		return nil, fmt.Errorf("no %q entry in noaa scales", currentScalesKey)
	}

	when, err := parseTime(strings.TrimSpace(entry.DateStamp) + " " + strings.TrimSpace(entry.TimeStamp))
	if err != nil {
		return nil, fmt.Errorf("noaa scales timestamp: %w", err)
	}

	// A null scale is zero, not absent. "No storm" is a real observation and
	// publishing nothing for it would leave the previous storm level as the
	// most recent sample on the dashboard.
	scaleValue := func(n number) float64 {
		if !n.ok() {
			return 0
		}
		return n.Value
	}

	return []metric.Sample{
		{Desc: metric.GeomagneticStormScale, Value: scaleValue(entry.G.Scale), Time: when},
		{Desc: metric.RadioBlackoutScale, Value: scaleValue(entry.R.Scale), Time: when},
		{Desc: metric.RadiationStormScale, Value: scaleValue(entry.S.Scale), Time: when},
	}, nil
}

// serialPattern extracts the serial number SWPC writes into an alert's message
// body. It is not a JSON field of its own, and without it two alerts of the
// same product would collide onto one series.
var serialPattern = regexp.MustCompile(`(?i)serial\s+number:\s*(\d+)`)

// alertRecord is one entry of the alerts product.
type alertRecord struct {
	ProductID     string `json:"product_id"`
	IssueDatetime string `json:"issue_datetime"`
	Message       string `json:"message"`
}

// parseAlerts publishes SWPC alerts, warnings and watches issued recently.
func parseAlerts(t *tier, body []byte, _ time.Time) ([]metric.Sample, error) {
	var records []alertRecord
	if err := json.Unmarshal(body, &records); err != nil {
		return nil, fmt.Errorf("decoding alerts: %w", err)
	}

	type dated struct {
		rec    alertRecord
		issued time.Time
		serial string
	}

	cutoff := t.now().UTC().Add(-alertMaxAge)
	recent := make([]dated, 0, len(records))
	for _, r := range records {
		issued, err := parseTime(r.IssueDatetime)
		if err != nil {
			continue
		}
		if issued.Before(cutoff) {
			continue
		}
		productID := strings.TrimSpace(r.ProductID)
		match := serialPattern.FindStringSubmatch(r.Message)
		if productID == "" || match == nil {
			// Both are label values, and metric.Sample rejects an empty label.
			// An alert we cannot identify uniquely is better dropped than
			// merged with an unrelated one.
			continue
		}
		recent = append(recent, dated{rec: r, issued: issued, serial: match[1]})
	}

	sort.SliceStable(recent, func(i, j int) bool { return recent[i].issued.After(recent[j].issued) })

	if len(recent) > maxAlerts {
		t.log.Warn("swpc published more recent alerts than the series cap allows; older ones dropped",
			"alerts", len(recent), "cap", maxAlerts)
		recent = recent[:maxAlerts]
	}

	samples := make([]metric.Sample, 0, len(recent))
	for _, a := range recent {
		samples = append(samples, metric.Sample{
			Desc:   metric.AlertActive,
			Labels: []string{a.rec.ProductID, a.serial},
			Value:  1,
			Time:   a.issued,
		})
	}
	return samples, nil
}

// parseProtons publishes the newest GOES integral proton flux per energy
// channel.
func parseProtons(_ *tier, body []byte, _ time.Time) ([]metric.Sample, error) {
	return parseIntegralFlux(body, metric.ProtonFlux)
}

// parseElectrons publishes the newest GOES integral electron flux per energy
// channel.
func parseElectrons(_ *tier, body []byte, _ time.Time) ([]metric.Sample, error) {
	return parseIntegralFlux(body, metric.ElectronFlux)
}

// parseIntegralFlux reduces a day of GOES integral flux to the newest reading
// per energy channel.
//
// The document is a full day at five-minute cadence across several channels,
// which is thousands of points; only the newest per channel is current, and
// publishing any of the others would backdate samples the sinks have already
// seen.
func parseIntegralFlux(body []byte, desc *metric.Descriptor) ([]metric.Sample, error) {
	rows, err := decodeRows(body)
	if err != nil {
		return nil, err
	}

	type latest struct {
		when time.Time
		flux float64
	}
	newest := make(map[string]latest)

	for _, r := range rows {
		energy, _ := r["energy"].(string)
		energy = strings.TrimSpace(energy)
		if energy == "" {
			continue
		}
		when, ok := r.timeField("time_tag")
		if !ok {
			continue
		}
		flux, ok := r.numberField("flux")
		if !ok {
			continue
		}
		if prev, seen := newest[energy]; seen && !when.After(prev.when) {
			continue
		}
		newest[energy] = latest{when: when, flux: flux}
	}

	energies := make([]string, 0, len(newest))
	for e := range newest {
		energies = append(energies, e)
	}
	// Sorted so a batch's sample order is deterministic, which matters for
	// tests and for anything that diffs two polls.
	sort.Strings(energies)

	samples := make([]metric.Sample, 0, len(energies))
	for _, e := range energies {
		v := newest[e]
		samples = append(samples, metric.Sample{
			Desc:   desc,
			Labels: []string{e},
			Value:  v.flux,
			Time:   v.when,
		})
	}
	return samples, nil
}

// parseHemisphericPower publishes the OVATION hemispheric power estimate for
// both hemispheres.
//
// # On aurora_equatorward_boundary_degrees
//
// The descriptor table carries an equatorward boundary metric and SWPC's
// /text/ovation_latest_aurora_n.txt is the obvious place to look for it. That
// file is a grid of energy flux by magnetic local time and magnetic latitude
// with no boundary stated anywhere; deriving one means inventing a flux
// threshold, choosing which local times count, and calling the result "the"
// boundary. Different thresholds move the answer by several degrees of
// latitude, and nothing in the file says which is right.
//
// The metric is therefore not published. A fabricated boundary that looks
// authoritative is worse than an absent one: an operator would plan an
// observing night around it without ever learning that the number is this
// package's opinion rather than NOAA's. If SWPC begins publishing a boundary
// directly, this is where it goes.
func parseHemisphericPower(_ *tier, body []byte, _ time.Time) ([]metric.Sample, error) {
	lines := textLines(body)
	if len(lines) == 0 {
		return nil, fmt.Errorf("no data rows")
	}

	// Columns are: observation time, forecast time, northern power, southern
	// power. The last row is the most recent forecast.
	fields := strings.Fields(lines[len(lines)-1])
	if len(fields) < 4 {
		return nil, fmt.Errorf("expected 4 columns, got %d in %q", len(fields), lines[len(lines)-1])
	}

	when, err := parseTime(fields[0])
	if err != nil {
		return nil, fmt.Errorf("hemispheric power timestamp: %w", err)
	}

	var samples []metric.Sample
	for i, hemisphere := range []string{"north", "south"} {
		// SWPC writes "(n/a)" for a hemisphere it has no estimate for.
		v, err := strconv.ParseFloat(fields[2+i], 64)
		if err != nil || isSentinel(v) {
			continue
		}
		samples = append(samples, metric.Sample{
			Desc:   metric.AuroraHemisphericPower,
			Labels: []string{hemisphere},
			Value:  v,
			Time:   when,
		})
	}
	return samples, nil
}

// parseDRAP publishes the D-region absorption grid, subsampled.
//
// The document is a fixed-width table: a header row of longitudes, a rule, then
// one row per latitude of the form "  67 |  0.1  0.1 ...". At full resolution
// that is 90 latitudes by 90 longitudes, or 8100 series from one endpoint,
// which is why the grid step exists and why the whole endpoint is opt-in.
//
// Zero is published rather than skipped. Unlike a sentinel, a D-RAP value of
// 0.0 MHz is a real statement — no absorption here — and suppressing it would
// leave a storm's absorption values standing on the dashboard after the storm
// had passed.
func parseDRAP(t *tier, body []byte, _ time.Time) ([]metric.Sample, error) {
	when, err := drapValidTime(body)
	if err != nil {
		return nil, err
	}

	// A zero step would divide by zero. New clamps it, but a tier built by hand
	// should not be able to panic the parser.
	step := t.drapStep
	if step < 1 {
		step = 1
	}

	var (
		longitudes []float64
		samples    []metric.Sample
		latIndex   int
	)

	for _, line := range textLines(body) {
		if strings.TrimSpace(strings.Trim(line, "- \t")) == "" {
			// The rule between the header and the data.
			continue
		}

		if !strings.Contains(line, "|") {
			// The longitude header, which is the only data line without a
			// latitude and a pipe.
			if longitudes != nil {
				continue
			}
			for _, f := range strings.Fields(line) {
				v, err := strconv.ParseFloat(f, 64)
				if err != nil {
					longitudes = nil
					break
				}
				longitudes = append(longitudes, v)
			}
			continue
		}

		if len(longitudes) == 0 {
			return nil, fmt.Errorf("data row before the longitude header")
		}

		left, right, _ := strings.Cut(line, "|")
		latitude, err := strconv.ParseFloat(strings.TrimSpace(left), 64)
		if err != nil {
			continue
		}

		index := latIndex
		latIndex++
		if index%step != 0 {
			continue
		}

		latLabel := formatCoordinate(latitude)
		for i, f := range strings.Fields(right) {
			if i >= len(longitudes) || i%step != 0 {
				continue
			}
			v, err := strconv.ParseFloat(f, 64)
			if err != nil || isSentinel(v) {
				continue
			}
			samples = append(samples, metric.Sample{
				Desc:   metric.DRAPMaxFrequency,
				Labels: []string{latLabel, formatCoordinate(longitudes[i])},
				Value:  v,
				Time:   when,
			})
		}
	}

	if len(samples) == 0 {
		return nil, fmt.Errorf("no grid values found")
	}
	return samples, nil
}

// drapValidTime reads the grid's own validity time from its comment header.
func drapValidTime(body []byte) (time.Time, error) {
	raw, ok := headerValue(body, "Product Valid At")
	if !ok {
		return time.Time{}, fmt.Errorf("no \"Product Valid At\" header")
	}
	when, err := parseTime(raw)
	if err != nil {
		return time.Time{}, fmt.Errorf("drap valid time: %w", err)
	}
	return when, nil
}

// parseF107 publishes the 10.7 cm radio flux and its ninety-day mean.
//
// The two come from different rows on purpose. Only the noon observation
// carries a ninety-day mean; the morning and afternoon observations leave it
// null. Reading both from the newest row would therefore publish the mean only
// eight hours in every twenty-four, so the mean is taken from the newest row
// that actually has one.
func parseF107(_ *tier, body []byte, _ time.Time) ([]metric.Sample, error) {
	rows, err := decodeRows(body)
	if err != nil {
		return nil, err
	}

	var samples []metric.Sample

	if rec, when, ok := latestRow(rows, "time_tag"); ok {
		if flux, ok := rec.numberField("flux"); ok {
			samples = append(samples, metric.Sample{Desc: metric.FluxSFU, Value: flux, Time: when})
		}
	}

	withMean := make([]row, 0, len(rows))
	for _, r := range rows {
		if _, ok := r.numberField("ninety_day_mean"); ok {
			withMean = append(withMean, r)
		}
	}
	if rec, when, ok := latestRow(withMean, "time_tag"); ok {
		mean, _ := rec.numberField("ninety_day_mean")
		samples = append(samples, metric.Sample{Desc: metric.FluxNinetyDayMeanSFU, Value: mean, Time: when})
	}

	if len(samples) == 0 {
		return nil, fmt.Errorf("no usable f107 rows")
	}
	return samples, nil
}

// stationPlanetary labels the indices that are a global average rather than one
// observatory's reading.
const stationPlanetary = "planetary"

// parsePlanetaryK publishes the three-hourly planetary K and the running A.
func parsePlanetaryK(_ *tier, body []byte, _ time.Time) ([]metric.Sample, error) {
	rows, err := decodeRows(body)
	if err != nil {
		return nil, err
	}
	rec, when, ok := latestRow(rows, "time_tag")
	if !ok {
		return nil, fmt.Errorf("no usable time_tag in %d rows", len(rows))
	}

	var samples []metric.Sample
	if kp, ok := rec.numberField("Kp"); ok {
		samples = append(samples, metric.Sample{
			Desc:   metric.KIndex,
			Labels: []string{stationPlanetary},
			Value:  kp,
			Time:   when,
		})
	}
	if a, ok := rec.numberField("a_running"); ok {
		samples = append(samples, metric.Sample{
			Desc:   metric.AIndex,
			Labels: []string{stationPlanetary},
			Value:  a,
			Time:   when,
		})
	}
	return samples, nil
}

// Column positions in the daily solar data table. The file is fixed-width with
// a commented header, and the columns of interest are the first six fields:
// year, month, day, 10.7 cm flux, SESC sunspot number, sunspot area.
const (
	dsdMinFields   = 6
	dsdSunspotCol  = 4
	dsdDateColSpan = 3
)

// parseDailyIndices publishes the SESC sunspot number from the daily table.
//
// The same row carries a 10.7 cm flux, which is deliberately not published from
// here: /json/f107_cm_flux.json is the same quantity at three times the cadence
// and both land on solar_flux_sfu, so publishing both would have two rows of
// one batch fighting over one series. The daily table is used for the sunspot
// number, which nothing else in this tier provides.
func parseDailyIndices(_ *tier, body []byte, _ time.Time) ([]metric.Sample, error) {
	lines := textLines(body)
	if len(lines) == 0 {
		return nil, fmt.Errorf("no data rows")
	}

	fields := strings.Fields(lines[len(lines)-1])
	if len(fields) < dsdMinFields {
		return nil, fmt.Errorf("expected at least %d columns, got %d in %q",
			dsdMinFields, len(fields), lines[len(lines)-1])
	}

	when, err := parseTime(strings.Join(fields[:dsdDateColSpan], "-"))
	if err != nil {
		return nil, fmt.Errorf("daily indices date: %w", err)
	}

	ssn, err := strconv.ParseFloat(fields[dsdSunspotCol], 64)
	if err != nil || isSentinel(ssn) {
		return nil, nil
	}
	return []metric.Sample{{Desc: metric.SunspotNumber, Value: ssn, Time: when}}, nil
}

// parseKyotoDst publishes the Kyoto Dst ring-current index.
func parseKyotoDst(_ *tier, body []byte, _ time.Time) ([]metric.Sample, error) {
	rows, err := decodeRows(body)
	if err != nil {
		return nil, err
	}
	rec, when, ok := latestRow(rows, "time_tag")
	if !ok {
		return nil, fmt.Errorf("no usable time_tag in %d rows", len(rows))
	}
	dst, ok := rec.numberField("dst")
	if !ok {
		return nil, nil
	}
	return []metric.Sample{{Desc: metric.DstNanotesla, Value: dst, Time: when}}, nil
}
