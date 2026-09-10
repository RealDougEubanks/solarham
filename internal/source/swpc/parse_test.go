package swpc

import (
	"encoding/json"
	"io"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/RealDougEubanks/solarham/internal/metric"
)

// fixedNow is the instant the testdata fixtures were captured, rounded to a
// convenient minute. Tests that care about an age window use it so the fixtures
// do not silently expire: an alert window measured against the wall clock would
// pass on the day the fixtures were captured and fail every day afterwards.
var fixedNow = time.Date(2026, 9, 9, 4, 0, 0, 0, time.UTC)

// discardLogger keeps test output clean without leaving a nil logger that the
// code under test would substitute for slog.Default.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// testTier builds a tier suitable for calling a parser directly. Parsers use
// only the logger, the D-RAP step and the clock.
func testTier(drapStep int) *tier {
	return &tier{
		name:     "swpc-test",
		log:      discardLogger(),
		drapStep: drapStep,
		now:      func() time.Time { return fixedNow },
	}
}

// fixture reads a captured SWPC response from testdata.
func fixture(t *testing.T, name string) []byte {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("reading fixture %s: %v", name, err)
	}
	return body
}

// sampleFor finds the single sample of a descriptor with the given label
// values, failing the test when it is absent or duplicated.
func sampleFor(t *testing.T, samples []metric.Sample, desc *metric.Descriptor, labels ...string) metric.Sample {
	t.Helper()
	var found []metric.Sample
outer:
	for _, s := range samples {
		if s.Desc != desc || len(s.Labels) != len(labels) {
			continue
		}
		for i, l := range labels {
			if s.Labels[i] != l {
				continue outer
			}
		}
		found = append(found, s)
	}
	if len(found) != 1 {
		t.Fatalf("wanted exactly one %s%v sample, got %d in %d samples",
			desc.FullName(), labels, len(found), len(samples))
	}
	return found[0]
}

// closeEnough compares floats without demanding bit equality of values that
// travelled through JSON.
func closeEnough(got, want float64) bool {
	return math.Abs(got-want) <= math.Abs(want)*1e-9+1e-12
}

func TestParseTimeAcceptsBothSWPCTimestampFormats(t *testing.T) {
	want := time.Date(2026, 9, 8, 20, 0, 0, 0, time.UTC)

	cases := map[string]string{
		"RFC 3339 with a zone":           "2026-09-08T20:00:00Z",
		"zoneless, which SWPC means UTC": "2026-09-08T20:00:00",
		"space separated":                "2026-09-08 20:00:00",
		"underscore separated":           "2026-09-08_20:00",
		"with a trailing UTC word":       "2026-09-08 20:00 UTC",
	}

	for name, input := range cases {
		t.Run(name, func(t *testing.T) {
			got, err := parseTime(input)
			if err != nil {
				t.Fatalf("parseTime(%q): %v", input, err)
			}
			if !got.Equal(want) {
				t.Errorf("parseTime(%q) = %s, want %s", input, got, want)
			}
			if got.Location() != time.UTC {
				t.Errorf("parseTime(%q) returned location %s, want UTC", input, got.Location())
			}
		})
	}
}

func TestParseTimeAcceptsFractionalSecondsAsUsedByAlerts(t *testing.T) {
	got, err := parseTime("2026-09-08 17:51:10.343")
	if err != nil {
		t.Fatalf("parseTime: %v", err)
	}
	want := time.Date(2026, 9, 8, 17, 51, 10, 343000000, time.UTC)
	if !got.Equal(want) {
		t.Errorf("got %s, want %s", got, want)
	}
}

func TestParseTimeRejectsSomethingThatIsNotATimestamp(t *testing.T) {
	for _, input := range []string{"", "   ", "not a time", "2026/09/08"} {
		if _, err := parseTime(input); err == nil {
			t.Errorf("parseTime(%q) succeeded, want an error", input)
		}
	}
}

func TestIsSentinelIdentifiesNOAANoDataMarkers(t *testing.T) {
	for _, v := range []float64{-9999, -999.9, -999, math.NaN(), math.Inf(1)} {
		if !isSentinel(v) {
			t.Errorf("isSentinel(%v) = false, want true", v)
		}
	}
	for _, v := range []float64{0, -1, 401, -22, -100.5} {
		if isSentinel(v) {
			t.Errorf("isSentinel(%v) = true, want false", v)
		}
	}
}

func TestDecodeFlareClassConvertsNotationToWattsPerSquareMeter(t *testing.T) {
	cases := map[string]float64{
		"A1.0": 1e-8,
		"B3.8": 3.8e-7,
		"C3.4": 3.4e-6,
		"M1.2": 1.2e-5,
		"X2.0": 2e-4,
		"X":    1e-4,
		"b3.5": 3.5e-7,
	}
	for input, want := range cases {
		got, err := decodeFlareClass(input)
		if err != nil {
			t.Fatalf("decodeFlareClass(%q): %v", input, err)
		}
		if !closeEnough(got, want) {
			t.Errorf("decodeFlareClass(%q) = %g, want %g", input, got, want)
		}
	}
}

func TestDecodeFlareClassRejectsNotationItDoesNotUnderstand(t *testing.T) {
	for _, input := range []string{"", "Q1.0", "M-", "MX"} {
		if _, err := decodeFlareClass(input); err == nil {
			t.Errorf("decodeFlareClass(%q) succeeded, want an error", input)
		}
	}
}

func TestDecodeRowsHandlesAnArrayOfObjects(t *testing.T) {
	rows, err := decodeRows([]byte(`[{"time_tag":"2026-09-09T00:00:00","Kp":2.67}]`))
	if err != nil {
		t.Fatalf("decodeRows: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(rows))
	}
	if v, ok := rows[0].numberField("Kp"); !ok || v != 2.67 {
		t.Errorf("Kp = %v (ok=%v), want 2.67", v, ok)
	}
}

func TestDecodeRowsHandlesAnArrayOfArraysWithAHeaderRow(t *testing.T) {
	body := []byte(`[["time_tag","Kp","a_running","station_count"],
	                 ["2026-09-08T21:00:00","1.67","6","8"],
	                 ["2026-09-09T00:00:00","2.67","12","8"]]`)

	rows, err := decodeRows(body)
	if err != nil {
		t.Fatalf("decodeRows: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2 (the header row is not data)", len(rows))
	}
	rec, when, ok := latestRow(rows, "time_tag")
	if !ok {
		t.Fatal("latestRow found no usable timestamp")
	}
	if want := time.Date(2026, 9, 9, 0, 0, 0, 0, time.UTC); !when.Equal(want) {
		t.Errorf("latest time = %s, want %s", when, want)
	}
	// The values arrive as strings in this shape and must still read as numbers.
	if v, ok := rec.numberField("a_running"); !ok || v != 12 {
		t.Errorf("a_running = %v (ok=%v), want 12", v, ok)
	}
}

func TestDecodeRowsReturnsAnErrorForMalformedJSON(t *testing.T) {
	for _, body := range []string{`{"not":"an array"}`, `[`, `[{"a":]`, ``} {
		if _, err := decodeRows([]byte(body)); err == nil {
			t.Errorf("decodeRows(%q) succeeded, want an error", body)
		}
	}
}

func TestNumberAcceptsNumbersStringsAndNull(t *testing.T) {
	var doc struct {
		Numeric number `json:"numeric"`
		Text    number `json:"text"`
		Null    number `json:"null"`
		Empty   number `json:"empty"`
		Star    number `json:"star"`
	}
	body := []byte(`{"numeric":2.67,"text":"3","null":null,"empty":"","star":"*"}`)
	if err := json.Unmarshal(body, &doc); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if !doc.Numeric.ok() || doc.Numeric.Value != 2.67 {
		t.Errorf("numeric = %+v, want 2.67", doc.Numeric)
	}
	if !doc.Text.ok() || doc.Text.Value != 3 {
		t.Errorf("text = %+v, want 3", doc.Text)
	}
	for name, n := range map[string]number{"null": doc.Null, "empty": doc.Empty, "star": doc.Star} {
		if n.ok() {
			t.Errorf("%s = %+v, want absent", name, n)
		}
	}
}

func TestParseWindSpeedReadsTheSummaryProduct(t *testing.T) {
	samples, err := parseWindSpeed(testTier(1), fixture(t, "solar-wind-speed.json"), fixedNow)
	if err != nil {
		t.Fatalf("parseWindSpeed: %v", err)
	}
	s := sampleFor(t, samples, metric.WindSpeed)
	if s.Value != 401 {
		t.Errorf("speed = %v, want 401", s.Value)
	}
	want := time.Date(2026, 9, 9, 3, 46, 0, 0, time.UTC)
	if !s.Time.Equal(want) {
		t.Errorf("time = %s, want the upstream time_tag %s", s.Time, want)
	}
}

func TestParseWindSpeedAcceptsABareObjectAsWellAsAnArray(t *testing.T) {
	body := []byte(`{"proton_speed": 512, "time_tag": "2026-09-09T03:46:00Z"}`)
	samples, err := parseWindSpeed(testTier(1), body, fixedNow)
	if err != nil {
		t.Fatalf("parseWindSpeed: %v", err)
	}
	if got := sampleFor(t, samples, metric.WindSpeed).Value; got != 512 {
		t.Errorf("speed = %v, want 512", got)
	}
}

func TestParseWindSpeedProducesNoSampleForASentinelValue(t *testing.T) {
	body := []byte(`[{"proton_speed": -999.9, "time_tag": "2026-09-09T03:46:00Z"}]`)
	samples, err := parseWindSpeed(testTier(1), body, fixedNow)
	if err != nil {
		t.Fatalf("parseWindSpeed: %v", err)
	}
	if len(samples) != 0 {
		t.Errorf("got %d samples for a no-data sentinel, want none: %+v", len(samples), samples)
	}
}

func TestParseWindSpeedProducesNoSampleForANullValue(t *testing.T) {
	body := []byte(`[{"proton_speed": null, "time_tag": "2026-09-09T03:46:00Z"}]`)
	samples, err := parseWindSpeed(testTier(1), body, fixedNow)
	if err != nil {
		t.Fatalf("parseWindSpeed: %v", err)
	}
	if len(samples) != 0 {
		t.Errorf("got %d samples for a null value, want none", len(samples))
	}
}

func TestParseWindMagFieldPublishesBtAndBzSeparately(t *testing.T) {
	samples, err := parseWindMagField(testTier(1), fixture(t, "solar-wind-mag-field.json"), fixedNow)
	if err != nil {
		t.Fatalf("parseWindMagField: %v", err)
	}
	if len(samples) != 2 {
		t.Fatalf("got %d samples, want 2", len(samples))
	}
	if got := sampleFor(t, samples, metric.WindMagneticField, "bt").Value; got != 6 {
		t.Errorf("bt = %v, want 6", got)
	}
	if got := sampleFor(t, samples, metric.WindMagneticField, "bz").Value; got != 1 {
		t.Errorf("bz = %v, want 1 (from bz_gsm)", got)
	}
}

func TestParseKpOneMinuteTakesTheMostRecentMinute(t *testing.T) {
	samples, err := parseKpOneMinute(testTier(1), fixture(t, "planetary-k-index-1m.json"), fixedNow)
	if err != nil {
		t.Fatalf("parseKpOneMinute: %v", err)
	}
	s := sampleFor(t, samples, metric.KIndexEstimated)
	want := time.Date(2026, 9, 9, 3, 48, 0, 0, time.UTC)
	if !s.Time.Equal(want) {
		t.Errorf("time = %s, want the last row's %s", s.Time, want)
	}
}

func TestParseXRayFlaresPublishesTheClassAndTheFluxItDenotes(t *testing.T) {
	samples, err := parseXRayFlares(testTier(1), fixture(t, "xray-flares-latest.json"), fixedNow)
	if err != nil {
		t.Fatalf("parseXRayFlares: %v", err)
	}
	if got := sampleFor(t, samples, metric.XRayClassInfo, "B3.5").Value; got != 1 {
		t.Errorf("info sample value = %v, want 1", got)
	}
	flux := sampleFor(t, samples, metric.XRayFlux, xrayBand)
	if !closeEnough(flux.Value, 3.5e-7) {
		t.Errorf("flux = %g, want 3.5e-07 decoded from B3.5", flux.Value)
	}
}

func TestParseXRayFlaresStillPublishesAClassItCannotDecode(t *testing.T) {
	body := []byte(`[{"time_tag":"2026-09-09T03:48:00Z","current_class":"Q9.9"}]`)
	samples, err := parseXRayFlares(testTier(1), body, fixedNow)
	if err != nil {
		t.Fatalf("parseXRayFlares: %v", err)
	}
	sampleFor(t, samples, metric.XRayClassInfo, "Q9.9")
	for _, s := range samples {
		if s.Desc == metric.XRayFlux {
			t.Error("published a flux derived from an unknown flare class letter")
		}
	}
}

func TestParseNOAAScalesReadsTheCurrentEntrysStringScales(t *testing.T) {
	samples, err := parseNOAAScales(testTier(1), fixture(t, "noaa-scales.json"), fixedNow)
	if err != nil {
		t.Fatalf("parseNOAAScales: %v", err)
	}
	if len(samples) != 3 {
		t.Fatalf("got %d samples, want 3 (G, R, S)", len(samples))
	}
	// Key "0" is the current entry; key "-1" is yesterday and carries G1, so a
	// parser reading the wrong key would report a minor storm here.
	for _, desc := range []*metric.Descriptor{
		metric.GeomagneticStormScale, metric.RadioBlackoutScale, metric.RadiationStormScale,
	} {
		if got := sampleFor(t, samples, desc).Value; got != 0 {
			t.Errorf("%s = %v, want 0 from the current entry", desc.FullName(), got)
		}
	}
	want := time.Date(2026, 9, 9, 3, 51, 0, 0, time.UTC)
	if got := sampleFor(t, samples, metric.GeomagneticStormScale).Time; !got.Equal(want) {
		t.Errorf("time = %s, want the entry's own DateStamp and TimeStamp %s", got, want)
	}
}

func TestParseNOAAScalesTreatsANullScaleAsZeroRatherThanAbsent(t *testing.T) {
	body := []byte(`{"0":{"DateStamp":"2026-09-09","TimeStamp":"03:50:00",
		"R":{"Scale":null},"S":{"Scale":null},"G":{"Scale":"3"}}}`)

	samples, err := parseNOAAScales(testTier(1), body, fixedNow)
	if err != nil {
		t.Fatalf("parseNOAAScales: %v", err)
	}
	if got := sampleFor(t, samples, metric.RadioBlackoutScale).Value; got != 0 {
		t.Errorf("R scale = %v, want 0 for null", got)
	}
	if got := sampleFor(t, samples, metric.RadiationStormScale).Value; got != 0 {
		t.Errorf("S scale = %v, want 0 for null", got)
	}
	if got := sampleFor(t, samples, metric.GeomagneticStormScale).Value; got != 3 {
		t.Errorf("G scale = %v, want 3 parsed from the string \"3\"", got)
	}
}

func TestParseNOAAScalesFailsWhenTheCurrentEntryIsMissing(t *testing.T) {
	body := []byte(`{"1":{"DateStamp":"2026-09-10","TimeStamp":"00:00:00","G":{"Scale":"0"}}}`)
	if _, err := parseNOAAScales(testTier(1), body, fixedNow); err == nil {
		t.Error("parseNOAAScales succeeded without a current entry, want an error")
	}
}

func TestParseAlertsPublishesOnlyAlertsFromTheLastDay(t *testing.T) {
	samples, err := parseAlerts(testTier(1), fixture(t, "alerts.json"), fixedNow)
	if err != nil {
		t.Fatalf("parseAlerts: %v", err)
	}
	if len(samples) == 0 {
		t.Fatal("got no alerts from a fixture full of recent ones")
	}
	cutoff := fixedNow.Add(-alertMaxAge)
	for _, s := range samples {
		if s.Time.Before(cutoff) {
			t.Errorf("published an alert issued %s, older than the %s window", s.Time, alertMaxAge)
		}
		if _, ok := s.LabelFor("serial"); !ok {
			t.Errorf("alert sample has no serial label: %+v", s)
		}
	}
	// The fixture's newest alert is K05A serial 2049.
	sampleFor(t, samples, metric.AlertActive, "K05A", "2049")
}

func TestParseAlertsDropsAlertsOlderThanTheWindow(t *testing.T) {
	stale := testTier(1)
	stale.now = func() time.Time { return fixedNow.Add(72 * time.Hour) }

	samples, err := parseAlerts(stale, fixture(t, "alerts.json"), fixedNow)
	if err != nil {
		t.Fatalf("parseAlerts: %v", err)
	}
	if len(samples) != 0 {
		t.Errorf("got %d samples from alerts three days stale, want none", len(samples))
	}
}

func TestParseAlertsTruncatesAtTheSeriesCap(t *testing.T) {
	// A storm's worth of alerts, each with its own serial, is exactly the
	// cardinality explosion the cap exists to stop.
	var b []byte
	b = append(b, '[')
	for i := 0; i < maxAlerts+25; i++ {
		if i > 0 {
			b = append(b, ',')
		}
		issued := fixedNow.Add(-time.Duration(i) * time.Minute).Format("2006-01-02 15:04:05.000")
		b = append(b, []byte(`{"product_id":"K05A","issue_datetime":"`+issued+
			`","message":"Serial Number: `+strconv.Itoa(9000+i)+`"}`)...)
	}
	b = append(b, ']')

	samples, err := parseAlerts(testTier(1), b, fixedNow)
	if err != nil {
		t.Fatalf("parseAlerts: %v", err)
	}
	if len(samples) != maxAlerts {
		t.Fatalf("got %d samples, want the cap of %d", len(samples), maxAlerts)
	}
	// Truncation keeps the newest, which is the one issued at fixedNow.
	sampleFor(t, samples, metric.AlertActive, "K05A", "9000")
	for _, s := range samples {
		if s.Time.Before(fixedNow.Add(-time.Duration(maxAlerts) * time.Minute)) {
			t.Errorf("kept an older alert %s in preference to a newer one", s.Time)
		}
	}
}

func TestParseAlertsSkipsAnAlertWithNoSerialNumber(t *testing.T) {
	body := []byte(`[{"product_id":"K05A","issue_datetime":"2026-09-09 03:00:00.000",
		"message":"no serial here"}]`)
	samples, err := parseAlerts(testTier(1), body, fixedNow)
	if err != nil {
		t.Fatalf("parseAlerts: %v", err)
	}
	if len(samples) != 0 {
		t.Errorf("got %d samples for an alert with no serial, want none", len(samples))
	}
}

func TestParseProtonsTakesTheNewestReadingOfEachEnergyChannel(t *testing.T) {
	samples, err := parseProtons(testTier(1), fixture(t, "integral-protons.json"), fixedNow)
	if err != nil {
		t.Fatalf("parseProtons: %v", err)
	}
	if len(samples) != 8 {
		t.Fatalf("got %d samples, want one per energy channel (8)", len(samples))
	}
	s := sampleFor(t, samples, metric.ProtonFlux, ">=10 MeV")
	if !closeEnough(s.Value, 0.19704563915729523) {
		t.Errorf(">=10 MeV flux = %v, want the newest row's value", s.Value)
	}
	want := time.Date(2026, 9, 9, 3, 40, 0, 0, time.UTC)
	if !s.Time.Equal(want) {
		t.Errorf("time = %s, want the newest row's %s", s.Time, want)
	}
}

func TestParseElectronsTakesTheNewestReadingOfEachEnergyChannel(t *testing.T) {
	samples, err := parseElectrons(testTier(1), fixture(t, "integral-electrons.json"), fixedNow)
	if err != nil {
		t.Fatalf("parseElectrons: %v", err)
	}
	s := sampleFor(t, samples, metric.ElectronFlux, ">=2 MeV")
	if !closeEnough(s.Value, 1016.5028686523438) {
		t.Errorf(">=2 MeV flux = %v, want the newest row's value", s.Value)
	}
}

func TestParseIntegralFluxSkipsRowsWithNoFlux(t *testing.T) {
	body := []byte(`[{"time_tag":"2026-09-09T03:40:00Z","flux":null,"energy":">=2 MeV"},
		{"time_tag":"2026-09-09T03:35:00Z","flux":-99999,"energy":">=10 MeV"}]`)
	samples, err := parseElectrons(testTier(1), body, fixedNow)
	if err != nil {
		t.Fatalf("parseElectrons: %v", err)
	}
	if len(samples) != 0 {
		t.Errorf("got %d samples from null and sentinel fluxes, want none", len(samples))
	}
}

func TestParseHemisphericPowerPublishesBothHemispheresFromTheLastRow(t *testing.T) {
	samples, err := parseHemisphericPower(testTier(1), fixture(t, "aurora-hemi-power.txt"), fixedNow)
	if err != nil {
		t.Fatalf("parseHemisphericPower: %v", err)
	}
	if len(samples) != 2 {
		t.Fatalf("got %d samples, want 2", len(samples))
	}
	if got := sampleFor(t, samples, metric.AuroraHemisphericPower, "north").Value; got != 12 {
		t.Errorf("north = %v, want 12", got)
	}
	if got := sampleFor(t, samples, metric.AuroraHemisphericPower, "south").Value; got != 12 {
		t.Errorf("south = %v, want 12", got)
	}
	want := time.Date(2026, 9, 9, 3, 50, 0, 0, time.UTC)
	if got := sampleFor(t, samples, metric.AuroraHemisphericPower, "north").Time; !got.Equal(want) {
		t.Errorf("time = %s, want the row's observation time %s", got, want)
	}
}

func TestParseHemisphericPowerSkipsAHemisphereMarkedUnavailable(t *testing.T) {
	body := []byte("# header\n2026-09-09_03:50    2026-09-09_04:52      12      (n/a)\n")
	samples, err := parseHemisphericPower(testTier(1), body, fixedNow)
	if err != nil {
		t.Fatalf("parseHemisphericPower: %v", err)
	}
	if len(samples) != 1 {
		t.Fatalf("got %d samples, want only the northern one", len(samples))
	}
	sampleFor(t, samples, metric.AuroraHemisphericPower, "north")
}

func TestParseF107TakesFluxAndMeanFromTheNewestRowThatHasEach(t *testing.T) {
	samples, err := parseF107(testTier(1), fixture(t, "f107.json"), fixedNow)
	if err != nil {
		t.Fatalf("parseF107: %v", err)
	}

	flux := sampleFor(t, samples, metric.FluxSFU)
	if flux.Value != 112 {
		t.Errorf("flux = %v, want the newest row's 112", flux.Value)
	}
	if want := time.Date(2026, 9, 8, 22, 0, 0, 0, time.UTC); !flux.Time.Equal(want) {
		t.Errorf("flux time = %s, want %s", flux.Time, want)
	}

	// Only the noon observation carries a mean, so it comes from an earlier row
	// than the flux does.
	mean := sampleFor(t, samples, metric.FluxNinetyDayMeanSFU)
	if mean.Value != 127 {
		t.Errorf("ninety day mean = %v, want 127", mean.Value)
	}
	if want := time.Date(2026, 9, 8, 20, 0, 0, 0, time.UTC); !mean.Time.Equal(want) {
		t.Errorf("mean time = %s, want the noon row's %s", mean.Time, want)
	}
}

func TestParsePlanetaryKPublishesKAndAForThePlanetaryStation(t *testing.T) {
	samples, err := parsePlanetaryK(testTier(1), fixture(t, "noaa-planetary-k-index.json"), fixedNow)
	if err != nil {
		t.Fatalf("parsePlanetaryK: %v", err)
	}
	if got := sampleFor(t, samples, metric.KIndex, stationPlanetary).Value; got != 2.67 {
		t.Errorf("Kp = %v, want 2.67", got)
	}
	if got := sampleFor(t, samples, metric.AIndex, stationPlanetary).Value; got != 12 {
		t.Errorf("a_running = %v, want 12", got)
	}
}

func TestParsePlanetaryKAlsoHandlesTheHeaderRowArrayFormat(t *testing.T) {
	body := []byte(`[["time_tag","Kp","a_running","station_count"],
		["2026-09-08T21:00:00","1.67","6","8"],
		["2026-09-09T00:00:00","2.67","12","8"]]`)

	samples, err := parsePlanetaryK(testTier(1), body, fixedNow)
	if err != nil {
		t.Fatalf("parsePlanetaryK: %v", err)
	}
	if got := sampleFor(t, samples, metric.KIndex, stationPlanetary).Value; got != 2.67 {
		t.Errorf("Kp = %v, want 2.67 from the last data row", got)
	}
}

func TestParseKyotoDstPublishesTheLatestValue(t *testing.T) {
	samples, err := parseKyotoDst(testTier(1), fixture(t, "kyoto-dst.json"), fixedNow)
	if err != nil {
		t.Fatalf("parseKyotoDst: %v", err)
	}
	s := sampleFor(t, samples, metric.DstNanotesla)
	if s.Value != -22 {
		t.Errorf("dst = %v, want -22", s.Value)
	}
	if want := time.Date(2026, 9, 9, 3, 0, 0, 0, time.UTC); !s.Time.Equal(want) {
		t.Errorf("time = %s, want %s", s.Time, want)
	}
}

func TestParseKyotoDstAlsoHandlesTheHeaderRowArrayFormat(t *testing.T) {
	body := []byte(`[["time_tag","dst"],["2026-09-09T02:00:00","-18"],["2026-09-09T03:00:00","-22"]]`)
	samples, err := parseKyotoDst(testTier(1), body, fixedNow)
	if err != nil {
		t.Fatalf("parseKyotoDst: %v", err)
	}
	if got := sampleFor(t, samples, metric.DstNanotesla).Value; got != -22 {
		t.Errorf("dst = %v, want -22", got)
	}
}

func TestParseDailyIndicesPublishesTheSunspotNumberFromTheLastRow(t *testing.T) {
	samples, err := parseDailyIndices(testTier(1), fixture(t, "daily-solar-indices.txt"), fixedNow)
	if err != nil {
		t.Fatalf("parseDailyIndices: %v", err)
	}
	s := sampleFor(t, samples, metric.SunspotNumber)
	if s.Value != 102 {
		t.Errorf("sunspot number = %v, want the last row's 102", s.Value)
	}
	if want := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC); !s.Time.Equal(want) {
		t.Errorf("time = %s, want the row's own date %s", s.Time, want)
	}
	// The same row carries a 10.7 cm flux, which the F10.7 endpoint owns.
	for _, sample := range samples {
		if sample.Desc == metric.FluxSFU {
			t.Error("published flux_sfu from the daily table, which would fight the f107 endpoint")
		}
	}
}

func TestParseDailyIndicesProducesNoSampleForASentinelSunspotNumber(t *testing.T) {
	body := []byte("#comment\n2026 09 08  110    -999      575      0    -999      *\n")
	samples, err := parseDailyIndices(testTier(1), body, fixedNow)
	if err != nil {
		t.Fatalf("parseDailyIndices: %v", err)
	}
	if len(samples) != 0 {
		t.Errorf("got %d samples for a sentinel sunspot number, want none", len(samples))
	}
}

func TestParseDRAPReadsTheWholeGridAtStepOne(t *testing.T) {
	samples, err := parseDRAP(testTier(1), fixture(t, "drap-global-frequencies.txt"), fixedNow)
	if err != nil {
		t.Fatalf("parseDRAP: %v", err)
	}
	// The fixture is 90 latitudes by 12 longitudes.
	if len(samples) != 90*12 {
		t.Fatalf("got %d samples, want %d", len(samples), 90*12)
	}
	s := sampleFor(t, samples, metric.DRAPMaxFrequency, "89", "-178")
	if s.Value != 0.1 {
		t.Errorf("value at 89/-178 = %v, want 0.1", s.Value)
	}
	want := time.Date(2026, 9, 9, 3, 49, 0, 0, time.UTC)
	if !s.Time.Equal(want) {
		t.Errorf("time = %s, want the grid's own valid time %s", s.Time, want)
	}
}

func TestParseDRAPSubsamplesBothAxesByTheGridStep(t *testing.T) {
	samples, err := parseDRAP(testTier(2), fixture(t, "drap-global-frequencies.txt"), fixedNow)
	if err != nil {
		t.Fatalf("parseDRAP: %v", err)
	}
	if len(samples) != 45*6 {
		t.Fatalf("got %d samples at step 2, want %d", len(samples), 45*6)
	}
	// Step 2 keeps every other row and column starting from the first.
	sampleFor(t, samples, metric.DRAPMaxFrequency, "89", "-178")
	for _, s := range samples {
		if lat, _ := s.LabelFor("latitude"); lat == "87" {
			t.Error("step 2 kept latitude 87, which is the row it should have skipped")
		}
		if lon, _ := s.LabelFor("longitude"); lon == "-174" {
			t.Error("step 2 kept longitude -174, which is the column it should have skipped")
		}
	}
}

func TestParseDRAPFailsWhenTheGridIsUnrecognisable(t *testing.T) {
	if _, err := parseDRAP(testTier(1), []byte("# nothing but a comment\n"), fixedNow); err == nil {
		t.Error("parseDRAP succeeded on a document with no grid, want an error")
	}
}

func TestFormatCoordinateIsStableAcrossPolls(t *testing.T) {
	cases := map[float64]string{67.5: "67.5", 68: "68", -89: "-89", 0: "0", -0.5: "-0.5"}
	for input, want := range cases {
		if got := formatCoordinate(input); got != want {
			t.Errorf("formatCoordinate(%v) = %q, want %q", input, got, want)
		}
	}
}

func TestParseWindPlasmaReadsTheNewestGoodRow(t *testing.T) {
	samples, err := parseWindPlasma(testTier(1), fixture(t, "ace-swepam.txt"), fixedNow)
	if err != nil {
		t.Fatalf("parseWindPlasma: %v", err)
	}
	s := sampleFor(t, samples, metric.WindDensity)
	if s.Value != 7.0 {
		t.Errorf("density = %v, want 7.0 from the last row", s.Value)
	}
	want := time.Date(2026, 9, 9, 13, 20, 0, 0, time.UTC)
	if !s.Time.Equal(want) {
		t.Errorf("time = %v, want %v built from the row's own date columns", s.Time, want)
	}
}

// Only density is taken from this file. parseWindSpeed already publishes speed
// from the summary product, and the two are different spacecraft sampled at
// different instants, so letting both write one series would make it flap.
func TestParseWindPlasmaPublishesDensityOnly(t *testing.T) {
	samples, err := parseWindPlasma(testTier(1), fixture(t, "ace-swepam.txt"), fixedNow)
	if err != nil {
		t.Fatalf("parseWindPlasma: %v", err)
	}
	if len(samples) != 1 {
		t.Fatalf("got %d samples, want exactly 1", len(samples))
	}
	if samples[0].Desc != metric.WindDensity {
		t.Errorf("published %s, want only solar_wind_density", samples[0].Desc.FullName())
	}
}

// A non-zero status flag means the row is unusable. Reading its density anyway
// is exactly how a -9999.9 reaches a dashboard as a real number.
func TestParseWindPlasmaSkipsFlaggedAndSentinelRows(t *testing.T) {
	samples, err := parseWindPlasma(testTier(1), fixture(t, "ace-swepam-degraded.txt"), fixedNow)
	if err != nil {
		t.Fatalf("parseWindPlasma: %v", err)
	}
	s := sampleFor(t, samples, metric.WindDensity)
	if s.Value != 5.9 {
		t.Errorf("density = %v, want 5.9: the newer rows are flagged or sentinel", s.Value)
	}
}

func TestParseWindPlasmaProducesNoSampleWhenEveryRowIsUnusable(t *testing.T) {
	// The upstream is reachable and simply has nothing usable. That is a
	// successful poll with no samples, not a failure.
	samples, err := parseWindPlasma(testTier(1), fixture(t, "ace-swepam-nodata.txt"), fixedNow)
	if err != nil {
		t.Fatalf("parseWindPlasma returned an error for a no-data file: %v", err)
	}
	if len(samples) != 0 {
		t.Errorf("got %d samples, want none: %+v", len(samples), samples)
	}
}

func TestParseWindPlasmaToleratesAHeaderOnlyFile(t *testing.T) {
	body := []byte("# YR MO DA  HHMM  Day  Day  S  Density  Speed  Temperature\n")
	samples, err := parseWindPlasma(testTier(1), body, fixedNow)
	if err != nil {
		t.Fatalf("parseWindPlasma: %v", err)
	}
	if len(samples) != 0 {
		t.Errorf("got %d samples from a header-only file, want none", len(samples))
	}
}

func TestParseWindPlasmaRejectsAMalformedTimeField(t *testing.T) {
	// A row whose HHMM is not four digits is skipped rather than being
	// timestamped by guesswork.
	body := []byte("2026 09 09  131   61292   48000    0        7.0      447.4     9.38e+04\n")
	samples, err := parseWindPlasma(testTier(1), body, fixedNow)
	if err != nil {
		t.Fatalf("parseWindPlasma: %v", err)
	}
	if len(samples) != 0 {
		t.Errorf("got %d samples for a malformed HHMM, want none", len(samples))
	}
}

func TestParseSwepamTimeBuildsUTCFromTheDateColumns(t *testing.T) {
	got, err := parseSwepamTime([]string{"2026", "09", "09", "0742"})
	if err != nil {
		t.Fatalf("parseSwepamTime: %v", err)
	}
	want := time.Date(2026, 9, 9, 7, 42, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Errorf("parseSwepamTime = %v, want %v", got, want)
	}
}
