package hamqsl

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/RealDougEubanks/solarham/internal/config"
	"github.com/RealDougEubanks/solarham/internal/metric"
	"github.com/RealDougEubanks/solarham/internal/source"
)

// discardLogger keeps test output readable. The behaviour under test is the
// samples and errors, never the log lines.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// baseConfig is a configuration that exercises the real code paths without
// waiting: a legal interval and a short timeout.
func baseConfig() config.Hamqsl {
	return config.Hamqsl{
		Enabled:  true,
		Interval: time.Hour,
		Timeout:  2 * time.Second,
		Retries:  -1, // negative means "one attempt", which most tests want.
	}
}

// newTestSource builds a source aimed at url. An empty url leaves the real feed
// address in place, which is what the constructor tests want.
func newTestSource(t *testing.T, cfg config.Hamqsl, url string) *Source {
	t.Helper()
	s, err := New(cfg, discardLogger())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if url != "" {
		s.url = url
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// fixture reads a captured response from testdata.
func fixture(t *testing.T, name string) []byte {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("reading fixture %s: %v", name, err)
	}
	return body
}

// serveBody starts a server that answers every request with body.
func serveBody(t *testing.T, body []byte) *httptest.Server {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/xml")
		_, _ = w.Write(body)
	}))
	t.Cleanup(ts.Close)
	return ts
}

// find returns the value of the sample matching desc and the given label
// values, and whether such a sample exists.
func find(b metric.Batch, desc *metric.Descriptor, labels ...string) (float64, bool) {
	for _, s := range b.Samples {
		if s.Desc != desc || len(s.Labels) != len(labels) {
			continue
		}
		match := true
		for i := range labels {
			if s.Labels[i] != labels[i] {
				match = false
				break
			}
		}
		if match {
			return s.Value, true
		}
	}
	return 0, false
}

// count reports how many samples in the batch belong to desc.
func count(b metric.Batch, desc *metric.Descriptor) int {
	n := 0
	for _, s := range b.Samples {
		if s.Desc == desc {
			n++
		}
	}
	return n
}

func TestNameAndDefaultsAreApplied(t *testing.T) {
	s := newTestSource(t, config.Hamqsl{Enabled: true}, "")

	if s.Name() != Name {
		t.Errorf("Name() = %q, want %q", s.Name(), Name)
	}
	if s.url != defaultURL {
		t.Errorf("url = %q, want %q", s.url, defaultURL)
	}
	if s.interval != defaultInterval {
		t.Errorf("interval = %v, want %v", s.interval, defaultInterval)
	}
	if s.retries != defaultRetries {
		t.Errorf("retries = %d, want %d", s.retries, defaultRetries)
	}
	if s.client.Timeout != defaultTimeout {
		t.Errorf("timeout = %v, want %v", s.client.Timeout, defaultTimeout)
	}
}

func TestIntervalIsClampedToTheCourtesyFloor(t *testing.T) {
	tests := []struct {
		name      string
		configued time.Duration
		want      time.Duration
	}{
		{"the thirty seconds the Python exporter used", 30 * time.Second, MinInterval},
		{"one second below the floor", MinInterval - time.Second, MinInterval},
		{"exactly the floor", MinInterval, MinInterval},
		{"a legal value above the floor", 30 * time.Minute, 30 * time.Minute},
		{"unset", 0, defaultInterval},
		{"negative", -time.Hour, defaultInterval},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := baseConfig()
			cfg.Interval = tt.configued
			s := newTestSource(t, cfg, "")
			if got := s.Interval(); got != tt.want {
				t.Errorf("Interval() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClampingWarnsRatherThanFailingStartup(t *testing.T) {
	var logged strings.Builder
	log := slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelDebug}))

	cfg := baseConfig()
	cfg.Interval = 30 * time.Second
	s, err := New(cfg, log)
	if err != nil {
		t.Fatalf("New returned an error for a too-small interval; it should clamp instead: %v", err)
	}
	if s.Interval() != MinInterval {
		t.Errorf("Interval() = %v, want %v", s.Interval(), MinInterval)
	}
	if !strings.Contains(logged.String(), "courtesy floor") {
		t.Errorf("clamping was not logged; got %q", logged.String())
	}
}

func TestFullFixtureParsesIntoTheExpectedSamples(t *testing.T) {
	ts := serveBody(t, fixture(t, "solarxml-2026-09-09.xml"))
	s := newTestSource(t, baseConfig(), ts.URL)

	b, err := s.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}

	// 8 bands x (ordinal + info) = 16, 5 VHF x 2 = 10, signal/noise min and
	// max = 2, geomagnetic field info = 1.
	const wantSamples = 29
	if b.Len() != wantSamples {
		t.Errorf("got %d samples, want %d", b.Len(), wantSamples)
		for _, sample := range b.Samples {
			t.Logf("  %s %v = %v", sample.Desc.FullName(), sample.Labels, sample.Value)
		}
	}
	if b.Source != Name {
		t.Errorf("batch source = %q, want %q", b.Source, Name)
	}

	if got, ok := find(b, metric.BandCondition, "80m-40m", "day"); !ok || got != 0 {
		t.Errorf("80m-40m day = %v (present %v), want 0 for Poor", got, ok)
	}
	if got, ok := find(b, metric.BandCondition, "30m-20m", "night"); !ok || got != 2 {
		t.Errorf("30m-20m night = %v (present %v), want 2 for Good", got, ok)
	}
	if got, ok := find(b, metric.BandConditionInfo, "17m-15m", "day", "Fair"); !ok || got != 1 {
		t.Errorf("17m-15m day info = %v (present %v), want 1", got, ok)
	}
	if got, ok := find(b, metric.VHFCondition, "vhf-aurora", "northern_hemi"); !ok || got != 0 {
		t.Errorf("vhf-aurora = %v (present %v), want 0 for Band Closed", got, ok)
	}
	if got, ok := find(b, metric.VHFConditionInfo, "E-Skip", "europe_4m", "Band Closed"); !ok || got != 1 {
		t.Errorf("E-Skip europe_4m info = %v (present %v), want 1", got, ok)
	}
	if got, ok := find(b, metric.SignalNoise, "min"); !ok || got != 2 {
		t.Errorf("signal noise min = %v (present %v), want 2", got, ok)
	}
	if got, ok := find(b, metric.SignalNoise, "max"); !ok || got != 3 {
		t.Errorf("signal noise max = %v (present %v), want 3", got, ok)
	}
	if got, ok := find(b, metric.GeomagneticFieldInfo, "UNSETTLD"); !ok || got != 1 {
		t.Errorf("geomagnetic field info = %v (present %v), want 1", got, ok)
	}

	for _, sample := range b.Samples {
		if err := sample.Validate(); err != nil {
			t.Errorf("invalid sample: %v", err)
		}
	}
}

// The swpc source is authoritative for everything hamqsl also reports. Emitting
// those series from here as well would produce two copies that disagree, which
// is worse than one copy from the better source.
func TestSourceDoesNotEmitSeriesNOAAOwns(t *testing.T) {
	ts := serveBody(t, fixture(t, "solarxml-2026-09-09.xml"))
	s := newTestSource(t, baseConfig(), ts.URL)

	b, err := s.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}

	for _, desc := range []*metric.Descriptor{
		metric.FluxSFU, metric.SunspotNumber, metric.AIndex, metric.KIndex,
		metric.XRayFlux, metric.XRayClassInfo, metric.ProtonFlux, metric.ElectronFlux,
		metric.WindSpeed, metric.AuroraBoundary, metric.FoF2, metric.MUF, metric.MUFFactor,
	} {
		if n := count(b, desc); n != 0 {
			t.Errorf("%s: got %d samples, want 0 — NOAA is authoritative for this", desc.FullName(), n)
		}
	}
}

// The feed misspells this element. The parser must match the wire, not the
// dictionary; the previous exporter looked for "electronflux" and silently got
// nothing back for the life of the container.
func TestElectonfluxSpellingMatchesTheFeed(t *testing.T) {
	doc, err := parse(fixture(t, "solarxml-2026-09-09.xml"))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got := strings.TrimSpace(doc.ElectonFlux); got != "10300" {
		t.Errorf("electonflux = %q, want %q", got, "10300")
	}
}

// The other three names the previous exporter guessed wrong.
func TestMisspelledElementNamesMatchTheFeed(t *testing.T) {
	doc, err := parse(fixture(t, "solarxml-2026-09-09.xml"))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	if got := strings.TrimSpace(doc.GeomagField); got != "UNSETTLD" {
		t.Errorf("geomagfield = %q, want %q (not geomagneticfield)", got, "UNSETTLD")
	}
	if got := strings.TrimSpace(doc.MUF); got != "NoRpt" {
		t.Errorf("muf = %q, want %q (not muff)", got, "NoRpt")
	}
	if got := strings.TrimSpace(doc.MUFFactor); got != "" {
		t.Errorf("muffactor = %q, want empty (two f's, not three)", got)
	}
}

func TestParseSkipsEmptyFoF2(t *testing.T) {
	doc, err := parse(fixture(t, "solarxml-2026-09-09.xml"))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if strings.TrimSpace(doc.FoF2) != "" {
		t.Fatalf("fixture no longer has an empty fof2: %q", doc.FoF2)
	}

	ts := serveBody(t, fixture(t, "solarxml-2026-09-09.xml"))
	s := newTestSource(t, baseConfig(), ts.URL)
	b, err := s.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if n := count(b, metric.FoF2); n != 0 {
		t.Errorf("empty fof2 produced %d samples, want 0 — absent is not zero", n)
	}
}

func TestPlaceholderValuesProduceNoSample(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  bool
	}{
		{"an empty element", "", true},
		{"the muf placeholder", "NoRpt", true},
		{"the kindexnt placeholder", "No Report", true},
		{"the same placeholder padded", "  No Report  ", true},
		{"a lowercase placeholder", "norpt", true},
		{"a real categorical value", "UNSETTLD", false},
		{"a real numeric value", "0.0", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isPlaceholder(tt.value); got != tt.want {
				t.Errorf("isPlaceholder(%q) = %v, want %v", tt.value, got, tt.want)
			}
		})
	}
}

// Every numeric element in the feed is padded, so trimming is not cosmetic: the
// previous exporter stored " 29" as a string and made arithmetic in Grafana
// impossible.
func TestLeadingWhitespaceIsTrimmedBeforeParsing(t *testing.T) {
	doc, err := parse(fixture(t, "solarxml-2026-09-09.xml"))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if doc.AIndex != " 29" {
		t.Fatalf("fixture no longer carries padded values: aindex = %q", doc.AIndex)
	}

	body := []byte(`<solar><solardata>
		<updated> 09 Sep 2026 0322 GMT</updated>
		<geomagfield>  UNSETTLD  </geomagfield>
		<signalnoise>  S2-S3  </signalnoise>
		<calculatedconditions><band name="80m-40m" time="day">  Good  </band></calculatedconditions>
	</solardata></solar>`)

	ts := serveBody(t, body)
	s := newTestSource(t, baseConfig(), ts.URL)
	b, err := s.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}

	if got, ok := find(b, metric.BandCondition, "80m-40m", "day"); !ok || got != 2 {
		t.Errorf("padded band condition = %v (present %v), want 2", got, ok)
	}
	if _, ok := find(b, metric.GeomagneticFieldInfo, "UNSETTLD"); !ok {
		t.Errorf("padded geomagfield was not trimmed before becoming a label")
	}
	if got, ok := find(b, metric.SignalNoise, "min"); !ok || got != 2 {
		t.Errorf("padded signalnoise min = %v (present %v), want 2", got, ok)
	}
}

func TestBandOrdinalMapping(t *testing.T) {
	tests := []struct {
		condition string
		want      float64
		known     bool
	}{
		{"Poor", 0, true},
		{"Fair", 1, true},
		{"Good", 2, true},
		{"poor", 0, true},
		{"GOOD", 2, true},
		{"  Fair  ", 1, true},
		{"Excellent", 0, false},
		{"", 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.condition, func(t *testing.T) {
			got, known := bandOrdinal(tt.condition)
			if known != tt.known {
				t.Fatalf("bandOrdinal(%q) known = %v, want %v", tt.condition, known, tt.known)
			}
			if known && got != tt.want {
				t.Errorf("bandOrdinal(%q) = %v, want %v", tt.condition, got, tt.want)
			}
		})
	}
}

func TestVHFOrdinalMapping(t *testing.T) {
	tests := []struct {
		condition string
		want      float64
		known     bool
	}{
		{"Band Closed", 0, true},
		{"Band Open", 1, true},
		{"band open", 1, true},
		{"Band  Closed", 0, true},
		{"50MHz es", 0, false},
		{"", 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.condition, func(t *testing.T) {
			got, known := vhfOrdinal(tt.condition)
			if known != tt.known {
				t.Fatalf("vhfOrdinal(%q) known = %v, want %v", tt.condition, known, tt.known)
			}
			if known && got != tt.want {
				t.Errorf("vhfOrdinal(%q) = %v, want %v", tt.condition, got, tt.want)
			}
		})
	}
}

func TestUnknownConditionEmitsInfoButNoOrdinal(t *testing.T) {
	body := []byte(`<solar><solardata>
		<updated> 09 Sep 2026 0322 GMT</updated>
		<calculatedconditions>
			<band name="80m-40m" time="day">Excellent</band>
		</calculatedconditions>
		<calculatedvhfconditions>
			<phenomenon name="E-Skip" location="europe">50MHz es</phenomenon>
		</calculatedvhfconditions>
	</solardata></solar>`)

	ts := serveBody(t, body)
	s := newTestSource(t, baseConfig(), ts.URL)
	b, err := s.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}

	if _, ok := find(b, metric.BandConditionInfo, "80m-40m", "day", "Excellent"); !ok {
		t.Errorf("unknown band condition did not produce an info sample")
	}
	if n := count(b, metric.BandCondition); n != 0 {
		t.Errorf("unknown band condition produced %d ordinal samples, want 0", n)
	}
	if _, ok := find(b, metric.VHFConditionInfo, "E-Skip", "europe", "50MHz es"); !ok {
		t.Errorf("unknown VHF condition did not produce an info sample")
	}
	if n := count(b, metric.VHFCondition); n != 0 {
		t.Errorf("unknown VHF condition produced %d ordinal samples, want 0", n)
	}
}

func TestSignalNoiseParsing(t *testing.T) {
	tests := []struct {
		name      string
		raw       string
		wantLower float64
		wantUpper float64
		ok        bool
	}{
		{"a range", "S2-S3", 2, 3, true},
		{"a padded range", "  S0-S1 ", 0, 1, true},
		{"a single value", "S4", 4, 4, true},
		{"a range without the S prefixes", "2-3", 2, 3, true},
		{"a reversed range", "S5-S2", 2, 5, true},
		{"a two-digit bound", "S8-S9", 8, 9, true},
		{"absent", "", 0, 0, false},
		{"a placeholder", "NoRpt", 0, 0, false},
		{"a half-open range", "S2-", 0, 0, false},
		{"nonsense", "quiet", 0, 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lower, upper, ok := parseSignalNoise(tt.raw)
			if ok != tt.ok {
				t.Fatalf("parseSignalNoise(%q) ok = %v, want %v", tt.raw, ok, tt.ok)
			}
			if !ok {
				return
			}
			if lower != tt.wantLower || upper != tt.wantUpper {
				t.Errorf("parseSignalNoise(%q) = %v, %v; want %v, %v",
					tt.raw, lower, upper, tt.wantLower, tt.wantUpper)
			}
		})
	}
}

func TestSingleValueSignalNoiseStillEmitsBothBounds(t *testing.T) {
	body := []byte(`<solar><solardata>
		<updated> 09 Sep 2026 0322 GMT</updated>
		<signalnoise>S4</signalnoise>
	</solardata></solar>`)

	ts := serveBody(t, body)
	s := newTestSource(t, baseConfig(), ts.URL)
	b, err := s.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}

	lower, lowerOK := find(b, metric.SignalNoise, "min")
	upper, upperOK := find(b, metric.SignalNoise, "max")
	if !lowerOK || !upperOK {
		t.Fatalf("single-value signalnoise produced min=%v max=%v; want both present", lowerOK, upperOK)
	}
	if lower != 4 || upper != 4 {
		t.Errorf("got min=%v max=%v, want 4 and 4", lower, upper)
	}
}

func TestMissingSignalNoiseEmitsNothing(t *testing.T) {
	body := []byte(`<solar><solardata><updated> 09 Sep 2026 0322 GMT</updated></solardata></solar>`)

	ts := serveBody(t, body)
	s := newTestSource(t, baseConfig(), ts.URL)
	b, err := s.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if n := count(b, metric.SignalNoise); n != 0 {
		t.Errorf("missing signalnoise produced %d samples, want 0", n)
	}
}

func TestUpdatedTimestampParsing(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want time.Time
		ok   bool
	}{
		{"the observed form, leading space and all", " 09 Sep 2026 0322 GMT",
			time.Date(2026, time.September, 9, 3, 22, 0, 0, time.UTC), true},
		{"with a colon", "09 Sep 2026 03:22 GMT",
			time.Date(2026, time.September, 9, 3, 22, 0, 0, time.UTC), true},
		{"a one-digit day", "9 Sep 2026 0322 GMT",
			time.Date(2026, time.September, 9, 3, 22, 0, 0, time.UTC), true},
		{"labelled UTC", "09 Sep 2026 0322 UTC",
			time.Date(2026, time.September, 9, 3, 22, 0, 0, time.UTC), true},
		{"empty", "", time.Time{}, false},
		{"a placeholder", "No Report", time.Time{}, false},
		{"a zone we cannot honestly resolve", "09 Sep 2026 0322 EST", time.Time{}, false},
		{"no zone at all", "09 Sep 2026 0322", time.Time{}, false},
		{"garbage", "yesterday-ish GMT", time.Time{}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := parseUpdated(tt.raw)
			if ok != tt.ok {
				t.Fatalf("parseUpdated(%q) ok = %v, want %v", tt.raw, ok, tt.ok)
			}
			if ok && !got.Equal(tt.want) {
				t.Errorf("parseUpdated(%q) = %v, want %v", tt.raw, got, tt.want)
			}
		})
	}
}

func TestUpdatedTimestampIsUsedForEverySample(t *testing.T) {
	ts := serveBody(t, fixture(t, "solarxml-2026-09-09.xml"))
	s := newTestSource(t, baseConfig(), ts.URL)

	b, err := s.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if b.Len() == 0 {
		t.Fatal("no samples to check")
	}

	want := time.Date(2026, time.September, 9, 3, 22, 0, 0, time.UTC)
	for _, sample := range b.Samples {
		if !sample.Time.Equal(want) {
			t.Errorf("%s %v carries time %v, want the feed's own %v",
				sample.Desc.FullName(), sample.Labels, sample.Time, want)
		}
	}
}

func TestUnparseableUpdatedFallsBackWithoutFailingThePoll(t *testing.T) {
	body := []byte(`<solar><solardata>
		<updated>whenever</updated>
		<geomagfield>QUIET</geomagfield>
	</solardata></solar>`)

	ts := serveBody(t, body)
	s := newTestSource(t, baseConfig(), ts.URL)

	before := time.Now().Add(-time.Second)
	b, err := s.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll returned an error for an unparseable <updated>; it should fall back: %v", err)
	}
	if b.Len() == 0 {
		t.Fatal("fallback produced no samples")
	}
	for _, sample := range b.Samples {
		if sample.Time.Before(before) {
			t.Errorf("sample time %v is not the response receipt time", sample.Time)
		}
	}
}

func TestUnparseableUpdatedWarnsOnlyOnce(t *testing.T) {
	var logged strings.Builder
	log := slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelWarn}))

	body := []byte(`<solar><solardata><updated>whenever</updated><geomagfield>QUIET</geomagfield></solardata></solar>`)
	ts := serveBody(t, body)

	s, err := New(baseConfig(), log)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	s.url = ts.URL
	t.Cleanup(func() { _ = s.Close() })

	for i := 0; i < 3; i++ {
		if _, err := s.Poll(context.Background()); err != nil {
			t.Fatalf("Poll %d: %v", i, err)
		}
	}

	if n := strings.Count(logged.String(), "could not be parsed"); n != 1 {
		t.Errorf("warned %d times across 3 polls, want 1 — an hourly feed would repeat this forever", n)
	}
}

func TestDegradedFeedParsesWithoutError(t *testing.T) {
	ts := serveBody(t, fixture(t, "solarxml-degraded.xml"))
	s := newTestSource(t, baseConfig(), ts.URL)

	b, err := s.Poll(context.Background())
	if err != nil {
		t.Fatalf("a degraded but valid feed must not fail a poll: %v", err)
	}

	// Two bands, each with an ordinal and an info sample, plus the geomagnetic
	// field state. Nothing else in this document has anything to say.
	const wantSamples = 5
	if b.Len() != wantSamples {
		t.Errorf("got %d samples, want %d", b.Len(), wantSamples)
		for _, sample := range b.Samples {
			t.Logf("  %s %v = %v", sample.Desc.FullName(), sample.Labels, sample.Value)
		}
	}
	if n := count(b, metric.VHFCondition) + count(b, metric.VHFConditionInfo); n != 0 {
		t.Errorf("an empty VHF section produced %d samples, want 0", n)
	}
	if n := count(b, metric.SignalNoise); n != 0 {
		t.Errorf("an absent signalnoise produced %d samples, want 0", n)
	}
	if _, ok := find(b, metric.GeomagneticFieldInfo, "QUIET"); !ok {
		t.Errorf("geomagnetic field state was lost")
	}
	for _, sample := range b.Samples {
		if sample.Time.IsZero() {
			t.Errorf("%s carries no timestamp despite the fallback", sample.Desc.FullName())
		}
	}
}

func TestMalformedXMLReturnsAnErrorRatherThanPanicking(t *testing.T) {
	full := fixture(t, "solarxml-2026-09-09.xml")

	tests := []struct {
		name string
		body []byte
	}{
		{"a truncated document", full[:len(full)/2]},
		{"an unclosed element", []byte(`<solar><solardata><geomagfield>QUIET`)},
		{"HTML from a CDN error page", []byte(`<html><body>503 Service Unavailable</body></html>`)},
		{"a wrong root element", []byte(`<weather><solardata/></weather>`)},
		{"not markup at all", []byte("service temporarily unavailable")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := serveBody(t, tt.body)
			s := newTestSource(t, baseConfig(), ts.URL)

			b, err := s.Poll(context.Background())
			if err == nil {
				t.Fatalf("Poll succeeded with %d samples; want an error", b.Len())
			}
			if b.Len() != 0 {
				t.Errorf("a failed poll returned %d samples, want 0", b.Len())
			}
		})
	}
}

func TestEmptyBodyIsAnError(t *testing.T) {
	ts := serveBody(t, nil)
	s := newTestSource(t, baseConfig(), ts.URL)

	if _, err := s.Poll(context.Background()); err == nil {
		t.Error("an empty body was accepted; want an error")
	}
}

func TestServerErrorRetriesThenSucceeds(t *testing.T) {
	var calls atomic.Int32
	body := fixture(t, "solarxml-2026-09-09.xml")

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_, _ = w.Write(body)
	}))
	t.Cleanup(ts.Close)

	cfg := baseConfig()
	cfg.Retries = 2
	s := newTestSource(t, cfg, ts.URL)

	b, err := s.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if got := calls.Load(); got != 3 {
		t.Errorf("made %d requests, want 3", got)
	}
	if b.Len() == 0 {
		t.Error("the successful retry produced no samples")
	}
}

func TestServerErrorExhaustsRetriesAndFails(t *testing.T) {
	var calls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusBadGateway)
	}))
	t.Cleanup(ts.Close)

	cfg := baseConfig()
	cfg.Retries = 1
	s := newTestSource(t, cfg, ts.URL)

	if _, err := s.Poll(context.Background()); err == nil {
		t.Fatal("Poll succeeded against a server returning 502")
	}
	if got := calls.Load(); got != 2 {
		t.Errorf("made %d requests, want 2", got)
	}
}

func TestPermanentStatusesAreNotRetried(t *testing.T) {
	tests := []struct {
		name string
		code int
	}{
		{"not found", http.StatusNotFound},
		{"forbidden", http.StatusForbidden},
		{"unauthorized", http.StatusUnauthorized},
		{"bad request", http.StatusBadRequest},
		{"gone", http.StatusGone},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var calls atomic.Int32
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.WriteHeader(tt.code)
			}))
			t.Cleanup(ts.Close)

			cfg := baseConfig()
			cfg.Retries = 3
			s := newTestSource(t, cfg, ts.URL)

			_, err := s.Poll(context.Background())
			if !errors.Is(err, ErrPermanent) {
				t.Errorf("error = %v, want one wrapping ErrPermanent", err)
			}
			if got := calls.Load(); got != 1 {
				t.Errorf("made %d requests, want 1 — a permanent failure must not be retried", got)
			}
		})
	}
}

// 408 is the server saying it gave up waiting, not that the request is wrong,
// so it is the one 4xx worth trying again.
func TestRequestTimeoutIsTreatedAsTransient(t *testing.T) {
	var calls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusRequestTimeout)
	}))
	t.Cleanup(ts.Close)

	cfg := baseConfig()
	cfg.Retries = 1
	s := newTestSource(t, cfg, ts.URL)

	_, err := s.Poll(context.Background())
	if errors.Is(err, ErrPermanent) {
		t.Errorf("408 was classified as permanent: %v", err)
	}
	if got := calls.Load(); got != 2 {
		t.Errorf("made %d requests, want 2", got)
	}
}

func TestRateLimitReturnsErrRateLimited(t *testing.T) {
	var calls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Retry-After", "120")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	t.Cleanup(ts.Close)

	cfg := baseConfig()
	cfg.Retries = 3
	s := newTestSource(t, cfg, ts.URL)

	_, err := s.Poll(context.Background())
	if !errors.Is(err, source.ErrRateLimited) {
		t.Fatalf("error = %v, want one wrapping source.ErrRateLimited", err)
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("made %d requests, want 1 — a rate limit must not be retried", got)
	}

	// A second poll must not touch the network at all: the server has already
	// said it does not want the traffic.
	if _, err := s.Poll(context.Background()); !errors.Is(err, source.ErrRateLimited) {
		t.Errorf("second poll error = %v, want ErrRateLimited from the cooldown", err)
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("made %d requests after the cooldown began, want 1", got)
	}
}

func TestRetryAfterHeaderIsHonoured(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want time.Duration
	}{
		{"delta seconds", "120", 2 * time.Minute},
		{"absent", "", defaultRateLimitCooldown},
		{"unparseable", "soon", defaultRateLimitCooldown},
		{"zero", "0", defaultRateLimitCooldown},
		{"absurdly long", "999999999", maxRateLimitCooldown},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := retryAfter(tt.raw); got != tt.want {
				t.Errorf("retryAfter(%q) = %v, want %v", tt.raw, got, tt.want)
			}
		})
	}
}

func TestContextCancellationIsHonoured(t *testing.T) {
	released := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-released
	}))
	t.Cleanup(func() {
		close(released)
		ts.Close()
	})

	s := newTestSource(t, baseConfig(), ts.URL)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	if _, err := s.Poll(ctx); err == nil {
		t.Fatal("Poll succeeded against a server that never answered")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("Poll took %v to notice cancellation", elapsed)
	}
}

func TestAlreadyCancelledContextMakesNoRequest(t *testing.T) {
	var calls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
	}))
	t.Cleanup(ts.Close)

	s := newTestSource(t, baseConfig(), ts.URL)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := s.Poll(ctx); err == nil {
		t.Fatal("Poll succeeded with a cancelled context")
	}
	if got := calls.Load(); got != 0 {
		t.Errorf("made %d requests with a cancelled context, want 0", got)
	}
}

func TestRequestIdentifiesThisClient(t *testing.T) {
	var agent atomic.Value
	agent.Store("")

	body := fixture(t, "solarxml-2026-09-09.xml")
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		agent.Store(r.Header.Get("User-Agent"))
		_, _ = w.Write(body)
	}))
	t.Cleanup(ts.Close)

	s := newTestSource(t, baseConfig(), ts.URL)
	if _, err := s.Poll(context.Background()); err != nil {
		t.Fatalf("Poll: %v", err)
	}

	got, _ := agent.Load().(string)
	if !strings.Contains(got, "solarham") {
		t.Errorf("User-Agent = %q, want it to name the project", got)
	}
	if !strings.Contains(got, "github.com") {
		t.Errorf("User-Agent = %q, want it to carry the repository URL so the feed's operator can find us", got)
	}
}

// The document is about 1.6 KB. A response orders of magnitude larger is a
// broken or hostile origin, and reading it whole would let it exhaust memory.
func TestOversizedBodyIsBounded(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/xml")
		_, _ = w.Write([]byte("<solar><solardata><geomagfield>"))
		chunk := make([]byte, 64<<10)
		for i := range chunk {
			chunk[i] = 'A'
		}
		for written := 0; written < maxBodyBytes+len(chunk); written += len(chunk) {
			if _, err := w.Write(chunk); err != nil {
				return
			}
		}
	}))
	t.Cleanup(ts.Close)

	s := newTestSource(t, baseConfig(), ts.URL)

	// Truncation at the cap makes the document unparseable, which is the
	// correct outcome: a body this size is not the feed.
	if _, err := s.Poll(context.Background()); err == nil {
		t.Error("an unbounded body was accepted; want an error")
	}
}

func TestTransportFailureIsReportedNotPanicked(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	url := ts.URL
	ts.Close() // nothing is listening now

	s := newTestSource(t, baseConfig(), url)

	if _, err := s.Poll(context.Background()); err == nil {
		t.Error("Poll succeeded against a closed listener")
	}
}

func TestCloseIsSafeToCallTwice(t *testing.T) {
	s := newTestSource(t, baseConfig(), "")
	if err := s.Close(); err != nil {
		t.Errorf("first Close: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
}
