package lasp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/RealDougEubanks/solarham/internal/config"
	"github.com/RealDougEubanks/solarham/internal/httpx"
	"github.com/RealDougEubanks/solarham/internal/metric"
	"github.com/RealDougEubanks/solarham/internal/source"
)

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

// discardLogger keeps test output readable. The behaviour under test is the
// samples and the errors, never the log lines.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// testClient builds an httpx client with no rate limiting, so a test that makes
// eight requests does not wait for the real five-second politeness spacing.
func testClient(t *testing.T) *httpx.Client {
	t.Helper()
	return httpx.New(httpx.Options{
		Timeout:  5 * time.Second,
		Fallback: httpx.HostPolicy{MinInterval: time.Nanosecond, Burst: 1000},
	}, discardLogger())
}

// fixture reads a captured upstream response from testdata.
func fixture(t *testing.T, name string) []byte {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("reading fixture %s: %v", name, err)
	}
	return body
}

// newTestSource builds a source aimed at baseURL with a fixed clock.
func newTestSource(t *testing.T, cfg config.LASP, baseURL string, now time.Time) *Source {
	t.Helper()
	cfg.BaseURL = baseURL
	s, err := New(cfg, testClient(t), discardLogger())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if !now.IsZero() {
		s.now = func() time.Time { return now }
	}
	return s
}

// sampleFor finds the one sample matching a descriptor and label values.
func sampleFor(t *testing.T, b metric.Batch, desc *metric.Descriptor, labels ...string) metric.Sample {
	t.Helper()
	var found []metric.Sample
	for _, s := range b.Samples {
		if s.Desc != desc {
			continue
		}
		if len(labels) > 0 && strings.Join(s.Labels, "\x00") != strings.Join(labels, "\x00") {
			continue
		}
		found = append(found, s)
	}
	if len(found) != 1 {
		t.Fatalf("wanted exactly one %s%v sample, got %d in %s",
			desc.FullName(), labels, len(found), describe(b))
	}
	return found[0]
}

// countFor reports how many samples of a descriptor a batch carries.
func countFor(b metric.Batch, desc *metric.Descriptor) int {
	n := 0
	for _, s := range b.Samples {
		if s.Desc == desc {
			n++
		}
	}
	return n
}

// describe renders a batch for a failure message.
func describe(b metric.Batch) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "batch of %d sample(s):", len(b.Samples))
	for _, s := range b.Samples {
		fmt.Fprintf(&sb, "\n  %s%v = %g at %s",
			s.Desc.FullName(), s.Labels, s.Value, s.Time.Format(time.RFC3339))
	}
	return sb.String()
}

// lisirdServer answers each dataset from its captured fixture and records the
// raw query strings it was sent.
type lisirdServer struct {
	mu      sync.Mutex
	queries []string
	hits    map[string]int
}

func serveLISIRD(t *testing.T) (*httptest.Server, *lisirdServer) {
	t.Helper()
	rec := &lisirdServer{hits: make(map[string]int)}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The dataset name is the last path element without ".csv".
		name := strings.TrimSuffix(filepath.Base(r.URL.Path), ".csv")

		rec.mu.Lock()
		rec.queries = append(rec.queries, r.URL.RawQuery)
		rec.hits[name]++
		rec.mu.Unlock()

		body, err := os.ReadFile(filepath.Join("testdata", "lisird_"+name+".csv"))
		if err != nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "text/csv")
		_, _ = w.Write(body)
	}))
	t.Cleanup(ts.Close)
	return ts, rec
}

func (l *lisirdServer) hitCount(dataset string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.hits[dataset]
}

func (l *lisirdServer) rawQueries() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.queries...)
}

// lisirdConfig enables LISIRD only, so the EVE path is out of the way.
func lisirdConfig() config.LASP {
	return config.LASP{Enabled: true, LISIRD: true}
}

// lisirdNow is a clock inside the query window of every captured fixture.
var lisirdNow = time.Date(2026, 9, 9, 15, 0, 0, 0, time.UTC)

// ---------------------------------------------------------------------------
// Fixture parsing, with exact expected values
// ---------------------------------------------------------------------------

func TestTheCapturedLISIRDFixturesParseIntoTheExpectedSamples(t *testing.T) {
	ts, _ := serveLISIRD(t)
	s := newTestSource(t, lisirdConfig(), ts.URL, lisirdNow)

	batch, err := s.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}

	// The last row of each CLS fixture, all at 2026-09-08. Every value here was
	// read off the captured file.
	for _, want := range []struct {
		wavelength string
		observed   float64
		adjusted   float64
	}{
		{"8", 106.0, 107.6},
		{"15", 103.0, 104.5},
		{"30", 84.0, 85.2},
		{"32", 294.0, 298.4},
	} {
		got := sampleFor(t, batch, metric.RadioFluxMultiFrequency, want.wavelength, "observed")
		if got.Value != want.observed {
			t.Errorf("the %s cm observed flux is %g, want %g", want.wavelength, got.Value, want.observed)
		}
		got = sampleFor(t, batch, metric.RadioFluxMultiFrequency, want.wavelength, "adjusted")
		if got.Value != want.adjusted {
			t.Errorf("the %s cm adjusted flux is %g, want %g", want.wavelength, got.Value, want.adjusted)
		}
	}

	// tsis_tsi_6hr, newest row JD 2461288.375 = 2026-09-04 21:00 UT. The
	// fixture also carries an all-zero placeholder row at 2461287.625, which
	// must not be the one published.
	tsi := sampleFor(t, batch, metric.TotalSolarIrradiance)
	if tsi.Value != 1362.2309 {
		t.Errorf("total solar irradiance is %g, want 1362.2309", tsi.Value)
	}
	if want := time.Date(2026, 9, 4, 21, 0, 0, 0, time.UTC); !tsi.Time.Equal(want) {
		t.Errorf("TSI is stamped %s, want %s", tsi.Time, want)
	}

	// bremen_composite_mgii, last row 2026 09 07.
	mgii := sampleFor(t, batch, metric.MgIIIndex)
	if mgii.Value != 0.15738 {
		t.Errorf("Mg II index is %g, want 0.15738", mgii.Value)
	}
	if want := time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC); !mgii.Time.Equal(want) {
		t.Errorf("Mg II is stamped %s, want %s", mgii.Time, want)
	}

	// composite_lyman_alpha, last row 29103.0 days since 1947-01-01T12:00.
	lya := sampleFor(t, batch, metric.LymanAlphaIrradiance)
	if lya.Value != 0.007350135201700001 {
		t.Errorf("Lyman-alpha irradiance is %g, want 0.007350135201700001", lya.Value)
	}
	if want := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC); !lya.Time.Equal(want) {
		t.Errorf("Lyman-alpha is stamped %s, want %s; the epoch is at noon, not midnight", lya.Time, want)
	}
}

func TestTheUncorrectedFluxColumnIsPublishedRatherThanTheBurstCorrectedOne(t *testing.T) {
	// The captured cls_radio_flux_f107 fixture contains a real burst-contaminated
	// row: on 2026-09-02, absolute_f107 reads 139.0 with quality flag 4 while
	// absolute_f107_c reads 99.3. The uncorrected reading is what every other
	// publisher of this series carries, so it is the one published.
	table, err := parseLISIRD(fixture(t, "lisird_cls_radio_flux_f107.csv"))
	if err != nil {
		t.Fatalf("parseLISIRD: %v", err)
	}
	if _, ok := table.columns["absolute_f107_c"]; !ok {
		t.Fatal("the fixture has no corrected column, so this test proves nothing")
	}

	var ds lisirdDataset
	for _, candidate := range f107Datasets {
		if candidate.name == "cls_radio_flux_f107" {
			ds = candidate
		}
	}
	if ds.emit == nil {
		t.Fatal("cls_radio_flux_f107 is not in f107Datasets")
	}

	// Find the flagged row and emit from it directly.
	var burst lisirdRow
	for _, rec := range table.rows {
		if strings.TrimSpace(rec[table.columns["time"]]) == "2026 9 2" {
			burst = lisirdRow{table: table, fields: rec}
		}
	}
	if burst.table == nil {
		t.Fatal("the fixture no longer contains the 2026-09-02 burst-contaminated row")
	}

	s := newTestSource(t, lisirdConfig(), "https://example.invalid", lisirdNow)
	var b metric.Batch
	ds.emit(s, &b, burst, time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC))

	got := sampleFor(t, b, metric.RadioFluxMultiFrequency, "10.7", "observed")
	if got.Value != 139.0 {
		t.Errorf("the observed 10.7 cm flux is %g, want the uncorrected 139; 99.3 is the corrected column", got.Value)
	}
	got = sampleFor(t, b, metric.RadioFluxMultiFrequency, "10.7", "adjusted")
	if got.Value != 141.5 {
		t.Errorf("the adjusted 10.7 cm flux is %g, want the uncorrected 141.5", got.Value)
	}
}

// ---------------------------------------------------------------------------
// The DRAO overlap
// ---------------------------------------------------------------------------

func TestTheDuplicateF107DatasetsAreNotFetchedByDefault(t *testing.T) {
	ts, rec := serveLISIRD(t)
	s := newTestSource(t, lisirdConfig(), ts.URL, lisirdNow)

	if _, err := s.Poll(context.Background()); err != nil {
		t.Fatalf("Poll: %v", err)
	}

	for _, ds := range []string{"penticton_radio_flux", "cls_radio_flux_f107"} {
		if n := rec.hitCount(ds); n != 0 {
			t.Errorf("%s was fetched %d time(s); it duplicates the drao source's series", ds, n)
		}
	}
}

func TestNo10PointSevenCentimetreSeriesIsPublishedByDefault(t *testing.T) {
	ts, _ := serveLISIRD(t)
	s := newTestSource(t, lisirdConfig(), ts.URL, lisirdNow)

	batch, err := s.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	for _, smp := range batch.Samples {
		if smp.Desc != metric.RadioFluxMultiFrequency {
			continue
		}
		if wl, _ := smp.LabelFor("wavelength_cm"); wl == "10.7" {
			t.Errorf("a 10.7 cm series was published by default: %v = %g; the drao source owns it", smp.Labels, smp.Value)
		}
	}
}

func TestEnableF107DatasetsAddsThemBack(t *testing.T) {
	ts, rec := serveLISIRD(t)
	s := newTestSource(t, lisirdConfig(), ts.URL, lisirdNow)
	s.EnableF107Datasets()

	batch, err := s.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	for _, ds := range []string{"penticton_radio_flux", "cls_radio_flux_f107"} {
		if n := rec.hitCount(ds); n != 1 {
			t.Errorf("%s was fetched %d time(s), want 1", ds, n)
		}
	}
	// penticton_radio_flux and cls_radio_flux_f107 both write 10.7, so the two
	// samples under each adjustment collide by design. That collision is the
	// documented reason both are off by default, and it must be visible here
	// rather than silent.
	if n := countFor(batch, metric.RadioFluxMultiFrequency); n <= 8 {
		t.Errorf("got %d radio flux samples, want more than the eight the four default wavelengths give", n)
	}
}

// ---------------------------------------------------------------------------
// The tsis_tsi_6hr zero placeholder
// ---------------------------------------------------------------------------

func TestAnAllZeroTSIRowProducesNoSample(t *testing.T) {
	// The captured fixture's newest row is all zeros, which is what a six-hour
	// bucket with no data looks like. The row before it must win.
	table, err := parseLISIRD(fixture(t, "lisird_tsis_tsi_6hr.csv"))
	if err != nil {
		t.Fatalf("parseLISIRD: %v", err)
	}
	row, at, err := table.newestRow()
	if err != nil {
		t.Fatalf("newestRow: %v", err)
	}

	// The newest row is a real reading, so it is published.
	if v, ok := row.positive("tsi_1au"); !ok || v <= 1000 {
		t.Errorf("the newest row gave (%g, %t), want a plausible irradiance", v, ok)
	}
	_ = at

	// The fixture's placeholder row is at JD 2461287.625. Its tsi_1au is 0.0,
	// and positive() must refuse it: a total solar irradiance of zero is a
	// claim the Sun went out.
	var sawZeroRow bool
	for _, rec := range table.rows {
		zero := lisirdRow{table: table, fields: rec}
		if rec[table.columns["time"]] != "2461287.625" {
			continue
		}
		sawZeroRow = true
		if v, ok := zero.positive("tsi_1au"); ok {
			t.Errorf("the all-zero placeholder row published a total solar irradiance of %g", v)
		}
	}
	if !sawZeroRow {
		t.Fatal("the fixture no longer contains the all-zero placeholder row, so this test proves nothing")
	}

	// End to end, the source publishes the last real reading instead.
	ts, _ := serveLISIRD(t)
	s := newTestSource(t, lisirdConfig(), ts.URL, lisirdNow)
	batch, err := s.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	tsi := sampleFor(t, batch, metric.TotalSolarIrradiance)
	if tsi.Value <= 1000 {
		t.Errorf("total solar irradiance is %g, want a plausible value near 1361 W/m2", tsi.Value)
	}
}

func TestPositiveRejectsEveryNonMeasurement(t *testing.T) {
	table := &lisirdTable{columns: map[string]int{"time": 0, "v": 1}}
	for _, bad := range []string{"0", "0.0", "-1", "-1.0e+00", "", "  ", "NaN", "Inf", "-Inf", "not-a-number"} {
		row := lisirdRow{table: table, fields: []string{"0", bad}}
		if v, ok := row.positive("v"); ok {
			t.Errorf("positive(%q) returned %g, want no sample", bad, v)
		}
	}
	row := lisirdRow{table: table, fields: []string{"0", "1362.4421"}}
	if v, ok := row.positive("v"); !ok || v != 1362.4421 {
		t.Errorf("positive(\"1362.4421\") = (%g, %t), want (1362.4421, true)", v, ok)
	}
	if v, ok := row.positive("absent"); ok {
		t.Errorf("a missing column returned %g, want no sample", v)
	}
}

// ---------------------------------------------------------------------------
// Time encodings — one converter, every format actually encountered
// ---------------------------------------------------------------------------

func TestEveryTimeEncodingTheDatasetsUseConvertsCorrectly(t *testing.T) {
	cases := []struct {
		name  string
		units string
		field string
		want  time.Time
	}{
		{
			// penticton_radio_flux and tsis_tsi_6hr. The fractional part is a
			// real time of day, which is why this must not be truncated to a
			// date. Note that JD .5 is midnight, so .197 lands on the previous
			// calendar day at 16:43:41 — an off-by-one waiting to happen.
			name:  "a Julian Date with a fractional day",
			units: "Julian Date",
			field: "2461285.197",
			want:  time.Date(2026, 9, 1, 16, 43, 41, 0, time.UTC),
		},
		{
			name:  "a Julian Date at exactly midnight UT",
			units: "Julian Date",
			field: "2461285.5",
			want:  time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC),
		},
		{
			// The CLS datasets write single digits: "2026 9 1".
			name:  "yyyy MM dd with unpadded fields",
			units: "yyyy MM dd",
			field: "2026 9 1",
			want:  time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			// bremen_composite_mgii writes the same pattern zero-padded.
			name:  "yyyy MM dd with zero-padded fields",
			units: "yyyy MM dd",
			field: "2026 09 01",
			want:  time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			// composite_lyman_alpha. The epoch is at noon, not midnight, which
			// is the detail that puts every sample twelve hours out if missed.
			name:  "days since a noon epoch",
			units: "days since 1947-01-01T12:00",
			field: "29100.0",
			want:  time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC),
		},
		{
			name:  "days since a midnight epoch",
			units: "days since 1970-01-01",
			field: "20699.0",
			want:  time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC),
		},
		{
			// LATIS's other offset form.
			name:  "milliseconds since the Unix epoch",
			units: "milliseconds since 1970-01-01",
			field: "1788393600000",
			want:  time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC),
		},
		{
			name:  "seconds since the Unix epoch",
			units: "seconds since 1970-01-01",
			field: "1788393600",
			want:  time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC),
		},
		{
			name:  "hours since an epoch",
			units: "hours since 1970-01-01T00:00:00",
			field: "496776",
			want:  time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC),
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := parseLISIRDTime(c.field, c.units)
			if err != nil {
				t.Fatalf("parseLISIRDTime(%q, %q): %v", c.field, c.units, err)
			}
			if !got.Equal(c.want) {
				t.Errorf("parseLISIRDTime(%q, %q) = %s, want %s",
					c.field, c.units, got.Format(time.RFC3339), c.want.Format(time.RFC3339))
			}
		})
	}
}

func TestEachCapturedFixturesTimeUnitsAreReadFromItsOwnHeader(t *testing.T) {
	// Nothing is assumed from the dataset name. This pins the units string each
	// captured file actually declares, so a change upstream shows up here.
	want := map[string]string{
		"lisird_penticton_radio_flux.csv":  "Julian Date",
		"lisird_tsis_tsi_6hr.csv":          "Julian Date",
		"lisird_cls_radio_flux_f8.csv":     "yyyy MM dd",
		"lisird_cls_radio_flux_f15.csv":    "yyyy MM dd",
		"lisird_cls_radio_flux_f30.csv":    "yyyy MM dd",
		"lisird_cls_radio_flux_f32.csv":    "yyyy MM dd",
		"lisird_cls_radio_flux_f107.csv":   "yyyy MM dd",
		"lisird_bremen_composite_mgii.csv": "yyyy MM dd",
		"lisird_composite_lyman_alpha.csv": "days since 1947-01-01T12:00",
	}

	for file, units := range want {
		t.Run(file, func(t *testing.T) {
			table, err := parseLISIRD(fixture(t, file))
			if err != nil {
				t.Fatalf("parseLISIRD: %v", err)
			}
			if table.timeUnits != units {
				t.Errorf("time units are %q, want %q", table.timeUnits, units)
			}
			// And every row's time must convert with those units.
			if _, _, err := table.newestRow(); err != nil {
				t.Errorf("newestRow: %v", err)
			}
		})
	}
}

func TestAnUnrecognisedOrMissingTimeUnitIsAnErrorRatherThanAGuess(t *testing.T) {
	cases := map[string][2]string{
		"no units at all":           {"12345", ""},
		"an unknown units string":   {"12345", "fortnights"},
		"an unknown offset unit":    {"12345", "fortnights since 1970-01-01"},
		"an unparseable epoch":      {"12345", "days since the beginning"},
		"a non-numeric offset":      {"soon", "days since 1970-01-01"},
		"a non-numeric Julian Date": {"soon", "Julian Date"},
		"an empty field":            {"", "Julian Date"},
		"a pattern arity mismatch":  {"2026 09", "yyyy MM dd"},
		"an unknown pattern field":  {"2026 09 01", "yyyy MM qq"},
	}

	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			if got, err := parseLISIRDTime(c[0], c[1]); err == nil {
				t.Errorf("%s was accepted as %s; a guessed timestamp is worse than none",
					name, got.Format(time.RFC3339))
			}
		})
	}
}

func TestAMisreadUnitProducingAnImplausibleYearIsRejected(t *testing.T) {
	// A Julian Date interpreted as days since 1970 lands in the year 8700. That
	// is exactly what a units-string change would look like, so it must fail
	// rather than reach a sink.
	if got, err := parseLISIRDTime("2461285.197", "days since 1970-01-01"); err == nil {
		t.Errorf("a Julian Date read as days since 1970 was accepted as %s", got.Format(time.RFC3339))
	}
}

// ---------------------------------------------------------------------------
// CSV header handling
// ---------------------------------------------------------------------------

func TestColumnUnitsContainingParenthesesAreSplitCorrectly(t *testing.T) {
	// The live header is `observed_flux (solar flux unit (SFU))`, so a balanced
	// paren match would take the name as "observed_flux (solar flux unit".
	name, units := splitColumnUnits("observed_flux (solar flux unit (SFU))")
	if name != "observed_flux" {
		t.Errorf("name is %q, want %q", name, "observed_flux")
	}
	if units != "solar flux unit (SFU)" {
		t.Errorf("units are %q, want %q", units, "solar flux unit (SFU)")
	}

	name, units = splitColumnUnits("absolute_f8_f")
	if name != "absolute_f8_f" || units != "" {
		t.Errorf("got (%q, %q), want (%q, \"\")", name, units, "absolute_f8_f")
	}
}

func TestTheNewestRowWinsRatherThanTheLastOneInTheFile(t *testing.T) {
	// LATIS returns ascending order, but the order is not relied on.
	body := "time (yyyy MM dd),v\n2026 9 3,3.0\n2026 9 1,1.0\n2026 9 2,2.0\n"
	table, err := parseLISIRD([]byte(body))
	if err != nil {
		t.Fatalf("parseLISIRD: %v", err)
	}
	row, at, err := table.newestRow()
	if err != nil {
		t.Fatalf("newestRow: %v", err)
	}
	if want := time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC); !at.Equal(want) {
		t.Errorf("newest row is at %s, want %s", at, want)
	}
	if v, ok := row.positive("v"); !ok || v != 3.0 {
		t.Errorf("value is (%g, %t), want (3, true)", v, ok)
	}
}

func TestARowWithAnUnreadableTimeIsSkippedRatherThanFatal(t *testing.T) {
	body := "time (yyyy MM dd),v\n2026 9 1,1.0\nnot-a-date,9.0\n"
	table, err := parseLISIRD([]byte(body))
	if err != nil {
		t.Fatalf("parseLISIRD: %v", err)
	}
	row, at, err := table.newestRow()
	if err != nil {
		t.Fatalf("one bad row should not lose the dataset, got: %v", err)
	}
	if want := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC); !at.Equal(want) {
		t.Errorf("newest row is at %s, want %s", at, want)
	}
	if v, _ := row.positive("v"); v != 1.0 {
		t.Errorf("value is %g, want 1", v)
	}
}

// ---------------------------------------------------------------------------
// The query
// ---------------------------------------------------------------------------

func TestTheTimeFilterIsPercentEncoded(t *testing.T) {
	ts, rec := serveLISIRD(t)
	s := newTestSource(t, lisirdConfig(), ts.URL, lisirdNow)

	if _, err := s.Poll(context.Background()); err != nil {
		t.Fatalf("Poll: %v", err)
	}

	queries := rec.rawQueries()
	if len(queries) == 0 {
		t.Fatal("no queries were recorded")
	}
	for _, raw := range queries {
		if !strings.Contains(raw, "time%3E=") {
			t.Errorf("query %q does not carry the percent-encoded operator; LATIS reads the raw query string "+
				"and ignores a literal \">\", returning the dataset's entire history", raw)
		}
		// And it must name a date ten days back from the fixed clock.
		want := lisirdNow.AddDate(0, 0, -lisirdWindowDays).Format("2006-01-02")
		if !strings.Contains(raw, want) {
			t.Errorf("query %q does not name the expected lower bound %s", raw, want)
		}
	}
}

func TestTheEncodedQueryStillDecodesToTheIntendedFilter(t *testing.T) {
	raw := fmt.Sprintf(lisirdPathFormat, "tsis_tsi_6hr", "2026-08-30")
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("the path format does not produce a parseable URL: %v", err)
	}
	if u.Path != "/lisird/latis/dap/tsis_tsi_6hr.csv" {
		t.Errorf("path is %q, want the dataset path", u.Path)
	}
	decoded, err := url.QueryUnescape(u.RawQuery)
	if err != nil {
		t.Fatalf("QueryUnescape: %v", err)
	}
	if decoded != "time>=2026-08-30" {
		t.Errorf("the query decodes to %q, want %q", decoded, "time>=2026-08-30")
	}
}

// ---------------------------------------------------------------------------
// The one-byte-body case
// ---------------------------------------------------------------------------

func TestADatasetThatAnswers200WithAnEmptyBodyIsAnErrorRatherThanSilentSuccess(t *testing.T) {
	// noaa_radio_flux.csv answers 200 with exactly one byte. A source that
	// treated that as "no samples" would report a healthy source publishing
	// nothing forever, which is the hardest failure to notice.
	for _, body := range []string{"", "\n", " ", "\r\n"} {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(body))
		}))

		s := newTestSource(t, lisirdConfig(), ts.URL, lisirdNow)
		batch, err := s.Poll(context.Background())
		if err == nil {
			t.Errorf("a %d-byte body was accepted, got %s", len(body), describe(batch))
		}
		ts.Close()
	}
}

// ---------------------------------------------------------------------------
// Schedule
// ---------------------------------------------------------------------------

func TestTheLISIRDOnlyScheduleIsThreeHourly(t *testing.T) {
	s := newTestSource(t, lisirdConfig(), "https://example.invalid", time.Time{})
	sched := s.Schedule()

	start := time.Date(2026, 9, 9, 0, 0, 0, 0, time.UTC)
	at := start
	polls := 0
	for at.Before(start.Add(24 * time.Hour)) {
		next := sched.NextAfter(at)
		if !next.After(at) {
			t.Fatalf("NextAfter(%s) returned %s, which would spin the poll loop", at, next)
		}
		if gap := next.Sub(at); gap != lisirdInterval {
			t.Fatalf("gap is %s, want %s", gap, lisirdInterval)
		}
		polls++
		at = next
	}
	if polls != 8 {
		t.Errorf("got %d polls in a simulated day, want 8", polls)
	}
}

func TestADailyDatasetIsNotPolledHourly(t *testing.T) {
	s := newTestSource(t, lisirdConfig(), "https://example.invalid", time.Time{})
	sched := s.Schedule()

	start := time.Date(2026, 9, 9, 0, 0, 0, 0, time.UTC)
	polls := 0
	for at := start; at.Before(start.Add(24 * time.Hour)); {
		at = sched.NextAfter(at)
		polls++
	}
	if polls > 12 {
		t.Errorf("the schedule made %d polls in a simulated day; hourly would be 24 and these are daily datasets", polls)
	}
	if sched.Interval() < lisirdMinInterval {
		t.Errorf("the nominal interval is %s, below the %s courtesy floor", sched.Interval(), lisirdMinInterval)
	}
}

func TestTheHourlyFloorIsEnforcedBySchedule(t *testing.T) {
	// A floor that only validation applies is not a floor.
	s := newTestSource(t, lisirdConfig(), "https://example.invalid", time.Time{})
	sched := s.Schedule()
	at := time.Date(2026, 9, 9, 0, 0, 0, 0, time.UTC)
	if gap := sched.NextAfter(at).Sub(at); gap < lisirdMinInterval {
		t.Errorf("the schedule allowed a %s gap, want no less than %s", gap, lisirdMinInterval)
	}
}

func TestLISIRDIsNotFetchedOnEveryTickWhenEVEIsAlsoEnabled(t *testing.T) {
	// EVE wants a one-minute schedule and these datasets are daily. Without the
	// per-poll gate, enabling both would turn six daily datasets into 8,640
	// requests a day.
	eveTS, _ := serveEVE(t, fixture(t, "eve_l0cs_head.txt"), fixture(t, "eve_l0cs_tail.txt"))
	_ = eveTS

	var lisirdHits atomic.Int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/lisird/") {
			lisirdHits.Add(1)
			name := strings.TrimSuffix(filepath.Base(r.URL.Path), ".csv")
			body, err := os.ReadFile(filepath.Join("testdata", "lisird_"+name+".csv"))
			if err != nil {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			_, _ = w.Write(body)
			return
		}
		w.WriteHeader(http.StatusPartialContent)
		if strings.HasPrefix(r.Header.Get("Range"), "bytes=0-") {
			_, _ = w.Write(fixture(t, "eve_l0cs_head.txt"))
			return
		}
		_, _ = w.Write(fixture(t, "eve_l0cs_tail.txt"))
	}))
	t.Cleanup(ts.Close)

	s := newTestSource(t, config.LASP{Enabled: true, EVE: true, LISIRD: true}, ts.URL, lisirdNow)

	// Ten one-minute polls. Only the first should touch LISIRD.
	for i := range 10 {
		s.now = func() time.Time { return lisirdNow.Add(time.Duration(i) * time.Minute) }
		if _, err := s.Poll(context.Background()); err != nil {
			t.Fatalf("poll %d: %v", i, err)
		}
	}

	// Seven datasets on the first tick and nothing after.
	if n := lisirdHits.Load(); n != int64(len(defaultDatasets)) {
		t.Errorf("LISIRD was fetched %d times over ten one-minute polls, want %d (one round)",
			n, len(defaultDatasets))
	}

	// And after three hours of simulated time it fires again.
	s.now = func() time.Time { return lisirdNow.Add(lisirdInterval + time.Minute) }
	if _, err := s.Poll(context.Background()); err != nil {
		t.Fatalf("Poll after the interval: %v", err)
	}
	if n := lisirdHits.Load(); n != int64(2*len(defaultDatasets)) {
		t.Errorf("LISIRD was fetched %d times, want %d after the interval elapsed",
			n, 2*len(defaultDatasets))
	}
}

func TestAFailedLISIRDFetchIsRetriedOnTheNextTickRatherThanInThreeHours(t *testing.T) {
	var fail atomic.Bool
	fail.Store(true)
	var hits atomic.Int64

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if fail.Load() {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		name := strings.TrimSuffix(filepath.Base(r.URL.Path), ".csv")
		body, err := os.ReadFile(filepath.Join("testdata", "lisird_"+name+".csv"))
		if err != nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_, _ = w.Write(body)
	}))
	t.Cleanup(ts.Close)

	s := newTestSource(t, config.LASP{Enabled: true, LISIRD: true, Retries: -1}, ts.URL, lisirdNow)
	if _, err := s.Poll(context.Background()); err == nil {
		t.Fatal("wanted an error when every dataset failed, got nil")
	}
	before := hits.Load()

	fail.Store(false)
	if _, err := s.Poll(context.Background()); err != nil {
		t.Fatalf("second Poll: %v", err)
	}
	if hits.Load() <= before {
		t.Error("a failed round was not retried on the next tick")
	}
}

// ---------------------------------------------------------------------------
// Partial failure
// ---------------------------------------------------------------------------

func TestOneFailingDatasetStillReturnsTheOthersSamples(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimSuffix(filepath.Base(r.URL.Path), ".csv")
		if name == "bremen_composite_mgii" {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		body, err := os.ReadFile(filepath.Join("testdata", "lisird_"+name+".csv"))
		if err != nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_, _ = w.Write(body)
	}))
	t.Cleanup(ts.Close)

	s := newTestSource(t, config.LASP{Enabled: true, LISIRD: true, Retries: -1}, ts.URL, lisirdNow)
	batch, err := s.Poll(context.Background())
	if err != nil {
		t.Fatalf("one failing dataset should not fail the poll, got: %v", err)
	}
	if n := countFor(batch, metric.MgIIIndex); n != 0 {
		t.Errorf("got %d Mg II samples from a failing dataset, want 0", n)
	}
	sampleFor(t, batch, metric.TotalSolarIrradiance)
}

func TestEveryDatasetFailingReturnsAnError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(ts.Close)

	s := newTestSource(t, config.LASP{Enabled: true, LISIRD: true, Retries: -1}, ts.URL, lisirdNow)
	if _, err := s.Poll(context.Background()); err == nil {
		t.Fatal("wanted an error when every dataset failed, got nil")
	}
}

func TestEVESamplesSurviveATotalLISIRDFailure(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/lisird/") {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusPartialContent)
		if strings.HasPrefix(r.Header.Get("Range"), "bytes=0-") {
			_, _ = w.Write(fixture(t, "eve_l0cs_head.txt"))
			return
		}
		_, _ = w.Write(fixture(t, "eve_l0cs_tail.txt"))
	}))
	t.Cleanup(ts.Close)

	s := newTestSource(t, config.LASP{Enabled: true, EVE: true, LISIRD: true, Retries: -1}, ts.URL, eveWantAt)
	batch, err := s.Poll(context.Background())
	if err != nil {
		t.Fatalf("a total LISIRD failure should not lose the minute-cadence EUV data, got: %v", err)
	}
	sampleFor(t, batch, metric.EUVSourceLatitude)
}

// ---------------------------------------------------------------------------
// HTTP behaviour
// ---------------------------------------------------------------------------

func TestALISIRD404IsNotRetried(t *testing.T) {
	var requests atomic.Int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(ts.Close)

	s := newTestSource(t, config.LASP{Enabled: true, LISIRD: true, Retries: 5}, ts.URL, lisirdNow)
	if _, err := s.Poll(context.Background()); err == nil {
		t.Fatal("wanted an error from a 404, got nil")
	}
	if n := requests.Load(); n != int64(len(defaultDatasets)) {
		t.Errorf("got %d requests, want %d — one per dataset with no retries on a 404",
			n, len(defaultDatasets))
	}
}

func TestALISIRD429ReturnsErrRateLimitedAndStopsTheRound(t *testing.T) {
	var requests atomic.Int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("Retry-After", "300")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	t.Cleanup(ts.Close)

	s := newTestSource(t, config.LASP{Enabled: true, LISIRD: true}, ts.URL, lisirdNow)
	batch, err := s.Poll(context.Background())
	if !errors.Is(err, source.ErrRateLimited) {
		t.Errorf("Poll returned %v, want it to wrap source.ErrRateLimited", err)
	}
	if len(batch.Samples) != 0 {
		t.Errorf("a 429 produced samples: %s", describe(batch))
	}
	if n := requests.Load(); n != 1 {
		t.Errorf("made %d requests after a 429; the round should stop at the first one", n)
	}
}

func TestALISIRDNotModifiedContributesNoSamplesAndNoAge(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotModified)
	}))
	t.Cleanup(ts.Close)

	s := newTestSource(t, config.LASP{Enabled: true, LISIRD: true}, ts.URL, lisirdNow)
	batch, err := s.Poll(context.Background())
	if err != nil {
		t.Fatalf("every dataset answering 304 is not a failure, got: %v", err)
	}
	if len(batch.Samples) != 0 {
		t.Errorf("a 304 produced samples: %s", describe(batch))
	}
}

func TestLISIRDPollHonoursContextCancellation(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	t.Cleanup(ts.Close)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	s := newTestSource(t, lisirdConfig(), ts.URL, lisirdNow)
	if _, err := s.Poll(ctx); err == nil {
		t.Fatal("wanted an error from a cancelled context, got nil")
	}
}

// ---------------------------------------------------------------------------
// Malformed input
// ---------------------------------------------------------------------------

func TestMalformedLISIRDInputReturnsAnErrorRatherThanPanicking(t *testing.T) {
	cases := map[string]string{
		"a header with no rows":        "time (Julian Date),v\n",
		"a header with no time column": "date (Julian Date),v\n2461285.5,1.0\n",
		"an unnamed column":            "(Julian Date),v\n2461285.5,1.0\n",
		"HTML from an error page":      "<html><body>Internal Server Error</body></html>",
		"unbalanced CSV quoting":       "time (Julian Date),v\n\"2461285.5,1.0\n",
		"a row of only commas":         "time (Julian Date),v\n,,\n",
		"no readable time in any row":  "time (Julian Date),v\nnope,1.0\nalso-nope,2.0\n",
	}

	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write([]byte(body))
			}))
			t.Cleanup(ts.Close)

			s := newTestSource(t, config.LASP{Enabled: true, LISIRD: true, Retries: -1}, ts.URL, lisirdNow)
			batch, err := s.Poll(context.Background())
			if err == nil {
				t.Fatalf("wanted an error, got %s", describe(batch))
			}
			if len(batch.Samples) != 0 {
				t.Errorf("a malformed body produced samples: %s", describe(batch))
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Data age
// ---------------------------------------------------------------------------

func TestLISIRDSourceDataAgeComesFromTheNewestPayloadTimestamp(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Fresh Last-Modified over days-old data.
		w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
		name := strings.TrimSuffix(filepath.Base(r.URL.Path), ".csv")
		body, err := os.ReadFile(filepath.Join("testdata", "lisird_"+name+".csv"))
		if err != nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_, _ = w.Write(body)
	}))
	t.Cleanup(ts.Close)

	s := newTestSource(t, lisirdConfig(), ts.URL, lisirdNow)
	batch, err := s.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}

	// The newest timestamp across the default datasets is the CLS series'
	// 2026-09-08, which is ahead of tsis_tsi_6hr (2026-09-04 21:00),
	// bremen_composite_mgii (2026-09-07) and composite_lyman_alpha
	// (2026-09-06 12:00).
	newest := time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)
	age := sampleFor(t, batch, metric.SourceDataAge, Name)
	if want := lisirdNow.Sub(newest).Seconds(); age.Value != want {
		t.Errorf("age is %g seconds, want %g from the newest payload timestamp", age.Value, want)
	}
}
