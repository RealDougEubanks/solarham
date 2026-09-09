package silso

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
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

// referenceNow is the instant every test pretends it is. The fixtures carry
// fixed dates and metric.SourceDataAge is computed against them.
var referenceNow = time.Date(2026, 9, 9, 18, 0, 0, 0, time.UTC)

// pathDaily is the 2.9 MB file this source must never request. It is written
// out here rather than imported, because the point of the test that uses it is
// that the production code contains no reference to it at all.
const pathDaily = "/SILSO/DATA/SN_d_tot_V2.0.csv"

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// capturingLogger records every log record so a test can assert the citation
// reached the log, which is this package's licence obligation.
type capturingLogger struct {
	mu      sync.Mutex
	records []slog.Record
}

func (c *capturingLogger) Enabled(context.Context, slog.Level) bool { return true }

func (c *capturingLogger) Handle(_ context.Context, r slog.Record) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.records = append(c.records, r.Clone())
	return nil
}

func (c *capturingLogger) WithAttrs([]slog.Attr) slog.Handler { return c }
func (c *capturingLogger) WithGroup(string) slog.Handler      { return c }

func (c *capturingLogger) attr(substr, key string) (string, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var (
		value string
		count int
	)
	for _, r := range c.records {
		if !strings.Contains(r.Message, substr) {
			continue
		}
		count++
		if count == 1 {
			r.Attrs(func(a slog.Attr) bool {
				if a.Key == key {
					value = a.Value.String()
					return false
				}
				return true
			})
		}
	}
	return value, count
}

// testClient is the shared httpx client with the politeness policy relaxed;
// the limiter has its own tests in httpx.
func testClient(t *testing.T) *httpx.Client {
	t.Helper()
	return httpx.New(httpx.Options{
		Timeout:  5 * time.Second,
		Fallback: httpx.HostPolicy{MinInterval: time.Nanosecond, Burst: 1000},
	}, testLogger())
}

// recordingServer serves the SILSO paths and records every path requested,
// including ones this source must never ask for.
type recordingServer struct {
	*httptest.Server
	hits atomic.Int64

	mu    sync.Mutex
	paths []string
}

// newServer routes each path to its handler. A path with no handler is
// answered 404 and still recorded, which is what makes the
// "never fetch the daily file" assertion meaningful.
func newServer(t *testing.T, handlers map[string]http.HandlerFunc) *recordingServer {
	t.Helper()
	rs := &recordingServer{}
	rs.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rs.hits.Add(1)
		rs.mu.Lock()
		rs.paths = append(rs.paths, r.URL.Path)
		rs.mu.Unlock()

		h, ok := handlers[r.URL.Path]
		if !ok {
			http.Error(w, "no handler for "+r.URL.Path, http.StatusNotFound)
			return
		}
		h(w, r)
	}))
	t.Cleanup(rs.Close)
	return rs
}

func (rs *recordingServer) requested() []string {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	out := make([]string, len(rs.paths))
	copy(out, rs.paths)
	return out
}

func (rs *recordingServer) countOf(path string) int {
	n := 0
	for _, p := range rs.requested() {
		if p == path {
			n++
		}
	}
	return n
}

func fixture(t *testing.T, name string) []byte {
	t.Helper()
	body, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatalf("reading fixture %s: %v", name, err)
	}
	return body
}

func serveBody(body []byte) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/csv")
		_, _ = w.Write(body)
	}
}

// bothFiles is the handler set for a healthy upstream.
func bothFiles(t *testing.T) map[string]http.HandlerFunc {
	t.Helper()
	return map[string]http.HandlerFunc{
		pathEISN:     serveBody(fixture(t, "EISN_current.csv")),
		pathSmoothed: serveBody(fixture(t, "SN_ms_tot_V2.0.csv")),
	}
}

func newSourceWithLog(t *testing.T, base string, log *slog.Logger) *Source {
	t.Helper()
	src, err := New(config.SILSO{BaseURL: base}, testClient(t), log)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	src.now = func() time.Time { return referenceNow }
	return src
}

func newSource(t *testing.T, base string) *Source {
	t.Helper()
	return newSourceWithLog(t, base, testLogger())
}

func sampleFor(t *testing.T, batch metric.Batch, desc *metric.Descriptor, labels ...string) metric.Sample {
	t.Helper()
	var found []metric.Sample
	for _, s := range batch.Samples {
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
			found = append(found, s)
		}
	}
	if len(found) != 1 {
		t.Fatalf("expected exactly one %s%v sample, found %d", desc.FullName(), labels, len(found))
	}
	return found[0]
}

func hasSample(batch metric.Batch, desc *metric.Descriptor) bool {
	for _, s := range batch.Samples {
		if s.Desc == desc {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// The file that must not be fetched
// ---------------------------------------------------------------------------

// TestTheTwoPointNineMegabyteDailyFileIsNeverRequested.
//
// SN_d_tot_V2.0.csv holds every daily sunspot number since 1818. Fetching it to
// read one line would be a rude way to learn a number that is already in a
// 414-byte file. The server records every path asked for, including ones it has
// no handler for, so an accidental request shows up here rather than as a
// silent 404 in production.
func TestTheTwoPointNineMegabyteDailyFileIsNeverRequested(t *testing.T) {
	ts := newServer(t, bothFiles(t))
	src := newSource(t, ts.URL)

	// Several polls, and enough simulated time for the daily gate to open, so
	// that every code path that can issue a request has done so.
	now := referenceNow
	src.now = func() time.Time { return now }
	for range 4 {
		if _, err := src.Poll(context.Background()); err != nil {
			t.Fatalf("Poll: %v", err)
		}
		now = now.Add(25 * time.Hour)
	}

	requested := src2set(ts.requested())
	for _, forbidden := range []string{
		pathDaily,
		"/SILSO/DATA/SN_d_tot_V2.0.csv",
		"/SILSO/DATA/SN_m_tot_V2.0.csv",
		"/SILSO/FORECASTS/KFprediMonthly.txt",
	} {
		if _, asked := requested[forbidden]; asked {
			t.Errorf("this source requested %s; it must not", forbidden)
		}
	}

	// And the only two paths it may touch.
	for path := range requested {
		if path != pathEISN && path != pathSmoothed {
			t.Errorf("this source requested an unexpected path %s", path)
		}
	}
}

func src2set(paths []string) map[string]struct{} {
	out := make(map[string]struct{}, len(paths))
	for _, p := range paths {
		out[p] = struct{}{}
	}
	return out
}

// ---------------------------------------------------------------------------
// The two schedules
// ---------------------------------------------------------------------------

// TestTheSourceSchedulesEverySixHoursForTheCurrentMonthEstimate. Four polls a
// day catches the morning and evening revisions of a file that is rewritten as
// stations report, without re-fetching 414 unchanged bytes on a fast loop.
func TestTheSourceSchedulesEverySixHoursForTheCurrentMonthEstimate(t *testing.T) {
	src := newSource(t, "https://example.invalid")

	if got := src.Interval(); got != eisnInterval {
		t.Errorf("Interval() = %s, want %s", got, eisnInterval)
	}
	next := src.Schedule().NextAfter(referenceNow)
	if gap := next.Sub(referenceNow); gap != eisnInterval {
		t.Errorf("the next poll is %s away, want %s", gap, eisnInterval)
	}
}

// TestTheSmoothedSeriesIsFetchedAtMostOnceADay, on a different clock from the
// estimate. It is regenerated monthly, and 126 KB every six hours to learn
// nothing is the waste this gate exists to prevent.
func TestTheSmoothedSeriesIsFetchedAtMostOnceADay(t *testing.T) {
	ts := newServer(t, bothFiles(t))
	src := newSource(t, ts.URL)

	now := referenceNow
	src.now = func() time.Time { return now }

	// Four polls six hours apart: one simulated day.
	for range 4 {
		if _, err := src.Poll(context.Background()); err != nil {
			t.Fatalf("Poll: %v", err)
		}
		now = now.Add(eisnInterval)
	}

	if got := ts.countOf(pathEISN); got != 4 {
		t.Errorf("the estimate was fetched %d times in four polls, want 4", got)
	}
	if got := ts.countOf(pathSmoothed); got != 1 {
		t.Errorf("the 126 KB monthly series was fetched %d times in a simulated day, want 1", got)
	}

	// One more poll, now more than a day past the first.
	now = now.Add(time.Hour)
	if _, err := src.Poll(context.Background()); err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if got := ts.countOf(pathSmoothed); got != 2 {
		t.Errorf("the monthly series was fetched %d times after a day elapsed, want 2", got)
	}
}

// TestTheSmoothedGateIsMarkedEvenWhenTheFetchAnswers304, so that a 304 is not
// itself repeated every six hours.
func TestTheSmoothedGateIsMarkedEvenWhenTheFetchAnswers304(t *testing.T) {
	handlers := bothFiles(t)
	handlers[pathSmoothed] = func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotModified)
	}
	ts := newServer(t, handlers)
	src := newSource(t, ts.URL)

	now := referenceNow
	src.now = func() time.Time { return now }
	for range 3 {
		if _, err := src.Poll(context.Background()); err != nil {
			t.Fatalf("Poll: %v", err)
		}
		now = now.Add(eisnInterval)
	}
	if got := ts.countOf(pathSmoothed); got != 1 {
		t.Errorf("a 304 on the monthly series was re-requested %d times, want 1", got)
	}
}

// TestSmoothedIntervalIsADay documents the gate as a value rather than only as
// behaviour.
func TestSmoothedIntervalIsADay(t *testing.T) {
	src := newSource(t, "https://example.invalid")
	if got := src.SmoothedInterval(); got != 24*time.Hour {
		t.Errorf("SmoothedInterval() = %s, want 24h", got)
	}
}

// ---------------------------------------------------------------------------
// The -1 sentinel
// ---------------------------------------------------------------------------

// TestTheMinusOneSentinelProducesNoSample is the substance of this package.
//
// Both files spell a missing value as -1, and the smoothed series ends with
// several months of -1.0 because a 13-month smoothing cannot be computed until
// six months after the fact. Publishing that raw puts a sunspot number of
// minus one on a dashboard.
func TestTheMinusOneSentinelProducesNoSample(t *testing.T) {
	eisn := "2026, 09, 08, 2026.686, 101,  10.0,  20,  28,\n" +
		"2026, 09, 09, 2026.689,  -1,  -1.0,  -1,  -1,\n"
	handlers := bothFiles(t)
	handlers[pathEISN] = serveBody([]byte(eisn))
	ts := newServer(t, handlers)
	src := newSource(t, ts.URL)

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}

	// The sentinel row is skipped entirely and the previous day published.
	if got := sampleFor(t, batch, metric.SunspotNumberEISN).Value; got != 101 {
		t.Errorf("estimated sunspot number = %v, want the 8th's 101 rather than the sentinel row", got)
	}
	for _, s := range batch.Samples {
		if s.Value < 0 {
			t.Errorf("a negative value reached the batch: %s%v = %v",
				s.Desc.FullName(), s.Labels, s.Value)
		}
	}
	want := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	if got := sampleFor(t, batch, metric.SunspotNumberEISN).Time; !got.Equal(want) {
		t.Errorf("sample time = %s, want %s", got, want)
	}
}

// TestASentinelInOneColumnOnlyDropsThatColumn. A day whose sunspot number is
// real but whose station count is missing should still publish the number.
func TestASentinelInOneColumnOnlyDropsThatColumn(t *testing.T) {
	eisn := "2026, 09, 09, 2026.689, 106,  -1.0,  17,  -1,\n"
	handlers := bothFiles(t)
	handlers[pathEISN] = serveBody([]byte(eisn))
	ts := newServer(t, handlers)
	src := newSource(t, ts.URL)

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if got := sampleFor(t, batch, metric.SunspotNumberEISN).Value; got != 106 {
		t.Errorf("estimated sunspot number = %v, want 106", got)
	}
	if hasSample(batch, metric.SunspotNumberEISNDeviation) {
		t.Errorf("a sentinel standard deviation was published")
	}
	if got := sampleFor(t, batch, metric.SunspotStationCount, stateCalculated).Value; got != 17 {
		t.Errorf("calculated station count = %v, want 17", got)
	}
	for _, s := range batch.Samples {
		if s.Desc == metric.SunspotStationCount {
			if state, _ := s.LabelFor("state"); state == stateTotal {
				t.Errorf("a sentinel total station count was published as %v", s.Value)
			}
		}
	}
}

// TestAZeroSunspotNumberIsPublished. A spotless day is a real observation and a
// perfectly ordinary one at solar minimum; the sentinel filter must not eat it.
func TestAZeroSunspotNumberIsPublished(t *testing.T) {
	eisn := "2026, 09, 09, 2026.689,   0,   0.0,  17,  19,\n"
	handlers := bothFiles(t)
	handlers[pathEISN] = serveBody([]byte(eisn))
	ts := newServer(t, handlers)
	src := newSource(t, ts.URL)

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	s := sampleFor(t, batch, metric.SunspotNumberEISN)
	if s.Value != 0 {
		t.Errorf("estimated sunspot number = %v, want 0 for a spotless day", s.Value)
	}
}

// TestTheSentinelFilterAcceptsEverySpellingBothFilesUse.
func TestTheSentinelFilterAcceptsEverySpellingBothFilesUse(t *testing.T) {
	for _, field := range []string{"-1", "-1.0", "-1.00", " -1 ", "-1.000"} {
		if _, ok := usable(field); ok {
			t.Errorf("usable(%q) treated the sentinel as a measurement", field)
		}
	}
	for _, field := range []string{"0", "0.0", "106", " 11.1 ", "99.8"} {
		if _, ok := usable(field); !ok {
			t.Errorf("usable(%q) rejected a real measurement", field)
		}
	}
	for _, field := range []string{"", "null", "n/a", "--"} {
		if _, ok := usable(field); ok {
			t.Errorf("usable(%q) parsed a non-number as a measurement", field)
		}
	}
}

// ---------------------------------------------------------------------------
// EISN parsing
// ---------------------------------------------------------------------------

// TestTheCapturedEISNFileIsParsedColumnByColumn.
//
// The last row of testdata/EISN_current.csv:
//
//	2026, 09, 09, 2026.689, 106,  11.1,  17,  19,
//
// year, month, day, decimal year, sunspot number, standard deviation,
// calculated station count, total station count, and a trailing comma.
func TestTheCapturedEISNFileIsParsedColumnByColumn(t *testing.T) {
	ts := newServer(t, bothFiles(t))
	src := newSource(t, ts.URL)

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if batch.Source != Name {
		t.Errorf("batch source = %q, want %q", batch.Source, Name)
	}

	if got := sampleFor(t, batch, metric.SunspotNumberEISN).Value; got != 106 {
		t.Errorf("estimated sunspot number = %v, want 106", got)
	}
	if got := sampleFor(t, batch, metric.SunspotNumberEISNDeviation).Value; got != 11.1 {
		t.Errorf("standard deviation = %v, want 11.1", got)
	}
	if got := sampleFor(t, batch, metric.SunspotStationCount, stateCalculated).Value; got != 17 {
		t.Errorf("calculated station count = %v, want 17", got)
	}
	if got := sampleFor(t, batch, metric.SunspotStationCount, stateTotal).Value; got != 19 {
		t.Errorf("total station count = %v, want 19", got)
	}

	// The newest row wins, not the first.
	want := time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)
	if got := sampleFor(t, batch, metric.SunspotNumberEISN).Time; !got.Equal(want) {
		t.Errorf("sample time = %s, want the newest row's date at midday %s", got, want)
	}

	for _, s := range batch.Samples {
		if err := s.Validate(); err != nil {
			t.Errorf("Poll emitted an invalid sample: %v", err)
		}
	}
}

// TestTheStationCountsAreTwoLabelledSeriesRatherThanTwoMetrics, so that the
// calculated-to-total ratio is a single division in any query language.
func TestTheStationCountsAreTwoLabelledSeriesRatherThanTwoMetrics(t *testing.T) {
	ts := newServer(t, bothFiles(t))
	src := newSource(t, ts.URL)

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}

	states := map[string]bool{}
	for _, s := range batch.Samples {
		if s.Desc != metric.SunspotStationCount {
			continue
		}
		state, ok := s.LabelFor("state")
		if !ok {
			t.Errorf("a station-count sample has no state label")
			continue
		}
		if states[state] {
			t.Errorf("state %q appeared twice", state)
		}
		states[state] = true
	}
	if len(states) != 2 || !states[stateCalculated] || !states[stateTotal] {
		t.Errorf("station-count states = %v, want exactly calculated and total", states)
	}
}

// TestSourceDataAgeComesFromTheEstimatesOwnDate. The granularity is a day, so
// this reads in hours rather than minutes; that is inherent to a daily value
// and is documented rather than fudged.
func TestSourceDataAgeComesFromTheEstimatesOwnDate(t *testing.T) {
	ts := newServer(t, bothFiles(t))
	src := newSource(t, ts.URL)

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	// referenceNow is 2026-09-09 18:00; the newest row is the 9th, stamped at
	// midday.
	if got := sampleFor(t, batch, metric.SourceDataAge, Name).Value; got != 6*3600 {
		t.Errorf("source data age = %v s, want %v", got, 6*3600)
	}
}

// TestAMonthWithNoComputedEstimateYetIsNotAFailure. On the first day or two of
// a month, before any station has reported, every row is a sentinel.
func TestAMonthWithNoComputedEstimateYetIsNotAFailure(t *testing.T) {
	handlers := bothFiles(t)
	handlers[pathEISN] = serveBody([]byte("2026, 10, 01, 2026.750,  -1,  -1.0,  -1,  -1,\n"))
	ts := newServer(t, handlers)
	src := newSource(t, ts.URL)

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if hasSample(batch, metric.SunspotNumberEISN) {
		t.Errorf("a sentinel-only month published an estimate")
	}
	// The smoothed series is still there, so the poll is not empty.
	if !hasSample(batch, metric.SunspotNumberSmoothed) {
		t.Errorf("the smoothed series was lost along with the empty estimate")
	}
}

// ---------------------------------------------------------------------------
// Smoothed series parsing
// ---------------------------------------------------------------------------

// TestTheCapturedSmoothedSeriesYieldsItsNewestRealValue.
//
// The file is semicolon-separated and its last several rows are -1.0, because
// a 13-month smoothing cannot be computed until six months after the fact. The
// newest real value in testdata/SN_ms_tot_V2.0.csv is:
//
//	2026;02;2026.122;  99.8; 15.7;  870;0
func TestTheCapturedSmoothedSeriesYieldsItsNewestRealValue(t *testing.T) {
	ts := newServer(t, bothFiles(t))
	src := newSource(t, ts.URL)

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}

	s := sampleFor(t, batch, metric.SunspotNumberSmoothed)
	if s.Value != 99.8 {
		t.Errorf("smoothed sunspot number = %v, want 99.8", s.Value)
	}
	// Mid-month: the value is centred on the month, and dating it to the first
	// would misplace it by a fortnight on an eleven-year curve.
	want := time.Date(2026, 2, 15, 12, 0, 0, 0, time.UTC)
	if !s.Time.Equal(want) {
		t.Errorf("smoothed sample time = %s, want mid-February %s", s.Time, want)
	}
}

// TestTheSmoothedSeriesDoesNotFeedTheDataAgeSeries. Its newest real value is
// inherently six or seven months old, so treating that as staleness would make
// the age series useless.
func TestTheSmoothedSeriesDoesNotFeedTheDataAgeSeries(t *testing.T) {
	ts := newServer(t, bothFiles(t))
	src := newSource(t, ts.URL)

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	age := sampleFor(t, batch, metric.SourceDataAge, Name).Value
	if age > 48*3600 {
		t.Errorf("source data age = %v s, which means the months-old smoothed value fed it", age)
	}
}

// TestASmoothedSeriesOfNothingButSentinelsIsAFailure, because that is what a
// truncated download or the wrong URL looks like, and the real file goes back
// to 1749.
func TestASmoothedSeriesOfNothingButSentinelsIsAFailure(t *testing.T) {
	handlers := bothFiles(t)
	handlers[pathSmoothed] = serveBody([]byte(
		"2026;07;2026.538;  -1.0; -1.0; 1371;0\n2026;08;2026.623;  -1.0; -1.0; 1248;0\n"))
	ts := newServer(t, handlers)
	src := newSource(t, ts.URL)

	// The estimate still succeeds, so the poll succeeds and the smoothed
	// failure is logged rather than returned.
	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if hasSample(batch, metric.SunspotNumberSmoothed) {
		t.Errorf("a sentinel-only smoothed series published a value")
	}
	if !hasSample(batch, metric.SunspotNumberEISN) {
		t.Errorf("the estimate was lost along with the broken smoothed series")
	}
}

// ---------------------------------------------------------------------------
// Malformed input
// ---------------------------------------------------------------------------

// TestAMalformedBodyErrorsRatherThanPanicking.
func TestAMalformedBodyErrorsRatherThanPanicking(t *testing.T) {
	for name, body := range map[string]string{
		"empty":              "",
		"html error page":    "<html><body>404</body></html>",
		"json":               `{"error":"gone"}`,
		"header only":        "# year, month, day\n",
		"truncated row":      "2026, 09\n",
		"nul bytes":          "\x00\x00\x00",
		"non-numeric fields": "abcd, ef, gh, ij, kl, mn, op, qr,\n",
	} {
		t.Run(name, func(t *testing.T) {
			handlers := bothFiles(t)
			handlers[pathEISN] = serveBody([]byte(body))
			ts := newServer(t, handlers)
			src := newSource(t, ts.URL)

			// The estimate failing is logged, not returned, as long as the
			// smoothed series succeeded — but it must never panic and must
			// never publish a value.
			batch, err := src.Poll(context.Background())
			if err == nil && hasSample(batch, metric.SunspotNumberEISN) {
				t.Errorf("a malformed EISN body produced an estimate sample")
			}
		})
	}
}

// TestABodyWithAMonthOutOfRangeIsRejected. Column shifts are the failure mode
// of a delimiter change, and a month of 689 is what one looks like.
func TestABodyWithAMonthOutOfRangeIsRejected(t *testing.T) {
	handlers := bothFiles(t)
	handlers[pathEISN] = serveBody([]byte("2026, 99, 09, 2026.689, 106,  11.1,  17,  19,\n"))
	ts := newServer(t, handlers)
	src := newSource(t, ts.URL)

	batch, err := src.Poll(context.Background())
	if err == nil && hasSample(batch, metric.SunspotNumberEISN) {
		t.Errorf("a row with month 99 produced an estimate sample")
	}
}

// TestAMiddleEmptyFieldDoesNotShiftTheColumns. Dropping an empty field in the
// middle of a row would publish the station count as the sunspot number.
func TestAMiddleEmptyFieldDoesNotShiftTheColumns(t *testing.T) {
	fields := splitFields("2026, 09, 09, 2026.689, , 11.1, 17, 19,")
	if len(fields) != 8 {
		t.Fatalf("splitFields returned %d fields %v, want 8 with the empty one kept", len(fields), fields)
	}
	if fields[4] != "" {
		t.Errorf("field 4 = %q, want the empty field preserved in place", fields[4])
	}
	if fields[5] != "11.1" {
		t.Errorf("field 5 = %q, want 11.1; the columns shifted", fields[5])
	}
}

// TestTheTrailingCommaDoesNotBecomeAField.
func TestTheTrailingCommaDoesNotBecomeAField(t *testing.T) {
	fields := splitFields("2026, 09, 09, 2026.689, 106, 11.1, 17, 19,")
	if len(fields) != 8 {
		t.Errorf("splitFields returned %d fields %v, want 8", len(fields), fields)
	}
}

// ---------------------------------------------------------------------------
// Transport behaviour
// ---------------------------------------------------------------------------

// TestANotFoundIsNotRetried.
func TestANotFoundIsNotRetried(t *testing.T) {
	ts := newServer(t, map[string]http.HandlerFunc{})
	src := newSource(t, ts.URL)

	if _, err := src.Poll(context.Background()); err == nil {
		t.Fatalf("Poll succeeded with both files 404")
	}
	// Two paths, one attempt each. Retries default to two, so a retried 404
	// would show four or six hits.
	if got := ts.hits.Load(); got != 2 {
		t.Errorf("%d requests were made, want 2 (one per file, no retries on a 404)", got)
	}
}

// TestATooManyRequestsIsReportedAsRateLimited.
func TestATooManyRequestsIsReportedAsRateLimited(t *testing.T) {
	limited := func(w http.ResponseWriter, _ *http.Request) {
		// A short Retry-After keeps the test quick. Without one, httpx
		// applies a five-minute host cooldown — which is the right production
		// behaviour, and is exactly what the second fetch of this poll would
		// then sit and wait for.
		w.Header().Set("Retry-After", "1")
		http.Error(w, "slow down", http.StatusTooManyRequests)
	}
	ts := newServer(t, map[string]http.HandlerFunc{
		pathEISN:     limited,
		pathSmoothed: limited,
	})
	src := newSource(t, ts.URL)

	_, err := src.Poll(context.Background())
	if !errors.Is(err, source.ErrRateLimited) {
		t.Errorf("error = %v, want source.ErrRateLimited", err)
	}
}

// TestBothFilesAnswering304IsNotModifiedRatherThanAFailure, so the scheduler
// logs "unchanged" instead of "no samples". This is the ordinary case for this
// source: the estimate changes a few times a day and the series once a month.
func TestBothFilesAnswering304IsNotModifiedRatherThanAFailure(t *testing.T) {
	notModified := func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotModified)
	}
	ts := newServer(t, map[string]http.HandlerFunc{
		pathEISN:     notModified,
		pathSmoothed: notModified,
	})
	src := newSource(t, ts.URL)

	_, err := src.Poll(context.Background())
	if !errors.Is(err, source.ErrNotModified) {
		t.Errorf("error = %v, want source.ErrNotModified", err)
	}
}

// TestOneFileFailingDoesNotLoseTheOther.
func TestOneFileFailingDoesNotLoseTheOther(t *testing.T) {
	handlers := bothFiles(t)
	handlers[pathSmoothed] = func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}
	ts := newServer(t, handlers)
	src := newSource(t, ts.URL)

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if !hasSample(batch, metric.SunspotNumberEISN) {
		t.Errorf("the estimate was lost when the smoothed series failed")
	}
	if hasSample(batch, metric.SunspotNumberSmoothed) {
		t.Errorf("the failing smoothed series produced a sample")
	}
}

// TestACancelledContextAbortsThePollWithoutRequesting.
func TestACancelledContextAbortsThePollWithoutRequesting(t *testing.T) {
	ts := newServer(t, bothFiles(t))
	src := newSource(t, ts.URL)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := src.Poll(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("error = %v, want context.Canceled", err)
	}
	if got := ts.hits.Load(); got != 0 {
		t.Errorf("a cancelled poll still made %d requests", got)
	}
}

// TestAContextCancelledMidFlightAbortsThePoll.
func TestAContextCancelledMidFlightAbortsThePoll(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	ts := newServer(t, map[string]http.HandlerFunc{
		pathEISN: func(w http.ResponseWriter, r *http.Request) {
			cancel()
			<-r.Context().Done()
		},
	})
	src := newSource(t, ts.URL)

	if _, err := src.Poll(ctx); err == nil {
		t.Errorf("Poll succeeded against a cancelled request")
	}
}

// ---------------------------------------------------------------------------
// The citation
// ---------------------------------------------------------------------------

// TestTheRequiredCitationIsLoggedOnce. CC BY-NC obliges an operator to
// reproduce it, and a citation buried in a source comment is not much use to
// somebody who enabled this from an environment variable.
func TestTheRequiredCitationIsLoggedOnce(t *testing.T) {
	logs := &capturingLogger{}
	ts := newServer(t, bothFiles(t))
	src := newSourceWithLog(t, ts.URL, slog.New(logs))

	for range 3 {
		if _, err := src.Poll(context.Background()); err != nil {
			t.Fatalf("Poll: %v", err)
		}
	}

	citation, count := logs.attr("citation", "citation")
	if count != 1 {
		t.Errorf("the citation was logged %d times across three polls, want 1", count)
	}
	for _, want := range []string{
		"WDC-SILSO",
		"Royal Observatory of Belgium",
		"https://doi.org/10.24414/qnza-ac80",
	} {
		if !strings.Contains(citation, want) {
			t.Errorf("the logged citation does not mention %q; it was %q", want, citation)
		}
	}
}

// TestTheCitationConstantMatchesWhatSILSORequires, verbatim.
func TestTheCitationConstantMatchesWhatSILSORequires(t *testing.T) {
	const want = "Source: WDC-SILSO, Royal Observatory of Belgium, Brussels, " +
		"DOI: https://doi.org/10.24414/qnza-ac80"
	if Citation != want {
		t.Errorf("Citation = %q, want %q", Citation, want)
	}
}

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

func TestNewRequiresTheSharedClient(t *testing.T) {
	if _, err := New(config.SILSO{}, nil, testLogger()); err == nil {
		t.Errorf("New accepted a nil client; politeness has to be enforced across sources")
	}
}

func TestNewRejectsABaseURLThatIsNotHTTP(t *testing.T) {
	for _, base := range []string{"ftp://www.sidc.be", "://", "file:///etc/passwd"} {
		if _, err := New(config.SILSO{BaseURL: base}, testClient(t), testLogger()); err == nil {
			t.Errorf("New accepted base URL %q", base)
		}
	}
}

func TestAnUnsetBaseURLDefaultsToTheLiveService(t *testing.T) {
	src, err := New(config.SILSO{}, testClient(t), testLogger())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if src.base != defaultBaseURL {
		t.Errorf("base = %q, want %q", src.base, defaultBaseURL)
	}
}
