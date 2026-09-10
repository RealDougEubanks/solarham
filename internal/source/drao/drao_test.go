package drao

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
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

// discardLogger keeps test output readable. The behaviour under test is the
// samples and the errors, never the log lines.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// testClient builds an httpx client with no rate limiting, so a test that makes
// two requests does not wait for the real ten-second politeness spacing.
func testClient(t *testing.T) *httpx.Client {
	t.Helper()
	return httpx.New(httpx.Options{
		Timeout:  5 * time.Second,
		Fallback: httpx.HostPolicy{MinInterval: time.Nanosecond, Burst: 100},
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
func newTestSource(t *testing.T, cfg config.DRAO, baseURL string, now time.Time) *Source {
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

// rangeServer serves the fixture as a real suffix-range response: it records
// the Range header it was sent and answers 206 with the last N bytes, exactly
// as nginx does.
type rangeServer struct {
	mu         sync.Mutex
	lastRange  string
	rangeCount int
	fullCount  int
}

func serveWithRange(t *testing.T, body []byte) (*httptest.Server, *rangeServer) {
	t.Helper()
	rec := &rangeServer{}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hdr := r.Header.Get("Range")

		rec.mu.Lock()
		rec.lastRange = hdr
		rec.mu.Unlock()

		var n int
		if _, err := fmt.Sscanf(hdr, "bytes=-%d", &n); err != nil || n <= 0 {
			rec.mu.Lock()
			rec.fullCount++
			rec.mu.Unlock()
			w.Header().Set("Accept-Ranges", "bytes")
			_, _ = w.Write(body)
			return
		}

		rec.mu.Lock()
		rec.rangeCount++
		rec.mu.Unlock()

		if n > len(body) {
			n = len(body)
		}
		tail := body[len(body)-n:]
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", len(body)-n, len(body)-1, len(body)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(tail)
	}))
	t.Cleanup(ts.Close)
	return ts, rec
}

func (r *rangeServer) header() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lastRange
}

func (r *rangeServer) counts() (ranged, full int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.rangeCount, r.fullCount
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

// The last row of testdata/fluxtable_tail.txt, which is 8,000 bytes captured
// live from a "Range: bytes=-8000" request:
//
//	20260908    230000      2461292.447   2315.36         0111.5       0113.2       0101.9
var (
	wantAt       = time.Date(2026, 9, 8, 23, 0, 0, 0, time.UTC)
	wantObserved = 111.5
	wantAdjusted = 113.2
	wantURSI     = 101.9
)

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

func TestNewRequiresASharedHTTPClient(t *testing.T) {
	if _, err := New(config.DRAO{}, nil, discardLogger()); err == nil {
		t.Fatal("wanted an error when no httpx client was supplied, got nil")
	}
}

func TestNewRejectsABaseURLThatIsNotAbsolute(t *testing.T) {
	for _, bad := range []string{"www.spaceweather.gc.ca", "/solar_flux_data", "://nope"} {
		if _, err := New(config.DRAO{BaseURL: bad}, testClient(t), discardLogger()); err == nil {
			t.Errorf("base URL %q was accepted; wanted an error", bad)
		}
	}
}

func TestNewAppliesTheDocumentedDefaults(t *testing.T) {
	s, err := New(config.DRAO{Enabled: true}, testClient(t), discardLogger())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if s.Name() != Name || Name != "drao" {
		t.Errorf("Name is %q, want %q", s.Name(), "drao")
	}
	if s.baseURL != defaultBaseURL {
		t.Errorf("base URL is %q, want %q", s.baseURL, defaultBaseURL)
	}
	if s.retries != defaultRetries {
		t.Errorf("retries is %d, want %d", s.retries, defaultRetries)
	}
}

// ---------------------------------------------------------------------------
// Fixture parsing, with exact expected values
// ---------------------------------------------------------------------------

func TestTheCapturedRangeResponseParsesIntoTheExpectedSamples(t *testing.T) {
	ts, _ := serveWithRange(t, fixture(t, "fluxtable_tail.txt"))
	s := newTestSource(t, config.DRAO{Enabled: true}, ts.URL, wantAt.Add(time.Hour))

	batch, err := s.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}

	flux := sampleFor(t, batch, metric.FluxSFU)
	if flux.Value != wantObserved {
		t.Errorf("FluxSFU is %g, want the observed flux %g", flux.Value, wantObserved)
	}
	if !flux.Time.Equal(wantAt) {
		t.Errorf("FluxSFU is stamped %s, want the row's own measurement time %s", flux.Time, wantAt)
	}

	for _, want := range []struct {
		adjustment string
		value      float64
	}{
		{adjustmentObserved, wantObserved},
		{adjustmentAdjusted, wantAdjusted},
		{adjustmentURSI, wantURSI},
	} {
		got := sampleFor(t, batch, metric.RadioFluxMultiFrequency, wavelengthCm, want.adjustment)
		if got.Value != want.value {
			t.Errorf("the %s flux is %g, want %g", want.adjustment, got.Value, want.value)
		}
		if !got.Time.Equal(wantAt) {
			t.Errorf("the %s flux is stamped %s, want %s", want.adjustment, got.Time, wantAt)
		}
	}
}

func TestTheNewestRowWinsRatherThanTheFirstOne(t *testing.T) {
	body := strings.Join([]string{
		"20260908    170000      2461292.197   2315.36         0109.1       0110.8       0099.7",
		"20260908    200000      2461292.322   2315.36         0110.4       0112.0       0100.8",
		"20260908    230000      2461292.447   2315.36         0111.5       0113.2       0101.9",
	}, "\n") + "\n"

	row, err := newestRow(body)
	if err != nil {
		t.Fatalf("newestRow: %v", err)
	}
	if row.observed != wantObserved {
		t.Errorf("chose the row with observed flux %g, want the newest at %g", row.observed, wantObserved)
	}
	if !row.at.Equal(wantAt) {
		t.Errorf("chose the row at %s, want %s", row.at, wantAt)
	}
}

func TestTheColumnHeaderIsSkippedWhenTheWholeFileArrives(t *testing.T) {
	body := "fluxdate    fluxtime    fluxjulian    fluxcarrington  fluxobsflux  fluxadjflux  fluxursi\n" +
		"20260908    230000      2461292.447   2315.36         0111.5       0113.2       0101.9\n"

	row, err := newestRow(body)
	if err != nil {
		t.Fatalf("newestRow: %v", err)
	}
	if row.observed != wantObserved {
		t.Errorf("observed flux is %g, want %g", row.observed, wantObserved)
	}
}

func TestZeroPaddedFluxColumnsParseAsDecimals(t *testing.T) {
	// The columns are written "0099.7", not "99.7". A parser that stripped the
	// padding by hand rather than letting ParseFloat handle it would read this
	// as 997.
	row, err := parseRow("20260908    230000      2461292.447   2315.36         0099.1       0009.8       0111.5")
	if err != nil {
		t.Fatalf("parseRow: %v", err)
	}
	if row.observed != 99.1 {
		t.Errorf("observed flux is %g, want 99.1", row.observed)
	}
	if row.adjusted != 9.8 {
		t.Errorf("adjusted flux is %g, want 9.8", row.adjusted)
	}
}

// ---------------------------------------------------------------------------
// The Range request
// ---------------------------------------------------------------------------

func TestPollAsksForOnlyTheTailOfTheTable(t *testing.T) {
	ts, rec := serveWithRange(t, fixture(t, "fluxtable_tail.txt"))
	s := newTestSource(t, config.DRAO{Enabled: true}, ts.URL, wantAt.Add(time.Hour))

	if _, err := s.Poll(context.Background()); err != nil {
		t.Fatalf("Poll: %v", err)
	}

	want := fmt.Sprintf("bytes=-%d", tailBytes)
	if got := rec.header(); got != want {
		t.Errorf("Range header was %q, want %q; without it every poll downloads 2 MB", got, want)
	}
	if ranged, full := rec.counts(); ranged != 1 || full != 0 {
		t.Errorf("got %d ranged and %d full requests, want 1 and 0", ranged, full)
	}
}

func TestA206PartialContentBodyIsAcceptedRatherThanTreatedAsAnError(t *testing.T) {
	// This is the httpx contract this source depends on. httpx classifies only
	// 3xx and above as failures, so a 206 falls through to the body read. If
	// that ever changes, this test is the one that says so.
	ts, _ := serveWithRange(t, fixture(t, "fluxtable_tail.txt"))
	s := newTestSource(t, config.DRAO{Enabled: true}, ts.URL, wantAt)

	body, err := s.fetchTail(context.Background())
	if err != nil {
		t.Fatalf("a 206 response was rejected: %v", err)
	}
	if len(body) == 0 {
		t.Fatal("a 206 response returned an empty body")
	}
}

func TestATruncatedFirstLineFromARangeResponseIsDiscardedRatherThanFatal(t *testing.T) {
	// What a suffix range actually returns: the first line is a row cut
	// mid-number.
	body := "292.197   2315.36         0109.1       0110.8       0099.7\n" +
		"20260908    230000      2461292.447   2315.36         0111.5       0113.2       0101.9\n"

	row, err := newestRow(body)
	if err != nil {
		t.Fatalf("a truncated leading line should be skipped, got: %v", err)
	}
	if row.observed != wantObserved {
		t.Errorf("observed flux is %g, want %g", row.observed, wantObserved)
	}
}

func TestAServerThatIgnoresTheRangeHeaderIsStillParsedButLogged(t *testing.T) {
	var logged strings.Builder
	log := slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelDebug}))

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(fixture(t, "fluxtable_tail.txt"))
	}))
	t.Cleanup(ts.Close)

	s, err := New(config.DRAO{Enabled: true, BaseURL: ts.URL}, testClient(t), log)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	s.now = func() time.Time { return wantAt }

	if _, err := s.Poll(context.Background()); err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if !strings.Contains(logged.String(), "Range") {
		t.Error("a full response instead of a 206 was not logged; that silently costs 2 MB per poll")
	}
}

func TestAResponseLargerThanTheBodyLimitFails(t *testing.T) {
	// The whole 2 MB table arriving because the server ignored Range must fail
	// loudly rather than be quietly tolerated forever.
	big := strings.Repeat("20260908    230000      2461292.447   2315.36         0111.5       0113.2       0101.9\n", 2000)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(big))
	}))
	t.Cleanup(ts.Close)

	s := newTestSource(t, config.DRAO{Enabled: true, Retries: -1}, ts.URL, wantAt)
	if _, err := s.Poll(context.Background()); err == nil {
		t.Fatalf("wanted an error from a %d-byte body over a %d-byte limit, got nil", len(big), maxBodyBytes)
	}
}

// ---------------------------------------------------------------------------
// Schedule — three times a day, not 288
// ---------------------------------------------------------------------------

func TestTheScheduleLandsJustAfterEachOfTheThreeDailyMeasurements(t *testing.T) {
	s := newTestSource(t, config.DRAO{Enabled: true}, "https://example.invalid", time.Time{})
	sched := s.Schedule()

	// Walk a simulated day and collect every poll time.
	//
	// The window starts at noon rather than midnight because the 90-minute
	// publication lag pushes the 23:00 poll to 00:30 the following day, so a
	// midnight-to-midnight window would contain two of the three polls and one
	// from the day before. Noon to noon contains exactly one of each.
	start := time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)
	end := start.Add(24 * time.Hour)

	var polls []time.Time
	for at := start; at.Before(end); {
		next := sched.NextAfter(at)
		if !next.After(at) {
			t.Fatalf("NextAfter(%s) returned %s, which would spin the poll loop", at, next)
		}
		if !next.Before(end) {
			break
		}
		polls = append(polls, next)
		at = next
	}

	if len(polls) != 3 {
		t.Fatalf("got %d polls in a simulated UT day, want 3: %v", len(polls), polls)
	}

	// DailyAt adds up to 90 seconds of random spread on top of the lag, so each
	// poll must land in the window [measurement+lag, measurement+lag+90s).
	midnight := start.Truncate(24 * time.Hour)
	for i, want := range measurementTimes {
		base := midnight.
			Add(time.Duration(want.Hour) * time.Hour).
			Add(time.Duration(want.Minute) * time.Minute).
			Add(publicationLag)
		if !base.After(start) {
			base = base.AddDate(0, 0, 1)
		}
		got := polls[i]
		if got.Before(base) || !got.Before(base.Add(90*time.Second+time.Second)) {
			t.Errorf("poll %d is at %s, want within 90s of %s (%s UT plus the %s publication lag)",
				i, got.Format(time.RFC3339), base.Format(time.RFC3339), want, publicationLag)
		}
	}
}

func TestTheSourceIsNotPolledHourly(t *testing.T) {
	s := newTestSource(t, config.DRAO{Enabled: true}, "https://example.invalid", time.Time{})
	sched := s.Schedule()

	start := time.Date(2026, 9, 9, 0, 0, 0, 0, time.UTC)
	polls := 0
	for at := start; at.Before(start.Add(7 * 24 * time.Hour)); {
		next := sched.NextAfter(at)
		polls++
		at = next
	}

	// Three a day for a week is 21, plus at most one that steps past the end.
	if polls > 24 {
		t.Errorf("the schedule made %d polls in a simulated week; three a day is 21, and hourly would be 168", polls)
	}
	if polls < 21 {
		t.Errorf("the schedule made only %d polls in a simulated week, want at least 21", polls)
	}
}

func TestTheNominalIntervalIsTheShortestGapBetweenMeasurements(t *testing.T) {
	s := newTestSource(t, config.DRAO{Enabled: true}, "https://example.invalid", time.Time{})
	// 17:00 to 20:00 and 20:00 to 23:00 are three hours; 23:00 to the next
	// 17:00 is eighteen. Staleness should be judged against the tightest.
	if got := s.Interval(); got != 3*time.Hour {
		t.Errorf("Interval is %s, want 3h", got)
	}
}

func TestTheSourceIsScheduledRatherThanIntervalOnly(t *testing.T) {
	s := newTestSource(t, config.DRAO{Enabled: true}, "https://example.invalid", time.Time{})
	if _, ok := any(s).(source.Scheduled); !ok {
		t.Error("the source does not implement source.Scheduled")
	}
	if s.Schedule() == nil {
		t.Error("Schedule returned nil, which would make the scheduler fall back to Interval")
	}
}

// ---------------------------------------------------------------------------
// Data age
// ---------------------------------------------------------------------------

func TestSourceDataAgeIsMeasuredFromTheRowsOwnTimestamp(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// A Last-Modified of right now over a two-day-old newest row. This is
		// the whole point of SourceDataAge and it must not be believed.
		w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(fixture(t, "fluxtable_tail.txt"))
	}))
	t.Cleanup(ts.Close)

	now := wantAt.Add(26 * time.Hour)
	s := newTestSource(t, config.DRAO{Enabled: true}, ts.URL, now)

	batch, err := s.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	age := sampleFor(t, batch, metric.SourceDataAge, Name)
	if want := (26 * time.Hour).Seconds(); age.Value != want {
		t.Errorf("age is %g seconds, want %g from the row's fluxdate and fluxtime", age.Value, want)
	}
	if !age.Time.Equal(wantAt) {
		t.Errorf("the age sample is stamped %s, want the row time %s", age.Time, wantAt)
	}
}

func TestAFutureDatedRowReportsZeroAgeRatherThanANegativeOne(t *testing.T) {
	ts, _ := serveWithRange(t, fixture(t, "fluxtable_tail.txt"))
	s := newTestSource(t, config.DRAO{Enabled: true}, ts.URL, wantAt.Add(-3*time.Hour))

	batch, err := s.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if age := sampleFor(t, batch, metric.SourceDataAge, Name); age.Value != 0 {
		t.Errorf("age is %g, want 0 rather than a negative age", age.Value)
	}
}

// ---------------------------------------------------------------------------
// Sentinels and implausible values
// ---------------------------------------------------------------------------

func TestAnImplausibleFluxProducesNoSample(t *testing.T) {
	cases := map[string]string{
		"a zero observed flux":     "20260908    230000      2461292.447   2315.36         0000.0       0113.2       0101.9",
		"a negative observed flux": "20260908    230000      2461292.447   2315.36         -111.5       0113.2       0101.9",
		"an absurdly high flux":    "20260908    230000      2461292.447   2315.36         99999.9      0113.2       0101.9",
		"a zero adjusted flux":     "20260908    230000      2461292.447   2315.36         0111.5       0000.0       0101.9",
		"a zero URSI flux":         "20260908    230000      2461292.447   2315.36         0111.5       0113.2       0000.0",
	}

	for name, row := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := parseRow(row); err == nil {
				t.Errorf("%s was accepted; a flux outside %g..%g is not a measurement",
					name, minPlausibleFlux, maxPlausibleFlux)
			}
		})
	}
}

func TestARowWithAnImplausibleFluxIsSkippedInFavourOfTheOneBeforeIt(t *testing.T) {
	body := strings.Join([]string{
		"20260908    200000      2461292.322   2315.36         0110.4       0112.0       0100.8",
		"20260908    230000      2461292.447   2315.36         0000.0       0000.0       0000.0",
	}, "\n") + "\n"

	row, err := newestRow(body)
	if err != nil {
		t.Fatalf("newestRow: %v", err)
	}
	if row.observed != 110.4 {
		t.Errorf("observed flux is %g; the all-zero newest row should have been skipped, leaving 110.4", row.observed)
	}
}

// ---------------------------------------------------------------------------
// HTTP behaviour
// ---------------------------------------------------------------------------

func TestNotModifiedIsASuccessWithNoSamples(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotModified)
	}))
	t.Cleanup(ts.Close)

	s := newTestSource(t, config.DRAO{Enabled: true}, ts.URL, wantAt)
	batch, err := s.Poll(context.Background())
	if !errors.Is(err, source.ErrNotModified) {
		t.Errorf("Poll returned %v, want source.ErrNotModified", err)
	}
	if len(batch.Samples) != 0 {
		t.Errorf("a 304 produced samples: %s", describe(batch))
	}
}

func TestASecondPollSendsTheValidatorsFromTheFirst(t *testing.T) {
	var sawConditional atomic.Bool
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("If-None-Match") != "" || r.Header.Get("If-Modified-Since") != "" {
			sawConditional.Store(true)
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("ETag", `"21383c-65b0df0a03e80"`)
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(fixture(t, "fluxtable_tail.txt"))
	}))
	t.Cleanup(ts.Close)

	s := newTestSource(t, config.DRAO{Enabled: true}, ts.URL, wantAt)
	if _, err := s.Poll(context.Background()); err != nil {
		t.Fatalf("first Poll: %v", err)
	}
	if _, err := s.Poll(context.Background()); !errors.Is(err, source.ErrNotModified) {
		t.Errorf("second Poll returned %v, want source.ErrNotModified", err)
	}
	if !sawConditional.Load() {
		t.Error("the second request carried no validators, so a 304 could never be earned")
	}
}

func TestNotFoundIsNotRetried(t *testing.T) {
	var requests atomic.Int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(ts.Close)

	// The dead FTP path's https equivalent answers 404, so this is not a
	// hypothetical: it is what a misconfigured base URL does.
	s := newTestSource(t, config.DRAO{Enabled: true, Retries: 5}, ts.URL, wantAt)
	if _, err := s.Poll(context.Background()); err == nil {
		t.Fatal("wanted an error from a 404, got nil")
	}
	if n := requests.Load(); n != 1 {
		t.Errorf("a 404 was requested %d times, want 1", n)
	}
}

func TestTooManyRequestsReturnsErrRateLimited(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "120")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	t.Cleanup(ts.Close)

	s := newTestSource(t, config.DRAO{Enabled: true}, ts.URL, wantAt)
	batch, err := s.Poll(context.Background())
	if !errors.Is(err, source.ErrRateLimited) {
		t.Errorf("Poll returned %v, want it to wrap source.ErrRateLimited", err)
	}
	if len(batch.Samples) != 0 {
		t.Errorf("a 429 produced samples: %s", describe(batch))
	}
}

func TestAServerErrorIsRetriedThenSucceeds(t *testing.T) {
	var attempts atomic.Int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if attempts.Add(1) == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(fixture(t, "fluxtable_tail.txt"))
	}))
	t.Cleanup(ts.Close)

	s := newTestSource(t, config.DRAO{Enabled: true, Retries: 2}, ts.URL, wantAt)
	batch, err := s.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if n := attempts.Load(); n != 2 {
		t.Errorf("made %d attempts, want 2", n)
	}
	sampleFor(t, batch, metric.FluxSFU)
}

func TestPollHonoursContextCancellation(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	t.Cleanup(ts.Close)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	s := newTestSource(t, config.DRAO{Enabled: true}, ts.URL, wantAt)
	if _, err := s.Poll(ctx); err == nil {
		t.Fatal("wanted an error from a cancelled context, got nil")
	}
}

// ---------------------------------------------------------------------------
// Malformed input
// ---------------------------------------------------------------------------

func TestMalformedInputReturnsAnErrorRatherThanPanicking(t *testing.T) {
	cases := map[string]string{
		"an empty body":                   "",
		"the header with no rows":         "fluxdate    fluxtime    fluxjulian    fluxcarrington  fluxobsflux  fluxadjflux  fluxursi\n",
		"HTML from an error page":         "<html><head><title>404</title></head><body>Not found</body></html>",
		"too few columns":                 "20260908    230000      2461292.447   0111.5\n",
		"too many columns":                "20260908    230000      2461292.447   2315.36   0111.5   0113.2   0101.9   extra\n",
		"a non-numeric date":              "2026AB08    230000      2461292.447   2315.36         0111.5       0113.2       0101.9\n",
		"a short date":                    "260908      230000      2461292.447   2315.36         0111.5       0113.2       0101.9\n",
		"a short time":                    "20260908    2300        2461292.447   2315.36         0111.5       0113.2       0101.9\n",
		"an impossible clock time":        "20260908    990000      2461292.447   2315.36         0111.5       0113.2       0101.9\n",
		"an impossible calendar date":     "20261308    230000      2461292.447   2315.36         0111.5       0113.2       0101.9\n",
		"a non-numeric julian column":     "20260908    230000      not-a-jd      2315.36         0111.5       0113.2       0101.9\n",
		"a non-numeric carrington column": "20260908    230000      2461292.447   not-a-rot       0111.5       0113.2       0101.9\n",
		"a non-numeric flux":              "20260908    230000      2461292.447   2315.36         no-flux      0113.2       0101.9\n",
	}

	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusPartialContent)
				_, _ = w.Write([]byte(body))
			}))
			t.Cleanup(ts.Close)

			s := newTestSource(t, config.DRAO{Enabled: true, Retries: -1}, ts.URL, wantAt)
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

func TestABodyOfOnlyBlankLinesIsAnError(t *testing.T) {
	if _, err := newestRow("\n\n   \n\r\n"); err == nil {
		t.Error("a body of only blank lines was accepted; wanted an error")
	}
}
