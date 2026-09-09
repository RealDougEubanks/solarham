package gfz

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
// two requests does not wait for the real politeness spacing. Rate limiting is
// httpx's own behaviour and is tested there.
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

// newTestSource builds a source aimed at ts with a fixed clock, so the
// SourceDataAge assertions are exact.
func newTestSource(t *testing.T, cfg config.GFZ, baseURL string, now time.Time) *Source {
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

// serveFiles starts a server answering each of the two nowcast paths from
// testdata, and counts requests per path.
func serveFiles(t *testing.T, kp, hp30 []byte) (*httptest.Server, *pathCounter) {
	t.Helper()
	counter := newPathCounter()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		counter.inc(r.URL.Path)
		w.Header().Set("Content-Type", "text/plain")
		switch r.URL.Path {
		case pathKp:
			_, _ = w.Write(kp)
		case pathHp30:
			_, _ = w.Write(hp30)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(ts.Close)
	return ts, counter
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
	sb.WriteString(fmt.Sprintf("batch of %d sample(s):", len(b.Samples)))
	for _, s := range b.Samples {
		sb.WriteString(fmt.Sprintf("\n  %s%v = %g at %s",
			s.Desc.FullName(), s.Labels, s.Value, s.Time.Format(time.RFC3339)))
	}
	return sb.String()
}

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

func TestNewRequiresASharedHTTPClient(t *testing.T) {
	if _, err := New(config.GFZ{}, nil, discardLogger()); err == nil {
		t.Fatal("wanted an error when no httpx client was supplied, got nil")
	}
}

func TestNewRejectsABaseURLThatIsNotAbsolute(t *testing.T) {
	for _, bad := range []string{"kp.gfz.de", "/app/files", "://nope"} {
		if _, err := New(config.GFZ{BaseURL: bad}, testClient(t), discardLogger()); err == nil {
			t.Errorf("base URL %q was accepted; wanted an error", bad)
		}
	}
}

func TestNewAppliesTheDocumentedDefaults(t *testing.T) {
	s, err := New(config.GFZ{Enabled: true}, testClient(t), discardLogger())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if s.Name() != Name || Name != "gfz" {
		t.Errorf("Name is %q, want %q", s.Name(), "gfz")
	}
	if s.baseURL != defaultBaseURL {
		t.Errorf("base URL is %q, want %q", s.baseURL, defaultBaseURL)
	}
	if s.Interval() != defaultInterval {
		t.Errorf("interval is %s, want %s", s.Interval(), defaultInterval)
	}
	if s.retries != defaultRetries {
		t.Errorf("retries is %d, want %d", s.retries, defaultRetries)
	}
}

func TestAnIntervalBelowTheCourtesyFloorIsClampedRatherThanRejected(t *testing.T) {
	s, err := New(config.GFZ{Interval: time.Second}, testClient(t), discardLogger())
	if err != nil {
		t.Fatalf("New should clamp rather than fail startup, got: %v", err)
	}
	if s.Interval() != minInterval {
		t.Errorf("interval is %s, want it clamped to %s", s.Interval(), minInterval)
	}
}

func TestANegativeRetryCountMeansASingleAttempt(t *testing.T) {
	s, err := New(config.GFZ{Retries: -1}, testClient(t), discardLogger())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if s.retries != 0 {
		t.Errorf("retries is %d, want 0 so that only one attempt is made", s.retries)
	}
}

// ---------------------------------------------------------------------------
// Fixture parsing, with exact expected values
// ---------------------------------------------------------------------------

// The last non-sentinel row of testdata/kp_ap_nowcast.txt is:
//
//	2026 09 09 12.0 13.50 34585.50000 34585.56250  2.667   12 0
//
// Its three rows after that are all sentinels, which is what makes this fixture
// worth having.
var (
	wantKpValue = 2.667
	wantKpAp    = 12.0
	wantKpMid   = time.Date(2026, 9, 9, 13, 30, 0, 0, time.UTC)
)

func TestTheCapturedKpFixtureParsesIntoTheExpectedSamples(t *testing.T) {
	ts, _ := serveFiles(t, fixture(t, "kp_ap_nowcast.txt"), nil)
	now := wantKpMid.Add(20 * time.Minute)
	s := newTestSource(t, config.GFZ{Enabled: true}, ts.URL, now)

	batch, err := s.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}

	if got := sampleFor(t, batch, metric.KIndex, "planetary"); got.Value != wantKpValue {
		t.Errorf("Kp is %g, want %g", got.Value, wantKpValue)
	} else if !got.Time.Equal(wantKpMid) {
		t.Errorf("Kp is stamped %s, want the interval mid time %s", got.Time, wantKpMid)
	}

	if got := sampleFor(t, batch, metric.APIndex, "3h"); got.Value != wantKpAp {
		t.Errorf("ap is %g, want %g", got.Value, wantKpAp)
	}

	// D=0 on that row, so the status must be preliminary.
	status := sampleFor(t, batch, metric.IndexStatusInfo, "kp", statusPreliminary)
	if status.Value != 1 {
		t.Errorf("the info sample carries %g, want the constant 1", status.Value)
	}

	// Kp only, so there must be no half-hourly series at all.
	if n := countFor(batch, metric.KIndexHalfHourly); n != 0 {
		t.Errorf("got %d half-hourly samples with HalfHourly unset, want 0", n)
	}
}

func TestTheCapturedHp30FixtureParsesIntoTheExpectedSamples(t *testing.T) {
	ts, _ := serveFiles(t, fixture(t, "kp_ap_nowcast.txt"), fixture(t, "hp30_ap30_nowcast.txt"))
	s := newTestSource(t, config.GFZ{Enabled: true, HalfHourly: true}, ts.URL, wantKpMid)

	batch, err := s.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}

	// The last non-sentinel row of testdata/hp30_ap30_nowcast.txt:
	//
	//	2026 09 09 15.0 15.25 34585.62500 34585.63542  3.000   15 0
	const wantHp30 = 3.000
	wantMid := time.Date(2026, 9, 9, 15, 15, 0, 0, time.UTC)

	got := sampleFor(t, batch, metric.KIndexHalfHourly, "30m")
	if got.Value != wantHp30 {
		t.Errorf("Hp30 is %g, want %g", got.Value, wantHp30)
	}
	if !got.Time.Equal(wantMid) {
		t.Errorf("Hp30 is stamped %s, want the interval mid time %s", got.Time, wantMid)
	}

	if ap := sampleFor(t, batch, metric.APIndex, "30m"); ap.Value != 15 {
		t.Errorf("ap30 is %g, want 15", ap.Value)
	}

	// Both products publish their own status, under distinct index labels, so
	// neither overwrites the other.
	sampleFor(t, batch, metric.IndexStatusInfo, "kp", statusPreliminary)
	sampleFor(t, batch, metric.IndexStatusInfo, "hp30", statusPreliminary)
}

func TestADefinitiveRowIsLabelledDefinitive(t *testing.T) {
	body := header() + strings.Join([]string{
		"2026 09 09 09.0 10.50 34585.37500 34585.43750  2.333    9 1",
	}, "\n") + "\n"

	ts, _ := serveFiles(t, []byte(body), nil)
	s := newTestSource(t, config.GFZ{Enabled: true}, ts.URL, wantKpMid)

	batch, err := s.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	sampleFor(t, batch, metric.IndexStatusInfo, "kp", statusDefinitive)
}

func TestHalfHourlyIsNotFetchedUnlessItIsEnabled(t *testing.T) {
	ts, counter := serveFiles(t, fixture(t, "kp_ap_nowcast.txt"), fixture(t, "hp30_ap30_nowcast.txt"))
	s := newTestSource(t, config.GFZ{Enabled: true}, ts.URL, wantKpMid)

	if _, err := s.Poll(context.Background()); err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if n := counter.get(pathHp30); n != 0 {
		t.Errorf("the 88 KB Hp30 file was fetched %d time(s) with HalfHourly unset, want 0", n)
	}
	if n := counter.get(pathKp); n != 1 {
		t.Errorf("the Kp file was fetched %d time(s), want 1", n)
	}
}

// ---------------------------------------------------------------------------
// Sentinels
// ---------------------------------------------------------------------------

func TestTheNewestRowIsTheNewestNonSentinelRow(t *testing.T) {
	body := header() + strings.Join([]string{
		"2026 09 09 09.0 10.50 34585.37500 34585.43750  2.333    9 0",
		"2026 09 09 12.0 13.50 34585.50000 34585.56250  2.667   12 0",
		"2026 09 09 15.0 16.50 34585.62500 34585.68750 -1.000   -1 0",
		"2026 09 09 18.0 19.50 34585.75000 34585.81250 -1.000   -1 0",
		"2026 09 09 21.0 22.50 34585.87500 34585.93750 -1.000   -1 0",
	}, "\n") + "\n"

	row, err := newestScaledRow(body)
	if err != nil {
		t.Fatalf("newestScaledRow: %v", err)
	}
	if row.index != 2.667 {
		t.Errorf("chose the row with Kp %g; want 2.667, the newest row that is not the sentinel", row.index)
	}
	if row.ap != 12 {
		t.Errorf("chose the row with ap %d, want 12", row.ap)
	}
}

func TestASentinelKpProducesNoSample(t *testing.T) {
	body := header() + "2026 09 09 15.0 16.50 34585.62500 34585.68750 -1.000   -1 0\n"

	ts, _ := serveFiles(t, []byte(body), nil)
	s := newTestSource(t, config.GFZ{Enabled: true}, ts.URL, wantKpMid)

	batch, err := s.Poll(context.Background())
	if err == nil {
		t.Fatalf("a file of nothing but sentinels should error, got %s", describe(batch))
	}
	if !errors.Is(err, errNoScaledRow) {
		t.Errorf("error is %v, want it to wrap errNoScaledRow", err)
	}
	for _, s := range batch.Samples {
		if s.Value < 0 {
			t.Errorf("the sentinel reached a sample: %s%v = %g", s.Desc.FullName(), s.Labels, s.Value)
		}
	}
	if len(batch.Samples) != 0 {
		t.Errorf("wanted no samples from an all-sentinel file, got %s", describe(batch))
	}
}

func TestASentinelApAloneAlsoProducesNoSample(t *testing.T) {
	// A row with a plausible Kp but a sentinel ap. Publishing the Kp and
	// silently dropping the ap would leave two series disagreeing about which
	// interval is current.
	body := header() +
		"2026 09 09 09.0 10.50 34585.37500 34585.43750  2.333    9 0\n" +
		"2026 09 09 12.0 13.50 34585.50000 34585.56250  2.667   -1 0\n"

	row, err := newestScaledRow(body)
	if err != nil {
		t.Fatalf("newestScaledRow: %v", err)
	}
	if row.index != 2.333 {
		t.Errorf("chose Kp %g; the row with a sentinel ap should have been skipped, leaving 2.333", row.index)
	}
}

func TestTheSentinelIsNeverPublishedFromTheRealFixtures(t *testing.T) {
	// The captured files both end in sentinel rows, so this is the regression
	// test for the whole point of the package.
	ts, _ := serveFiles(t, fixture(t, "kp_ap_nowcast.txt"), fixture(t, "hp30_ap30_nowcast.txt"))
	s := newTestSource(t, config.GFZ{Enabled: true, HalfHourly: true}, ts.URL, wantKpMid)

	batch, err := s.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if len(batch.Samples) == 0 {
		t.Fatal("wanted samples from the real fixtures, got none")
	}
	for _, smp := range batch.Samples {
		if smp.Desc == metric.SourceDataAge || smp.Desc == metric.IndexStatusInfo {
			continue
		}
		if smp.Value < 0 {
			t.Errorf("a negative value reached a sample: %s%v = %g",
				smp.Desc.FullName(), smp.Labels, smp.Value)
		}
	}
}

// ---------------------------------------------------------------------------
// Data age
// ---------------------------------------------------------------------------

func TestSourceDataAgeIsMeasuredFromThePayloadTimestamp(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// A Last-Modified of right now over a payload whose newest interval is
		// hours old. This is the stale-but-200 failure mode, and the age must
		// come from the payload, not this header.
		w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
		_, _ = w.Write(fixture(t, "kp_ap_nowcast.txt"))
	}))
	t.Cleanup(ts.Close)

	now := wantKpMid.Add(3 * time.Hour)
	s := newTestSource(t, config.GFZ{Enabled: true}, ts.URL, now)

	batch, err := s.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}

	age := sampleFor(t, batch, metric.SourceDataAge, Name)
	if want := (3 * time.Hour).Seconds(); age.Value != want {
		t.Errorf("age is %g seconds, want %g measured from the interval mid time", age.Value, want)
	}
}

func TestAFutureDatedPayloadReportsZeroAgeRatherThanANegativeOne(t *testing.T) {
	ts, _ := serveFiles(t, fixture(t, "kp_ap_nowcast.txt"), nil)
	s := newTestSource(t, config.GFZ{Enabled: true}, ts.URL, wantKpMid.Add(-time.Hour))

	batch, err := s.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if age := sampleFor(t, batch, metric.SourceDataAge, Name); age.Value != 0 {
		t.Errorf("age is %g, want 0 rather than a negative age", age.Value)
	}
}

// ---------------------------------------------------------------------------
// Schedule
// ---------------------------------------------------------------------------

func TestTheScheduleIsAFixedIntervalAtTheConfiguredCadence(t *testing.T) {
	s := newTestSource(t, config.GFZ{Enabled: true, Interval: 10 * time.Minute}, "https://example.invalid", time.Time{})

	start := time.Date(2026, 9, 9, 0, 0, 0, 0, time.UTC)
	at := start
	// One simulated day. Every step must be exactly the interval.
	for i := range 144 {
		next := s.Schedule().NextAfter(at)
		if gap := next.Sub(at); gap != 10*time.Minute {
			t.Fatalf("step %d: gap is %s, want 10m", i, gap)
		}
		at = next
	}
	if want := start.Add(24 * time.Hour); !at.Equal(want) {
		t.Errorf("144 ten-minute polls landed on %s, want %s", at, want)
	}
}

func TestTheScheduleRefusesToPollFasterThanTheCourtesyFloor(t *testing.T) {
	// Interval is clamped in New too, but the schedule must enforce it
	// independently: a floor that only validation applies is not a floor.
	s := newTestSource(t, config.GFZ{Enabled: true}, "https://example.invalid", time.Time{})
	s.interval = time.Second

	at := time.Date(2026, 9, 9, 0, 0, 0, 0, time.UTC)
	next := s.Schedule().NextAfter(at)
	if gap := next.Sub(at); gap < minInterval {
		t.Errorf("the schedule allowed a %s gap, want no less than %s", gap, minInterval)
	}
}

func TestTheSourceIsScheduledRatherThanIntervalOnly(t *testing.T) {
	s := newTestSource(t, config.GFZ{Enabled: true}, "https://example.invalid", time.Time{})
	if _, ok := any(s).(source.Scheduled); !ok {
		t.Error("the source does not implement source.Scheduled")
	}
	if s.Schedule() == nil {
		t.Error("Schedule returned nil, which would make the scheduler fall back to Interval")
	}
}

// ---------------------------------------------------------------------------
// HTTP behaviour
// ---------------------------------------------------------------------------

func TestASecondPollSendsTheValidatorsFromTheFirst(t *testing.T) {
	var sawConditional atomic.Bool
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("If-None-Match") != "" || r.Header.Get("If-Modified-Since") != "" {
			sawConditional.Store(true)
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("ETag", `"abc"`)
		w.Header().Set("Last-Modified", wantKpMid.Format(http.TimeFormat))
		_, _ = w.Write(fixture(t, "kp_ap_nowcast.txt"))
	}))
	t.Cleanup(ts.Close)

	s := newTestSource(t, config.GFZ{Enabled: true}, ts.URL, wantKpMid)
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

func TestEveryFileAnsweringNotModifiedIsNotAFailure(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotModified)
	}))
	t.Cleanup(ts.Close)

	s := newTestSource(t, config.GFZ{Enabled: true, HalfHourly: true}, ts.URL, wantKpMid)
	batch, err := s.Poll(context.Background())
	if !errors.Is(err, source.ErrNotModified) {
		t.Errorf("Poll returned %v, want source.ErrNotModified", err)
	}
	if len(batch.Samples) != 0 {
		t.Errorf("a 304 produced samples: %s", describe(batch))
	}
}

func TestOneFileFailingStillReturnsTheOthersSamples(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == pathHp30 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_, _ = w.Write(fixture(t, "kp_ap_nowcast.txt"))
	}))
	t.Cleanup(ts.Close)

	cfg := config.GFZ{Enabled: true, HalfHourly: true, Retries: -1}
	s := newTestSource(t, cfg, ts.URL, wantKpMid)

	batch, err := s.Poll(context.Background())
	if err != nil {
		t.Fatalf("one failing file should not fail the poll, got: %v", err)
	}
	sampleFor(t, batch, metric.KIndex, "planetary")
	if n := countFor(batch, metric.KIndexHalfHourly); n != 0 {
		t.Errorf("got %d Hp30 samples from a failing file, want 0", n)
	}
}

func TestEveryFileFailingReturnsAnError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(ts.Close)

	s := newTestSource(t, config.GFZ{Enabled: true, Retries: -1}, ts.URL, wantKpMid)
	if _, err := s.Poll(context.Background()); err == nil {
		t.Fatal("wanted an error when every file failed, got nil")
	}
}

func TestNotFoundIsNotRetried(t *testing.T) {
	var requests atomic.Int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(ts.Close)

	// Five retries configured, so a retried 404 would show up as six requests.
	s := newTestSource(t, config.GFZ{Enabled: true, Retries: 5}, ts.URL, wantKpMid)
	if _, err := s.Poll(context.Background()); err == nil {
		t.Fatal("wanted an error from a 404, got nil")
	}
	if n := requests.Load(); n != 1 {
		t.Errorf("a 404 was requested %d times, want 1: retrying it just repeats the load", n)
	}
}

func TestTooManyRequestsReturnsErrRateLimited(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "60")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	t.Cleanup(ts.Close)

	s := newTestSource(t, config.GFZ{Enabled: true}, ts.URL, wantKpMid)
	batch, err := s.Poll(context.Background())
	if !errors.Is(err, source.ErrRateLimited) {
		t.Errorf("Poll returned %v, want it to wrap source.ErrRateLimited", err)
	}
	if len(batch.Samples) != 0 {
		t.Errorf("a 429 produced samples: %s", describe(batch))
	}
}

func TestPollHonoursContextCancellation(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	t.Cleanup(ts.Close)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	s := newTestSource(t, config.GFZ{Enabled: true}, ts.URL, wantKpMid)
	if _, err := s.Poll(ctx); err == nil {
		t.Fatal("wanted an error from a cancelled context, got nil")
	}
}

// ---------------------------------------------------------------------------
// Malformed input
// ---------------------------------------------------------------------------

func TestMalformedInputReturnsAnErrorRatherThanPanicking(t *testing.T) {
	cases := map[string]string{
		"an empty body":                "",
		"header only":                  header(),
		"a row with too few fields":    header() + "2026 09 09 12.0 13.50 2.667 12 0\n",
		"a row with too many fields":   header() + "2026 09 09 12.0 13.50 34585.5 34585.5 2.667 12 0 extra\n",
		"a non-numeric Kp":             header() + "2026 09 09 12.0 13.50 34585.50000 34585.56250  NaNish   12 0\n",
		"a non-numeric ap":             header() + "2026 09 09 12.0 13.50 34585.50000 34585.56250  2.667   xx 0\n",
		"an out-of-range month":        header() + "2026 13 09 12.0 13.50 34585.50000 34585.56250  2.667   12 0\n",
		"an out-of-range hour":         header() + "2026 09 09 25.0 13.50 34585.50000 34585.56250  2.667   12 0\n",
		"a definitive flag that is 2":  header() + "2026 09 09 12.0 13.50 34585.50000 34585.56250  2.667   12 2\n",
		"HTML from a captive portal":   "<html><body>Sign in to continue</body></html>",
		"a truncated numeric field":    header() + "2026 09 09 12.0 13.5\n",
		"a row of nothing but hyphens": header() + "- - - - - - - - - -\n",
	}

	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			ts, _ := serveFiles(t, []byte(body), nil)
			s := newTestSource(t, config.GFZ{Enabled: true, Retries: -1}, ts.URL, wantKpMid)

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

func TestBlankLinesAndCommentsAreSkippedRatherThanFailing(t *testing.T) {
	body := header() +
		"\n" +
		"# a citation line added after this code was written\n" +
		"2026 09 09 12.0 13.50 34585.50000 34585.56250  2.667   12 0\n" +
		"\n"

	row, err := newestScaledRow(body)
	if err != nil {
		t.Fatalf("newestScaledRow: %v", err)
	}
	if row.index != 2.667 {
		t.Errorf("Kp is %g, want 2.667", row.index)
	}
}

// ---------------------------------------------------------------------------
// Row parsing details
// ---------------------------------------------------------------------------

func TestTheIntervalMidTimeIsUsedRatherThanTheStartTime(t *testing.T) {
	row, err := parseRow("2026 09 09 12.0 13.50 34585.50000 34585.56250  2.667   12 0")
	if err != nil {
		t.Fatalf("parseRow: %v", err)
	}
	wantStart := time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)
	wantMid := time.Date(2026, 9, 9, 13, 30, 0, 0, time.UTC)
	if !row.start.Equal(wantStart) {
		t.Errorf("start is %s, want %s", row.start, wantStart)
	}
	if !row.mid.Equal(wantMid) {
		t.Errorf("mid is %s, want %s", row.mid, wantMid)
	}
}

func TestQuarterHourColumnsFromTheHp30FileConvertExactly(t *testing.T) {
	// The Hp30 file uses 00.25 and 00.75 style mid times, which is where float
	// noise would show up if the conversion were naive.
	row, err := parseRow("2026 08 11 00.5 00.75 34556.02083 34556.03125  0.667    3 0")
	if err != nil {
		t.Fatalf("parseRow: %v", err)
	}
	wantMid := time.Date(2026, 8, 11, 0, 45, 0, 0, time.UTC)
	if !row.mid.Equal(wantMid) {
		t.Errorf("mid is %s, want exactly %s", row.mid, wantMid)
	}
}

// ---------------------------------------------------------------------------
// Helpers used by the synthetic bodies above
// ---------------------------------------------------------------------------

// header returns a minimal but realistic comment header. Skipping is by '#'
// prefix rather than by line count, so the exact number of lines here does not
// matter — which is the behaviour being relied on.
func header() string {
	return strings.Join([]string{
		"# LICENSE: CC BY 4.0",
		"# SOURCE: Geomagnetic Observatory Niemegk, GFZ Helmholtz Centre for Geosciences",
		"# ASCII, blank separated and fixed length, missing data indicated by -1.000 for Kp and -1 for ap",
		"#YYY MM DD hh.h hh._m        days      days_m     Kp   ap D",
	}, "\n") + "\n"
}

// pathCounter counts requests per URL path, so a test can assert that a file
// which should not have been fetched was not fetched.
type pathCounter struct {
	mu sync.Mutex
	n  map[string]int
}

func newPathCounter() *pathCounter { return &pathCounter{n: make(map[string]int)} }

func (c *pathCounter) inc(path string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.n[path]++
}

func (c *pathCounter) get(path string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n[path]
}
