package lotw

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/RealDougEubanks/solarham/internal/config"
	"github.com/RealDougEubanks/solarham/internal/httpx"
	"github.com/RealDougEubanks/solarham/internal/metric"
	"github.com/RealDougEubanks/solarham/internal/source"
)

// fileTime is the Last-Modified of the captured fixture, to the second. Every
// expected count below is derived from this instant, because that is the whole
// point of the package.
var fileTime = time.Date(2026, 9, 7, 5, 51, 37, 0, time.UTC)

// Counts in testdata/lotw-user-activity.csv, which is 225 rows sampled from the
// live 235,278-row file so that each window is populated. Verified against the
// real file before truncation.
const (
	fixtureRows  = 225
	fixture7d    = 40
	fixture30d   = 65
	fixture365d  = 125
	fixtureBytes = 5983
)

// testLogger discards output but keeps the debug path exercised, so a bad
// argument in a log line fails a test rather than production.
func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// testClient is the shared httpx client with its politeness relaxed.
//
// The real policy table gives an unlisted host — which 127.0.0.1 is — a
// thirty-second floor with burst 1. That is correct in production and would
// make this file take minutes, so tests get their own client. It is constructed
// per test so that the conditional-GET validator cache and the 429 cooldown,
// both of which live on the client, cannot leak between tests.
func testClient(t *testing.T) *httpx.Client {
	t.Helper()
	return httpx.New(httpx.Options{
		Timeout:  5 * time.Second,
		Fallback: httpx.HostPolicy{MinInterval: time.Microsecond, Burst: 1000},
	}, testLogger())
}

func fixture(t *testing.T, name string) []byte {
	t.Helper()
	body, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatalf("reading fixture: %v", err)
	}
	return body
}

// server serves one handler at the activity-file path and counts requests.
type server struct {
	*httptest.Server
	hits atomic.Int64
}

func newServer(t *testing.T, h http.HandlerFunc) *server {
	t.Helper()
	s := &server{}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.hits.Add(1)
		h(w, r)
	}))
	t.Cleanup(s.Close)
	return s
}

// serveCSV serves the body with the given Last-Modified, as Apache does.
func serveCSV(body []byte, lastModified time.Time) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/csv")
		if !lastModified.IsZero() {
			w.Header().Set("Last-Modified", lastModified.UTC().Format(http.TimeFormat))
			w.Header().Set("ETag", `"601b4a-65ade361d3c6d"`)
		}
		_, _ = w.Write(body)
	}
}

// newSource builds a source pointed at ts with its clock pinned to now.
func newSource(t *testing.T, ts *server, now time.Time) *Source {
	t.Helper()
	src, err := New(config.LoTW{Enabled: true, URL: ts.URL + "/lotw-user-activity.csv"},
		testClient(t), testLogger())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	src.now = func() time.Time { return now }
	return src
}

// valueFor returns the value of the sample carrying these label values.
func valueFor(t *testing.T, batch metric.Batch, desc *metric.Descriptor, labels ...string) float64 {
	t.Helper()
	for _, s := range batch.Samples {
		if s.Desc != desc || len(s.Labels) != len(labels) {
			continue
		}
		match := true
		for i, l := range labels {
			if s.Labels[i] != l {
				match = false
				break
			}
		}
		if match {
			return s.Value
		}
	}
	t.Fatalf("no sample for %s%v in %d samples", desc.FullName(), labels, len(batch.Samples))
	return 0
}

// ---------------------------------------------------------------------------
// The point of the package: windows measured against the file, not the clock.
// ---------------------------------------------------------------------------

func TestActivityWindowsAreComputedAgainstTheFilesLastModifiedRatherThanTheWallClock(t *testing.T) {
	body := fixture(t, "lotw-user-activity.csv")

	// The same bytes, with the same Last-Modified, read at four instants
	// spanning a full rebuild cycle. The file refreshes roughly weekly, so all
	// four of these are ordinary states for a live exporter to be in.
	for _, tc := range []struct {
		name string
		now  time.Time
	}{
		{"a minute after the rebuild", fileTime.Add(time.Minute)},
		{"a day after the rebuild", fileTime.Add(24 * time.Hour)},
		{"four days after the rebuild", fileTime.Add(4 * 24 * time.Hour)},
		{"a week after the rebuild", fileTime.Add(7 * 24 * time.Hour)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ts := newServer(t, serveCSV(body, fileTime))
			batch, err := newSource(t, ts, tc.now).Poll(context.Background())
			if err != nil {
				t.Fatalf("Poll: %v", err)
			}

			for _, w := range []struct {
				label string
				want  float64
			}{{"7d", fixture7d}, {"30d", fixture30d}, {"365d", fixture365d}} {
				if got := valueFor(t, batch, metric.ActiveOperators, ServiceLabel, w.label); got != w.want {
					t.Errorf("active operators in %s = %v, want %v; the count moved with the wall "+
						"clock, which is the drift this source exists to avoid", w.label, got, w.want)
				}
			}
		})
	}
}

func TestUsingTheWallClockInsteadWouldHaveChangedTheCounts(t *testing.T) {
	// Guards the test above from being vacuous. If the fixture happened to hold
	// no rows near the window edges, that test would pass no matter which
	// reference the code used, and the bug it is written to catch would sail
	// through.
	src := &Source{log: testLogger()}
	body := fixture(t, "lotw-user-activity.csv")

	atFile, _, _, err := src.count(body, fileTime)
	if err != nil {
		t.Fatalf("count at the file time: %v", err)
	}
	aWeekLater, _, _, err := src.count(body, fileTime.Add(7*24*time.Hour))
	if err != nil {
		t.Fatalf("count a week later: %v", err)
	}

	if atFile[0] == aWeekLater[0] {
		t.Errorf("the 7d count is %v against both references, so this fixture cannot "+
			"distinguish a file-relative window from a clock-relative one", atFile[0])
	}
	if aWeekLater[0] >= atFile[0] {
		t.Errorf("7d count against a week-old reference = %v, want fewer than %v: "+
			"a stale reference should shed recent uploads", aWeekLater[0], atFile[0])
	}
}

func TestWindowBoundariesAreInclusiveOfAnUploadExactlyOneWindowOld(t *testing.T) {
	// Hand-built rather than sampled, because the live file has no rows within
	// a second of a boundary and this is the one edge worth pinning.
	exactly := fileTime.Add(-7 * 24 * time.Hour)
	body := []byte("K1EXACT," + exactly.Format("2006-01-02,15:04:05") + "\n" +
		"K1OVER," + exactly.Add(-time.Second).Format("2006-01-02,15:04:05") + "\n" +
		"K1UNDER," + exactly.Add(time.Second).Format("2006-01-02,15:04:05") + "\n")

	ts := newServer(t, serveCSV(body, fileTime))
	batch, err := newSource(t, ts, fileTime.Add(time.Hour)).Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}

	if got := valueFor(t, batch, metric.ActiveOperators, ServiceLabel, "7d"); got != 2 {
		t.Errorf("7d count = %v, want 2 (the row exactly 7 days old is inside the "+
			"window, the one a second older is not)", got)
	}
	if got := valueFor(t, batch, metric.ActiveOperators, ServiceLabel, "30d"); got != 3 {
		t.Errorf("30d count = %v, want 3", got)
	}
}

// ---------------------------------------------------------------------------
// Counts and samples
// ---------------------------------------------------------------------------

func TestRegisteredOperatorsEqualsTheNumberOfRowsInTheFile(t *testing.T) {
	body := fixture(t, "lotw-user-activity.csv")
	if len(body) != fixtureBytes {
		t.Fatalf("fixture is %d bytes, want %d; the recorded counts describe a "+
			"different file", len(body), fixtureBytes)
	}

	ts := newServer(t, serveCSV(body, fileTime))
	batch, err := newSource(t, ts, fileTime.Add(time.Hour)).Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}

	if got := valueFor(t, batch, metric.RegisteredOperators, ServiceLabel); got != fixtureRows {
		t.Errorf("registered operators = %v, want %v", got, float64(fixtureRows))
	}
}

func TestActiveOperatorsNeverExceedRegisteredOperatorsAndTheWindowsNest(t *testing.T) {
	ts := newServer(t, serveCSV(fixture(t, "lotw-user-activity.csv"), fileTime))
	batch, err := newSource(t, ts, fileTime.Add(time.Hour)).Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}

	week := valueFor(t, batch, metric.ActiveOperators, ServiceLabel, "7d")
	month := valueFor(t, batch, metric.ActiveOperators, ServiceLabel, "30d")
	year := valueFor(t, batch, metric.ActiveOperators, ServiceLabel, "365d")
	total := valueFor(t, batch, metric.RegisteredOperators, ServiceLabel)

	if !(week <= month && month <= year && year <= total) {
		t.Errorf("windows do not nest: 7d=%v 30d=%v 365d=%v total=%v", week, month, year, total)
	}
}

func TestSourceDataAgeIsMeasuredFromTheFilesOwnLastModified(t *testing.T) {
	ts := newServer(t, serveCSV(fixture(t, "lotw-user-activity.csv"), fileTime))
	now := fileTime.Add(50 * time.Hour)
	batch, err := newSource(t, ts, now).Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}

	if got, want := valueFor(t, batch, metric.SourceDataAge, Name), (50 * time.Hour).Seconds(); got != want {
		t.Errorf("source data age = %v, want %v", got, want)
	}
}

func TestTheOperatorCountsAreStampedWithTheFileTimeAndTheAgeWithTheFetchTime(t *testing.T) {
	ts := newServer(t, serveCSV(fixture(t, "lotw-user-activity.csv"), fileTime))
	now := fileTime.Add(3 * time.Hour)
	batch, err := newSource(t, ts, now).Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}

	for _, s := range batch.Samples {
		want := fileTime
		if s.Desc == metric.SourceDataAge {
			want = now
		}
		if !s.Time.Equal(want) {
			t.Errorf("%s%v is stamped %s, want %s", s.Desc.FullName(), s.Labels,
				s.Time.Format(time.RFC3339), want.Format(time.RFC3339))
		}
	}
}

func TestEverySampleIsValidAgainstItsDescriptor(t *testing.T) {
	ts := newServer(t, serveCSV(fixture(t, "lotw-user-activity.csv"), fileTime))
	batch, err := newSource(t, ts, fileTime.Add(time.Hour)).Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if batch.Len() != len(windows)+2 {
		t.Errorf("batch has %d samples, want %d", batch.Len(), len(windows)+2)
	}
	for _, s := range batch.Samples {
		if err := s.Validate(); err != nil {
			t.Errorf("invalid sample: %v", err)
		}
	}
}

func TestNoCallsignAppearsInAnyEmittedLabel(t *testing.T) {
	// The file is 235,278 callsigns. One of them reaching a label would be an
	// unbounded series set, so this is asserted rather than assumed.
	ts := newServer(t, serveCSV([]byte("K1ZZZ,2026-09-06,00:00:00\n"), fileTime))
	batch, err := newSource(t, ts, fileTime.Add(time.Hour)).Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	for _, s := range batch.Samples {
		for _, l := range s.Labels {
			if l == "K1ZZZ" {
				t.Errorf("%s carries the callsign K1ZZZ as a label value", s.Desc.FullName())
			}
		}
		for _, name := range s.Desc.Labels {
			if name == "callsign" {
				t.Errorf("%s has a callsign label", s.Desc.FullName())
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Degraded input
// ---------------------------------------------------------------------------

func TestMalformedRowsAreSkippedWithoutFailingTheFile(t *testing.T) {
	ts := newServer(t, serveCSV(fixture(t, "lotw-user-activity-degraded.csv"), fileTime))
	batch, err := newSource(t, ts, fileTime.Add(time.Hour)).Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v; five bad rows among five good ones should not fail the file", err)
	}

	// The degraded fixture holds five parseable rows — two inside 7 days, one
	// more inside 30, one more inside a year, one from 1999 — and five
	// unparseable ones: an empty callsign, an impossible date, a two-field row,
	// a four-field row and an unparseable date. It also holds a blank line,
	// which is skipped silently rather than counted as malformed.
	for _, tc := range []struct {
		desc   *metric.Descriptor
		labels []string
		want   float64
	}{
		{metric.RegisteredOperators, []string{ServiceLabel}, 5},
		{metric.ActiveOperators, []string{ServiceLabel, "7d"}, 2},
		{metric.ActiveOperators, []string{ServiceLabel, "30d"}, 3},
		{metric.ActiveOperators, []string{ServiceLabel, "365d"}, 4},
	} {
		if got := valueFor(t, batch, tc.desc, tc.labels...); got != tc.want {
			t.Errorf("%s%v = %v, want %v", tc.desc.FullName(), tc.labels, got, tc.want)
		}
	}
}

func TestAFileWithNothingParseableInItIsAnErrorRatherThanAZeroCount(t *testing.T) {
	// A format change must not publish "every amateur operator has gone quiet".
	ts := newServer(t, serveCSV([]byte("callsign,last_upload_date,last_upload_time\n<html>oops</html>\n"), fileTime))
	batch, err := newSource(t, ts, fileTime.Add(time.Hour)).Poll(context.Background())
	if err == nil {
		t.Fatalf("Poll succeeded with %d samples, want an error", batch.Len())
	}
	if batch.Len() != 0 {
		t.Errorf("a failed poll returned %d samples, want none", batch.Len())
	}
}

func TestAMissingLastModifiedFallsBackToTheFetchTime(t *testing.T) {
	// Not observed live — Apache has always sent the header — but the fallback
	// has to be defined, and it has to be the documented one.
	ts := newServer(t, serveCSV([]byte("K1AAA,2026-09-06,12:00:00\n"), time.Time{}))
	now := fileTime.Add(time.Hour)
	batch, err := newSource(t, ts, now).Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if got := valueFor(t, batch, metric.SourceDataAge, Name); got != 0 {
		t.Errorf("source data age = %v, want 0 when the reference is the fetch time", got)
	}
	if got := valueFor(t, batch, metric.ActiveOperators, ServiceLabel, "7d"); got != 1 {
		t.Errorf("7d count = %v, want 1", got)
	}
}

func TestAFileStampedInTheFutureDoesNotPublishANegativeAge(t *testing.T) {
	ts := newServer(t, serveCSV([]byte("K1AAA,2026-09-06,12:00:00\n"), fileTime))
	batch, err := newSource(t, ts, fileTime.Add(-time.Hour)).Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if got := valueFor(t, batch, metric.SourceDataAge, Name); got != 0 {
		t.Errorf("source data age = %v, want it clamped to 0", got)
	}
}

// ---------------------------------------------------------------------------
// HTTP behaviour
// ---------------------------------------------------------------------------

func TestAConditionalRequestAnsweredNotModifiedReportsErrNotModified(t *testing.T) {
	var sawConditional atomic.Bool
	ts := newServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("If-None-Match") != "" || r.Header.Get("If-Modified-Since") != "" {
			sawConditional.Store(true)
		}
		w.WriteHeader(http.StatusNotModified)
	})

	src := newSource(t, ts, fileTime.Add(time.Hour))
	batch, err := src.Poll(context.Background())
	if !errors.Is(err, source.ErrNotModified) {
		t.Fatalf("Poll error = %v, want source.ErrNotModified", err)
	}
	if batch.Len() != 0 {
		t.Errorf("an unchanged file produced %d samples, want none", batch.Len())
	}

	// The second poll is what proves the validators were stored and replayed,
	// which is the whole reason six polls in seven cost nothing.
	if _, err := src.Poll(context.Background()); !errors.Is(err, source.ErrNotModified) {
		t.Fatalf("second Poll error = %v, want source.ErrNotModified", err)
	}
}

func TestTheRequestReplaysTheValidatorsFromThePreviousResponse(t *testing.T) {
	var conditional atomic.Int64
	ts := newServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("If-None-Match") != "" {
			conditional.Add(1)
		}
		serveCSV([]byte("K1AAA,2026-09-06,12:00:00\n"), fileTime)(w, r)
	})

	src := newSource(t, ts, fileTime.Add(time.Hour))
	for range 2 {
		if _, err := src.Poll(context.Background()); err != nil {
			t.Fatalf("Poll: %v", err)
		}
	}
	if conditional.Load() != 1 {
		t.Errorf("%d of 2 requests carried If-None-Match, want 1 (the first has "+
			"no validator to send yet)", conditional.Load())
	}
}

func TestNotFoundIsPermanentAndIsNotRetried(t *testing.T) {
	ts := newServer(t, func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "gone", http.StatusNotFound)
	})

	src := newSource(t, ts, fileTime)
	src.retries = 3
	if _, err := src.Poll(context.Background()); !errors.Is(err, httpx.ErrPermanent) {
		t.Fatalf("Poll error = %v, want httpx.ErrPermanent", err)
	}
	if ts.hits.Load() != 1 {
		t.Errorf("the server was hit %d times for a 404, want 1: retrying a 404 "+
			"only repeats the load", ts.hits.Load())
	}
}

func TestTooManyRequestsIsReportedAsRateLimitedAndIsNotRetried(t *testing.T) {
	ts := newServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "600")
		http.Error(w, "slow down", http.StatusTooManyRequests)
	})

	src := newSource(t, ts, fileTime)
	src.retries = 3
	_, err := src.Poll(context.Background())
	if !errors.Is(err, source.ErrRateLimited) {
		t.Fatalf("Poll error = %v, want source.ErrRateLimited", err)
	}
	if !errors.Is(err, httpx.ErrRateLimited) {
		t.Errorf("Poll error = %v, want it to wrap httpx.ErrRateLimited too", err)
	}
	if ts.hits.Load() != 1 {
		t.Errorf("the server was hit %d times for a 429, want 1", ts.hits.Load())
	}
}

func TestServerErrorsAreRetriedAndCanSucceed(t *testing.T) {
	var attempts atomic.Int64
	ts := newServer(t, func(w http.ResponseWriter, r *http.Request) {
		if attempts.Add(1) == 1 {
			http.Error(w, "bad gateway", http.StatusBadGateway)
			return
		}
		serveCSV([]byte("K1AAA,2026-09-06,12:00:00\n"), fileTime)(w, r)
	})

	batch, err := newSource(t, ts, fileTime.Add(time.Hour)).Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if got := valueFor(t, batch, metric.RegisteredOperators, ServiceLabel); got != 1 {
		t.Errorf("registered operators = %v, want 1", got)
	}
	if attempts.Load() != 2 {
		t.Errorf("the server was hit %d times, want 2", attempts.Load())
	}
}

func TestPollHonoursContextCancellation(t *testing.T) {
	ts := newServer(t, serveCSV(fixture(t, "lotw-user-activity.csv"), fileTime))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	src := newSource(t, ts, fileTime)
	batch, err := src.Poll(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Poll error = %v, want context.Canceled", err)
	}
	if batch.Len() != 0 {
		t.Errorf("a cancelled poll returned %d samples, want none", batch.Len())
	}
	if ts.hits.Load() != 0 {
		t.Errorf("a cancelled poll made %d requests, want none", ts.hits.Load())
	}
}

// ---------------------------------------------------------------------------
// Schedule and construction
// ---------------------------------------------------------------------------

func TestTheScheduleIsOncePerDayAndNotHourly(t *testing.T) {
	src, err := New(config.LoTW{Enabled: true}, testClient(t), testLogger())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	sched := src.Schedule()

	if got, want := sched.Interval(), 24*time.Hour; got != want {
		t.Errorf("schedule interval = %s, want %s", got, want)
	}
	if got, want := src.Interval(), sched.Interval(); got != want {
		t.Errorf("Interval() = %s but the schedule says %s; the two must not disagree", got, want)
	}

	// Twenty-four consecutive slots must fall on twenty-four distinct days.
	// Anything hourly, or twice daily, fails this.
	at := time.Date(2026, 9, 9, 0, 0, 0, 0, time.UTC)
	days := map[string]bool{}
	for range 24 {
		at = sched.NextAfter(at)
		days[at.Format("2006-01-02")] = true

		// 06:00 plus a thirty-minute lag plus up to ninety seconds of spread.
		if h, m := at.UTC().Hour(), at.UTC().Minute(); h != 6 || m < 30 || m > 32 {
			t.Errorf("poll scheduled for %s, want 06:30 UTC plus a little spread",
				at.UTC().Format(time.RFC3339))
		}
	}
	if len(days) != 24 {
		t.Errorf("24 consecutive polls landed on %d distinct days, want 24", len(days))
	}
}

func TestNewFillsInTheDefaultURLAndRejectsConfigurationItCannotWorkWith(t *testing.T) {
	src, err := New(config.LoTW{Enabled: true}, testClient(t), testLogger())
	if err != nil {
		t.Fatalf("New with an empty URL: %v", err)
	}
	if src.url != DefaultURL {
		t.Errorf("url = %q, want the default %q", src.url, DefaultURL)
	}

	for _, tc := range []struct {
		name   string
		cfg    config.LoTW
		client *httpx.Client
	}{
		{"no client", config.LoTW{Enabled: true}, nil},
		{"a URL with no host", config.LoTW{URL: "https://"}, testClient(t)},
		{"a non-HTTP scheme", config.LoTW{URL: "ftp://lotw.arrl.org/x.csv"}, testClient(t)},
		{"an unparseable URL", config.LoTW{URL: "http://[::1"}, testClient(t)},
	} {
		if _, err := New(tc.cfg, tc.client, testLogger()); err == nil {
			t.Errorf("New accepted %s", tc.name)
		}
	}
}

func TestTheSourceIdentifiesItselfConsistently(t *testing.T) {
	src, err := New(config.LoTW{Enabled: true}, testClient(t), testLogger())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if src.Name() != Name || Name != "lotw" {
		t.Errorf("Name() = %q, const Name = %q, want both %q", src.Name(), Name, "lotw")
	}
}
