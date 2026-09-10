package donki

import (
	"context"
	"errors"
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

// fixtureTime is the instant the tests pretend it is. The responses in testdata
// were captured live from webtools.ccmc.gsfc.nasa.gov on 2026-09-09; freezing
// the clock at noon that day puts the default 72-hour window's lower edge in
// the middle of 6 September, which is where the fixtures have events on both
// sides of it. Window filtering is therefore actually exercised rather than
// trivially satisfied.
var fixtureTime = time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)

// fixture reads a captured response.
func fixture(t *testing.T, name string) []byte {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("reading fixture %s: %v", name, err)
	}
	return body
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
}

// testHTTPClient returns a shared client with the politeness spacing turned
// down to a millisecond. The spacing is the point of httpx in production, but a
// test server on 127.0.0.1 falls to the fallback policy of thirty seconds per
// request, which would make a three-endpoint poll take a minute and a half.
func testHTTPClient() *httpx.Client {
	return httpx.New(httpx.Options{
		Timeout:  5 * time.Second,
		Fallback: httpx.HostPolicy{MinInterval: time.Millisecond, Burst: 16},
	}, discardLogger())
}

// recorder collects what the test server was asked for. Poll fetches the
// endpoints concurrently, so this is behind a mutex.
type recorder struct {
	mu    sync.Mutex
	query map[string]string
}

func newRecorder() *recorder {
	return &recorder{query: make(map[string]string)}
}

func (r *recorder) note(req *http.Request) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.query[req.URL.Path] = req.URL.RawQuery
}

func (r *recorder) rawQuery(path string) (string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	q, ok := r.query[path]
	return q, ok
}

// fixtureFor maps each endpoint path to the captured response for it.
var fixtureFor = map[string]string{
	pathCME:         "cme.json",
	pathCMEAnalysis: "cme_analysis.json",
	pathFLR:         "flr.json",
}

const basePath = "/DONKI/WS/get"

// donkiServer answers the three endpoints from testdata. override, when it
// returns true, has already written the response.
func donkiServer(t *testing.T, override func(w http.ResponseWriter, r *http.Request) bool) *httptest.Server {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if override != nil && override(w, r) {
			return
		}
		name, ok := fixtureFor[trimBase(r.URL.Path)]
		if !ok {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(fixture(t, name))
	}))
	t.Cleanup(ts.Close)
	return ts
}

// trimBase strips the base path so a handler can switch on the endpoint name.
func trimBase(path string) string {
	return strings.TrimPrefix(path, basePath)
}

// newTestSource builds a source against ts with a frozen clock.
func newTestSource(t *testing.T, ts *httptest.Server, window time.Duration) *Source {
	t.Helper()
	src, err := New(config.DONKI{
		Enabled:  true,
		BaseURL:  ts.URL + basePath,
		Interval: 30 * time.Minute,
		Window:   window,
	}, testHTTPClient(), discardLogger())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	src.now = func() time.Time { return fixtureTime }
	return src
}

// find returns the first sample for a descriptor with the given label values.
func find(samples []metric.Sample, desc *metric.Descriptor, labels ...string) (metric.Sample, bool) {
	for _, s := range samples {
		if s.Desc != desc || len(labels) != len(s.Labels) {
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
			return s, true
		}
	}
	return metric.Sample{}, false
}

func wantGauge(t *testing.T, samples []metric.Sample, desc *metric.Descriptor, value float64, labels ...string) metric.Sample {
	t.Helper()
	got, ok := find(samples, desc, labels...)
	if !ok {
		t.Fatalf("no sample for %s%v", desc.FullName(), labels)
	}
	if got.Value != value {
		t.Errorf("%s%v = %v, want %v", desc.FullName(), labels, got.Value, value)
	}
	return got
}

// TestTheMostRecentCMEAnalysisIsPublished checks the selection rule. The
// captured CMEAnalysis response is not in time order — its first record is
// twenty minutes newer than its second — so taking the first or the last entry
// would publish the wrong CME.
func TestTheMostRecentCMEAnalysisIsPublished(t *testing.T) {
	ts := donkiServer(t, nil)
	src := newTestSource(t, ts, 72*time.Hour)

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}

	// The newest analysis in the fixture is time21_5 2026-09-08T21:47Z.
	at := time.Date(2026, 9, 8, 21, 47, 0, 0, time.UTC)

	speed := wantGauge(t, batch.Samples, metric.CMESpeed, 365, "most_accurate")
	if !speed.Time.Equal(at) {
		t.Errorf("CME speed timestamped %s, want the analysis's own time21_5 %s", speed.Time, at)
	}
	wantGauge(t, batch.Samples, metric.CMEHalfAngle, 21, "most_accurate")
	wantGauge(t, batch.Samples, metric.CMESourceLatitude, 19)
	wantGauge(t, batch.Samples, metric.CMESourceLongitude, 35)

	// Exactly one accuracy series, not one per analysis in the window.
	speeds := 0
	for _, s := range batch.Samples {
		if s.Desc == metric.CMESpeed {
			speeds++
		}
	}
	if speeds != 1 {
		t.Errorf("published %d CME speed series, want 1 for the most recent analysis", speeds)
	}
}

func TestTheAccuracyLabelComesFromIsMostAccurate(t *testing.T) {
	ts := donkiServer(t, func(w http.ResponseWriter, r *http.Request) bool {
		if trimBase(r.URL.Path) != pathCMEAnalysis {
			return false
		}
		_, _ = w.Write([]byte(`[{"time21_5":"2026-09-08T22:00Z","speed":900.0,` +
			`"halfAngle":45.0,"latitude":-3.0,"longitude":12.0,"isMostAccurate":false}]`))
		return true
	})
	src := newTestSource(t, ts, 72*time.Hour)

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	wantGauge(t, batch.Samples, metric.CMESpeed, 900, "other")
	if _, ok := find(batch.Samples, metric.CMESpeed, "most_accurate"); ok {
		t.Error("a preliminary analysis was labelled most_accurate")
	}
}

func TestCMEsAreCountedOverTheWindow(t *testing.T) {
	ts := donkiServer(t, nil)

	// The captured window holds seven CMEs, two of which start before noon on
	// 6 September and so fall outside a 72-hour window ending at the frozen
	// clock. The request has to ask for whole days, so this also proves events
	// outside the window are filtered after parsing rather than trusted from
	// the response.
	src := newTestSource(t, ts, 72*time.Hour)
	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	wantGauge(t, batch.Samples, metric.CMEEventCount, 5)

	wide := newTestSource(t, ts, 120*time.Hour)
	batch, err = wide.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll with a wider window: %v", err)
	}
	wantGauge(t, batch.Samples, metric.CMEEventCount, 7)
}

func TestFlaresAreCountedByClassLetter(t *testing.T) {
	ts := donkiServer(t, nil)

	// The captured flares are C5.0 at 2026-09-06T10:42Z and C2.4 at
	// 2026-09-08T12:24Z. Only the second is inside a 72-hour window.
	src := newTestSource(t, ts, 72*time.Hour)
	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	wantGauge(t, batch.Samples, metric.FlareEventCount, 1, "C")

	wide := newTestSource(t, ts, 120*time.Hour)
	batch, err = wide.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll with a wider window: %v", err)
	}
	wantGauge(t, batch.Samples, metric.FlareEventCount, 2, "C")
}

// TestAQuietClassPublishesZeroRatherThanNoSeries covers the choice to always
// emit every class. A missing series reads as a broken exporter on a dashboard;
// a zero reads as "no X-class flares this week", which is the true statement.
func TestAQuietClassPublishesZeroRatherThanNoSeries(t *testing.T) {
	ts := donkiServer(t, nil)
	src := newTestSource(t, ts, 72*time.Hour)

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	for _, class := range []string{"B", "M", "X"} {
		wantGauge(t, batch.Samples, metric.FlareEventCount, 0, class)
	}
}

func TestAClassLetterOutsideTheUsualFourIsStillPublished(t *testing.T) {
	ts := donkiServer(t, func(w http.ResponseWriter, r *http.Request) bool {
		if trimBase(r.URL.Path) != pathFLR {
			return false
		}
		_, _ = w.Write([]byte(`[{"flrID":"x","beginTime":"2026-09-08T01:00Z",` +
			`"peakTime":"2026-09-08T01:05Z","classType":"A9.1"}]`))
		return true
	})
	src := newTestSource(t, ts, 72*time.Hour)

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	wantGauge(t, batch.Samples, metric.FlareEventCount, 1, "A")
}

func TestAFlareWithNoPeakTimeFallsBackToItsBeginTime(t *testing.T) {
	ts := donkiServer(t, func(w http.ResponseWriter, r *http.Request) bool {
		if trimBase(r.URL.Path) != pathFLR {
			return false
		}
		_, _ = w.Write([]byte(`[{"flrID":"in-progress","beginTime":"2026-09-09T11:50Z",` +
			`"peakTime":"","classType":"M1.2"}]`))
		return true
	})
	src := newTestSource(t, ts, 72*time.Hour)

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	wantGauge(t, batch.Samples, metric.FlareEventCount, 1, "M")
}

// TestAnEmptyWindowProducesNoEventSamplesAndNoError covers the two-byte "[]"
// DONKI answers a quiet window with. It is a success, not a failure — the Sun
// is allowed to be quiet.
func TestAnEmptyWindowProducesNoEventSamplesAndNoError(t *testing.T) {
	ts := donkiServer(t, func(w http.ResponseWriter, r *http.Request) bool {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(fixture(t, "empty.json"))
		return true
	})
	src := newTestSource(t, ts, 72*time.Hour)

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: an empty catalogue window is not a failure, got %v", err)
	}

	// The counts are still published, at zero, because zero is the answer.
	wantGauge(t, batch.Samples, metric.CMEEventCount, 0)
	for _, class := range flareClasses {
		wantGauge(t, batch.Samples, metric.FlareEventCount, 0, class)
	}
	// Nothing that requires an actual event is published.
	if _, ok := find(batch.Samples, metric.CMESpeed, "most_accurate"); ok {
		t.Error("published a CME speed with no CME in the window")
	}
	if _, ok := find(batch.Samples, metric.SourceDataAge, Name); ok {
		t.Error("published a data age with no event timestamp to measure it from")
	}
}

func TestTheWindowBoundariesAppearInTheQueryString(t *testing.T) {
	rec := newRecorder()
	ts := donkiServer(t, func(w http.ResponseWriter, r *http.Request) bool {
		rec.note(r)
		return false
	})
	src := newTestSource(t, ts, 72*time.Hour)

	if _, err := src.Poll(context.Background()); err != nil {
		t.Fatalf("Poll: %v", err)
	}

	for _, path := range []string{pathCME, pathCMEAnalysis, pathFLR} {
		raw, ok := rec.rawQuery(basePath + path)
		if !ok {
			t.Errorf("endpoint %s was never requested", path)
			continue
		}
		values, err := url.ParseQuery(raw)
		if err != nil {
			t.Errorf("endpoint %s query %q is unparseable: %v", path, raw, err)
			continue
		}
		// 72 hours before noon on 9 September is noon on 6 September, and
		// endDate is tomorrow so an event catalogued minutes ago is inside it.
		if got, want := values.Get("startDate"), "2026-09-06"; got != want {
			t.Errorf("endpoint %s startDate = %q, want %q", path, got, want)
		}
		if got, want := values.Get("endDate"), "2026-09-10"; got != want {
			t.Errorf("endpoint %s endDate = %q, want %q", path, got, want)
		}
	}
}

func TestTheNotificationsEndpointIsNeverRequested(t *testing.T) {
	rec := newRecorder()
	ts := donkiServer(t, func(w http.ResponseWriter, r *http.Request) bool {
		rec.note(r)
		return false
	})
	src := newTestSource(t, ts, 72*time.Hour)

	if _, err := src.Poll(context.Background()); err != nil {
		t.Fatalf("Poll: %v", err)
	}
	// SWPC alerts are already published by the swpc source; fetching DONKI's
	// copy would give two disagreeing entries per alert.
	for _, unwanted := range []string{"/notifications", "/GST", "/SEP", "/IPS"} {
		if _, ok := rec.rawQuery(basePath + unwanted); ok {
			t.Errorf("requested %s, which this source must not publish", unwanted)
		}
	}
}

func TestSourceDataAgeComesFromTheNewestEventInThePayload(t *testing.T) {
	ts := donkiServer(t, nil)
	src := newTestSource(t, ts, 72*time.Hour)

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}

	// The newest timestamp anywhere in the window is the 21:47Z analysis on
	// 8 September, 14h13m before the frozen clock.
	newest := time.Date(2026, 9, 8, 21, 47, 0, 0, time.UTC)
	want := fixtureTime.Sub(newest).Seconds()

	got, ok := find(batch.Samples, metric.SourceDataAge, Name)
	if !ok {
		t.Fatalf("no %s sample", metric.SourceDataAge.FullName())
	}
	if got.Value != want {
		t.Errorf("data age = %v seconds, want %v", got.Value, want)
	}
	if !got.Time.Equal(newest) {
		t.Errorf("data age timestamped %s, want the newest event time %s", got.Time, newest)
	}
}

func TestMalformedJSONIsAnErrorRatherThanAPanic(t *testing.T) {
	cases := map[string]string{
		"an HTML error page from an intermediary": "<html><body>502 Bad Gateway</body></html>",
		"a truncated array":                       `[{"activityID":"x","startTime":"2026-09-08T13:59Z"`,
		"an object where an array belongs":        `{"error":"nope"}`,
		"an empty body":                           "",
	}

	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			ts := donkiServer(t, func(w http.ResponseWriter, r *http.Request) bool {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(body))
				return true
			})
			src := newTestSource(t, ts, 72*time.Hour)

			batch, err := src.Poll(context.Background())
			if err == nil {
				t.Fatalf("Poll succeeded on a malformed body; got %d samples", len(batch.Samples))
			}
			if len(batch.Samples) != 0 {
				t.Errorf("published %d samples from a malformed body, want 0", len(batch.Samples))
			}
		})
	}
}

func TestOneFailingEndpointDoesNotCostThePollTheRest(t *testing.T) {
	ts := donkiServer(t, func(w http.ResponseWriter, r *http.Request) bool {
		if trimBase(r.URL.Path) != pathCMEAnalysis {
			return false
		}
		w.WriteHeader(http.StatusInternalServerError)
		return true
	})
	src := newTestSource(t, ts, 72*time.Hour)

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: one bad endpoint must not fail the poll, got %v", err)
	}
	wantGauge(t, batch.Samples, metric.FlareEventCount, 1, "C")
	wantGauge(t, batch.Samples, metric.CMEEventCount, 5)
	if _, ok := find(batch.Samples, metric.CMESpeed, "most_accurate"); ok {
		t.Error("published a CME speed although the analysis endpoint failed")
	}
}

func TestA429IsReportedAsRateLimited(t *testing.T) {
	ts := donkiServer(t, func(w http.ResponseWriter, r *http.Request) bool {
		w.Header().Set("Retry-After", "600")
		w.WriteHeader(http.StatusTooManyRequests)
		return true
	})
	src := newTestSource(t, ts, 72*time.Hour)

	// The shared client pauses the whole host after a 429, so an endpoint that
	// had not yet been dispatched waits on that pause. The deadline keeps the
	// test quick; what is asserted is the classification, not the timing.
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	batch, err := src.Poll(ctx)
	if !errors.Is(err, source.ErrRateLimited) {
		t.Fatalf("Poll error = %v, want source.ErrRateLimited", err)
	}
	if len(batch.Samples) != 0 {
		t.Errorf("published %d samples while rate limited, want 0", len(batch.Samples))
	}
}

func TestACancelledContextStopsThePollBeforeAnyRequest(t *testing.T) {
	var requests atomic.Int64
	ts := donkiServer(t, func(w http.ResponseWriter, r *http.Request) bool {
		requests.Add(1)
		return false
	})
	src := newTestSource(t, ts, 72*time.Hour)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := src.Poll(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("Poll error = %v, want context.Canceled", err)
	}
	if got := requests.Load(); got != 0 {
		t.Errorf("made %d requests after the context was cancelled, want 0", got)
	}
}

func TestTheIntervalFloorIsEnforced(t *testing.T) {
	src, err := New(config.DONKI{
		Enabled:  true,
		Interval: time.Minute,
	}, testHTTPClient(), discardLogger())
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if src.Interval() != minInterval {
		t.Errorf("Interval() = %s, want the floor %s", src.Interval(), minInterval)
	}
	if got := src.Schedule().Interval(); got < minInterval {
		t.Errorf("Schedule().Interval() = %s, below the floor %s", got, minInterval)
	}

	// The floor also has to survive an operator who configures a sane interval
	// and expects the schedule to honour it.
	at := time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)
	if next := src.Schedule().NextAfter(at); next.Sub(at) < minInterval {
		t.Errorf("next poll is %s after the last, below the floor %s", next.Sub(at), minInterval)
	}
}

func TestNewDefaultsAndValidation(t *testing.T) {
	t.Run("an empty base URL falls back to the key-free CCMC origin", func(t *testing.T) {
		src, err := New(config.DONKI{Enabled: true}, testHTTPClient(), discardLogger())
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		if src.baseURL != defaultBaseURL {
			t.Errorf("baseURL = %q, want %q", src.baseURL, defaultBaseURL)
		}
	})

	t.Run("an unset interval defaults to thirty minutes", func(t *testing.T) {
		src, err := New(config.DONKI{Enabled: true}, testHTTPClient(), discardLogger())
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		if src.Interval() != defaultInterval {
			t.Errorf("Interval() = %s, want %s", src.Interval(), defaultInterval)
		}
	})

	t.Run("an unset window defaults to seventy-two hours", func(t *testing.T) {
		src, err := New(config.DONKI{Enabled: true}, testHTTPClient(), discardLogger())
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		if src.Window() != defaultWindow {
			t.Errorf("Window() = %s, want %s", src.Window(), defaultWindow)
		}
	})

	t.Run("a window shorter than the curation lag is widened", func(t *testing.T) {
		src, err := New(config.DONKI{
			Enabled: true,
			Window:  time.Minute,
		}, testHTTPClient(), discardLogger())
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		if src.Window() != minWindow {
			t.Errorf("Window() = %s, want the minimum %s", src.Window(), minWindow)
		}
	})

	t.Run("an enormous window is narrowed", func(t *testing.T) {
		src, err := New(config.DONKI{
			Enabled: true,
			Window:  365 * 24 * time.Hour,
		}, testHTTPClient(), discardLogger())
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		if src.Window() != maxWindow {
			t.Errorf("Window() = %s, want the maximum %s", src.Window(), maxWindow)
		}
	})

	t.Run("a base URL that is not http or https is rejected", func(t *testing.T) {
		if _, err := New(config.DONKI{
			Enabled: true,
			BaseURL: "gopher://webtools.ccmc.gsfc.nasa.gov/DONKI/WS/get",
		}, testHTTPClient(), discardLogger()); err == nil {
			t.Error("New accepted a gopher base URL")
		}
	})

	t.Run("a nil shared client is rejected", func(t *testing.T) {
		if _, err := New(config.DONKI{Enabled: true}, nil, discardLogger()); err == nil {
			t.Error("New accepted a nil httpx client; every fetch must go through the shared one")
		}
	})

	t.Run("the default base URL is the CCMC origin rather than api.nasa.gov", func(t *testing.T) {
		// api.nasa.gov requires a key and its demo key measured ten requests
		// an hour. Defaulting there would throttle the exporter permanently.
		if !strings.Contains(defaultBaseURL, "webtools.ccmc.gsfc.nasa.gov") {
			t.Errorf("defaultBaseURL = %q, want the CCMC origin", defaultBaseURL)
		}
		if strings.Contains(defaultBaseURL, "api.nasa.gov") {
			t.Errorf("defaultBaseURL = %q, which needs an API key and is rate limited", defaultBaseURL)
		}
	})
}

func TestNameAndInterfaceCompliance(t *testing.T) {
	ts := donkiServer(t, nil)
	src := newTestSource(t, ts, 72*time.Hour)

	if src.Name() != Name {
		t.Errorf("Name() = %q, want %q", src.Name(), Name)
	}
	if src.Schedule() == nil {
		t.Error("Schedule() returned nil, which would fall back to a bare interval")
	}
}

func TestFlareClassReadsOnlyTheLeadingLetter(t *testing.T) {
	cases := map[string]struct {
		want string
		ok   bool
	}{
		"C2.4":  {"C", true},
		"x9.3":  {"X", true},
		"M1":    {"M", true},
		" b7.0": {"B", true},
		"":      {"", false},
		"Z1.0":  {"", false},
		"1.0":   {"", false},
	}
	for in, want := range cases {
		got, ok := flareClass(in)
		if ok != want.ok || got != want.want {
			t.Errorf("flareClass(%q) = (%q, %v), want (%q, %v)", in, got, ok, want.want, want.ok)
		}
	}
}

func TestParseDONKITimeAcceptsTheFormsTheCatalogueUses(t *testing.T) {
	for _, raw := range []string{
		"2026-09-08T21:47Z",
		"2026-09-08T21:47:00Z",
		"2026-08-10T12:34:00-FLR",
	} {
		_, err := parseDONKITime(raw)
		switch raw {
		case "2026-08-10T12:34:00-FLR":
			if err == nil {
				t.Errorf("parseDONKITime(%q) succeeded; an event id is not a timestamp", raw)
			}
		default:
			if err != nil {
				t.Errorf("parseDONKITime(%q): %v", raw, err)
			}
		}
	}
}
