package iswa

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

// fixtureTime is when the responses in testdata were captured live from
// iswa.gsfc.nasa.gov. Every test freezes the clock a few minutes after it, so
// the trailing windows and the freshness metric are measured against the
// fixtures rather than against the day the test happens to run.
var fixtureTime = time.Date(2026, 9, 9, 15, 45, 0, 0, time.UTC)

// fixture reads a captured response.
func fixture(t *testing.T, name string) []byte {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("reading fixture %s: %v", name, err)
	}
	return body
}

// discardLogger keeps test output readable; these sources log warnings by
// design and the tests assert on behaviour, not on log lines.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
}

// testHTTPClient returns a shared client with the politeness spacing turned
// down to a millisecond.
//
// The spacing is the point of httpx in production and must not be bypassed
// there, but a test server on 127.0.0.1 falls to the fallback policy of thirty
// seconds per request, which would make a four-dataset poll take two minutes.
func testHTTPClient() *httpx.Client {
	return httpx.New(httpx.Options{
		Timeout:  5 * time.Second,
		Fallback: httpx.HostPolicy{MinInterval: time.Millisecond, Burst: 64},
	}, discardLogger())
}

// recorder collects what the test server was asked for. Poll fetches datasets
// concurrently, so this is behind a mutex.
type recorder struct {
	mu       sync.Mutex
	requests map[string]int
	query    map[string]string
}

func newRecorder() *recorder {
	return &recorder{requests: make(map[string]int), query: make(map[string]string)}
}

func (r *recorder) note(req *http.Request) {
	id := req.URL.Query().Get("id")
	if id == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.requests[id]++
	r.query[id] = req.URL.RawQuery
}

func (r *recorder) count(id string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.requests[id]
}

func (r *recorder) rawQuery(id string) (string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	q, ok := r.query[id]
	return q, ok
}

// hapiServer answers /data by serving testdata/<id>.csv for the requested
// dataset id. override, when it returns true, has already written the response.
func hapiServer(t *testing.T, override func(w http.ResponseWriter, r *http.Request) bool) *httptest.Server {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if override != nil && override(w, r) {
			return
		}
		if r.URL.Path != "/hapi"+pathData {
			http.NotFound(w, r)
			return
		}
		id := r.URL.Query().Get("id")
		body, err := os.ReadFile(filepath.Join("testdata", id+".csv"))
		if err != nil {
			// This is what the live server does for a window it has no rows
			// for: HTTP 404 with a HAPI 1406 status in the body.
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write(fixture(t, "no_data_1406.json"))
			return
		}
		w.Header().Set("Content-Type", "text/csv")
		_, _ = w.Write(body)
	}))
	t.Cleanup(ts.Close)
	return ts
}

// newTestSource builds a source against ts with a frozen clock.
func newTestSource(t *testing.T, ts *httptest.Server, craft ...string) *Source {
	t.Helper()
	src, err := New(config.ISWA{
		Enabled:      true,
		BaseURL:      ts.URL + "/hapi",
		FastInterval: time.Minute,
		Spacecraft:   craft,
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
		if s.Desc != desc {
			continue
		}
		if len(labels) != len(s.Labels) {
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

// wantGauge asserts a descriptor was published with a value and a timestamp.
func wantGauge(t *testing.T, samples []metric.Sample, desc *metric.Descriptor, value float64, at time.Time, labels ...string) {
	t.Helper()
	got, ok := find(samples, desc, labels...)
	if !ok {
		t.Fatalf("no sample for %s%v", desc.FullName(), labels)
	}
	if got.Value != value {
		t.Errorf("%s%v = %v, want %v", desc.FullName(), labels, got.Value, value)
	}
	if !got.Time.Equal(at) {
		t.Errorf("%s%v timestamped %s, want %s", desc.FullName(), labels, got.Time, at)
	}
}

func TestPollPublishesTheNewestRowOfEveryDataset(t *testing.T) {
	ts := hapiServer(t, nil)
	src := newTestSource(t, ts, "swpc")

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if batch.Source != Name {
		t.Errorf("batch source = %q, want %q", batch.Source, Name)
	}

	magAt := time.Date(2026, 9, 9, 15, 40, 0, 0, time.UTC)
	plasmaAt := time.Date(2026, 9, 9, 15, 39, 0, 0, time.UTC)

	wantGauge(t, batch.Samples, metric.WindMagneticField, 7.09, magAt, "bt")
	wantGauge(t, batch.Samples, metric.WindMagneticField, 4.77, magAt, "bz")
	wantGauge(t, batch.Samples, metric.WindSpeed, 448.2, plasmaAt)
	wantGauge(t, batch.Samples, metric.WindDensity, 5.16, plasmaAt)
	wantGauge(t, batch.Samples, metric.DstNanotesla, -18,
		time.Date(2026, 9, 9, 15, 0, 0, 0, time.UTC))
	wantGauge(t, batch.Samples, metric.KIndexHalfHourly, 3,
		time.Date(2026, 9, 9, 15, 15, 0, 0, time.UTC), "30m")
	wantGauge(t, batch.Samples, metric.APIndex, 15,
		time.Date(2026, 9, 9, 15, 15, 0, 0, time.UTC), "30m")
}

func TestSourceDataAgeIsMeasuredFromTheNewestUpstreamTimestamp(t *testing.T) {
	ts := hapiServer(t, nil)
	src := newTestSource(t, ts, "swpc")

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}

	// The newest row in any fixture is the magnetic field at 15:40:00Z, five
	// minutes before the frozen clock.
	got, ok := find(batch.Samples, metric.SourceDataAge, Name)
	if !ok {
		t.Fatalf("no %s sample", metric.SourceDataAge.FullName())
	}
	if got.Value != 300 {
		t.Errorf("data age = %v seconds, want 300", got.Value)
	}
}

// TestAHAPIStatus1411InsideAnHTTP200IsAnError is the most important test in
// this package. HAPI permits a server to report its own errors under HTTP 200,
// so a client that branches on the HTTP status alone parses an error document
// as data and publishes nothing while reporting success.
//
// The 1411 fixture is hand-written rather than captured: probing the live
// server found that a mis-ordered parameters argument produced something worse
// than 1411 — a 200 with the columns silently returned in the declared order —
// which is why this package never sends a parameters argument at all. The
// status handling is still required, because the HAPI specification permits
// this shape and other servers use it.
func TestAHAPIStatus1411InsideAnHTTP200IsAnError(t *testing.T) {
	ts := hapiServer(t, func(w http.ResponseWriter, r *http.Request) bool {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(fixture(t, "error_1411.json"))
		return true
	})
	src := newTestSource(t, ts, "swpc")

	batch, err := src.Poll(context.Background())
	if err == nil {
		t.Fatalf("Poll succeeded on a HAPI 1411 error; got %d samples", len(batch.Samples))
	}
	if !strings.Contains(err.Error(), "1411") {
		t.Errorf("error does not name the HAPI code: %v", err)
	}
	if !strings.Contains(err.Error(), "Parameter out of order") {
		t.Errorf("error does not carry the server's message: %v", err)
	}
	if len(batch.Samples) != 0 {
		t.Errorf("published %d samples from an error document, want 0", len(batch.Samples))
	}
}

func TestAHAPIStatus1201IsAnEmptyBatchAndNotAnError(t *testing.T) {
	ts := hapiServer(t, func(w http.ResponseWriter, r *http.Request) bool {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(fixture(t, "status_1201.json"))
		return true
	})
	src := newTestSource(t, ts, "swpc")

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll reported an error for an empty time range: %v", err)
	}
	if len(batch.Samples) != 0 {
		t.Errorf("published %d samples for an empty time range, want 0", len(batch.Samples))
	}
}

// TestDuplicateColumnNamesAreTolerated covers imap_mag, whose /info declares
// its Time parameter twice — a live server-side bug. The dataset is a working
// four-second magnetometer and must not be lost to a cosmetic defect.
func TestDuplicateColumnNamesAreTolerated(t *testing.T) {
	ts := hapiServer(t, nil)
	src := newTestSource(t, ts, "imap")

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}

	at := time.Date(2026, 9, 9, 15, 43, 32, 0, time.UTC)
	wantGauge(t, batch.Samples, metric.WindMagneticField, 6.186, at, "bt")
	wantGauge(t, batch.Samples, metric.WindMagneticField, 2.999, at, "bz")
}

func TestTheRequestedWindowIsShortAndNamesNoParameters(t *testing.T) {
	rec := newRecorder()
	ts := hapiServer(t, func(w http.ResponseWriter, r *http.Request) bool {
		rec.note(r)
		return false
	})
	src := newTestSource(t, ts, "swpc", "imap")

	if _, err := src.Poll(context.Background()); err != nil {
		t.Fatalf("Poll: %v", err)
	}

	// A trailing window of at most a few multiples of the dataset's cadence.
	// The whole point is not to request a day of one-minute data to read one
	// value, so a generous ceiling still catches that mistake.
	wantWindow := map[string]time.Duration{
		"swpc_rtsw_mag_P1M":       10 * time.Minute,
		"swpc_rtsw_plasma_P1M":    10 * time.Minute,
		"imap_mag":                2 * time.Minute,
		"dst_quicklook":           6 * time.Hour,
		"gfz_obs_geo_30m_indices": 3 * time.Hour,
	}

	for id, want := range wantWindow {
		raw, ok := rec.rawQuery(id)
		if !ok {
			t.Errorf("dataset %s was never requested", id)
			continue
		}
		values, err := url.ParseQuery(raw)
		if err != nil {
			t.Errorf("dataset %s query %q is unparseable: %v", id, raw, err)
			continue
		}
		min, err := time.Parse(time.RFC3339, values.Get("time.min"))
		if err != nil {
			t.Errorf("dataset %s time.min %q is not RFC3339: %v", id, values.Get("time.min"), err)
			continue
		}
		max, err := time.Parse(time.RFC3339, values.Get("time.max"))
		if err != nil {
			t.Errorf("dataset %s time.max %q is not RFC3339: %v", id, values.Get("time.max"), err)
			continue
		}
		if got := max.Sub(min); got != want {
			t.Errorf("dataset %s requested a %s window, want %s", id, got, want)
		}
		if !max.Equal(fixtureTime) {
			t.Errorf("dataset %s requested time.max %s, want the current instant %s", id, max, fixtureTime)
		}
		if values.Has("parameters") {
			t.Errorf("dataset %s request sent a parameters argument, which this package must never do: %s", id, raw)
		}
		if values.Get("include") != "header" {
			t.Errorf("dataset %s request omitted include=header, so the columns cannot be named: %s", id, raw)
		}
		if values.Get("format") != "csv" {
			t.Errorf("dataset %s requested format %q, want csv", id, values.Get("format"))
		}
	}

	// imap_mag publishes every four seconds. Two minutes is thirty rows; ten
	// minutes would be a hundred and fifty, of which one is used.
	if got := wantWindow["imap_mag"]; got > 2*time.Minute {
		t.Errorf("imap_mag window is %s, which at a four-second cadence is a lot of rows to discard", got)
	}
}

// TestOnlyThePrimaryMonitorIsPublished checks the isPrimary tie-break. The
// wind descriptors carry no spacecraft label, so exactly one monitor may be
// published and it must be the one SWPC is currently using.
func TestOnlyThePrimaryMonitorIsPublished(t *testing.T) {
	ts := hapiServer(t, nil)
	// ACE is listed first deliberately: configuration order must lose to the
	// isPrimary flag, not win.
	src := newTestSource(t, ts, "ace", "swpc")

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}

	// In the captured fixtures SWPC reports isPrimary=1 and ACE reports 0.
	wantGauge(t, batch.Samples, metric.WindMagneticField, 7.09,
		time.Date(2026, 9, 9, 15, 40, 0, 0, time.UTC), "bt")
	wantGauge(t, batch.Samples, metric.WindSpeed, 448.2,
		time.Date(2026, 9, 9, 15, 39, 0, 0, time.UTC))

	// One series per component, not one per spacecraft.
	bt := 0
	for _, s := range batch.Samples {
		if s.Desc == metric.WindMagneticField && s.Labels[0] == "bt" {
			bt++
		}
	}
	if bt != 1 {
		t.Errorf("published %d bt series, want exactly 1; the descriptor has no spacecraft label to separate them", bt)
	}
}

func TestAMonitorWithNoPrimaryFlagFallsBackToConfigurationOrder(t *testing.T) {
	ts := hapiServer(t, nil)
	src := newTestSource(t, ts, "imap")

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if _, ok := find(batch.Samples, metric.WindMagneticField, "bt"); !ok {
		t.Error("imap_mag carries no isPrimary flag and was dropped rather than used as the only choice")
	}
}

// TestA404OnTheDataEndpointIsNotRetriedAndReportsNoData covers how this server
// signals an empty trailing window: HTTP 404 carrying HAPI status 1406.
func TestA404OnTheDataEndpointIsNotRetriedAndReportsNoData(t *testing.T) {
	var requests atomic.Int64
	ts := hapiServer(t, func(w http.ResponseWriter, r *http.Request) bool {
		requests.Add(1)
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write(fixture(t, "no_data_1406.json"))
		return true
	})

	src, err := New(config.ISWA{
		Enabled:      true,
		BaseURL:      ts.URL + "/hapi",
		FastInterval: time.Minute,
		Spacecraft:   []string{"swpc"},
		Retries:      3,
	}, testHTTPClient(), discardLogger())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	src.now = func() time.Time { return fixtureTime }

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: an empty window is not a failure, got %v", err)
	}
	if len(batch.Samples) != 0 {
		t.Errorf("published %d samples from an empty window, want 0", len(batch.Samples))
	}
	if got, want := requests.Load(), int64(len(src.datasets)); got != want {
		t.Errorf("made %d requests for %d datasets; a 404 must not be retried", got, want)
	}
}

func TestA429IsReportedAsRateLimited(t *testing.T) {
	ts := hapiServer(t, func(w http.ResponseWriter, r *http.Request) bool {
		w.Header().Set("Retry-After", "600")
		w.WriteHeader(http.StatusTooManyRequests)
		return true
	})
	src := newTestSource(t, ts, "swpc")

	// The shared client pauses the whole host after a 429, so the datasets
	// that had not yet been dispatched wait on that pause. A deadline keeps the
	// test quick; the outcome asserted is the classification, not the timing.
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

func TestMalformedCSVIsAnErrorRatherThanAPanic(t *testing.T) {
	bodies := map[string]string{
		"a row with too few fields": "#{\"status\":{\"code\":1200,\"message\":\"OK\"}," +
			"\"parameters\":[{\"name\":\"Time\"},{\"name\":\"B_t\"},{\"name\":\"B_z\"}]}\n" +
			"2026-09-09T15:40:00Z,7.09\n",
		"an unterminated quoted field": "#{\"status\":{\"code\":1200,\"message\":\"OK\"}," +
			"\"parameters\":[{\"name\":\"Time\"},{\"name\":\"B_t\"}]}\n" +
			"2026-09-09T15:40:00Z,\"7.09\n",
		"CSV with no header at all": "2026-09-09T15:40:00Z,7.09,4.77\n",
		"a header that is not JSON": "#not json at all\n2026-09-09T15:40:00Z,7.09\n",
		"rows whose time column is unreadable": "#{\"status\":{\"code\":1200,\"message\":\"OK\"}," +
			"\"parameters\":[{\"name\":\"Time\"},{\"name\":\"B_t\"}]}\n" +
			"not-a-time,7.09\n",
	}

	for name, body := range bodies {
		t.Run(name, func(t *testing.T) {
			ts := hapiServer(t, func(w http.ResponseWriter, r *http.Request) bool {
				_, _ = w.Write([]byte(body))
				return true
			})
			src := newTestSource(t, ts, "swpc")

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

func TestACancelledContextStopsThePollBeforeAnyRequest(t *testing.T) {
	var requests atomic.Int64
	ts := hapiServer(t, func(w http.ResponseWriter, r *http.Request) bool {
		requests.Add(1)
		return false
	})
	src := newTestSource(t, ts, "swpc")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := src.Poll(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("Poll error = %v, want context.Canceled", err)
	}
	if got := requests.Load(); got != 0 {
		t.Errorf("made %d requests after the context was cancelled, want 0", got)
	}
}

func TestADatasetIsNotRefetchedInsideItsOwnPublicationCadence(t *testing.T) {
	rec := newRecorder()
	ts := hapiServer(t, func(w http.ResponseWriter, r *http.Request) bool {
		rec.note(r)
		return false
	})
	src := newTestSource(t, ts, "swpc")

	clock := fixtureTime
	src.now = func() time.Time { return clock }

	if _, err := src.Poll(context.Background()); err != nil {
		t.Fatalf("first Poll: %v", err)
	}
	// Two minutes later the one-minute solar wind has new rows; the hourly Dst
	// and the half-hourly indices do not.
	clock = clock.Add(2 * time.Minute)
	if _, err := src.Poll(context.Background()); err != nil {
		t.Fatalf("second Poll: %v", err)
	}

	if got := rec.count("swpc_rtsw_mag_P1M"); got != 2 {
		t.Errorf("one-minute magnetic field fetched %d times over two minutes, want 2", got)
	}
	if got := rec.count("dst_quicklook"); got != 1 {
		t.Errorf("hourly Dst fetched %d times over two minutes, want 1", got)
	}
	if got := rec.count("gfz_obs_geo_30m_indices"); got != 1 {
		t.Errorf("half-hourly indices fetched %d times over two minutes, want 1", got)
	}
}

func TestAPollWithNothingDueIsAnEmptySuccess(t *testing.T) {
	ts := hapiServer(t, nil)
	src := newTestSource(t, ts, "swpc")

	if _, err := src.Poll(context.Background()); err != nil {
		t.Fatalf("first Poll: %v", err)
	}
	// The clock has not moved, so no dataset has published anything new.
	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("second Poll: %v", err)
	}
	if len(batch.Samples) != 0 {
		t.Errorf("second Poll produced %d samples with nothing due, want 0", len(batch.Samples))
	}
	if batch.Fetched.IsZero() {
		t.Error("an empty poll still records when it ran")
	}
}

func TestOneFailingDatasetDoesNotCostThePollTheRest(t *testing.T) {
	ts := hapiServer(t, func(w http.ResponseWriter, r *http.Request) bool {
		if r.URL.Query().Get("id") != "dst_quicklook" {
			return false
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(fixture(t, "error_1411.json"))
		return true
	})
	src := newTestSource(t, ts, "swpc")

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: one bad dataset must not fail the poll, got %v", err)
	}
	if _, ok := find(batch.Samples, metric.WindSpeed); !ok {
		t.Error("the solar wind was dropped because the Dst dataset failed")
	}
	if _, ok := find(batch.Samples, metric.DstNanotesla); ok {
		t.Error("published a Dst value from an error document")
	}
}

func TestNewDefaultsAndValidation(t *testing.T) {
	t.Run("an empty base URL falls back to the public HAPI endpoint", func(t *testing.T) {
		src, err := New(config.ISWA{Enabled: true}, testHTTPClient(), discardLogger())
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		if src.baseURL != defaultBaseURL {
			t.Errorf("baseURL = %q, want %q", src.baseURL, defaultBaseURL)
		}
	})

	t.Run("no spacecraft configured selects the SWPC real-time product alone", func(t *testing.T) {
		src, err := New(config.ISWA{Enabled: true}, testHTTPClient(), discardLogger())
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		for _, d := range src.datasets {
			if d.craft != "" && d.craft != "swpc" {
				t.Errorf("selected %s for spacecraft %q, want only swpc and the geomagnetic datasets", d.id, d.craft)
			}
		}
		if src.multiCraft {
			t.Error("one monitor was configured but the source thinks several were")
		}
	})

	t.Run("more than one monitor is recorded so the warning can be issued", func(t *testing.T) {
		src, err := New(config.ISWA{
			Enabled:    true,
			Spacecraft: []string{"swpc", "ace"},
		}, testHTTPClient(), discardLogger())
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		if !src.multiCraft {
			t.Error("two monitors were configured but only-the-primary was not flagged")
		}
	})

	t.Run("aliases for the SWPC product are not counted as separate monitors", func(t *testing.T) {
		src, err := New(config.ISWA{
			Enabled:    true,
			Spacecraft: []string{"swpc", "dscovr"},
		}, testHTTPClient(), discardLogger())
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		if src.multiCraft {
			t.Error("swpc and dscovr name the same product and must not trip the multi-monitor warning")
		}
	})

	t.Run("an unknown spacecraft is ignored rather than fatal", func(t *testing.T) {
		src, err := New(config.ISWA{
			Enabled:    true,
			Spacecraft: []string{"voyager"},
		}, testHTTPClient(), discardLogger())
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		// Falls back to the default selection rather than starting with nothing.
		if len(src.datasets) == 0 {
			t.Fatal("no datasets selected")
		}
	})

	t.Run("an interval below the floor is raised", func(t *testing.T) {
		src, err := New(config.ISWA{
			Enabled:      true,
			FastInterval: time.Second,
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
	})

	t.Run("a base URL that is not http or https is rejected", func(t *testing.T) {
		if _, err := New(config.ISWA{
			Enabled: true,
			BaseURL: "ftp://iswa.gsfc.nasa.gov/hapi",
		}, testHTTPClient(), discardLogger()); err == nil {
			t.Error("New accepted an ftp base URL")
		}
	})

	t.Run("a nil shared client is rejected", func(t *testing.T) {
		if _, err := New(config.ISWA{Enabled: true}, nil, discardLogger()); err == nil {
			t.Error("New accepted a nil httpx client; every fetch must go through the shared one")
		}
	})
}

func TestNameAndInterfaceCompliance(t *testing.T) {
	ts := hapiServer(t, nil)
	src := newTestSource(t, ts, "swpc")

	if src.Name() != Name {
		t.Errorf("Name() = %q, want %q", src.Name(), Name)
	}
	if src.Schedule() == nil {
		t.Error("Schedule() returned nil, which would fall back to a bare interval")
	}
}
