package httpserver

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/RealDougEubanks/solarham/internal/config"
	"github.com/RealDougEubanks/solarham/internal/source"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
}

// testConfig is a server configuration with no secrets in it. Tests that care
// about leakage build their own.
func testConfig() config.HTTP {
	return config.HTTP{
		Addr:            "127.0.0.1:0",
		ReadTimeout:     time.Second,
		ShutdownTimeout: time.Second,
	}
}

// freshSource is a source that succeeded a moment ago and is well inside its
// staleness window.
func freshSource(name string) source.Status {
	return source.Status{
		Name:        name,
		Interval:    time.Minute.String(),
		LastSuccess: time.Now().Add(-5 * time.Second),
		Samples:     4,
		Successes:   12,
		StaleAfter:  3 * time.Minute,
	}
}

// staleSource succeeded once, long enough ago to be outside its window.
func staleSource(name string) source.Status {
	st := freshSource(name)
	st.LastSuccess = time.Now().Add(-10 * time.Minute)
	st.LastFailure = time.Now().Add(-time.Minute)
	st.LastError = "unexpected status 503"
	st.Stale = true
	st.Failures = 3
	return st
}

// neverSucceededSource is the state a container is in for its first few
// seconds, and after an upstream that has been down since startup.
func neverSucceededSource(name string) source.Status {
	return source.Status{
		Name:        name,
		Interval:    time.Minute.String(),
		LastFailure: time.Now().Add(-time.Second),
		LastError:   "dial tcp: connection refused",
		Failures:    2,
		Stale:       true,
		StaleAfter:  3 * time.Minute,
	}
}

// newTestServer builds a handler over a fixed set of source statuses.
func newTestServer(t *testing.T, cfg config.HTTP, statuses ...source.Status) http.Handler {
	t.Helper()
	return newTestServerWithDeps(t, cfg, Deps{
		Statuses:  func() []source.Status { return statuses },
		SinkNames: func() []string { return []string{"prometheus", "otlp"} },
		Build:     BuildInfo{Version: "1.2.3", Commit: "abc1234", BuildDate: "2026-09-08T00:00:00Z"},
	})
}

func newTestServerWithDeps(t *testing.T, cfg config.HTTP, deps Deps) http.Handler {
	t.Helper()
	s, err := New(cfg, deps, discardLogger())
	if err != nil {
		t.Fatalf("New returned %v, want nil", err)
	}
	return s.Handler()
}

func get(t *testing.T, h http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec
}

func decode[T any](t *testing.T, rec *httptest.ResponseRecorder) T {
	t.Helper()
	var out T
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decoding the response failed: %v\nbody: %s", err, rec.Body.String())
	}
	return out
}

// assertNoStore checks the header every health endpoint must set. Health state
// is live and instance-specific, so a cached answer is a wrong answer.
func assertNoStore(t *testing.T, rec *httptest.ResponseRecorder) {
	t.Helper()
	if got := rec.Header().Get("Cache-Control"); got != "no-store" {
		t.Errorf("Cache-Control is %q, want %q", got, "no-store")
	}
}

// TestLivenessIsAlwaysOKEvenWhenEverythingElseIsBroken is deliberate behaviour,
// not an oversight. Liveness that depends on a backend turns an upstream outage
// into a restart loop, which is strictly worse than the outage.
func TestLivenessIsAlwaysOKEvenWhenEverythingElseIsBroken(t *testing.T) {
	cases := map[string]http.Handler{
		"every source failing and every sink absent": newTestServerWithDeps(t, testConfig(), Deps{
			Statuses: func() []source.Status {
				return []source.Status{neverSucceededSource("hamqsl"), staleSource("swpc"), staleSource("kc2g")}
			},
			SinkNames: func() []string { return nil },
		}),
		"no scheduler and no sinks wired at all": newTestServerWithDeps(t, testConfig(), Deps{}),
		"a healthy instance":                     newTestServer(t, testConfig(), freshSource("swpc")),
	}

	for name, h := range cases {
		t.Run(name, func(t *testing.T) {
			rec := get(t, h, "/healthz")
			if rec.Code != http.StatusOK {
				t.Fatalf("/healthz = %d, want 200 regardless of any other state", rec.Code)
			}
			assertNoStore(t, rec)
		})
	}
}

func TestReadinessIs503WhenNoSourceHasEverSucceeded(t *testing.T) {
	// A freshly started container must report itself unready until it has
	// actually fetched something.
	h := newTestServer(t, testConfig(),
		neverSucceededSource("swpc"), neverSucceededSource("hamqsl"))

	rec := get(t, h, "/readyz")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("/readyz = %d, want 503", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "no successful poll yet") {
		t.Errorf("/readyz body does not explain the never-succeeded source:\n%s", rec.Body.String())
	}
}

func TestReadinessIs200WhileOneSourceIsStillWarmingUp(t *testing.T) {
	// A source that has not answered yet must not hold the whole exporter
	// unready once anything else is serving: on a cold start the slow-cadence
	// sources can be minutes behind the fast ones, and the instance is
	// perfectly able to serve what it already has.
	h := newTestServer(t, testConfig(), freshSource("swpc"), neverSucceededSource("hamqsl"))

	if rec := get(t, h, "/readyz"); rec.Code != http.StatusOK {
		t.Fatalf("/readyz = %d, want 200 while one source warms up\n%s", rec.Code, rec.Body.String())
	}
}

func TestReadinessIs200WhenEverySourceIsFresh(t *testing.T) {
	h := newTestServer(t, testConfig(), freshSource("swpc"), freshSource("hamqsl"), freshSource("kc2g"))

	rec := get(t, h, "/readyz")
	if rec.Code != http.StatusOK {
		t.Fatalf("/readyz = %d, want 200\n%s", rec.Code, rec.Body.String())
	}
	assertNoStore(t, rec)
	if got := strings.TrimSpace(rec.Body.String()); got != "ready" {
		t.Errorf("/readyz body is %q, want %q", got, "ready")
	}
}

// TestReadinessIgnoresASingleStaleSource pins the reason readiness was
// loosened. Under the old rule a single third-party outage -- celestrak.org
// returning 503 for a day -- pinned /readyz at 503 even though the exporter was
// serving 15 of 16 sources perfectly. Nothing an orchestrator can do fixes a
// dead upstream, so that verdict caused restarts and page noise without ever
// restoring the source.
func TestReadinessIgnoresASingleStaleSource(t *testing.T) {
	h := newTestServer(t, testConfig(), freshSource("swpc"), staleSource("hamqsl"), freshSource("kc2g"))

	if rec := get(t, h, "/readyz"); rec.Code != http.StatusOK {
		t.Fatalf("/readyz = %d, want 200 with one of three sources stale\n%s",
			rec.Code, rec.Body.String())
	}
}

// TestHealthStaysStrictWhenReadinessIsLenient is the other half of that trade:
// loosening readiness must not lose the signal, only move it to the endpoint an
// uptime monitor watches.
func TestHealthStaysStrictWhenReadinessIsLenient(t *testing.T) {
	h := newTestServer(t, testConfig(), freshSource("swpc"), staleSource("hamqsl"))

	if rec := get(t, h, "/readyz"); rec.Code != http.StatusOK {
		t.Errorf("/readyz = %d, want 200", rec.Code)
	}
	rec := get(t, h, "/health")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("/health = %d, want 503 -- the strict signal must survive", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "hamqsl") {
		t.Errorf("/health does not name the stale source:\n%s", rec.Body.String())
	}
}

func TestStrictReadinessPolicyFailsOnASingleStaleSource(t *testing.T) {
	cfg := testConfig()
	cfg.ReadyRequireAll = true
	h := newTestServer(t, cfg, freshSource("swpc"), staleSource("hamqsl"), freshSource("kc2g"))

	rec := get(t, h, "/readyz")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("/readyz = %d, want 503 under the strict policy", rec.Code)
	}
	assertNoStore(t, rec)

	body := rec.Body.String()
	if !strings.Contains(body, "hamqsl") {
		t.Errorf("/readyz body does not name the stale source:\n%s", body)
	}
	// The healthy sources must not be reported as stale, or an operator is
	// sent to look at the wrong upstream.
	for _, healthy := range []string{"swpc", "kc2g"} {
		if strings.Contains(body, healthy) {
			t.Errorf("/readyz body names the healthy source %q:\n%s", healthy, body)
		}
	}
}

func TestReadinessIs503WhenNoSourcesAreEnabled(t *testing.T) {
	// An exporter with nothing to poll can never produce data. A green probe
	// there would hide a misconfiguration.
	h := newTestServer(t, testConfig())

	rec := get(t, h, "/readyz")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("/readyz = %d, want 503", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "no sources") {
		t.Errorf("/readyz body does not say why:\n%s", rec.Body.String())
	}
}

// TestStaleAfterOverridesThePerSourceWindow covers the single flat window an
// operator can set when five different per-source windows are more than they
// want to reason about.
func TestStaleAfterOverridesThePerSourceWindow(t *testing.T) {
	// A source the scheduler considers fresh: three minutes of slack and a
	// success thirty seconds ago.
	st := freshSource("swpc")
	st.LastSuccess = time.Now().Add(-30 * time.Second)

	cfg := testConfig()
	cfg.StaleAfter = 10 * time.Second
	if rec := get(t, newTestServer(t, cfg, st), "/readyz"); rec.Code != http.StatusServiceUnavailable {
		t.Errorf("/readyz = %d with a 10s override over a 30s-old success, want 503", rec.Code)
	}

	// The same source with a generous override is ready even though the
	// scheduler's own window would have been tighter.
	tight := freshSource("swpc")
	tight.LastSuccess = time.Now().Add(-10 * time.Minute)
	tight.Stale = true

	loose := testConfig()
	loose.StaleAfter = time.Hour
	if rec := get(t, newTestServer(t, loose, tight), "/readyz"); rec.Code != http.StatusOK {
		t.Errorf("/readyz = %d with a 1h override over a 10m-old success, want 200", rec.Code)
	}
}

func TestHealthReportsBuildSourcesAndSinksAndIs200WhenHealthy(t *testing.T) {
	h := newTestServer(t, testConfig(), freshSource("swpc"), freshSource("hamqsl"))

	rec := get(t, h, "/health")
	if rec.Code != http.StatusOK {
		t.Fatalf("/health = %d, want 200\n%s", rec.Code, rec.Body.String())
	}
	assertNoStore(t, rec)
	if got := rec.Header().Get("Content-Type"); !strings.HasPrefix(got, "application/json") {
		t.Errorf("Content-Type is %q, want JSON", got)
	}

	body := decode[healthResponse](t, rec)
	if body.Status != StatusOK {
		t.Errorf("status is %q, want %q", body.Status, StatusOK)
	}
	if body.Build.Version != "1.2.3" || body.Build.Commit != "abc1234" {
		t.Errorf("build is %+v, want the supplied version and commit", body.Build)
	}
	if len(body.Sources) != 2 {
		t.Fatalf("sources has %d entries, want 2", len(body.Sources))
	}
	for _, s := range body.Sources {
		if s.Status != StatusOK {
			t.Errorf("source %s is %q, want %q", s.Name, s.Status, StatusOK)
		}
		if s.LastSuccess == "" {
			t.Errorf("source %s reports no last success", s.Name)
		}
	}
	if strings.Join(body.Sinks, ",") != "prometheus,otlp" {
		t.Errorf("configuredSinks is %v, want the configured names", body.Sinks)
	}
}

func TestHealthIs503AndReportsDegradedOrFail(t *testing.T) {
	cases := []struct {
		name     string
		statuses []source.Status
		want     string
	}{
		{"one of two stale", []source.Status{freshSource("swpc"), staleSource("hamqsl")}, StatusDegraded},
		{"all stale", []source.Status{staleSource("swpc"), staleSource("hamqsl")}, StatusFail},
		{"never succeeded", []source.Status{neverSucceededSource("swpc")}, StatusFail},
		{"no sources at all", nil, StatusFail},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newTestServer(t, testConfig(), tc.statuses...)

			rec := get(t, h, "/health")
			if rec.Code != http.StatusServiceUnavailable {
				t.Fatalf("/health = %d, want 503\n%s", rec.Code, rec.Body.String())
			}
			body := decode[healthResponse](t, rec)
			if body.Status != tc.want {
				t.Errorf("status is %q, want %q", body.Status, tc.want)
			}

			// The two endpoints answer different questions and agree only
			// when the whole dataset is gone. /health is strict: anything
			// short of every source fresh is 503. /readyz asks whether this
			// instance can serve at all, so it only joins /health once
			// nothing is fresh -- the case that actually points at something
			// local and fixable.
			ready := get(t, h, "/readyz")
			wantReady := http.StatusServiceUnavailable
			if tc.want == StatusDegraded {
				wantReady = http.StatusOK
			}
			if ready.Code != wantReady {
				t.Errorf("/readyz = %d, want %d while /health reports %q",
					ready.Code, wantReady, tc.want)
			}
		})
	}
}

// TestHealthLeaksNoSecretsURLsOrHostnames is the important one. /health is
// unauthenticated so external monitors can reach it, which means it must not
// double as a reconnaissance tool: no upstream URLs, no broker addresses, no
// tokens.
func TestHealthLeaksNoSecretsURLsOrHostnames(t *testing.T) {
	const (
		influxToken = "influx-token-abcdef123456"
		brokerURL   = "mqtts://mqtt.internal.example.com:8883"
		hamqslURL   = "https://www.hamqsl.com/solarxml.php"
		swpcURL     = "https://services.swpc.noaa.gov/products/alerts.json?key=hunter2"
		listenAddr  = "10.0.0.7:9101"
	)

	// Free text from an upstream failure is the most plausible route for an
	// endpoint to reach the response body, so the statuses carry exactly that.
	statuses := []source.Status{
		{
			Name:        "hamqsl",
			Interval:    time.Hour.String(),
			LastFailure: time.Now(),
			LastError:   `Get "` + hamqslURL + `": dial tcp: connection refused`,
			StaleAfter:  3 * time.Hour,
		},
		{
			Name:        "swpc",
			Interval:    time.Minute.String(),
			LastSuccess: time.Now(),
			LastFailure: time.Now().Add(-time.Minute),
			LastError:   `Get "` + swpcURL + `": 503`,
			StaleAfter:  3 * time.Minute,
		},
	}

	cfg := testConfig()
	cfg.Addr = listenAddr

	h := newTestServerWithDeps(t, cfg, Deps{
		Statuses: func() []source.Status { return statuses },
		// Sink names are published; they must remain names, never endpoints.
		SinkNames: func() []string { return []string{"prometheus", "otlp", "mqtt", "influxv2"} },
		Build:     BuildInfo{Version: "1.2.3", Commit: "abc1234", BuildDate: "2026-09-08T00:00:00Z"},
	})

	forbidden := []string{
		influxToken, brokerURL, hamqslURL, swpcURL, listenAddr,
		"hunter2", "hamqsl.com", "services.swpc.noaa.gov", "mqtt.internal.example.com",
		"10.0.0.7", "://", "@",
	}

	for _, path := range []string{"/health", "/healthz", "/readyz", "/"} {
		body := get(t, h, path).Body.String()
		for _, secret := range forbidden {
			if strings.Contains(body, secret) {
				t.Errorf("%s leaked %q:\n%s", path, secret, body)
			}
		}
	}

	// The sink names themselves must survive: knowing which backends are
	// configured is the whole point of publishing them.
	body := get(t, h, "/health").Body.String()
	for _, name := range []string{"prometheus", "otlp", "mqtt", "influxv2"} {
		if !strings.Contains(body, name) {
			t.Errorf("/health does not report the configured sink %q", name)
		}
	}
}

func TestMetricsIsServedWhenAHandlerIsInjected(t *testing.T) {
	metrics := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("# HELP solar_flux_sfu\n"))
	})

	t.Run("at the configured path", func(t *testing.T) {
		h := newTestServerWithDeps(t, testConfig(), Deps{
			MetricsHandler: metrics,
			MetricsPath:    "/custom-metrics",
			Statuses:       func() []source.Status { return []source.Status{freshSource("swpc")} },
		})
		if rec := get(t, h, "/custom-metrics"); rec.Code != http.StatusOK {
			t.Fatalf("/custom-metrics = %d, want 200", rec.Code)
		}
		// The index must advertise what is actually mounted.
		if body := get(t, h, "/").Body.String(); !strings.Contains(body, "/custom-metrics") {
			t.Errorf("the index does not list the mounted metrics path:\n%s", body)
		}
	})

	t.Run("at the default path when none is configured", func(t *testing.T) {
		h := newTestServerWithDeps(t, testConfig(), Deps{
			MetricsHandler: metrics,
			Statuses:       func() []source.Status { return []source.Status{freshSource("swpc")} },
		})
		if rec := get(t, h, DefaultMetricsPath); rec.Code != http.StatusOK {
			t.Fatalf("%s = %d, want 200", DefaultMetricsPath, rec.Code)
		}
	})
}

// TestMetricsIs404AndDoesNotPanicWhenTheHandlerIsNil covers the typed-nil trap:
// a nil *prommetrics.Sink stored in an http.Handler is a non-nil interface and
// panics on the first request. The route is simply not registered instead.
func TestMetricsIs404AndDoesNotPanicWhenTheHandlerIsNil(t *testing.T) {
	h := newTestServerWithDeps(t, testConfig(), Deps{
		MetricsHandler: nil,
		MetricsPath:    "/metrics",
		Statuses:       func() []source.Status { return []source.Status{freshSource("swpc")} },
	})

	// A panic here fails the test through the ordinary panic path; the assert
	// is that a request is answered at all, with a 404.
	rec := get(t, h, "/metrics")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("/metrics = %d with no handler injected, want 404", rec.Code)
	}

	// And the index must not advertise an endpoint that answers 404.
	if body := get(t, h, "/").Body.String(); strings.Contains(body, "/metrics") {
		t.Errorf("the index lists /metrics when no handler is mounted:\n%s", body)
	}
}

func TestIndexListsTheEndpoints(t *testing.T) {
	h := newTestServer(t, testConfig(), freshSource("swpc"))

	rec := get(t, h, "/")
	if rec.Code != http.StatusOK {
		t.Fatalf("/ = %d, want 200", rec.Code)
	}
	assertNoStore(t, rec)

	body := decode[map[string]any](t, rec)
	if body["service"] != "solarham-exporter" {
		t.Errorf("service is %v, want solarham-exporter", body["service"])
	}
	endpoints, ok := body["endpoints"].([]any)
	if !ok {
		t.Fatalf("endpoints is %v, want a list", body["endpoints"])
	}
	listed := make(map[string]bool, len(endpoints))
	for _, e := range endpoints {
		listed[e.(string)] = true
	}
	for _, want := range []string{"/healthz", "/readyz", "/health"} {
		if !listed[want] {
			t.Errorf("the index does not list %s", want)
		}
	}
}

func TestUnknownPathsAre404(t *testing.T) {
	h := newTestServer(t, testConfig(), freshSource("swpc"))
	for _, path := range []string{"/nope", "/health/extra", "/healthz/", "/readyzz"} {
		if rec := get(t, h, path); rec.Code != http.StatusNotFound {
			t.Errorf("%s = %d, want 404", path, rec.Code)
		}
	}
}

func TestNewRequiresAnAddress(t *testing.T) {
	if _, err := New(config.HTTP{}, Deps{}, discardLogger()); err == nil {
		t.Fatal("New accepted an empty address, want an error")
	}
}

func TestNewWithoutALoggerDoesNotPanic(t *testing.T) {
	s, err := New(testConfig(), Deps{}, nil)
	if err != nil {
		t.Fatalf("New returned %v, want nil", err)
	}
	if rec := get(t, s.Handler(), "/healthz"); rec.Code != http.StatusOK {
		t.Fatalf("/healthz = %d, want 200", rec.Code)
	}
}

func TestBuildInfoFallsBackToUnknown(t *testing.T) {
	h := newTestServerWithDeps(t, testConfig(), Deps{})
	build := decode[healthResponse](t, get(t, h, "/health")).Build
	if build.Version != unknown || build.Commit != unknown || build.BuildDate != unknown {
		t.Errorf("build is %+v, want %q in every unstamped field", build, unknown)
	}
}

// TestServeOverARealListener exercises the routes through a real HTTP round
// trip, so status and header handling are checked against net/http rather than
// only against the recorder.
func TestServeOverARealListener(t *testing.T) {
	s, err := New(testConfig(), Deps{
		Statuses: func() []source.Status { return []source.Status{freshSource("swpc")} },
	}, discardLogger())
	if err != nil {
		t.Fatalf("New returned %v, want nil", err)
	}

	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	resp, err := srv.Client().Get(srv.URL + "/readyz")
	if err != nil {
		t.Fatalf("GET /readyz returned %v, want nil", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("/readyz = %d, want 200", resp.StatusCode)
	}
	if got := resp.Header.Get("Cache-Control"); got != "no-store" {
		t.Errorf("Cache-Control is %q, want no-store", got)
	}
}

// TestStartShutsDownPromptlyWhenTheContextIsCancelled checks the lifecycle the
// process depends on: a signal produces a clean exit rather than an error, and
// it happens well inside the supervisor's own kill timer.
func TestStartShutsDownPromptlyWhenTheContextIsCancelled(t *testing.T) {
	cfg := testConfig()
	cfg.ShutdownTimeout = time.Second

	s, err := New(cfg, Deps{}, discardLogger())
	if err != nil {
		t.Fatalf("New returned %v, want nil", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Start(ctx) }()

	// Give the listener a moment to bind before asking it to stop.
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Start returned %v on a cancelled context, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not return after its context was cancelled")
	}
}

func TestStartReportsAListenFailure(t *testing.T) {
	cfg := testConfig()
	// Port 1 is privileged, so binding it fails for an unprivileged test
	// process. A port that cannot be bound must surface as an error rather
	// than a silently dead server.
	cfg.Addr = "127.0.0.1:1"

	s, err := New(cfg, Deps{}, discardLogger())
	if err != nil {
		t.Fatalf("New returned %v, want nil", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := s.Start(ctx); err == nil {
		t.Fatal("Start returned nil for an unbindable address, want an error")
	}
}

func TestScrubReplacesURLShapedText(t *testing.T) {
	cases := map[string]string{
		`Get "https://user:pw@host/x?k=v": refused`: `Get REDACTED refused`,
		"dial tcp 10.0.0.7:9101: refused":           "dial tcp 10.0.0.7:9101: refused",
		"":                                          "",
	}
	for in, want := range cases {
		if got := scrub(in); got != want {
			t.Errorf("scrub(%q) = %q, want %q", in, got, want)
		}
	}
}
