package swpc

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/RealDougEubanks/solarham/internal/config"
	"github.com/RealDougEubanks/solarham/internal/metric"
	"github.com/RealDougEubanks/solarham/internal/source"
)

// fixtureFor maps every endpoint path to the captured response in testdata, so
// a test server can answer the real paths with the real shapes.
var fixtureFor = map[string]string{
	pathWindSpeed:     "solar-wind-speed.json",
	pathWindMagField:  "solar-wind-mag-field.json",
	pathKpOneMinute:   "planetary-k-index-1m.json",
	pathXRayFlares:    "xray-flares-latest.json",
	pathNOAAScales:    "noaa-scales.json",
	pathAlerts:        "alerts.json",
	pathProtons:       "integral-protons.json",
	pathElectrons:     "integral-electrons.json",
	pathHemisphericPw: "aurora-hemi-power.txt",
	pathDRAP:          "drap-global-frequencies.txt",
	pathF107:          "f107.json",
	pathPlanetaryK:    "noaa-planetary-k-index.json",
	pathDailyIndices:  "daily-solar-indices.txt",
	pathKyotoDst:      "kyoto-dst.json",
}

// fixtureServer answers every known SWPC path from testdata. override, when it
// returns true, has already written the response for that request.
func fixtureServer(t *testing.T, override func(w http.ResponseWriter, r *http.Request) bool) *httptest.Server {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if override != nil && override(w, r) {
			return
		}
		name, ok := fixtureFor[r.URL.Path]
		if !ok {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write(fixture(t, name))
	}))
	t.Cleanup(ts.Close)
	return ts
}

// baseConfig is a configuration with no retries and a short timeout, so a test
// that expects a failure does not spend seconds backing off before reporting
// it. Retries are enabled explicitly by the tests that are about retrying.
func baseConfig(baseURL string) config.SWPC {
	return config.SWPC{
		Enabled: true,
		BaseURL: baseURL,
		Timeout: 5 * time.Second,
		Retries: 0,
	}
}

// newTestSources builds the three tiers against ts with a frozen clock, so the
// alert age window is measured against when the fixtures were captured rather
// than against the day the test happens to run.
func newTestSources(t *testing.T, cfg config.SWPC) *Sources {
	t.Helper()
	s, err := New(cfg, discardLogger())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	for _, tier := range []*tier{s.fast, s.medium, s.slow} {
		tier.now = func() time.Time { return fixedNow }
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// singleEndpointTier isolates one endpoint, which is how the retry and status
// code behaviour is tested without five other requests in the way.
func singleEndpointTier(baseURL string, ep endpoint, retries int) *tier {
	return &tier{
		name:      "swpc-test",
		interval:  time.Minute,
		endpoints: []endpoint{ep},
		client:    newClient(baseURL, 5*time.Second, retries, discardLogger()),
		log:       discardLogger(),
		drapStep:  1,
		now:       func() time.Time { return fixedNow },
	}
}

func TestNewRejectsABaseURLThatIsNotAnAbsoluteURL(t *testing.T) {
	for _, bad := range []string{"services.swpc.noaa.gov", "://nope", "/just/a/path"} {
		if _, err := New(config.SWPC{BaseURL: bad}, discardLogger()); err == nil {
			t.Errorf("New with BaseURL %q succeeded, want an error", bad)
		}
	}
}

func TestNewDefaultsTheBaseURLAndIntervals(t *testing.T) {
	s, err := New(config.SWPC{Enabled: true}, discardLogger())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if s.fast.client.base != defaultBaseURL {
		t.Errorf("base = %q, want %q", s.fast.client.base, defaultBaseURL)
	}
	want := map[string]time.Duration{
		nameFast:   defaultFastInterval,
		nameMedium: defaultMediumInterval,
		nameSlow:   defaultSlowInterval,
	}
	for _, src := range s.All() {
		if got := src.Interval(); got != want[src.Name()] {
			t.Errorf("%s interval = %s, want %s", src.Name(), got, want[src.Name()])
		}
	}
}

func TestAllReturnsTheThreeTiersInCadenceOrder(t *testing.T) {
	cfg := baseConfig("https://example.invalid")
	cfg.FastInterval = 30 * time.Second
	cfg.MediumInterval = 2 * time.Minute
	cfg.SlowInterval = 30 * time.Minute

	s, err := New(cfg, discardLogger())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	all := s.All()
	if len(all) != 3 {
		t.Fatalf("All returned %d sources, want 3", len(all))
	}
	wantNames := []string{nameFast, nameMedium, nameSlow}
	wantIntervals := []time.Duration{30 * time.Second, 2 * time.Minute, 30 * time.Minute}
	for i, src := range all {
		if src.Name() != wantNames[i] {
			t.Errorf("source %d is %q, want %q", i, src.Name(), wantNames[i])
		}
		if src.Interval() != wantIntervals[i] {
			t.Errorf("%s interval = %s, want %s", src.Name(), src.Interval(), wantIntervals[i])
		}
	}
}

func TestTheThreeTiersShareOneHTTPClientAndCache(t *testing.T) {
	s := newTestSources(t, baseConfig("https://example.invalid"))
	if s.fast.client != s.medium.client || s.medium.client != s.slow.client {
		t.Error("the tiers hold different clients, so the conditional GET cache is not shared")
	}
}

func TestPollProducesSamplesFromEveryFastEndpoint(t *testing.T) {
	ts := fixtureServer(t, nil)
	s := newTestSources(t, baseConfig(ts.URL))

	batch, err := s.fast.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if batch.Source != nameFast {
		t.Errorf("batch source = %q, want %q", batch.Source, nameFast)
	}

	for _, desc := range []*metric.Descriptor{
		metric.WindSpeed, metric.KIndexEstimated, metric.XRayClassInfo, metric.XRayFlux,
		metric.GeomagneticStormScale, metric.RadioBlackoutScale, metric.RadiationStormScale,
	} {
		if !hasDescriptor(batch.Samples, desc) {
			t.Errorf("batch carries no %s sample", desc.FullName())
		}
	}
	sampleFor(t, batch.Samples, metric.WindMagneticField, "bt")
	sampleFor(t, batch.Samples, metric.WindMagneticField, "bz")

	if !hasDescriptor(batch.Samples, metric.AlertActive) {
		t.Error("batch carries no alert samples")
	}
	for _, s := range batch.Samples {
		if err := s.Validate(); err != nil {
			t.Errorf("invalid sample reached the batch: %v", err)
		}
	}
}

func TestPollProducesSamplesFromEveryMediumEndpoint(t *testing.T) {
	ts := fixtureServer(t, nil)
	s := newTestSources(t, baseConfig(ts.URL))

	batch, err := s.medium.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	sampleFor(t, batch.Samples, metric.ProtonFlux, ">=10 MeV")
	sampleFor(t, batch.Samples, metric.ElectronFlux, ">=2 MeV")
	sampleFor(t, batch.Samples, metric.AuroraHemisphericPower, "north")
	sampleFor(t, batch.Samples, metric.AuroraHemisphericPower, "south")
}

func TestPollProducesSamplesFromEverySlowEndpoint(t *testing.T) {
	ts := fixtureServer(t, nil)
	s := newTestSources(t, baseConfig(ts.URL))

	batch, err := s.slow.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	for _, desc := range []*metric.Descriptor{
		metric.FluxSFU, metric.FluxNinetyDayMeanSFU, metric.SunspotNumber, metric.DstNanotesla,
	} {
		if !hasDescriptor(batch.Samples, desc) {
			t.Errorf("batch carries no %s sample", desc.FullName())
		}
	}
	sampleFor(t, batch.Samples, metric.KIndex, stationPlanetary)
	sampleFor(t, batch.Samples, metric.AIndex, stationPlanetary)
}

func TestDRAPIsAbsentUnlessItIsEnabled(t *testing.T) {
	ts := fixtureServer(t, nil)

	off := newTestSources(t, baseConfig(ts.URL))
	batch, err := off.medium.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if hasDescriptor(batch.Samples, metric.DRAPMaxFrequency) {
		t.Error("published D-RAP samples without DRAPEnabled")
	}

	cfg := baseConfig(ts.URL)
	cfg.DRAPEnabled = true
	cfg.DRAPGridStep = 3
	on := newTestSources(t, cfg)
	batch, err = on.medium.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll with D-RAP enabled: %v", err)
	}
	if !hasDescriptor(batch.Samples, metric.DRAPMaxFrequency) {
		t.Fatal("DRAPEnabled produced no D-RAP samples")
	}

	// The fixture grid is 90 latitudes by 12 longitudes; a step of 3 keeps
	// every third row and column.
	var count int
	for _, s := range batch.Samples {
		if s.Desc == metric.DRAPMaxFrequency {
			count++
		}
	}
	if want := 30 * 4; count != want {
		t.Errorf("got %d D-RAP samples at step 3, want %d", count, want)
	}
}

func TestASecondPollSendsTheValidatorsFromTheFirst(t *testing.T) {
	const etag = `"3b-65b04c629dbc5"`
	const lastModified = "Wed, 09 Sep 2026 03:52:02 GMT"

	var (
		requests  atomic.Int32
		sawETag   atomic.Bool
		sawModSin atomic.Bool
	)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) > 1 {
			if r.Header.Get("If-None-Match") == etag {
				sawETag.Store(true)
			}
			if r.Header.Get("If-Modified-Since") == lastModified {
				sawModSin.Store(true)
			}
		}
		w.Header().Set("ETag", etag)
		w.Header().Set("Last-Modified", lastModified)
		_, _ = w.Write(fixture(t, "solar-wind-speed.json"))
	}))
	defer ts.Close()

	tr := singleEndpointTier(ts.URL, endpoint{pathWindSpeed, parseWindSpeed}, 0)
	for i := 0; i < 2; i++ {
		if _, err := tr.Poll(context.Background()); err != nil {
			t.Fatalf("poll %d: %v", i+1, err)
		}
	}
	if !sawETag.Load() {
		t.Error("the second request carried no If-None-Match")
	}
	if !sawModSin.Load() {
		t.Error("the second request carried no If-Modified-Since")
	}
}

func TestAnEndpointAnsweringNotModifiedIsASuccessWithNoSamples(t *testing.T) {
	// Only the wind speed document is unchanged; everything else is fresh.
	ts := fixtureServer(t, func(w http.ResponseWriter, r *http.Request) bool {
		if r.URL.Path != pathWindSpeed {
			return false
		}
		w.WriteHeader(http.StatusNotModified)
		return true
	})
	s := newTestSources(t, baseConfig(ts.URL))

	batch, err := s.fast.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if hasDescriptor(batch.Samples, metric.WindSpeed) {
		t.Error("a 304 produced a wind speed sample")
	}
	if !hasDescriptor(batch.Samples, metric.KIndexEstimated) {
		t.Error("one endpoint's 304 suppressed the others' samples")
	}
}

func TestEveryEndpointAnsweringNotModifiedReturnsErrNotModified(t *testing.T) {
	ts := fixtureServer(t, func(w http.ResponseWriter, _ *http.Request) bool {
		w.WriteHeader(http.StatusNotModified)
		return true
	})
	s := newTestSources(t, baseConfig(ts.URL))

	batch, err := s.fast.Poll(context.Background())
	if !errors.Is(err, source.ErrNotModified) {
		t.Fatalf("Poll error = %v, want source.ErrNotModified", err)
	}
	if batch.Len() != 0 {
		t.Errorf("got %d samples alongside ErrNotModified, want none", batch.Len())
	}
}

func TestOneFailingEndpointStillReturnsTheOthersSamples(t *testing.T) {
	ts := fixtureServer(t, func(w http.ResponseWriter, r *http.Request) bool {
		if r.URL.Path != pathAlerts {
			return false
		}
		w.WriteHeader(http.StatusInternalServerError)
		return true
	})
	s := newTestSources(t, baseConfig(ts.URL))

	batch, err := s.fast.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll returned an error for one failed endpoint: %v", err)
	}
	if hasDescriptor(batch.Samples, metric.AlertActive) {
		t.Error("the failing endpoint somehow produced samples")
	}
	if !hasDescriptor(batch.Samples, metric.WindSpeed) {
		t.Error("a single failure cost the whole tier its samples")
	}
}

func TestEveryEndpointFailingReturnsAnError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()

	s := newTestSources(t, baseConfig(ts.URL))
	batch, err := s.fast.Poll(context.Background())
	if err == nil {
		t.Fatal("Poll succeeded with every endpoint failing, want an error")
	}
	if batch.Len() != 0 {
		t.Errorf("got %d samples from a wholly failed poll, want none", batch.Len())
	}
	// Every cause is reported, not just whichever goroutine finished first.
	for _, path := range []string{pathWindSpeed, pathAlerts, pathNOAAScales} {
		if !strings.Contains(err.Error(), path) {
			t.Errorf("error does not mention %s: %v", path, err)
		}
	}
}

func TestTooManyRequestsReturnsErrRateLimited(t *testing.T) {
	var requests atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.Header().Set("Retry-After", "60")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer ts.Close()

	tr := singleEndpointTier(ts.URL, endpoint{pathWindSpeed, parseWindSpeed}, 3)
	_, err := tr.Poll(context.Background())
	if !errors.Is(err, source.ErrRateLimited) {
		t.Fatalf("Poll error = %v, want source.ErrRateLimited", err)
	}
	if got := requests.Load(); got != 1 {
		t.Errorf("made %d requests after a 429, want 1: retrying a rate limit is more refusals, not resilience", got)
	}
	if _, cooling := tr.client.coolingDown(); !cooling {
		t.Error("the client did not record a cooldown after a 429")
	}
}

func TestACoolingDownClientDoesNotTouchTheNetwork(t *testing.T) {
	var requests atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.Header().Set("Retry-After", "120")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer ts.Close()

	tr := singleEndpointTier(ts.URL, endpoint{pathWindSpeed, parseWindSpeed}, 0)
	if _, err := tr.Poll(context.Background()); !errors.Is(err, source.ErrRateLimited) {
		t.Fatalf("first poll error = %v, want source.ErrRateLimited", err)
	}
	if _, err := tr.Poll(context.Background()); !errors.Is(err, source.ErrRateLimited) {
		t.Fatalf("second poll error = %v, want source.ErrRateLimited", err)
	}
	if got := requests.Load(); got != 1 {
		t.Errorf("made %d requests, want 1: the second poll should have been suppressed", got)
	}
}

func TestRetryAfterAcceptsSecondsAndHTTPDates(t *testing.T) {
	now := time.Date(2026, 9, 9, 4, 0, 0, 0, time.UTC)
	if got := retryAfter("90", now); got != 90*time.Second {
		t.Errorf("retryAfter(\"90\") = %s, want 90s", got)
	}
	if got := retryAfter("Wed, 09 Sep 2026 04:01:00 GMT", now); got != time.Minute {
		t.Errorf("retryAfter(http date) = %s, want 1m", got)
	}
	for _, input := range []string{"", "later", "-5", "Wed, 09 Sep 2026 03:00:00 GMT"} {
		if got := retryAfter(input, now); got != 0 {
			t.Errorf("retryAfter(%q) = %s, want 0 so the caller applies its own default", input, got)
		}
	}
}

func TestAPermanentStatusIsNotRetried(t *testing.T) {
	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound} {
		var requests atomic.Int32
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			requests.Add(1)
			w.WriteHeader(status)
		}))

		tr := singleEndpointTier(ts.URL, endpoint{pathWindSpeed, parseWindSpeed}, 4)
		if _, err := tr.Poll(context.Background()); err == nil {
			t.Errorf("status %d: Poll succeeded, want an error", status)
		}
		if got := requests.Load(); got != 1 {
			t.Errorf("status %d: made %d requests, want 1", status, got)
		}
		ts.Close()
	}
}

func TestATransientStatusIsRetriedUntilItSucceeds(t *testing.T) {
	var requests atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if requests.Add(1) < 3 {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		_, _ = w.Write(fixture(t, "solar-wind-speed.json"))
	}))
	defer ts.Close()

	tr := singleEndpointTier(ts.URL, endpoint{pathWindSpeed, parseWindSpeed}, 3)
	batch, err := tr.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if got := requests.Load(); got != 3 {
		t.Errorf("made %d requests, want 3 (two failures then a success)", got)
	}
	if got := sampleFor(t, batch.Samples, metric.WindSpeed).Value; got != 401 {
		t.Errorf("speed = %v, want 401", got)
	}
}

func TestPollHonoursContextCancellation(t *testing.T) {
	ts := fixtureServer(t, nil)
	s := newTestSources(t, baseConfig(ts.URL))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	batch, err := s.fast.Poll(ctx)
	if err == nil {
		t.Fatal("Poll succeeded on a cancelled context, want an error")
	}
	if batch.Len() != 0 {
		t.Errorf("got %d samples from a cancelled poll, want none", batch.Len())
	}
}

func TestMalformedJSONReturnsAnErrorRatherThanPanicking(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"proton_speed": `))
	}))
	defer ts.Close()

	tr := singleEndpointTier(ts.URL, endpoint{pathWindSpeed, parseWindSpeed}, 0)
	if _, err := tr.Poll(context.Background()); err == nil {
		t.Fatal("Poll succeeded on malformed JSON, want an error")
	}
}

func TestMalformedTextReturnsAnErrorRatherThanPanicking(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("not a table at all\n"))
	}))
	defer ts.Close()

	for _, ep := range []endpoint{
		{pathHemisphericPw, parseHemisphericPower},
		{pathDRAP, parseDRAP},
		{pathDailyIndices, parseDailyIndices},
	} {
		tr := singleEndpointTier(ts.URL, ep, 0)
		if _, err := tr.Poll(context.Background()); err == nil {
			t.Errorf("%s: Poll succeeded on unparseable text, want an error", ep.path)
		}
	}
}

func TestEveryRequestNamesTheProject(t *testing.T) {
	var agent atomic.Value
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		agent.Store(r.Header.Get("User-Agent"))
		_, _ = w.Write(fixture(t, "solar-wind-speed.json"))
	}))
	defer ts.Close()

	tr := singleEndpointTier(ts.URL, endpoint{pathWindSpeed, parseWindSpeed}, 0)
	if _, err := tr.Poll(context.Background()); err != nil {
		t.Fatalf("Poll: %v", err)
	}
	got, _ := agent.Load().(string)
	if !strings.Contains(got, "solarham") {
		t.Errorf("User-Agent = %q, want it to name the project", got)
	}
}

func TestATierNeverRunsMoreThanFourRequestsAtOnce(t *testing.T) {
	var inFlight, peak atomic.Int32
	ts := fixtureServer(t, func(w http.ResponseWriter, r *http.Request) bool {
		current := inFlight.Add(1)
		for {
			old := peak.Load()
			if current <= old || peak.CompareAndSwap(old, current) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		inFlight.Add(-1)
		return false
	})
	s := newTestSources(t, baseConfig(ts.URL))

	if _, err := s.fast.Poll(context.Background()); err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if got := peak.Load(); got > maxConcurrentFetches {
		t.Errorf("peak concurrency was %d, want at most %d", got, maxConcurrentFetches)
	}
	if got := peak.Load(); got < 2 {
		t.Errorf("peak concurrency was %d, so the endpoints were not fetched concurrently at all", got)
	}
}

// hasDescriptor reports whether any sample belongs to the given metric.
func hasDescriptor(samples []metric.Sample, desc *metric.Descriptor) bool {
	for _, s := range samples {
		if s.Desc == desc {
			return true
		}
	}
	return false
}
