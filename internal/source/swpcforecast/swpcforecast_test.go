package swpcforecast

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
	"sync"
	"testing"
	"time"

	"github.com/RealDougEubanks/solarham/internal/config"
	"github.com/RealDougEubanks/solarham/internal/httpx"
	"github.com/RealDougEubanks/solarham/internal/metric"
	"github.com/RealDougEubanks/solarham/internal/source"
)

// fixtureTime is the newest timestamp in the recorded fixtures: the Kp
// forecast's last observed row, 2026-09-09T12:00:00Z. Every expectation about
// SourceDataAge is measured from it.
var fixtureTime = time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)

// pollTime is the injected wall clock used by the tests, one hour after
// fixtureTime, so a correct SourceDataAge is 3600 seconds and any value derived
// from the real clock is off by years.
var pollTime = fixtureTime.Add(time.Hour)

// fixtures maps each document path to its recorded testdata file.
var fixtures = map[string]string{
	pathProbabilities:  "solar_probabilities.json",
	pathRegions:        "solar_regions.json",
	pathKpForecast:     "kp_forecast.json",
	pathFluxForecast:   "predicted_f107cm_flux.json",
	pathAIndexForecast: "predicted_fredericksburg_a_index.json",
}

// quietLogger discards output so a test that exercises the warning paths does
// not bury the failure it is looking for.
func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// testClient returns an httpx client with the loopback host's politeness
// spacing removed. The spacing is the right default against a real publisher
// and pure latency against an httptest server two goroutines away.
func testClient(t *testing.T) *httpx.Client {
	t.Helper()
	return httpx.New(httpx.Options{
		Timeout:  5 * time.Second,
		Policies: map[string]httpx.HostPolicy{"127.0.0.1": {MinInterval: time.Microsecond, Burst: 64}},
		Fallback: httpx.HostPolicy{MinInterval: time.Microsecond, Burst: 64},
	}, quietLogger())
}

// fixtureServer serves the recorded documents and counts requests per path.
func fixtureServer(t *testing.T) (*httptest.Server, func(string) int) {
	t.Helper()

	var (
		mu     sync.Mutex
		counts = map[string]int{}
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		counts[r.URL.Path]++
		mu.Unlock()

		name, ok := fixtures[r.URL.Path]
		if !ok {
			http.NotFound(w, r)
			return
		}
		body, err := os.ReadFile(filepath.Join("testdata", name))
		if err != nil {
			t.Errorf("reading fixture %s: %v", name, err)
			http.Error(w, "fixture missing", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)

	return srv, func(path string) int {
		mu.Lock()
		defer mu.Unlock()
		return counts[path]
	}
}

// newTestSource builds a source aimed at srv with a frozen clock.
func newTestSource(t *testing.T, cfg config.SolarProbabilities, srv *httptest.Server) *Source {
	t.Helper()
	src, err := New(cfg, testClient(t), quietLogger())
	if err != nil {
		t.Fatalf("New returned an unexpected error: %v", err)
	}
	src.base = srv.URL
	src.now = func() time.Time { return pollTime }
	return src
}

// key identifies a series the way a sink would: metric name plus label values.
func key(s metric.Sample) string {
	return s.Desc.FullName() + "{" + strings.Join(s.Labels, ",") + "}"
}

// index collapses a batch into a series-keyed map, failing on a duplicate
// series because two samples for one series in one batch means one of them is
// silently discarded downstream.
func index(t *testing.T, b metric.Batch) map[string]metric.Sample {
	t.Helper()
	out := make(map[string]metric.Sample, len(b.Samples))
	for _, s := range b.Samples {
		k := key(s)
		if _, dup := out[k]; dup {
			t.Errorf("batch contains series %s twice", k)
		}
		out[k] = s
	}
	return out
}

func TestNewRejectsAMissingHTTPClient(t *testing.T) {
	if _, err := New(config.SolarProbabilities{}, nil, quietLogger()); err == nil {
		t.Error("New accepted a nil httpx client; the shared client is what enforces politeness across sources and must not be optional")
	}
}

func TestPollParsesEveryRecordedDocumentIntoTheExpectedSeries(t *testing.T) {
	srv, _ := fixtureServer(t)
	src := newTestSource(t, config.SolarProbabilities{}, srv)

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll returned an unexpected error: %v", err)
	}

	// 13 probability series (3 classes x 3 horizons, 3 proton horizons, one
	// polar cap info) + 6 region aggregates + 5 Kp horizons + 3 flux horizons
	// + 3 A-index horizons + 1 data age.
	const want = 31
	if got := batch.Len(); got != want {
		t.Errorf("Poll produced %d samples, want %d", got, want)
		for _, s := range batch.Samples {
			t.Logf("  %s = %v @ %s", key(s), s.Value, s.Time.Format(time.RFC3339))
		}
	}

	if batch.Source != Name {
		t.Errorf("batch source is %q, want %q", batch.Source, Name)
	}

	for _, s := range batch.Samples {
		if err := s.Validate(); err != nil {
			t.Errorf("Poll emitted an invalid sample: %v", err)
		}
	}

	byKey := index(t, batch)

	// Values taken straight from the recorded fixtures.
	expected := map[string]float64{
		"solar_flare_probability_percent{C,1d}":                55,
		"solar_flare_probability_percent{C,2d}":                50,
		"solar_flare_probability_percent{C,3d}":                45,
		"solar_flare_probability_percent{M,1d}":                10,
		"solar_flare_probability_percent{M,2d}":                10,
		"solar_flare_probability_percent{M,3d}":                5,
		"solar_flare_probability_percent{X,1d}":                1,
		"solar_flare_probability_percent{X,2d}":                1,
		"solar_flare_probability_percent{X,3d}":                1,
		"solar_proton_event_probability_percent{1d}":           1,
		"solar_proton_event_probability_percent{2d}":           1,
		"solar_proton_event_probability_percent{3d}":           1,
		"solar_polar_cap_absorption_info{green}":               1,
		"solar_active_region_count{}":                          9,
		"solar_active_region_spot_total{}":                     22,
		"solar_active_region_area_total_millionths{}":          510,
		"solar_active_region_max_flare_probability_percent{C}": 25,
		"solar_active_region_max_flare_probability_percent{M}": 5,
		"solar_active_region_max_flare_probability_percent{X}": 1,
		"solar_k_index_forecast{3h}":                           3.0,
		"solar_k_index_forecast{6h}":                           5.0,
		"solar_k_index_forecast{1d}":                           5.67,
		"solar_k_index_forecast{2d}":                           2.33,
		"solar_k_index_forecast{3d}":                           3.0,
		"solar_flux_forecast_sfu{1d}":                          110,
		"solar_flux_forecast_sfu{2d}":                          110,
		"solar_flux_forecast_sfu{3d}":                          110,
		"solar_a_index_forecast{1d}":                           10,
		"solar_a_index_forecast{2d}":                           8,
		"solar_a_index_forecast{3d}":                           7,
	}
	for k, wantValue := range expected {
		got, ok := byKey[k]
		if !ok {
			t.Errorf("series %s is missing from the batch", k)
			continue
		}
		if got.Value != wantValue {
			t.Errorf("series %s is %v, want %v", k, got.Value, wantValue)
		}
	}
}

func TestPollStampsSamplesWithTheUpstreamsOwnIssueTimeRatherThanTheClock(t *testing.T) {
	srv, _ := fixtureServer(t)
	src := newTestSource(t, config.SolarProbabilities{}, srv)

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll returned an unexpected error: %v", err)
	}
	byKey := index(t, batch)

	// The probabilities document is dated 2026-09-09T00:00:00 and the Kp
	// forecast's last observation is 2026-09-09T12:00:00. Neither is the poll
	// time, so a sample stamped with pollTime is a sample stamped from the
	// clock.
	cases := map[string]time.Time{
		"solar_flare_probability_percent{X,1d}": time.Date(2026, 9, 9, 0, 0, 0, 0, time.UTC),
		"solar_k_index_forecast{3h}":            fixtureTime,
		"solar_active_region_count{}":           time.Date(2026, 9, 9, 0, 0, 0, 0, time.UTC),
	}
	for k, want := range cases {
		got, ok := byKey[k]
		if !ok {
			t.Errorf("series %s is missing from the batch", k)
			continue
		}
		if !got.Time.Equal(want) {
			t.Errorf("series %s is stamped %s, want the upstream's own %s",
				k, got.Time.Format(time.RFC3339), want.Format(time.RFC3339))
		}
	}
}

func TestSourceDataAgeIsComputedFromThePayloadTimestampNotTheWallClock(t *testing.T) {
	srv, _ := fixtureServer(t)
	src := newTestSource(t, config.SolarProbabilities{}, srv)

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll returned an unexpected error: %v", err)
	}

	age, ok := index(t, batch)["solar_source_data_age_seconds{"+Name+"}"]
	if !ok {
		t.Fatal("the batch carries no source data age; a source that cannot report its own staleness hides the dominant failure mode of these upstreams")
	}

	// pollTime is exactly one hour after the newest fixture timestamp.
	if want := 3600.0; age.Value != want {
		t.Errorf("source data age is %v seconds, want %v; it must be the injected clock minus the newest payload timestamp", age.Value, want)
	}
}

func TestPerRegionSeriesAreAbsentByDefaultAndPresentWhenEnabled(t *testing.T) {
	srv, _ := fixtureServer(t)

	off, err := newTestSource(t, config.SolarProbabilities{}, srv).Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll with regions disabled returned an unexpected error: %v", err)
	}
	for _, s := range off.Samples {
		if s.Desc == metric.ActiveRegionFlareProbability {
			t.Errorf("per-region flare probability %s was published with cfg.Regions unset; region numbers churn, so this series set must be opt-in", key(s))
		}
	}

	on, err := newTestSource(t, config.SolarProbabilities{Regions: true}, srv).Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll with regions enabled returned an unexpected error: %v", err)
	}

	var perRegion int
	for _, s := range on.Samples {
		if s.Desc == metric.ActiveRegionFlareProbability {
			perRegion++
		}
	}
	// Nine regions on the newest observed date, three classes each.
	if want := 27; perRegion != want {
		t.Errorf("cfg.Regions produced %d per-region series, want %d", perRegion, want)
	}

	byKey := index(t, on)
	if got, ok := byKey["solar_active_region_flare_probability_percent{4530,C}"]; !ok {
		t.Error("region 4530's C-class probability is missing")
	} else if got.Value != 25 {
		t.Errorf("region 4530's C-class probability is %v, want 25", got.Value)
	}
}

func TestRegionsAreFilteredToTheNewestObservedDate(t *testing.T) {
	// The fixture deliberately carries two observed dates. Summing the whole
	// document would report a fortnight of regions as simultaneously on the
	// disc, which is the mistake this filter exists to prevent.
	srv, _ := fixtureServer(t)
	batch, err := newTestSource(t, config.SolarProbabilities{}, srv).Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll returned an unexpected error: %v", err)
	}

	count, ok := index(t, batch)["solar_active_region_count{}"]
	if !ok {
		t.Fatal("the batch carries no active region count")
	}
	if count.Value != 9 {
		t.Errorf("active region count is %v, want 9 (the newest observed date only, not all 18 records)", count.Value)
	}
}

func TestScheduleReportsTheFourConfiguredDailyTimesAndNothingMoreOften(t *testing.T) {
	src, err := New(config.SolarProbabilities{}, testClient(t), quietLogger())
	if err != nil {
		t.Fatalf("New returned an unexpected error: %v", err)
	}
	sched := src.Schedule()

	// Walk a full simulated day from midnight, collecting every poll the
	// schedule asks for.
	var (
		start = time.Date(2026, 9, 9, 0, 0, 0, 0, time.UTC)
		end   = start.Add(24 * time.Hour)
		polls []time.Time
	)
	for at := start; ; {
		next := sched.NextAfter(at)
		if !next.After(at) {
			t.Fatalf("NextAfter(%s) returned %s, which is not in the future; the poll loop would spin",
				at.Format(time.RFC3339), next.Format(time.RFC3339))
		}
		if !next.Before(end) {
			break
		}
		polls = append(polls, next)
		at = next
	}

	if want := 4; len(polls) != want {
		t.Errorf("the schedule asked for %d polls in one UTC day, want %d: %v", len(polls), want, polls)
	}

	// Each poll must land on one of the declared slots plus the publication
	// lag, within the schedule's 90-second anti-thundering-herd spread.
	slots := []int{1, 7, 13, 19}
	for i, at := range polls {
		if i >= len(slots) {
			break
		}
		earliest := start.Add(time.Duration(slots[i]) * time.Hour)
		latest := earliest.Add(90 * time.Second)
		if at.Before(earliest) || at.After(latest) {
			t.Errorf("poll %d is at %s, want between %s and %s (%02d:00 UTC plus spread)",
				i, at.Format(time.RFC3339), earliest.Format(time.RFC3339), latest.Format(time.RFC3339), slots[i])
		}
	}

	// The shortest gap between slots is six hours, and nothing about the
	// schedule may shorten it. A source that polled these once-a-day forecast
	// documents hourly would fetch the same bytes twenty-four times over.
	for i := 1; i < len(polls); i++ {
		if gap := polls[i].Sub(polls[i-1]); gap < 6*time.Hour-90*time.Second {
			t.Errorf("consecutive polls are %s apart, which is more often than the six-hour slot spacing allows", gap)
		}
	}
}

func TestIntervalAgreesWithTheSchedule(t *testing.T) {
	src, err := New(config.SolarProbabilities{}, testClient(t), quietLogger())
	if err != nil {
		t.Fatalf("New returned an unexpected error: %v", err)
	}
	if got, want := src.Interval(), src.Schedule().Interval(); got != want {
		t.Errorf("Interval is %s but the schedule reports %s; the two must never disagree", got, want)
	}
	if want := 6 * time.Hour; src.Interval() != want {
		t.Errorf("Interval is %s, want %s", src.Interval(), want)
	}
}

func TestAnUnchangedDocumentSetReportsNotModifiedRatherThanFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotModified)
	}))
	defer srv.Close()

	src := newTestSource(t, config.SolarProbabilities{}, srv)
	batch, err := src.Poll(context.Background())
	if !errors.Is(err, source.ErrNotModified) {
		t.Errorf("Poll returned %v, want source.ErrNotModified; an unchanged forecast is the expected case, not a failure", err)
	}
	if batch.Len() != 0 {
		t.Errorf("a not-modified poll produced %d samples, want none", batch.Len())
	}
}

func TestAMissingDocumentIsNotRetried(t *testing.T) {
	var (
		mu     sync.Mutex
		counts = map[string]int{}
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		counts[r.URL.Path]++
		mu.Unlock()
		http.NotFound(w, r)
	}))
	defer srv.Close()

	src := newTestSource(t, config.SolarProbabilities{}, srv)
	if _, err := src.Poll(context.Background()); err == nil {
		t.Error("Poll succeeded with every document 404ing, want an error")
	}

	mu.Lock()
	defer mu.Unlock()
	for path, n := range counts {
		if n != 1 {
			t.Errorf("%s was requested %d times; a 404 will fail identically next time, so retrying it only adds load", path, n)
		}
	}
	if len(counts) != len(endpoints) {
		t.Errorf("%d distinct paths were requested, want %d", len(counts), len(endpoints))
	}
}

func TestARefusalForBeingTooFrequentReportsRateLimited(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// A short Retry-After keeps the test quick while still exercising the
		// header path; without one httpx imposes a five-minute cooldown.
		w.Header().Set("Retry-After", "1")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	src := newTestSource(t, config.SolarProbabilities{}, srv)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	batch, err := src.Poll(ctx)
	if !errors.Is(err, source.ErrRateLimited) {
		t.Errorf("Poll returned %v, want source.ErrRateLimited so the scheduler backs off rather than retrying", err)
	}
	if batch.Len() != 0 {
		t.Errorf("a rate-limited poll produced %d samples, want none", batch.Len())
	}
}

func TestMalformedJSONErrorsRatherThanPanicking(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"this is": "not the array we expected"`)
	}))
	defer srv.Close()

	src := newTestSource(t, config.SolarProbabilities{}, srv)
	batch, err := src.Poll(context.Background())
	if err == nil {
		t.Fatal("Poll succeeded on truncated JSON, want an error")
	}
	if batch.Len() != 0 {
		t.Errorf("a failed poll produced %d samples, want none", batch.Len())
	}
	if strings.Contains(err.Error(), "panic") {
		t.Errorf("Poll recovered a panic instead of reporting a parse error: %v", err)
	}
}

func TestAnEmptyDocumentIsReportedRatherThanPublishedAsZero(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `[]`)
	}))
	defer srv.Close()

	src := newTestSource(t, config.SolarProbabilities{}, srv)
	batch, err := src.Poll(context.Background())
	if err == nil {
		t.Error("Poll succeeded on empty documents; an empty forecast file is an upstream fault, not a forecast of zero")
	}
	if batch.Len() != 0 {
		t.Errorf("a failed poll produced %d samples, want none", batch.Len())
	}
}

func TestOneFailedDocumentDoesNotLoseTheOthers(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == pathRegions {
			http.Error(w, "region summary is sick", http.StatusInternalServerError)
			return
		}
		name, ok := fixtures[r.URL.Path]
		if !ok {
			http.NotFound(w, r)
			return
		}
		body, err := os.ReadFile(filepath.Join("testdata", name))
		if err != nil {
			t.Errorf("reading fixture %s: %v", name, err)
			return
		}
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	src := newTestSource(t, config.SolarProbabilities{}, srv)
	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll failed because one of five documents did: %v", err)
	}

	byKey := index(t, batch)
	if _, ok := byKey["solar_flare_probability_percent{X,1d}"]; !ok {
		t.Error("the flare probabilities were lost because the region summary failed")
	}
	if _, ok := byKey["solar_active_region_count{}"]; ok {
		t.Error("an active region count was published although the region summary failed")
	}
}

func TestPollHonoursContextCancellation(t *testing.T) {
	// The server blocks until its request context is cancelled, so a Poll that
	// ignored ctx would hang here rather than fail.
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer srv.Close()

	src := newTestSource(t, config.SolarProbabilities{}, srv)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, err := src.Poll(ctx); err == nil {
			t.Error("Poll succeeded with a cancelled context, want an error")
		}
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Poll did not return within five seconds of a cancelled context")
	}
}

func TestKpHorizonsAreBucketedFromTheNewestObservedRow(t *testing.T) {
	// Hand-built rather than recorded: the last observation is at 00:00, so
	// every offset below is trivially checkable by eye.
	doc := `[
	  {"time_tag":"2026-01-01T21:00:00","kp":1.0,"observed":"observed"},
	  {"time_tag":"2026-01-02T00:00:00","kp":2.0,"observed":"observed"},
	  {"time_tag":"2026-01-02T03:00:00","kp":4.0,"observed":"estimated"},
	  {"time_tag":"2026-01-02T06:00:00","kp":3.0,"observed":"estimated"},
	  {"time_tag":"2026-01-03T00:00:00","kp":7.0,"observed":"predicted"},
	  {"time_tag":"2026-01-04T00:00:00","kp":5.0,"observed":"predicted"},
	  {"time_tag":"2026-01-05T00:00:00","kp":6.0,"observed":"predicted"},
	  {"time_tag":"2026-01-09T00:00:00","kp":9.0,"observed":"predicted"}
	]`

	samples, issued, err := parseKpForecast(nil, []byte(doc))
	if err != nil {
		t.Fatalf("parseKpForecast returned an unexpected error: %v", err)
	}
	if want := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC); !issued.Equal(want) {
		t.Errorf("the forecast issue time is %s, want the newest observed row's %s",
			issued.Format(time.RFC3339), want.Format(time.RFC3339))
	}

	got := map[string]float64{}
	for _, s := range samples {
		got[s.Labels[0]] = s.Value
	}
	want := map[string]float64{"3h": 4.0, "6h": 3.0, "1d": 7.0, "2d": 5.0, "3d": 6.0}
	if len(got) != len(want) {
		t.Errorf("got %d horizon buckets %v, want %d", len(got), got, len(want))
	}
	for label, wantValue := range want {
		if got[label] != wantValue {
			t.Errorf("horizon %s is %v, want %v", label, got[label], wantValue)
		}
	}
	// The row seven days out is beyond the product's three-day range and must
	// not be squeezed into the 3d bucket.
	if got["3d"] == 9.0 {
		t.Error("a row 168 hours out was bucketed as 3d; anything past 72 hours is padding and must be dropped")
	}
}

func TestKpForecastWithNoObservedRowIsRejected(t *testing.T) {
	// Without an observation there is no anchor for the horizons, and falling
	// back to wall-clock time would shift every bucket whenever the exporter's
	// clock and SWPC's publication drifted apart.
	doc := `[{"time_tag":"2026-01-02T03:00:00","kp":4.0,"observed":"predicted"}]`
	if _, _, err := parseKpForecast(nil, []byte(doc)); err == nil {
		t.Error("parseKpForecast accepted a document with no observed row, want an error")
	}
}

func TestANullForecastValueProducesNoSampleRatherThanAZero(t *testing.T) {
	// SWPC writes null for a field it has no value for. A zero X-class
	// probability published as a fact is a different claim from no forecast.
	doc := `[{"date":"2026-01-02T00:00:00","c_class_1_day":40,"x_class_1_day":null,"polar_cap_absorption":"green"}]`
	samples, _, err := parseProbabilities(nil, []byte(doc))
	if err != nil {
		t.Fatalf("parseProbabilities returned an unexpected error: %v", err)
	}
	for _, s := range samples {
		if s.Desc == metric.FlareProbability && s.Labels[0] == "X" {
			t.Errorf("a null x_class_1_day was published as %v", s.Value)
		}
	}
	var sawC bool
	for _, s := range samples {
		if s.Desc == metric.FlareProbability && s.Labels[0] == "C" && s.Labels[1] == "1d" {
			sawC = true
			if s.Value != 40 {
				t.Errorf("c_class_1_day is %v, want 40", s.Value)
			}
		}
	}
	if !sawC {
		t.Error("the C-class 1-day probability was dropped along with the null field")
	}
}

func TestTheNewestRecordIsChosenByDateRatherThanByPosition(t *testing.T) {
	// The live documents happen to be newest-first, but "the file is sorted"
	// is not a contract, and reading the wrong record would publish a
	// month-old forecast with today's timestamp.
	doc := `[
	  {"date":"2026-01-01T00:00:00","c_class_1_day":10},
	  {"date":"2026-01-05T00:00:00","c_class_1_day":90},
	  {"date":"2026-01-03T00:00:00","c_class_1_day":50}
	]`
	samples, issued, err := parseProbabilities(nil, []byte(doc))
	if err != nil {
		t.Fatalf("parseProbabilities returned an unexpected error: %v", err)
	}
	if want := time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC); !issued.Equal(want) {
		t.Errorf("issue time is %s, want %s", issued.Format(time.RFC3339), want.Format(time.RFC3339))
	}
	if len(samples) == 0 || samples[0].Value != 90 {
		t.Errorf("published %v, want the record dated 2026-01-05 (90)", samples)
	}
}

func TestFixtureDocumentsAreServedFromTestdataAndNotTheNetwork(t *testing.T) {
	// A guard on the fixtures themselves: a missing file would otherwise show
	// up as a confusing parse failure in every other test.
	for path, name := range fixtures {
		if _, err := os.Stat(filepath.Join("testdata", name)); err != nil {
			t.Errorf("fixture for %s is missing: %v", path, err)
		}
	}
	if len(fixtures) != len(endpoints) {
		t.Errorf("%d fixtures for %d endpoints; every document needs a recorded copy", len(fixtures), len(endpoints))
	}
}
