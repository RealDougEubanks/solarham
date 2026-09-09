package kc2g

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/RealDougEubanks/solarham/internal/config"
	"github.com/RealDougEubanks/solarham/internal/metric"
	"github.com/RealDougEubanks/solarham/internal/source"
)

// referenceNow is the instant every test pretends it is. The stations fixture
// holds fixed timestamps, and the freshness filter is the substance of this
// package, so the clock has to be pinned rather than left as time.Now.
var referenceNow = time.Date(2026, 9, 8, 12, 5, 0, 0, time.UTC)

// essnBody is a trimmed but structurally faithful essn.json response.
const essnBody = `{
  "6h":  [{"time": 1788000000, "ssn": 49.10, "sfi": 98.40},
          {"time": 1788003600, "ssn": 51.02, "sfi": 100.10}],
  "24h": [{"time": 1788000000, "ssn": 50.11, "sfi": 99.90},
          {"time": 1788003600, "ssn": 53.17, "sfi": 102.71}]
}`

// testLogger discards output but keeps the debug path exercised, so a panic or
// a bad argument in a log line fails a test rather than production.
func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

func stationsFixture(t *testing.T) []byte {
	t.Helper()
	body, err := os.ReadFile("testdata/stations.json")
	if err != nil {
		t.Fatalf("reading fixture: %v", err)
	}
	return body
}

// newServer serves the two endpoints from the given handlers, counting hits.
type testServer struct {
	*httptest.Server
	stationHits atomic.Int64
	essnHits    atomic.Int64
}

func newServer(t *testing.T, stations, essn http.HandlerFunc) *testServer {
	t.Helper()
	ts := &testServer{}
	mux := http.NewServeMux()
	mux.HandleFunc(pathStations, func(w http.ResponseWriter, r *http.Request) {
		ts.stationHits.Add(1)
		if stations == nil {
			http.Error(w, "no stations handler", http.StatusInternalServerError)
			return
		}
		stations(w, r)
	})
	mux.HandleFunc(pathESSN, func(w http.ResponseWriter, r *http.Request) {
		ts.essnHits.Add(1)
		if essn == nil {
			http.Error(w, "no essn handler", http.StatusInternalServerError)
			return
		}
		essn(w, r)
	})
	ts.Server = httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	return ts
}

func serveBytes(body []byte) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	}
}

func serveStatus(code int, headers map[string]string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		for k, v := range headers {
			w.Header().Set(k, v)
		}
		http.Error(w, http.StatusText(code), code)
	}
}

// newSource builds a source aimed at the test server with a pinned clock.
func newSource(t *testing.T, baseURL string, mutate func(*config.KC2G)) *Source {
	t.Helper()
	cfg := config.KC2G{
		Enabled:          true,
		BaseURL:          baseURL,
		Interval:         15 * time.Minute,
		Timeout:          5 * time.Second,
		Retries:          0,
		MinConfidence:    50,
		MaxAge:           time.Hour,
		EffectiveIndices: false,
	}
	if mutate != nil {
		mutate(&cfg)
	}
	src, err := New(cfg, testLogger())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	src.now = func() time.Time { return referenceNow }
	return src
}

// keys renders a batch as "metric{label,label}=value" strings, sorted, which is
// a readable way to assert on a whole batch at once.
func keys(t *testing.T, b metric.Batch) []string {
	t.Helper()
	out := make([]string, 0, len(b.Samples))
	for _, s := range b.Samples {
		if err := s.Validate(); err != nil {
			t.Errorf("invalid sample emitted: %v", err)
		}
		out = append(out, s.Desc.FullName()+"{"+strings.Join(s.Labels, ",")+"}="+
			formatFloat(s.Value))
	}
	sort.Strings(out)
	return out
}

func formatFloat(v float64) string {
	return strconv.FormatFloat(v, 'g', -1, 64)
}

func has(list []string, want string) bool {
	for _, v := range list {
		if v == want {
			return true
		}
	}
	return false
}

func TestTheFullFixtureProducesExactlyTheExpectedSetOfSamples(t *testing.T) {
	srv := newServer(t, serveBytes(stationsFixture(t)), nil)
	src := newSource(t, srv.URL, nil)

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}

	got := keys(t, batch)
	want := []string{
		// Fresh, fully populated, high confidence.
		"solar_fof2_megahertz{AU930,Austin, TX, USA}=8.6",
		"solar_hmf2_kilometers{AU930,Austin, TX, USA}=241.8",
		"solar_muf_factor{AU930,Austin, TX, USA}=3.352",
		"solar_muf_megahertz{AU930,Austin, TX, USA}=28.827",
		"solar_station_confidence_score{AU930}=100",
		// Non-numeric md, so no factor sample.
		"solar_fof2_megahertz{BC840,Boulder, CO, USA}=7.7",
		"solar_hmf2_kilometers{BC840,Boulder, CO, USA}=260",
		"solar_muf_megahertz{BC840,Boulder, CO, USA}=22.1",
		"solar_station_confidence_score{BC840}=92",
		// Null hmf2, so no height sample.
		"solar_fof2_megahertz{JR055,Moscow, Russia}=4.4",
		"solar_muf_factor{JR055,Moscow, Russia}=2.727",
		"solar_muf_megahertz{JR055,Moscow, Russia}=12",
		"solar_station_confidence_score{JR055}=88",
		// Manually scaled: cs 999 publishes as a full 100.
		"solar_fof2_megahertz{MHJ45,Kokubunji, Japan}=5.1",
		"solar_hmf2_kilometers{MHJ45,Kokubunji, Japan}=300",
		"solar_muf_factor{MHJ45,Kokubunji, Japan}=2.98",
		"solar_muf_megahertz{MHJ45,Kokubunji, Japan}=15.2",
		"solar_station_confidence_score{MHJ45}=100",
		// Empty name falls back to the code.
		"solar_fof2_megahertz{PRJ18,PRJ18}=6.6",
		"solar_hmf2_kilometers{PRJ18,PRJ18}=250",
		"solar_muf_factor{PRJ18,PRJ18}=3.03",
		"solar_muf_megahertz{PRJ18,PRJ18}=20",
		"solar_station_confidence_score{PRJ18}=75",
	}
	sort.Strings(want)

	if len(got) != len(want) {
		t.Fatalf("got %d samples, want %d:\ngot:  %v\nwant: %v", len(got), len(want), got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("sample %d: got %q, want %q", i, got[i], want[i])
		}
	}
}

func TestStationsSilentForMonthsAreDroppedEvenThoughTheDocumentIsFresh(t *testing.T) {
	srv := newServer(t, serveBytes(stationsFixture(t)), nil)
	src := newSource(t, srv.URL, nil)

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	for _, s := range batch.Samples {
		if s.Labels[0] == "EA036" {
			t.Fatalf("station EA036 last reported in March but was published: %+v", s)
		}
	}

	// Widening MaxAge past the gap brings the same station back, which proves
	// the filter and not something else was responsible.
	relaxed := newSource(t, srv.URL, func(c *config.KC2G) { c.MaxAge = 200 * 24 * time.Hour })
	batch, err = relaxed.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if !has(keys(t, batch), "solar_fof2_megahertz{EA036,El Arenosillo, Spain}=7.75") {
		t.Fatal("EA036 should be published once MaxAge covers its age")
	}
}

func TestStationsBelowTheConfidenceThresholdAreDropped(t *testing.T) {
	srv := newServer(t, serveBytes(stationsFixture(t)), nil)

	src := newSource(t, srv.URL, nil)
	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	for _, s := range batch.Samples {
		if s.Labels[0] == "LM42B" {
			t.Fatalf("station LM42B scores 25 but was published: %+v", s)
		}
	}

	lenient := newSource(t, srv.URL, func(c *config.KC2G) { c.MinConfidence = 10 })
	batch, err = lenient.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if !has(keys(t, batch), "solar_station_confidence_score{LM42B}=25") {
		t.Fatal("LM42B should be published once the threshold drops below its score")
	}
}

func TestManuallyScaledStationsAreKeptAndUnknownConfidenceIsDropped(t *testing.T) {
	srv := newServer(t, serveBytes(stationsFixture(t)), nil)
	// A threshold of zero would admit anything expressed as a percentage. The
	// -1 sentinel must still be refused: an unreported score is not a good one.
	src := newSource(t, srv.URL, func(c *config.KC2G) { c.MinConfidence = 0 })

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	got := keys(t, batch)

	if !has(got, "solar_station_confidence_score{MHJ45}=100") {
		t.Error("cs 999 means manually scaled and should publish as full confidence")
	}
	for _, s := range batch.Samples {
		if s.Labels[0] == "RL052" {
			t.Fatalf("station RL052 reports cs -1 but was published: %+v", s)
		}
	}
}

func TestTheStationAllowListRestrictsPublicationAndIgnoresCase(t *testing.T) {
	srv := newServer(t, serveBytes(stationsFixture(t)), nil)
	src := newSource(t, srv.URL, func(c *config.KC2G) {
		c.Stations = []string{"au930", " Bc840 "}
	})

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	seen := map[string]bool{}
	for _, s := range batch.Samples {
		seen[s.Labels[0]] = true
	}
	if len(seen) != 2 || !seen["AU930"] || !seen["BC840"] {
		t.Fatalf("allow-list should admit AU930 and BC840 only, got %v", seen)
	}
}

func TestTheQuotedMUFFactorIsParsedAsANumber(t *testing.T) {
	srv := newServer(t, serveBytes(stationsFixture(t)), nil)
	src := newSource(t, srv.URL, func(c *config.KC2G) { c.Stations = []string{"AU930"} })

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	var found bool
	for _, s := range batch.Samples {
		if s.Desc == metric.MUFFactor {
			found = true
			if s.Value != 3.352 {
				t.Errorf("md: got %v, want 3.352", s.Value)
			}
		}
	}
	if !found {
		t.Fatal("no MUF factor sample was produced from the string md field")
	}
}

func TestAMissingOrUnparseableFieldProducesNoSampleForThatFieldOnly(t *testing.T) {
	srv := newServer(t, serveBytes(stationsFixture(t)), nil)
	src := newSource(t, srv.URL, func(c *config.KC2G) { c.Stations = []string{"JR055", "BC840"} })

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	for _, s := range batch.Samples {
		if s.Labels[0] == "JR055" && s.Desc == metric.HmF2 {
			t.Error("JR055 reports a null hmf2 and should produce no height sample")
		}
		if s.Labels[0] == "BC840" && s.Desc == metric.MUFFactor {
			t.Error("BC840 reports a non-numeric md and should produce no factor sample")
		}
	}
	got := keys(t, batch)
	if !has(got, "solar_fof2_megahertz{JR055,Moscow, Russia}=4.4") {
		t.Error("the rest of JR055 should still publish")
	}
	if !has(got, "solar_hmf2_kilometers{BC840,Boulder, CO, USA}=260") {
		t.Error("the rest of BC840 should still publish")
	}
}

func TestAnEmptyStationNameFallsBackToTheStationCode(t *testing.T) {
	srv := newServer(t, serveBytes(stationsFixture(t)), nil)
	src := newSource(t, srv.URL, func(c *config.KC2G) { c.Stations = []string{"PRJ18"} })

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if len(batch.Samples) == 0 {
		t.Fatal("PRJ18 produced nothing")
	}
	for _, s := range batch.Samples {
		name, ok := s.LabelFor("station_name")
		if !ok {
			continue
		}
		if name != "PRJ18" {
			t.Errorf("station_name: got %q, want the code as fallback", name)
		}
	}
}

func TestEachSampleCarriesItsOwnStationTimestampReadAsUTC(t *testing.T) {
	srv := newServer(t, serveBytes(stationsFixture(t)), nil)
	src := newSource(t, srv.URL, func(c *config.KC2G) { c.Stations = []string{"AU930", "MHJ45"} })

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	want := map[string]time.Time{
		"AU930": time.Date(2026, 9, 8, 12, 0, 5, 0, time.UTC),
		"MHJ45": time.Date(2026, 9, 8, 12, 1, 0, 0, time.UTC),
	}
	for _, s := range batch.Samples {
		if !s.Time.Equal(want[s.Labels[0]]) {
			t.Errorf("%s %s: got time %s, want %s",
				s.Labels[0], s.Desc.Name, s.Time, want[s.Labels[0]])
		}
		if s.Time.Location() != time.UTC {
			t.Errorf("%s: timestamp is not UTC: %s", s.Labels[0], s.Time.Location())
		}
	}
}

func TestEffectiveIndicesComeFromTheNewestTwentyFourHourEntry(t *testing.T) {
	srv := newServer(t, serveBytes(stationsFixture(t)), serveBytes([]byte(essnBody)))
	src := newSource(t, srv.URL, func(c *config.KC2G) {
		c.EffectiveIndices = true
		c.Stations = []string{"AU930"}
	})

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}

	var ssn, sfi *metric.Sample
	for i := range batch.Samples {
		switch batch.Samples[i].Desc {
		case metric.EffectiveSunspotNumber:
			ssn = &batch.Samples[i]
		case metric.EffectiveFluxSFU:
			sfi = &batch.Samples[i]
		}
	}
	if ssn == nil || sfi == nil {
		t.Fatalf("effective indices missing from batch: %v", keys(t, batch))
	}
	if ssn.Value != 53.17 {
		t.Errorf("effective SSN: got %v, want 53.17 from the newest 24h entry", ssn.Value)
	}
	if sfi.Value != 102.71 {
		t.Errorf("effective flux: got %v, want 102.71", sfi.Value)
	}
	wantTime := time.Unix(1788003600, 0).UTC()
	if !ssn.Time.Equal(wantTime) {
		t.Errorf("epoch conversion: got %s, want %s", ssn.Time, wantTime)
	}
}

func TestEffectiveIndicesFallBackToAnotherSeriesWhenTwentyFourHourIsAbsent(t *testing.T) {
	body := `{"6h": [{"time": 1788003600, "ssn": 51.02, "sfi": 100.10}]}`
	srv := newServer(t, serveBytes(stationsFixture(t)), serveBytes([]byte(body)))
	src := newSource(t, srv.URL, func(c *config.KC2G) {
		c.EffectiveIndices = true
		c.Stations = []string{"AU930"}
	})

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if !has(keys(t, batch), "solar_effective_sunspot_number{}=51.02") {
		t.Fatalf("the 6h series should be used when 24h is absent, got %v", keys(t, batch))
	}
}

func TestEffectiveIndicesAreNotFetchedWhenDisabled(t *testing.T) {
	srv := newServer(t, serveBytes(stationsFixture(t)), serveBytes([]byte(essnBody)))
	src := newSource(t, srv.URL, nil) // EffectiveIndices defaults to false here.

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if srv.essnHits.Load() != 0 {
		t.Errorf("essn endpoint was requested %d times while disabled", srv.essnHits.Load())
	}
	for _, s := range batch.Samples {
		if s.Desc == metric.EffectiveSunspotNumber || s.Desc == metric.EffectiveFluxSFU {
			t.Fatalf("effective index published while disabled: %+v", s)
		}
	}
}

func TestOneEndpointFailingDoesNotLoseTheOther(t *testing.T) {
	srv := newServer(t, serveBytes(stationsFixture(t)), serveStatus(http.StatusInternalServerError, nil))
	src := newSource(t, srv.URL, func(c *config.KC2G) {
		c.EffectiveIndices = true
		c.Stations = []string{"AU930"}
	})

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("a poll that salvaged the stations document should not fail: %v", err)
	}
	if len(batch.Samples) == 0 {
		t.Fatal("station samples were lost because the essn endpoint failed")
	}

	// And the reverse: the essn data survives a failing stations document.
	srv2 := newServer(t, serveStatus(http.StatusInternalServerError, nil), serveBytes([]byte(essnBody)))
	src2 := newSource(t, srv2.URL, func(c *config.KC2G) { c.EffectiveIndices = true })
	batch, err = src2.Poll(context.Background())
	if err != nil {
		t.Fatalf("a poll that salvaged the effective indices should not fail: %v", err)
	}
	if len(batch.Samples) != 2 {
		t.Fatalf("got %d samples, want the two effective indices", len(batch.Samples))
	}
}

func TestBothEndpointsFailingIsReportedAsAnError(t *testing.T) {
	srv := newServer(t,
		serveStatus(http.StatusInternalServerError, nil),
		serveStatus(http.StatusInternalServerError, nil))
	src := newSource(t, srv.URL, func(c *config.KC2G) { c.EffectiveIndices = true })

	batch, err := src.Poll(context.Background())
	if err == nil {
		t.Fatal("expected an error when nothing could be fetched")
	}
	if batch.Len() != 0 {
		t.Errorf("expected an empty batch, got %d samples", batch.Len())
	}
}

func TestTooManyRequestsIsReportedAsRateLimitedAndHonoursRetryAfter(t *testing.T) {
	srv := newServer(t,
		serveStatus(http.StatusTooManyRequests, map[string]string{"Retry-After": "600"}),
		nil)
	src := newSource(t, srv.URL, func(c *config.KC2G) { c.Retries = 2 })

	_, err := src.Poll(context.Background())
	if !errors.Is(err, source.ErrRateLimited) {
		t.Fatalf("got %v, want source.ErrRateLimited", err)
	}
	if hits := srv.stationHits.Load(); hits != 1 {
		t.Errorf("a 429 was retried %d times; it must not be retried", hits-1)
	}

	// The Retry-After holds the source off without touching the network again.
	if _, err := src.Poll(context.Background()); !errors.Is(err, source.ErrRateLimited) {
		t.Fatalf("second poll: got %v, want source.ErrRateLimited", err)
	}
	if hits := srv.stationHits.Load(); hits != 1 {
		t.Errorf("the cooldown was ignored: %d requests made", hits)
	}
}

func TestNotFoundIsPermanentAndIsNotRetried(t *testing.T) {
	srv := newServer(t, serveStatus(http.StatusNotFound, nil), nil)
	src := newSource(t, srv.URL, func(c *config.KC2G) { c.Retries = 3 })

	if _, err := src.Poll(context.Background()); err == nil {
		t.Fatal("expected an error for 404")
	}
	if hits := srv.stationHits.Load(); hits != 1 {
		t.Errorf("404 was requested %d times, want 1", hits)
	}
}

func TestUnauthorizedAndForbiddenAreNotRetried(t *testing.T) {
	for _, code := range []int{http.StatusUnauthorized, http.StatusForbidden} {
		srv := newServer(t, serveStatus(code, nil), nil)
		src := newSource(t, srv.URL, func(c *config.KC2G) { c.Retries = 3 })

		if _, err := src.Poll(context.Background()); err == nil {
			t.Fatalf("status %d: expected an error", code)
		}
		if hits := srv.stationHits.Load(); hits != 1 {
			t.Errorf("status %d was requested %d times, want 1", code, hits)
		}
	}
}

func TestServerErrorsAreRetriedAndCanSucceed(t *testing.T) {
	fixture := stationsFixture(t)
	var calls atomic.Int64
	handler := func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			http.Error(w, "upstream wobble", http.StatusBadGateway)
			return
		}
		serveBytes(fixture)(w, r)
	}
	srv := newServer(t, handler, nil)
	src := newSource(t, srv.URL, func(c *config.KC2G) {
		c.Retries = 2
		c.Stations = []string{"AU930"}
	})

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll after a retried 502: %v", err)
	}
	if calls.Load() != 2 {
		t.Errorf("got %d requests, want a retry after the 502", calls.Load())
	}
	if len(batch.Samples) == 0 {
		t.Fatal("the retry produced no samples")
	}
}

func TestPollHonoursContextCancellation(t *testing.T) {
	srv := newServer(t, serveBytes(stationsFixture(t)), nil)
	src := newSource(t, srv.URL, nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	batch, err := src.Poll(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}
	if batch.Len() != 0 {
		t.Errorf("expected no samples from a cancelled poll, got %d", batch.Len())
	}
	if srv.stationHits.Load() != 0 {
		t.Errorf("a cancelled poll still made %d requests", srv.stationHits.Load())
	}
}

func TestMalformedJSONIsAnErrorRatherThanAPanic(t *testing.T) {
	cases := map[string]string{
		"truncated array":     `[{"station": {"code": "AU930"`,
		"object not an array": `{"station": "AU930"}`,
		"not json at all":     `<!doctype html><html>captive portal</html>`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			srv := newServer(t, serveBytes([]byte(body)), nil)
			src := newSource(t, srv.URL, nil)

			batch, err := src.Poll(context.Background())
			if err == nil {
				t.Fatalf("expected a parse error, got %d samples", batch.Len())
			}
		})
	}
}

func TestIntervalIsClampedToTheFiveMinuteFloor(t *testing.T) {
	cases := []struct {
		configured time.Duration
		want       time.Duration
	}{
		{0, defaultInterval},
		{30 * time.Second, minInterval},
		{minInterval, minInterval},
		{30 * time.Minute, 30 * time.Minute},
	}
	for _, tc := range cases {
		src := newSource(t, "https://prop.kc2g.com", func(c *config.KC2G) { c.Interval = tc.configured })
		if got := src.Interval(); got != tc.want {
			t.Errorf("Interval for a configured %s: got %s, want %s", tc.configured, got, tc.want)
		}
	}
}

func TestNewRejectsConfigurationItCannotWorkWith(t *testing.T) {
	cases := map[string]config.KC2G{
		"no max age": {BaseURL: "https://prop.kc2g.com", MaxAge: 0},
		"bad scheme": {BaseURL: "ftp://prop.kc2g.com", MaxAge: time.Hour},
		"no host":    {BaseURL: "https://", MaxAge: time.Hour},
		"not a url":  {BaseURL: "://nope", MaxAge: time.Hour},
	}
	for name, cfg := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := New(cfg, testLogger()); err == nil {
				t.Fatal("expected New to reject this configuration")
			}
		})
	}
}

func TestTheSourceIdentifiesItselfConsistently(t *testing.T) {
	src := newSource(t, "https://prop.kc2g.com", nil)
	if src.Name() != Name || Name != "kc2g" {
		t.Fatalf("Name: got %q, want %q", src.Name(), "kc2g")
	}
}
