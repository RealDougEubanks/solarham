package usgsgeomag

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/RealDougEubanks/solarham/internal/config"
	"github.com/RealDougEubanks/solarham/internal/httpx"
	"github.com/RealDougEubanks/solarham/internal/metric"
	"github.com/RealDougEubanks/solarham/internal/source"
)

// referenceNow is the instant every test pretends it is. The live fixtures
// carry fixed timestamps and the age calculations are the substance of this
// package, so the clock is pinned rather than left as time.Now. It is five
// minutes after the newest observation in the captured series.
var referenceNow = time.Date(2026, 9, 9, 15, 50, 0, 0, time.UTC)

// testLogger discards output but keeps the debug path exercised, so a bad
// argument in a log line fails a test rather than production.
func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// testClient is the shared client with its politeness spacing wound down.
// Spacing is the point of httpx in production and pure latency in a test.
func testClient(t *testing.T) *httpx.Client {
	t.Helper()
	return httpx.New(httpx.Options{
		Timeout:  5 * time.Second,
		Fallback: httpx.HostPolicy{MinInterval: time.Millisecond, Burst: 1000},
	}, testLogger())
}

func fixture(t *testing.T, name string) []byte {
	t.Helper()
	body, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatalf("reading fixture %s: %v", name, err)
	}
	return body
}

// recordingServer serves the three endpoints and counts and records requests.
type recordingServer struct {
	*httptest.Server
	dbdtHits atomic.Int64
	dataHits atomic.Int64
	obsHits  atomic.Int64

	dbdtQueries chan string
	dataQueries chan string
}

// newServer wires handlers for the three endpoints. A nil handler answers 500.
func newServer(t *testing.T, dbdt, data, obs http.HandlerFunc) *recordingServer {
	t.Helper()
	rs := &recordingServer{
		dbdtQueries: make(chan string, 64),
		dataQueries: make(chan string, 64),
	}
	mux := http.NewServeMux()
	mux.HandleFunc(pathDbDt, func(w http.ResponseWriter, r *http.Request) {
		rs.dbdtHits.Add(1)
		select {
		case rs.dbdtQueries <- r.URL.RawQuery:
		default:
		}
		if dbdt == nil {
			http.Error(w, "no dbdt handler", http.StatusInternalServerError)
			return
		}
		dbdt(w, r)
	})
	mux.HandleFunc(pathData, func(w http.ResponseWriter, r *http.Request) {
		rs.dataHits.Add(1)
		select {
		case rs.dataQueries <- r.URL.RawQuery:
		default:
		}
		if data == nil {
			http.Error(w, "no data handler", http.StatusInternalServerError)
			return
		}
		data(w, r)
	})
	mux.HandleFunc(pathObservatories, func(w http.ResponseWriter, r *http.Request) {
		rs.obsHits.Add(1)
		if obs == nil {
			http.Error(w, "no observatory handler", http.StatusInternalServerError)
			return
		}
		obs(w, r)
	})
	rs.Server = httptest.NewServer(mux)
	t.Cleanup(rs.Close)
	return rs
}

// serveFixture answers every request with one fixture file.
func serveFixture(t *testing.T, name string) http.HandlerFunc {
	t.Helper()
	body := fixture(t, name)
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	}
}

// newSource builds a source aimed at a test server with the clock pinned.
func newSource(t *testing.T, baseURL string, observatories ...string) *Source {
	t.Helper()
	if len(observatories) == 0 {
		observatories = []string{"BOU"}
	}
	src, err := New(config.USGSGeomag{
		Enabled:       true,
		BaseURL:       baseURL,
		Observatories: observatories,
		Interval:      time.Minute,
		Timeout:       3 * time.Second,
	}, testClient(t), testLogger())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	src.now = func() time.Time { return referenceNow }
	return src
}

// find returns the single sample for a descriptor and label set, failing if
// there is not exactly one.
func find(t *testing.T, b metric.Batch, desc *metric.Descriptor, labels ...string) metric.Sample {
	t.Helper()
	var found []metric.Sample
	for _, s := range b.Samples {
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
		t.Fatalf("want exactly one %s%v, got %d", desc.FullName(), labels, len(found))
	}
	return found[0]
}

// count returns how many samples in the batch use a descriptor.
func count(b metric.Batch, desc *metric.Descriptor) int {
	n := 0
	for _, s := range b.Samples {
		if s.Desc == desc {
			n++
		}
	}
	return n
}

func TestNewDefaultsToTheCuratedObservatorySetRatherThanAllForty(t *testing.T) {
	src, err := New(config.USGSGeomag{Enabled: true}, testClient(t), testLogger())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if got, want := strings.Join(src.observatories, ","), "BOU,FRD,NEW,TUC,SIT"; got != want {
		t.Errorf("default observatories = %q, want %q", got, want)
	}
	if src.Interval() != defaultInterval {
		t.Errorf("default interval = %v, want %v", src.Interval(), defaultInterval)
	}
	if src.Name() != Name || Name != "usgs-geomag" {
		t.Errorf("Name = %q, want usgs-geomag", src.Name())
	}
	if src.Schedule().Interval() != defaultInterval {
		t.Errorf("schedule interval = %v, want %v", src.Schedule().Interval(), defaultInterval)
	}
}

func TestNewRejectsABaseURLThatIsNotHTTP(t *testing.T) {
	for _, bad := range []string{"ftp://geomag.usgs.gov", "not a url at all", "https://"} {
		if _, err := New(config.USGSGeomag{BaseURL: bad}, testClient(t), testLogger()); err == nil {
			t.Errorf("New accepted base URL %q", bad)
		}
	}
}

func TestNewRejectsANilHTTPClientRatherThanBuildingItsOwn(t *testing.T) {
	if _, err := New(config.USGSGeomag{}, nil, testLogger()); err == nil {
		t.Error("New accepted a nil client")
	}
}

func TestNewDeduplicatesAndUpperCasesConfiguredObservatoryCodes(t *testing.T) {
	src, err := New(config.USGSGeomag{
		Observatories: []string{" bou ", "BOU", "frd", ""},
	}, testClient(t), testLogger())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if got, want := strings.Join(src.observatories, ","), "BOU,FRD"; got != want {
		t.Errorf("observatories = %q, want %q", got, want)
	}
}

func TestPollParsesTheCapturedFixtureIntoExactRateAndComponentValues(t *testing.T) {
	srv := newServer(t,
		serveFixture(t, "dbdt_bou.json"),
		serveFixture(t, "data_bou.json"),
		serveFixture(t, "observatories.json"))
	src := newSource(t, srv.URL)

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}

	// dbdt values, in nT/min, from the newest non-null minute (15:41). The
	// three minutes after it are null in the capture.
	rates := map[string]float64{"X": -0.7, "Y": -0.139, "Z": -0.103}
	for component, want := range rates {
		s := find(t, batch, metric.GeomagneticFieldRate, "BOU", component)
		if s.Value != want {
			t.Errorf("rate %s = %v, want %v", component, s.Value, want)
		}
		if want := time.Date(2026, 9, 9, 15, 41, 0, 0, time.UTC); !s.Time.Equal(want) {
			t.Errorf("rate %s timestamp = %v, want %v", component, s.Time.UTC(), want)
		}
	}

	// Field components, in nT. X and Y run out at 15:42, Z at 15:43.
	fields := map[string]struct {
		value float64
		at    time.Time
	}{
		"X": {20350.128, time.Date(2026, 9, 9, 15, 42, 0, 0, time.UTC)},
		"Y": {3352.939, time.Date(2026, 9, 9, 15, 42, 0, 0, time.UTC)},
		"Z": {46643.828, time.Date(2026, 9, 9, 15, 43, 0, 0, time.UTC)},
	}
	for component, want := range fields {
		s := find(t, batch, metric.GeomagneticFieldComponent, "BOU", component)
		if s.Value != want.value {
			t.Errorf("component %s = %v, want %v", component, s.Value, want.value)
		}
		if !s.Time.Equal(want.at) {
			t.Errorf("component %s timestamp = %v, want %v", component, s.Time.UTC(), want.at)
		}
	}

	// F is present in both documents and deliberately not published.
	if got := count(batch, metric.GeomagneticFieldRate); got != 3 {
		t.Errorf("published %d rate samples, want 3 (X, Y, Z and not F)", got)
	}
	if got := count(batch, metric.GeomagneticFieldComponent); got != 3 {
		t.Errorf("published %d component samples, want 3 (X, Y, Z and not F)", got)
	}

	// Peak-to-peak horizontal range over the capture's 17 usable minutes.
	span := find(t, batch, metric.GeomagneticFieldRange, "BOU")
	if want := 5.8076517099943885; math.Abs(span.Value-want) > 1e-9 {
		t.Errorf("horizontal range = %v, want %v", span.Value, want)
	}
}

func TestNullValuesAtTheTrailingEdgeAreSkippedRatherThanPublishedAsZero(t *testing.T) {
	srv := newServer(t,
		serveFixture(t, "dbdt_bou.json"),
		serveFixture(t, "data_bou.json"),
		serveFixture(t, "observatories.json"))
	src := newSource(t, srv.URL)

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}

	// The last four minutes of every dbdt channel and the last three of X are
	// null in the capture. A source that decoded null as zero would publish
	// 0 nT/min at 15:45 and 0 nT for the field, which is a plausible-looking
	// and completely wrong reading.
	for _, s := range batch.Samples {
		if s.Desc != metric.GeomagneticFieldRate && s.Desc != metric.GeomagneticFieldComponent {
			continue
		}
		if s.Value == 0 {
			t.Errorf("%s%v published as zero, which is the null sentinel decoded wrongly",
				s.Desc.FullName(), s.Labels)
		}
		if !s.Time.Before(time.Date(2026, 9, 9, 15, 44, 0, 0, time.UTC)) {
			t.Errorf("%s%v is stamped %v, inside the unprocessed trailing nulls",
				s.Desc.FullName(), s.Labels, s.Time.UTC())
		}
	}
}

func TestAnObservatoryWhoseEveryValueIsNullPublishesNoSamplesAtAll(t *testing.T) {
	srv := newServer(t,
		serveFixture(t, "dbdt_all_null.json"),
		func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "no data", http.StatusServiceUnavailable)
		},
		serveFixture(t, "observatories.json"))
	src := newSource(t, srv.URL, "FRD")

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if len(batch.Samples) != 0 {
		t.Errorf("published %d samples from an all-null document, want none: %+v",
			len(batch.Samples), batch.Samples)
	}
}

func TestHourlyHorizontalRangeMatchesTheHandCheckedFixture(t *testing.T) {
	srv := newServer(t,
		func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "not under test", http.StatusServiceUnavailable)
		},
		serveFixture(t, "range_handchecked.json"),
		serveFixture(t, "observatories.json"))
	src := newSource(t, srv.URL)

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}

	// Horizontal magnitudes in the fixture are 50000 (13:30 minus 90 minutes,
	// outside the window), 5000, 10000 and 15000. The one-hour window ending
	// at the newest usable instant is [12:30, 13:30], so the answer is
	// 15000 - 5000. Including the 12:00 point would give 45000.
	s := find(t, batch, metric.GeomagneticFieldRange, "BOU")
	if want := 10000.0; math.Abs(s.Value-want) > 1e-6 {
		t.Errorf("horizontal range = %v, want %v", s.Value, want)
	}
	if want := time.Date(2026, 9, 9, 13, 30, 0, 0, time.UTC); !s.Time.Equal(want) {
		t.Errorf("range timestamp = %v, want the newest contributing instant %v", s.Time.UTC(), want)
	}
}

func TestAnInstantMissingEitherHorizontalComponentIsExcludedFromTheRange(t *testing.T) {
	// The hand-checked fixture has X null at 13:10 with Y = 1000, and Y null
	// at 13:20 with X = 5000. Either treated as a horizontal magnitude on its
	// own would drag the minimum down to 1000 and give a range of 14000.
	srv := newServer(t, nil, serveFixture(t, "range_handchecked.json"),
		serveFixture(t, "observatories.json"))
	src := newSource(t, srv.URL)

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	s := find(t, batch, metric.GeomagneticFieldRange, "BOU")
	if math.Abs(s.Value-10000.0) > 1e-6 {
		t.Errorf("horizontal range = %v, want 10000; a half-observed instant leaked in", s.Value)
	}
}

func TestObservatoryMetadataIsFetchedOnceRatherThanOnEveryPoll(t *testing.T) {
	srv := newServer(t,
		serveFixture(t, "dbdt_bou.json"),
		serveFixture(t, "data_bou.json"),
		serveFixture(t, "observatories.json"))
	src := newSource(t, srv.URL)

	for i := range 4 {
		if _, err := src.Poll(context.Background()); err != nil {
			t.Fatalf("poll %d: %v", i, err)
		}
	}

	if got := srv.obsHits.Load(); got != 1 {
		t.Errorf("fetched /ws/observatories/ %d times across four polls, want 1", got)
	}
	if got := srv.dbdtHits.Load(); got != 4 {
		t.Errorf("fetched dbdt %d times across four polls, want 4", got)
	}
	if got := src.observatoryName("BOU"); got != "Boulder" {
		t.Errorf("observatory name for BOU = %q, want Boulder", got)
	}
}

func TestObservatoryMetadataIsRetriedOnALaterPollAfterItFails(t *testing.T) {
	var obsCalls atomic.Int64
	srv := newServer(t,
		serveFixture(t, "dbdt_bou.json"),
		serveFixture(t, "data_bou.json"),
		func(w http.ResponseWriter, r *http.Request) {
			if obsCalls.Add(1) == 1 {
				http.Error(w, "down", http.StatusServiceUnavailable)
				return
			}
			serveFixture(t, "observatories.json")(w, r)
		})
	src := newSource(t, srv.URL)

	if _, err := src.Poll(context.Background()); err != nil {
		t.Fatalf("first poll: %v", err)
	}
	if got := src.observatoryName("BOU"); got != "" {
		t.Errorf("name table populated from a failed fetch: %q", got)
	}
	if _, err := src.Poll(context.Background()); err != nil {
		t.Fatalf("second poll: %v", err)
	}
	if got := src.observatoryName("BOU"); got != "Boulder" {
		t.Errorf("name for BOU after retry = %q, want Boulder", got)
	}

	// And having succeeded, it stops asking.
	if _, err := src.Poll(context.Background()); err != nil {
		t.Fatalf("third poll: %v", err)
	}
	if got := obsCalls.Load(); got != 2 {
		t.Errorf("fetched metadata %d times, want 2 (one failure, one success)", got)
	}
}

func TestTheQueryStringRequestsAShortTrailingWindowRatherThanAWholeDay(t *testing.T) {
	srv := newServer(t,
		serveFixture(t, "dbdt_bou.json"),
		serveFixture(t, "data_bou.json"),
		serveFixture(t, "observatories.json"))
	src := newSource(t, srv.URL)

	if _, err := src.Poll(context.Background()); err != nil {
		t.Fatalf("Poll: %v", err)
	}

	cases := []struct {
		label  string
		query  chan string
		window time.Duration
		extra  map[string]string
	}{
		{"dbdt", srv.dbdtQueries, rateWindow, nil},
		{"data", srv.dataQueries, rangeWindow, map[string]string{
			"type":            dataType,
			"sampling_period": samplingPeriod,
		}},
	}

	for _, tc := range cases {
		var raw string
		select {
		case raw = <-tc.query:
		default:
			t.Fatalf("%s endpoint was never called", tc.label)
		}
		values, err := parseQuery(raw)
		if err != nil {
			t.Fatalf("%s query %q: %v", tc.label, raw, err)
		}
		if got := values["id"]; got != "BOU" {
			t.Errorf("%s id = %q, want BOU", tc.label, got)
		}
		if got := values["format"]; got != "json" {
			t.Errorf("%s format = %q, want json", tc.label, got)
		}
		start, err := time.Parse(queryTimeLayout, values["starttime"])
		if err != nil {
			t.Fatalf("%s starttime %q: %v", tc.label, values["starttime"], err)
		}
		end, err := time.Parse(queryTimeLayout, values["endtime"])
		if err != nil {
			t.Fatalf("%s endtime %q: %v", tc.label, values["endtime"], err)
		}
		if !end.Equal(referenceNow) {
			t.Errorf("%s endtime = %v, want the poll instant %v", tc.label, end, referenceNow)
		}
		if got := end.Sub(start); got != tc.window {
			t.Errorf("%s window = %v, want %v", tc.label, got, tc.window)
		}
		if got := end.Sub(start); got >= 24*time.Hour {
			t.Errorf("%s asked for %v, which is a bulk download rather than a poll", tc.label, got)
		}
		for k, want := range tc.extra {
			if got := values[k]; got != want {
				t.Errorf("%s %s = %q, want %q", tc.label, k, got, want)
			}
		}
	}
}

// parseQuery decodes a raw query string into single-valued pairs.
func parseQuery(raw string) (map[string]string, error) {
	values, err := url.ParseQuery(raw)
	if err != nil {
		return nil, err
	}
	out := make(map[string]string, len(values))
	for k, v := range values {
		if len(v) > 0 {
			out[k] = v[0]
		}
	}
	return out, nil
}

func TestSourceAndStationDataAgeAreDerivedFromThePayloadTimestamps(t *testing.T) {
	srv := newServer(t,
		serveFixture(t, "dbdt_bou.json"),
		serveFixture(t, "data_bou.json"),
		serveFixture(t, "observatories.json"))
	src := newSource(t, srv.URL)

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}

	// Newest observation anywhere in the capture is Z at 15:43, and the pinned
	// clock is 15:50, so the age is seven minutes — not zero, which is what a
	// source stamping samples at request time would report.
	want := (7 * time.Minute).Seconds()
	station := find(t, batch, metric.StationDataAge, Name, "BOU")
	if station.Value != want {
		t.Errorf("station data age = %v s, want %v s", station.Value, want)
	}
	sourceAge := find(t, batch, metric.SourceDataAge, Name)
	if sourceAge.Value != want {
		t.Errorf("source data age = %v s, want %v s", sourceAge.Value, want)
	}
}

func TestOneObservatoryFailingStillPublishesTheOthers(t *testing.T) {
	srv := newServer(t,
		func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Query().Get("id") == "FRD" {
				http.Error(w, "instrument offline", http.StatusServiceUnavailable)
				return
			}
			serveFixture(t, "dbdt_bou.json")(w, r)
		},
		func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Query().Get("id") == "FRD" {
				http.Error(w, "instrument offline", http.StatusServiceUnavailable)
				return
			}
			serveFixture(t, "data_bou.json")(w, r)
		},
		serveFixture(t, "observatories.json"))
	src := newSource(t, srv.URL, "BOU", "FRD")

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll returned an error though one observatory answered: %v", err)
	}
	if count(batch, metric.GeomagneticFieldRate) != 3 {
		t.Errorf("want three rate samples from the healthy observatory, got %d",
			count(batch, metric.GeomagneticFieldRate))
	}
	for _, s := range batch.Samples {
		if len(s.Labels) > 0 && s.Labels[0] == "FRD" {
			t.Errorf("published %s for the failed observatory", s.Desc.FullName())
		}
	}
	if find(t, batch, metric.StationDataAge, Name, "BOU").Value == 0 {
		t.Error("station data age for the healthy observatory should be non-zero")
	}
}

func TestEveryObservatoryFailingReturnsAnError(t *testing.T) {
	down := func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "down", http.StatusServiceUnavailable)
	}
	srv := newServer(t, down, down, serveFixture(t, "observatories.json"))
	src := newSource(t, srv.URL, "BOU", "FRD")

	batch, err := src.Poll(context.Background())
	if err == nil {
		t.Fatal("Poll succeeded with every observatory down")
	}
	if len(batch.Samples) != 0 {
		t.Errorf("failed poll carried %d samples", len(batch.Samples))
	}
}

func TestA404IsNotRetried(t *testing.T) {
	var hits atomic.Int64
	srv := newServer(t,
		func(w http.ResponseWriter, _ *http.Request) {
			hits.Add(1)
			http.NotFound(w, nil)
		},
		func(w http.ResponseWriter, _ *http.Request) {
			http.NotFound(w, nil)
		},
		serveFixture(t, "observatories.json"))

	src, err := New(config.USGSGeomag{
		BaseURL:       srv.URL,
		Observatories: []string{"BOU"},
		Retries:       3,
		Timeout:       3 * time.Second,
	}, testClient(t), testLogger())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	src.now = func() time.Time { return referenceNow }

	if _, err := src.Poll(context.Background()); err == nil {
		t.Fatal("Poll succeeded against a 404")
	}
	if got := hits.Load(); got != 1 {
		t.Errorf("dbdt was requested %d times for a 404 with three retries configured, want 1", got)
	}
}

func TestA429IsReportedAsRateLimited(t *testing.T) {
	limited := func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "1")
		w.WriteHeader(http.StatusTooManyRequests)
	}
	srv := newServer(t, limited, limited, serveFixture(t, "observatories.json"))
	src := newSource(t, srv.URL)

	_, err := src.Poll(context.Background())
	if err == nil {
		t.Fatal("Poll succeeded against a 429")
	}
	if !errors.Is(err, source.ErrRateLimited) {
		t.Errorf("error = %v, want it to wrap source.ErrRateLimited", err)
	}
}

func TestMalformedJSONReturnsAnErrorRatherThanPanicking(t *testing.T) {
	bodies := []string{
		`{"times": [`,
		`not json at all`,
		``,
		`{"times": [], "values": []}`,
		`{"metadata": {"status": 500}, "times": ["2026-09-09T15:00:00Z"]}`,
		`{"times": ["2026-09-09T15:00:00Z"], "values": [{"id":"X","values":[]}]}`,
	}
	for _, body := range bodies {
		serve := func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, body)
		}
		srv := newServer(t, serve, serve, serveFixture(t, "observatories.json"))
		src := newSource(t, srv.URL)

		batch, err := src.Poll(context.Background())
		if err == nil && len(batch.Samples) > 0 {
			t.Errorf("body %q produced %d samples", body, len(batch.Samples))
		}
	}
}

func TestACancelledContextStopsThePollWithoutPublishing(t *testing.T) {
	srv := newServer(t,
		serveFixture(t, "dbdt_bou.json"),
		serveFixture(t, "data_bou.json"),
		serveFixture(t, "observatories.json"))
	src := newSource(t, srv.URL, "BOU", "FRD", "NEW")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	batch, err := src.Poll(ctx)
	if err == nil {
		t.Fatal("Poll succeeded with a cancelled context")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("error = %v, want it to wrap context.Canceled", err)
	}
	if len(batch.Samples) != 0 {
		t.Errorf("cancelled poll carried %d samples", len(batch.Samples))
	}
}

func TestACancelledContextMidPollDoesNotWalkTheRemainingObservatories(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var dbdt atomic.Int64
	srv := newServer(t,
		func(w http.ResponseWriter, r *http.Request) {
			if dbdt.Add(1) == 1 {
				cancel()
			}
			http.Error(w, "gone away", http.StatusServiceUnavailable)
		},
		func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "gone away", http.StatusServiceUnavailable)
		},
		serveFixture(t, "observatories.json"))
	src := newSource(t, srv.URL, "BOU", "FRD", "NEW", "TUC", "SIT")

	if _, err := src.Poll(ctx); err == nil {
		t.Fatal("Poll succeeded after cancellation")
	}
	if got := dbdt.Load(); got > 2 {
		t.Errorf("kept requesting after cancellation: %d dbdt requests for five observatories", got)
	}
}

func TestOversizedResponsesAreRefusedRatherThanRead(t *testing.T) {
	huge := strings.Repeat("x", maxBodyBytes+1024)
	serve := func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, huge)
	}
	srv := newServer(t, serve, serve, serveFixture(t, "observatories.json"))
	src := newSource(t, srv.URL)

	if _, err := src.Poll(context.Background()); err == nil {
		t.Fatal("Poll accepted an oversized body")
	}
}
