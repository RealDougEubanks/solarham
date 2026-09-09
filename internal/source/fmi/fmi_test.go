package fmi

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
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

// referenceNow is the instant every test pretends it is.
//
// It is the exact wall clock at which the captured fixtures were fetched from
// the live service, and it is two days after the observations inside them.
// That is not a contrived value: on 2026-09-09 at 15:45 UTC, FMI served a
// document with Last-Modified 15:40:57 the same day whose every station
// reported Time 2026-09-07 11:55. Pinning the clock here is what lets the age
// assertions below be exact.
var referenceNow = time.Date(2026, 9, 9, 15, 50, 0, 0, time.UTC)

// capturedRIndexAge is referenceNow minus the R-index fixture's payload Time
// of 2026-09-07 11:55:00Z: two days, three hours and fifty-five minutes.
const capturedRIndexAge = 186900.0

// capturedRXAge is referenceNow minus the RX fixture's payload time of
// 2026-09-07T11:40:08Z.
const capturedRXAge = 187792.0

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// testClient is the shared client with its politeness spacing wound down;
// space.fmi.fi is spaced at ten seconds in production, which a test cannot
// wait for.
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

// testServer serves the two documents and counts requests.
type testServer struct {
	*httptest.Server
	rHits  atomic.Int64
	rxHits atomic.Int64
}

func newServer(t *testing.T, rIndex, rx http.HandlerFunc) *testServer {
	t.Helper()
	ts := &testServer{}
	mux := http.NewServeMux()
	mux.HandleFunc(pathRIndex, func(w http.ResponseWriter, r *http.Request) {
		ts.rHits.Add(1)
		if rIndex == nil {
			http.Error(w, "no handler", http.StatusInternalServerError)
			return
		}
		rIndex(w, r)
	})
	mux.HandleFunc(pathRX, func(w http.ResponseWriter, r *http.Request) {
		ts.rxHits.Add(1)
		if rx == nil {
			http.Error(w, "no handler", http.StatusInternalServerError)
			return
		}
		rx(w, r)
	})
	ts.Server = httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	return ts
}

// serveFixture answers with a fixture and a deliberately fresh Last-Modified,
// which is precisely the trap this source has to survive.
func serveFixture(t *testing.T, name string) http.HandlerFunc {
	t.Helper()
	body := fixture(t, name)
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Last-Modified", referenceNow.Format(http.TimeFormat))
		_, _ = w.Write(body)
	}
}

func serveBody(body string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, body)
	}
}

func newSource(t *testing.T, baseURL string) *Source {
	t.Helper()
	src, err := New(config.FMI{
		Enabled: true,
		BaseURL: baseURL,
		Timeout: 3 * time.Second,
	}, testClient(t), testLogger())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	src.now = func() time.Time { return referenceNow }
	return src
}

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

func lookup(b metric.Batch, desc *metric.Descriptor, labels ...string) (metric.Sample, bool) {
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
			return s, true
		}
	}
	return metric.Sample{}, false
}

func count(b metric.Batch, desc *metric.Descriptor) int {
	n := 0
	for _, s := range b.Samples {
		if s.Desc == desc {
			n++
		}
	}
	return n
}

func TestNewAppliesTheClaimedFiveMinuteCadenceByDefault(t *testing.T) {
	src, err := New(config.FMI{Enabled: true}, testClient(t), testLogger())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if src.Interval() != 5*time.Minute {
		t.Errorf("default interval = %v, want 5m", src.Interval())
	}
	if src.Name() != Name || Name != "fmi" {
		t.Errorf("Name = %q, want fmi", src.Name())
	}
	if src.Schedule().Interval() != 5*time.Minute {
		t.Errorf("schedule interval = %v, want 5m", src.Schedule().Interval())
	}
}

func TestNewRejectsABadBaseURLAndANilClient(t *testing.T) {
	for _, bad := range []string{"ftp://space.fmi.fi", "https://", ":::"} {
		if _, err := New(config.FMI{BaseURL: bad}, testClient(t), testLogger()); err == nil {
			t.Errorf("New accepted base URL %q", bad)
		}
	}
	if _, err := New(config.FMI{}, nil, testLogger()); err == nil {
		t.Error("New accepted a nil client")
	}
}

func TestPollParsesTheCapturedFixturesIntoExactValues(t *testing.T) {
	srv := newServer(t,
		serveFixture(t, "r_index_latest_en.json"),
		serveFixture(t, "RX_latest_en.json"))
	src := newSource(t, srv.URL)

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}

	// R-index, per station, exactly as captured.
	wantIndex := map[string]float64{
		"KEV": 126, "KIL": 154, "IVA": 143, "MUO": 141, "RAN": 124,
		"OUJ": 106, "MEK": 76, "HAN": 97, "NUR": 94, "TAR": 64,
	}
	for code, want := range wantIndex {
		s := find(t, batch, metric.AuroraRegionalIndex, code)
		if s.Value != want {
			t.Errorf("R-index %s = %v, want %v", code, s.Value, want)
		}
		if want := time.Date(2026, 9, 7, 11, 55, 0, 0, time.UTC); !s.Time.Equal(want) {
			t.Errorf("R-index %s timestamp = %v, want the payload's own %v",
				code, s.Time.UTC(), want)
		}
	}
	// Eleven stations in the document, ten with an observation.
	if got := count(batch, metric.AuroraRegionalIndex); got != 10 {
		t.Errorf("published %d R-index samples, want 10 of the 11 stations", got)
	}

	// Worded probability.
	if _, ok := lookup(batch, metric.AuroraProbabilityInfo, "KEV", "Medium probability of auroras"); !ok {
		t.Error("no probability sample for KEV")
	}

	// RX, published as the field range. MAS appears in RX but not in the
	// R-index document, which is itself worth pinning.
	wantRX := map[string]float64{
		"KEV": 39, "MAS": 45, "KIL": 58, "IVA": 42, "MUO": 53,
		"OUJ": 43, "MEK": 32, "HAN": 36, "NUR": 40, "TAR": 28,
	}
	for code, want := range wantRX {
		s := find(t, batch, metric.GeomagneticFieldRange, code)
		if s.Value != want {
			t.Errorf("RX %s = %v, want %v", code, s.Value, want)
		}
	}
	if got := count(batch, metric.GeomagneticFieldRange); got != 10 {
		t.Errorf("published %d field-range samples, want 10", got)
	}
}

// This is the test the package exists for.
func TestStaleContentBehindAFreshLastModifiedIsPublishedWithItsTrueAge(t *testing.T) {
	// The handler sends Last-Modified equal to the poll instant — the file was
	// genuinely just rewritten — while the fixture's payload timestamps are
	// two days older. A source that trusted the header would report an age of
	// zero and an operator would never know.
	srv := newServer(t,
		serveFixture(t, "r_index_latest_en.json"),
		serveFixture(t, "RX_latest_en.json"))
	src := newSource(t, srv.URL)

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}

	for _, code := range []string{"KEV", "KIL", "IVA", "MUO", "PEL", "RAN", "OUJ", "MEK", "HAN", "NUR", "TAR"} {
		s := find(t, batch, metric.StationDataAge, Name, code)
		if s.Value != capturedRIndexAge {
			t.Errorf("station %s age = %v s, want %v s (two days, three hours, fifty-five minutes)",
				code, s.Value, capturedRIndexAge)
		}
	}

	// The newest payload timestamp across both documents is the R-index's
	// 11:55, so the source age matches it rather than the RX document's
	// slightly older 11:40:08.
	if got := find(t, batch, metric.SourceDataAge, Name).Value; got != capturedRIndexAge {
		t.Errorf("source data age = %v s, want %v s", got, capturedRIndexAge)
	}
	if capturedRXAge <= capturedRIndexAge {
		t.Fatalf("fixture assumption broken: RX age %v should exceed R-index age %v",
			capturedRXAge, capturedRIndexAge)
	}

	// And the values are still published. A two-day-old R-index is a real
	// measurement of two days ago; dropping it would leave an empty dashboard
	// with no explanation, where a climbing age is a diagnosable alarm.
	if got := find(t, batch, metric.AuroraRegionalIndex, "KEV").Value; got != 126 {
		t.Errorf("KEV R-index = %v, want the stale-but-real 126", got)
	}
	if got := find(t, batch, metric.GeomagneticFieldRange, "KEV").Value; got != 39 {
		t.Errorf("KEV RX = %v, want the stale-but-real 39", got)
	}
}

func TestPerStationAgeTracksEachStationsOwnTimestampNotTheDocuments(t *testing.T) {
	// The mixed fixture has KEV current, KIL two days behind and PEL two days
	// behind with no observation. A document-wide age would report one number
	// for all three.
	srv := newServer(t,
		serveFixture(t, "r_index_mixed_staleness.json"),
		serveBody(`{"info":{},"data":{}}`))
	src := newSource(t, srv.URL)

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}

	if got := find(t, batch, metric.StationDataAge, Name, "KEV").Value; got != 0 {
		t.Errorf("KEV age = %v s, want 0 s; its Time equals the poll instant", got)
	}
	for _, code := range []string{"KIL", "PEL"} {
		if got := find(t, batch, metric.StationDataAge, Name, code).Value; got != capturedRIndexAge {
			t.Errorf("%s age = %v s, want %v s", code, got, capturedRIndexAge)
		}
	}
	// The newest station is current, so the source as a whole is current even
	// though two of its three stations are not. That is why both descriptors
	// exist.
	if got := find(t, batch, metric.SourceDataAge, Name).Value; got != 0 {
		t.Errorf("source data age = %v s, want 0 s", got)
	}
}

func TestANullRIndexProducesNoIndexOrProbabilitySample(t *testing.T) {
	srv := newServer(t,
		serveFixture(t, "r_index_latest_en.json"),
		serveFixture(t, "RX_latest_en.json"))
	src := newSource(t, srv.URL)

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}

	// PEL is present in the capture with "R-index": null and
	// "Probability of auroras": "Missing last observation".
	if s, ok := lookup(batch, metric.AuroraRegionalIndex, "PEL"); ok {
		t.Errorf("published an R-index of %v for a station whose observation is missing", s.Value)
	}
	for _, s := range batch.Samples {
		if s.Desc != metric.AuroraProbabilityInfo {
			continue
		}
		if s.Labels[0] == "PEL" {
			t.Errorf("published a probability for PEL: %q", s.Labels[1])
		}
		if s.Labels[1] == missingObservation {
			t.Errorf("published %q as an auroral probability category", s.Labels[1])
		}
	}

	// PEL's RX is null too, so it is missing from both products and counted
	// twice under the same reason.
	if s, ok := lookup(batch, metric.GeomagneticFieldRange, "PEL"); ok {
		t.Errorf("published an RX of %v for a null value", s.Value)
	}
	if got := find(t, batch, metric.StationsFiltered, Name, reasonMissingObservation).Value; got != 2 {
		t.Errorf("filtered count = %v, want 2 (PEL's null R-index and its null RX)", got)
	}

	// But its age is still published, so a station stuck on null does not
	// simply vanish from the output.
	if _, ok := lookup(batch, metric.StationDataAge, Name, "PEL"); !ok {
		t.Error("no station data age for PEL; a silent station must stay visible")
	}
}

func TestFilterCountsArePublishedEvenWhenNothingWasFiltered(t *testing.T) {
	srv := newServer(t,
		serveFixture(t, "r_index_mixed_staleness.json"),
		serveBody(`{"info":{},"data":{"KEV":{"time":"2026-09-09T15:50:00+0000","RX":{"value":11,"activity level":"low"},"station":"Kevo"}}}`))
	src := newSource(t, srv.URL)

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	for _, reason := range []string{reasonMissingObservation, reasonUnparseableTime} {
		if _, ok := lookup(batch, metric.StationsFiltered, Name, reason); !ok {
			t.Errorf("no filtered count for reason %q; an absent series cannot be distinguished from zero", reason)
		}
	}
	if got := find(t, batch, metric.StationsFiltered, Name, reasonUnparseableTime).Value; got != 0 {
		t.Errorf("unparseable-timestamp count = %v, want 0", got)
	}
}

func TestAStationWithAnUnparseableTimestampIsDroppedAndCounted(t *testing.T) {
	body := `{"info":{},"data":{
	  "KEV":{"Time":"2026-09-09 15:50:00+00:00","R-index":10,"Probability of auroras":"Low probability of auroras","Station":"Kevo"},
	  "BAD":{"Time":"whenever","R-index":99,"Probability of auroras":"Low probability of auroras","Station":"Nowhere"}}}`
	srv := newServer(t, serveBody(body), serveBody(`{"info":{},"data":{}}`))
	src := newSource(t, srv.URL)

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if s, ok := lookup(batch, metric.AuroraRegionalIndex, "BAD"); ok {
		t.Errorf("published %v for a station with no readable timestamp; there is no honest age to attach", s.Value)
	}
	if got := find(t, batch, metric.StationsFiltered, Name, reasonUnparseableTime).Value; got != 1 {
		t.Errorf("unparseable-timestamp count = %v, want 1", got)
	}
	if got := find(t, batch, metric.AuroraRegionalIndex, "KEV").Value; got != 10 {
		t.Errorf("KEV R-index = %v, want 10; one bad station must not lose the rest", got)
	}
}

func TestOneDocumentFailingStillPublishesTheOther(t *testing.T) {
	srv := newServer(t,
		serveFixture(t, "r_index_latest_en.json"),
		func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "down", http.StatusServiceUnavailable)
		})
	src := newSource(t, srv.URL)

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if count(batch, metric.AuroraRegionalIndex) != 10 {
		t.Errorf("want ten R-index samples with RX down, got %d", count(batch, metric.AuroraRegionalIndex))
	}
	if count(batch, metric.GeomagneticFieldRange) != 0 {
		t.Errorf("published %d field-range samples from a failed document", count(batch, metric.GeomagneticFieldRange))
	}
}

func TestBothDocumentsFailingReturnsAnError(t *testing.T) {
	down := func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "down", http.StatusServiceUnavailable)
	}
	srv := newServer(t, down, down)
	src := newSource(t, srv.URL)

	batch, err := src.Poll(context.Background())
	if err == nil {
		t.Fatal("Poll succeeded with both documents down")
	}
	if len(batch.Samples) != 0 {
		t.Errorf("failed poll carried %d samples", len(batch.Samples))
	}
}

func TestBothDocumentsAnsweringNotModifiedIsASuccessfulUneventfulPoll(t *testing.T) {
	notModified := func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotModified)
	}
	srv := newServer(t, notModified, notModified)
	src := newSource(t, srv.URL)

	batch, err := src.Poll(context.Background())
	if !errors.Is(err, source.ErrNotModified) {
		t.Errorf("error = %v, want source.ErrNotModified", err)
	}
	if len(batch.Samples) != 0 {
		t.Errorf("a 304 poll carried %d samples", len(batch.Samples))
	}
}

func TestA404IsNotRetried(t *testing.T) {
	var hits atomic.Int64
	srv := newServer(t,
		func(w http.ResponseWriter, _ *http.Request) {
			hits.Add(1)
			http.NotFound(w, nil)
		},
		func(w http.ResponseWriter, _ *http.Request) { http.NotFound(w, nil) })

	src, err := New(config.FMI{
		BaseURL: srv.URL,
		Retries: 3,
		Timeout: 3 * time.Second,
	}, testClient(t), testLogger())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	src.now = func() time.Time { return referenceNow }

	if _, err := src.Poll(context.Background()); err == nil {
		t.Fatal("Poll succeeded against a 404")
	}
	if got := hits.Load(); got != 1 {
		t.Errorf("requested the R-index document %d times for a 404 with three retries configured, want 1", got)
	}
}

func TestA429IsReportedAsRateLimited(t *testing.T) {
	limited := func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "1")
		w.WriteHeader(http.StatusTooManyRequests)
	}
	srv := newServer(t, limited, limited)
	src := newSource(t, srv.URL)

	batch, err := src.Poll(context.Background())
	if err == nil {
		t.Fatal("Poll succeeded against a 429")
	}
	if !errors.Is(err, source.ErrRateLimited) {
		t.Errorf("error = %v, want it to wrap source.ErrRateLimited", err)
	}
	if len(batch.Samples) != 0 {
		t.Errorf("rate-limited poll carried %d samples", len(batch.Samples))
	}
}

func TestMalformedJSONReturnsAnErrorOrNoSamplesRatherThanPanicking(t *testing.T) {
	bodies := []string{
		`{"data": {`,
		`not json`,
		`[]`,
		`{"data": {"KEV": "a string where an object belongs"}}`,
		`{"data": {"KEV": {"Time": null, "R-index": "not a number"}}}`,
		strings.Repeat(`{"data":{}}`, 2),
	}
	for _, body := range bodies {
		srv := newServer(t, serveBody(body), serveBody(body))
		src := newSource(t, srv.URL)

		batch, err := src.Poll(context.Background())
		if err != nil {
			continue
		}
		for _, s := range batch.Samples {
			switch s.Desc {
			case metric.AuroraRegionalIndex, metric.GeomagneticFieldRange, metric.AuroraProbabilityInfo:
				t.Errorf("body %q produced a value sample %s%v", body, s.Desc.FullName(), s.Labels)
			}
		}
	}
}

func TestACancelledContextStopsThePollWithoutPublishing(t *testing.T) {
	srv := newServer(t,
		serveFixture(t, "r_index_latest_en.json"),
		serveFixture(t, "RX_latest_en.json"))
	src := newSource(t, srv.URL)

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
	if srv.rHits.Load() != 0 {
		t.Errorf("made %d requests after cancellation", srv.rHits.Load())
	}
}

func TestSamplesAreOrderedDeterministicallyAcrossPolls(t *testing.T) {
	srv := newServer(t,
		serveFixture(t, "r_index_latest_en.json"),
		serveFixture(t, "RX_latest_en.json"))
	src := newSource(t, srv.URL)

	var first []string
	for poll := range 3 {
		batch, err := src.Poll(context.Background())
		if err != nil {
			t.Fatalf("poll %d: %v", poll, err)
		}
		keys := make([]string, 0, len(batch.Samples))
		for _, s := range batch.Samples {
			keys = append(keys, s.Key())
		}
		if poll == 0 {
			first = keys
			continue
		}
		if strings.Join(keys, "|") != strings.Join(first, "|") {
			t.Errorf("poll %d produced a different sample order; map iteration is leaking through", poll)
		}
	}
}
