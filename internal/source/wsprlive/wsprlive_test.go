package wsprlive

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/RealDougEubanks/solarham/internal/config"
	"github.com/RealDougEubanks/solarham/internal/httpx"
	"github.com/RealDougEubanks/solarham/internal/metric"
	"github.com/RealDougEubanks/solarham/internal/source"
)

// referenceNow is the instant every test pretends it is. The fixture carries
// fixed timestamps and metric.SourceDataAge is computed against them, so the
// clock has to be pinned rather than left as time.Now.
var referenceNow = time.Date(2026, 9, 9, 15, 50, 0, 0, time.UTC)

// testLogger discards output but keeps the debug path exercised, so a panic or
// a bad argument in a log line fails a test rather than production.
func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// testClient is the shared httpx client with the politeness policy relaxed.
//
// The real policy table spaces db1.wspr.live by 30 seconds, and an httptest
// server is on 127.0.0.1 which falls to the strict 30-second fallback. Tests
// exercise the parsing and the schedule, not the limiter — httpx has its own
// tests for that — so the spacing is dialled down rather than waited out.
func testClient(t *testing.T) *httpx.Client {
	t.Helper()
	return httpx.New(httpx.Options{
		Timeout:  5 * time.Second,
		Fallback: httpx.HostPolicy{MinInterval: time.Nanosecond, Burst: 1000},
	}, testLogger())
}

// countingServer serves one handler and records every request URL it saw.
type countingServer struct {
	*httptest.Server
	hits atomic.Int64
	urls chan string
}

func newServer(t *testing.T, handler http.HandlerFunc) *countingServer {
	t.Helper()
	cs := &countingServer{urls: make(chan string, 16)}
	cs.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cs.hits.Add(1)
		select {
		case cs.urls <- r.URL.String():
		default:
		}
		handler(w, r)
	}))
	t.Cleanup(cs.Close)
	return cs
}

func (cs *countingServer) lastURL(t *testing.T) string {
	t.Helper()
	var last string
	for {
		select {
		case u := <-cs.urls:
			last = u
		default:
			if last == "" {
				t.Fatalf("the server recorded no requests")
			}
			return last
		}
	}
}

func fixture(t *testing.T) []byte {
	t.Helper()
	body, err := os.ReadFile("testdata/bands.json")
	if err != nil {
		t.Fatalf("reading fixture: %v", err)
	}
	return body
}

// serveBody answers every request with the given body.
func serveBody(body []byte) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	}
}

// newSource builds a source aimed at ts with the clock pinned.
func newSource(t *testing.T, base string, cfg config.WSPRLive) *Source {
	t.Helper()
	cfg.BaseURL = base
	src, err := New(cfg, testClient(t), testLogger())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	src.now = func() time.Time { return referenceNow }
	return src
}

// compactResult wraps rows in the ClickHouse JSONCompact envelope this package
// parses, so a test can state exactly the rows it cares about.
func compactResult(rows string) []byte {
	return []byte(`{"meta":[{"name":"band","type":"Int16"},{"name":"spots","type":"UInt64"},` +
		`{"name":"tx","type":"UInt64"},{"name":"rx","type":"UInt64"},` +
		`{"name":"avg_snr","type":"Float64"},{"name":"avg_km","type":"Float64"},` +
		`{"name":"newest","type":"DateTime"}],"data":[` + rows + `],"rows":1}`)
}

// sampleFor finds the one sample matching a descriptor and label set.
func sampleFor(t *testing.T, batch metric.Batch, desc *metric.Descriptor, labels ...string) metric.Sample {
	t.Helper()
	var found []metric.Sample
	for _, s := range batch.Samples {
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
		t.Fatalf("expected exactly one %s%v sample, found %d", desc.FullName(), labels, len(found))
	}
	return found[0]
}

func hasSample(batch metric.Batch, desc *metric.Descriptor, labels ...string) bool {
	for _, s := range batch.Samples {
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
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// Band mapping
// ---------------------------------------------------------------------------

// TestEveryDocumentedBandCodeMapsToItsMetreLabel walks the whole published
// band list. The codes are MHz digits, not wavelengths, and getting one wrong
// mislabels a whole band's activity on every dashboard downstream, so every
// entry is asserted rather than a representative sample.
func TestEveryDocumentedBandCodeMapsToItsMetreLabel(t *testing.T) {
	cases := []struct {
		code int
		want string
	}{
		{-1, "LF"},
		{0, "MF"},
		{1, "160m"},
		{3, "80m"},
		{5, "60m"},
		{7, "40m"},
		{10, "30m"},
		{14, "20m"},
		{18, "17m"},
		{21, "15m"},
		{24, "12m"},
		{28, "10m"},
		{50, "6m"},
		{70, "4m"},
		{144, "2m"},
		{430, "70cm"},
		{1296, "23cm"},
	}

	if len(cases) != len(bandLabels) {
		t.Errorf("the table covers %d codes but bandLabels holds %d; a code was added without a test",
			len(cases), len(bandLabels))
	}

	for _, tc := range cases {
		if got := bandLabel(tc.code); got != tc.want {
			t.Errorf("band code %d mapped to %q, want %q", tc.code, got, tc.want)
		}
	}
}

// TestEveryDocumentedBandCodeReachesItsLabelThroughAPoll is the same coverage
// asserted end to end, because a correct map is no use if parse never consults
// it.
func TestEveryDocumentedBandCodeReachesItsLabelThroughAPoll(t *testing.T) {
	var rows []string
	for code := range bandLabels {
		rows = append(rows, `[`+strconv.Itoa(code)+`,10,2,3,-15.5,900,"2026-09-09 15:44:00"]`)
	}
	ts := newServer(t, serveBody(compactResult(strings.Join(rows, ","))))
	src := newSource(t, ts.URL, config.WSPRLive{})

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	for code, label := range bandLabels {
		if !hasSample(batch, metric.SpotCount, network, label) {
			t.Errorf("band code %d produced no spot count labelled %q", code, label)
		}
	}
}

// parseQuery reads the parameters off a recorded request URL.
func parseQuery(raw string) (url.Values, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, err
	}
	return u.Query(), nil
}

// decodeQueryParam returns the decoded SQL the request carried.
func decodeQueryParam(raw string) (string, error) {
	params, err := parseQuery(raw)
	if err != nil {
		return "", err
	}
	return params.Get("query"), nil
}

func keysOf(v url.Values) []string {
	out := make([]string, 0, len(v))
	for k := range v {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// TestAnUnmappedBandCodeStillProducesASeriesLabelledWithTheRawCode covers the
// codes wspr.live returns that its own documentation does not list: 13 and 40
// have both been seen live. Dropping them would silently lose activity on a
// band the network has just started reporting.
func TestAnUnmappedBandCodeStillProducesASeriesLabelledWithTheRawCode(t *testing.T) {
	ts := newServer(t, serveBody(compactResult(
		`[13,25,5,13,-22.32,1443.9,"2026-09-09 15:40:00"],`+
			`[40,12,1,2,37,0,"2026-09-09 15:41:00"]`)))
	src := newSource(t, ts.URL, config.WSPRLive{})

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}

	if got := sampleFor(t, batch, metric.SpotCount, network, "band-13").Value; got != 25 {
		t.Errorf("band-13 spot count = %v, want 25", got)
	}
	if got := sampleFor(t, batch, metric.SpotCount, network, "band-40").Value; got != 12 {
		t.Errorf("band-40 spot count = %v, want 12", got)
	}
	if _, mapped := bandLabels[13]; mapped {
		t.Errorf("band code 13 is now in bandLabels; this test no longer covers the unmapped path")
	}
}

// ---------------------------------------------------------------------------
// Request shape
// ---------------------------------------------------------------------------

// TestTheQueryCarriesTheTimeBoundAndGroupBy asserts the two constraints
// wspr.live's documentation asks for. An unbounded or ungrouped query is the
// thing most likely to get this exporter blocked.
func TestTheQueryCarriesTheTimeBoundAndGroupBy(t *testing.T) {
	ts := newServer(t, serveBody(fixture(t)))
	src := newSource(t, ts.URL, config.WSPRLive{Window: 20 * time.Minute})

	if _, err := src.Poll(context.Background()); err != nil {
		t.Fatalf("Poll: %v", err)
	}

	raw := ts.lastURL(t)
	decoded, err := decodeQueryParam(raw)
	if err != nil {
		t.Fatalf("request URL %q: %v", raw, err)
	}

	for _, want := range []string{
		"WHERE time > now() - INTERVAL 20 MINUTE",
		"GROUP BY band",
		"FORMAT JSONCompact",
		"FROM wspr.rx",
	} {
		if !strings.Contains(decoded, want) {
			t.Errorf("query %q does not contain %q", decoded, want)
		}
	}
	if strings.Contains(decoded, "JOIN") {
		t.Errorf("query %q contains a JOIN, which wspr.live asks callers to avoid", decoded)
	}
}

// TestTheRequestSendsOnlyTheQueryParameter guards the documented interface:
// wspr.live strips everything except query, so sending anything else is dead
// weight that makes the request look less like what they expect.
func TestTheRequestSendsOnlyTheQueryParameter(t *testing.T) {
	ts := newServer(t, serveBody(fixture(t)))
	src := newSource(t, ts.URL, config.WSPRLive{})

	if _, err := src.Poll(context.Background()); err != nil {
		t.Fatalf("Poll: %v", err)
	}

	raw := ts.lastURL(t)
	params, err := parseQuery(raw)
	if err != nil {
		t.Fatalf("request URL %q: %v", raw, err)
	}
	if len(params) != 1 {
		t.Errorf("request carried %d parameters %v, want only query", len(params), keysOf(params))
	}
	if _, ok := params["query"]; !ok {
		t.Errorf("request has no query parameter; it carried %v", keysOf(params))
	}
}

// TestAWindowIsConvertedToWholeMinutes covers the boundary where a sub-minute
// window would otherwise produce "INTERVAL 0 MINUTE" and select everything.
func TestAWindowIsConvertedToWholeMinutes(t *testing.T) {
	src := newSource(t, "https://example.invalid", config.WSPRLive{Window: 90 * time.Second})
	// 90s is below minWindow, so it is raised to one WSPR cycle.
	if got := src.Window(); got != minWindow {
		t.Errorf("window = %s, want %s", got, minWindow)
	}
	if q := src.query(); !strings.Contains(q, "INTERVAL 2 MINUTE") {
		t.Errorf("query %q does not bound the range at 2 minutes", q)
	}
	if strings.Contains(src.query(), "INTERVAL 0 MINUTE") {
		t.Errorf("query bounds the range at zero minutes, which would select the whole table")
	}
}

// ---------------------------------------------------------------------------
// Schedule
// ---------------------------------------------------------------------------

// TestAConfiguredIntervalBelowTheWSPRCycleIsFloored is the courtesy limit. It
// lives in the schedule rather than in validation, because a limit an operator
// can lower by editing a setting is not a limit.
func TestAConfiguredIntervalBelowTheWSPRCycleIsFloored(t *testing.T) {
	src := newSource(t, "https://example.invalid", config.WSPRLive{Interval: 5 * time.Second})

	if got := src.Interval(); got != minInterval {
		t.Errorf("Interval() = %s, want the %s floor", got, minInterval)
	}

	sched := src.Schedule()
	next := sched.NextAfter(referenceNow)
	if gap := next.Sub(referenceNow); gap < minInterval {
		t.Errorf("the schedule polls again after %s, inside the %s floor", gap, minInterval)
	}
}

// TestAnUnsetIntervalDefaultsToFiveMinutes documents the default cadence: two
// or three WSPR cycles per poll, far inside the documented 20-requests-a-minute
// limit that every user of the service shares.
func TestAnUnsetIntervalDefaultsToFiveMinutes(t *testing.T) {
	src := newSource(t, "https://example.invalid", config.WSPRLive{})
	if got := src.Interval(); got != defaultInterval {
		t.Errorf("Interval() = %s, want %s", got, defaultInterval)
	}
}

// TestAConfiguredIntervalAboveTheFloorIsHonoured makes sure the clamp is a
// floor and not a fixed value.
func TestAConfiguredIntervalAboveTheFloorIsHonoured(t *testing.T) {
	src := newSource(t, "https://example.invalid", config.WSPRLive{Interval: 30 * time.Minute})
	if got := src.Interval(); got != 30*time.Minute {
		t.Errorf("Interval() = %s, want 30m", got)
	}
}

// ---------------------------------------------------------------------------
// Parsing
// ---------------------------------------------------------------------------

// TestPollParsesTheCapturedJSONCompactResponse runs the real recorded body
// through the parser and checks one full band against the fixture by hand.
func TestPollParsesTheCapturedJSONCompactResponse(t *testing.T) {
	ts := newServer(t, serveBody(fixture(t)))
	src := newSource(t, ts.URL, config.WSPRLive{Window: 15 * time.Minute})

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if batch.Source != Name {
		t.Errorf("batch source = %q, want %q", batch.Source, Name)
	}

	// The 20 m row of testdata/bands.json:
	//   [14, 19223, 482, 571, -16, 2457.6, "2026-09-09 15:46:00"]
	if got := sampleFor(t, batch, metric.SpotCount, network, "20m").Value; got != 19223 {
		t.Errorf("20m spot count = %v, want 19223", got)
	}
	if got := sampleFor(t, batch, metric.SpotStationCount, network, "20m", "tx").Value; got != 482 {
		t.Errorf("20m tx station count = %v, want 482", got)
	}
	if got := sampleFor(t, batch, metric.SpotStationCount, network, "20m", "rx").Value; got != 571 {
		t.Errorf("20m rx station count = %v, want 571", got)
	}
	if got := sampleFor(t, batch, metric.SpotSNRMean, network, "20m").Value; got != -16 {
		t.Errorf("20m mean SNR = %v, want -16", got)
	}
	if got := sampleFor(t, batch, metric.SpotDistanceMean, network, "20m").Value; got != 2457.6 {
		t.Errorf("20m mean distance = %v, want 2457.6", got)
	}

	want := time.Date(2026, 9, 9, 15, 46, 0, 0, time.UTC)
	if got := sampleFor(t, batch, metric.SpotCount, network, "20m").Time; !got.Equal(want) {
		t.Errorf("20m sample time = %s, want the row's own max(time) %s", got, want)
	}

	// Negative band codes must survive the round trip; -1 is LF.
	if got := sampleFor(t, batch, metric.SpotCount, network, "LF").Value; got != 9 {
		t.Errorf("LF spot count = %v, want 9", got)
	}

	for _, s := range batch.Samples {
		if err := s.Validate(); err != nil {
			t.Errorf("Poll emitted an invalid sample: %v", err)
		}
	}
}

// TestSpotRateIsDerivedFromTheCountAndWindow checks the arithmetic rather than
// asking the upstream for a second aggregate over the same rows.
func TestSpotRateIsDerivedFromTheCountAndWindow(t *testing.T) {
	ts := newServer(t, serveBody(compactResult(
		`[14,300,20,30,-15,1000,"2026-09-09 15:46:00"]`)))
	src := newSource(t, ts.URL, config.WSPRLive{Window: 15 * time.Minute})

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if got := sampleFor(t, batch, metric.SpotRate, network, "20m").Value; got != 20 {
		t.Errorf("spot rate = %v spots/min, want 300 spots over 15 minutes = 20", got)
	}
}

// TestSourceDataAgeIsMeasuredFromThePayloadsNewestSpot is the freshness check.
// HTTP 200 with stale content is the dominant failure mode across every
// upstream here, so age must never come from receipt time when the payload
// carries a timestamp of its own.
func TestSourceDataAgeIsMeasuredFromThePayloadsNewestSpot(t *testing.T) {
	ts := newServer(t, serveBody(compactResult(
		`[7,100,10,20,-14,700,"2026-09-09 15:40:00"],`+
			`[14,200,20,30,-15,1000,"2026-09-09 15:45:00"]`)))
	src := newSource(t, ts.URL, config.WSPRLive{})

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	// referenceNow is 15:50:00; the newest row is 15:45:00.
	if got := sampleFor(t, batch, metric.SourceDataAge, Name).Value; got != 300 {
		t.Errorf("source data age = %v s, want 300", got)
	}
}

// TestAFutureTimestampClampsTheAgeToZero covers clock skew between us and the
// upstream. A negative age reads as a broken exporter rather than a surprising
// publisher.
func TestAFutureTimestampClampsTheAgeToZero(t *testing.T) {
	ts := newServer(t, serveBody(compactResult(
		`[14,200,20,30,-15,1000,"2026-09-09 16:10:00"]`)))
	src := newSource(t, ts.URL, config.WSPRLive{})

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if got := sampleFor(t, batch, metric.SourceDataAge, Name).Value; got != 0 {
		t.Errorf("source data age = %v s, want 0 for a future timestamp", got)
	}
}

// TestANullMeanIsSkippedRatherThanPublishedAsZero: a null avg is "not
// measured", and publishing it as 0 dB would look like a perfectly readable
// signal.
func TestANullMeanIsSkippedRatherThanPublishedAsZero(t *testing.T) {
	ts := newServer(t, serveBody(compactResult(
		`[14,200,20,30,null,null,"2026-09-09 15:45:00"]`)))
	src := newSource(t, ts.URL, config.WSPRLive{})

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if hasSample(batch, metric.SpotSNRMean, network, "20m") {
		t.Errorf("a null mean SNR was published")
	}
	if hasSample(batch, metric.SpotDistanceMean, network, "20m") {
		t.Errorf("a null mean distance was published")
	}
	if !hasSample(batch, metric.SpotCount, network, "20m") {
		t.Errorf("the spot count was dropped along with the null means")
	}
}

// TestAQuotedNumericColumnIsAccepted: ClickHouse renders 64-bit integers as
// quoted strings under output_format_json_quote_64bit_integers, which is on by
// default in some deployments.
func TestAQuotedNumericColumnIsAccepted(t *testing.T) {
	ts := newServer(t, serveBody(compactResult(
		`[14,"19375","486","570",-15.89,2413.8,"2026-09-09 15:45:00"]`)))
	src := newSource(t, ts.URL, config.WSPRLive{})

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if got := sampleFor(t, batch, metric.SpotCount, network, "20m").Value; got != 19375 {
		t.Errorf("quoted spot count parsed as %v, want 19375", got)
	}
}

// TestAMissingColumnIsReportedAsASchemaChange. Introspection is blocked
// upstream, so the columns are hard-coded and the only way an operator learns
// the schema moved is this error naming the column.
func TestAMissingColumnIsReportedAsASchemaChange(t *testing.T) {
	body := `{"meta":[{"name":"band","type":"Int16"},{"name":"spots","type":"UInt64"}],` +
		`"data":[[14,100]],"rows":1}`
	ts := newServer(t, serveBody([]byte(body)))
	src := newSource(t, ts.URL, config.WSPRLive{})

	_, err := src.Poll(context.Background())
	if err == nil {
		t.Fatalf("Poll accepted a response missing most of its columns")
	}
	if !strings.Contains(err.Error(), `"tx"`) {
		t.Errorf("error %q does not name the missing column", err)
	}
}

// TestAMalformedBodyErrorsRatherThanPanicking. ClickHouse answers a rejected
// query with a plain-text body, so this is a real code path, not a
// hypothetical.
func TestAMalformedBodyErrorsRatherThanPanicking(t *testing.T) {
	for name, body := range map[string]string{
		"clickhouse plain-text error": "Code: 60. DB::Exception: Table wspr.rx does not exist.",
		"truncated json":              `{"meta":[{"name":"band"`,
		"empty":                       "",
		"html error page":             "<html><body>502 Bad Gateway</body></html>",
	} {
		t.Run(name, func(t *testing.T) {
			ts := newServer(t, serveBody([]byte(body)))
			src := newSource(t, ts.URL, config.WSPRLive{})
			if _, err := src.Poll(context.Background()); err == nil {
				t.Errorf("Poll accepted a malformed body")
			}
		})
	}
}

// TestAResultWithNoRowsIsASuccessWithNoSamples. A band-quiet minute is not a
// failure, and neither is a maintenance window that empties the table.
func TestAResultWithNoRowsIsASuccessWithNoSamples(t *testing.T) {
	ts := newServer(t, serveBody(compactResult("")))
	src := newSource(t, ts.URL, config.WSPRLive{})

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if batch.Len() != 0 {
		t.Errorf("an empty result produced %d samples, want 0", batch.Len())
	}
}

// ---------------------------------------------------------------------------
// Transport behaviour
// ---------------------------------------------------------------------------

// TestANotFoundIsNotRetried. Repeating a 404 against a service that shares a
// 20-requests-a-minute budget with everybody else is abuse rather than
// resilience.
func TestANotFoundIsNotRetried(t *testing.T) {
	ts := newServer(t, func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "not found", http.StatusNotFound)
	})
	src := newSource(t, ts.URL, config.WSPRLive{Retries: 3})

	if _, err := src.Poll(context.Background()); err == nil {
		t.Fatalf("Poll succeeded against a 404")
	}
	if got := ts.hits.Load(); got != 1 {
		t.Errorf("a 404 was requested %d times, want 1", got)
	}
}

// TestATooManyRequestsIsReportedAsRateLimited so the scheduler backs off
// rather than continuing to knock on a door the service has just closed.
func TestATooManyRequestsIsReportedAsRateLimited(t *testing.T) {
	ts := newServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "60")
		http.Error(w, "slow down", http.StatusTooManyRequests)
	})
	src := newSource(t, ts.URL, config.WSPRLive{Retries: 3})

	_, err := src.Poll(context.Background())
	if !errors.Is(err, source.ErrRateLimited) {
		t.Errorf("error = %v, want source.ErrRateLimited", err)
	}
	if !errors.Is(err, httpx.ErrRateLimited) {
		t.Errorf("error = %v, want it to wrap httpx.ErrRateLimited", err)
	}
	if got := ts.hits.Load(); got != 1 {
		t.Errorf("a 429 was requested %d times, want 1", got)
	}
}

// TestANotModifiedIsReportedAsSuch rather than as a failure, so the scheduler
// can log "unchanged" instead of "no samples".
func TestANotModifiedIsReportedAsSuch(t *testing.T) {
	ts := newServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotModified)
	})
	src := newSource(t, ts.URL, config.WSPRLive{})

	_, err := src.Poll(context.Background())
	if !errors.Is(err, source.ErrNotModified) {
		t.Errorf("error = %v, want source.ErrNotModified", err)
	}
}

// TestACancelledContextAbortsThePollWithoutRequesting.
func TestACancelledContextAbortsThePollWithoutRequesting(t *testing.T) {
	ts := newServer(t, serveBody(fixture(t)))
	src := newSource(t, ts.URL, config.WSPRLive{})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := src.Poll(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("error = %v, want context.Canceled", err)
	}
}

// TestAContextCancelledMidFlightAbortsThePoll covers the case where the
// request has already been issued.
func TestAContextCancelledMidFlightAbortsThePoll(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	ts := newServer(t, func(w http.ResponseWriter, r *http.Request) {
		cancel()
		<-r.Context().Done()
	})
	src := newSource(t, ts.URL, config.WSPRLive{})

	if _, err := src.Poll(ctx); err == nil {
		t.Errorf("Poll succeeded against a cancelled request")
	}
}

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

func TestNewRequiresTheSharedClient(t *testing.T) {
	if _, err := New(config.WSPRLive{}, nil, testLogger()); err == nil {
		t.Errorf("New accepted a nil client; politeness has to be enforced across sources")
	}
}

func TestNewRejectsABaseURLThatIsNotHTTP(t *testing.T) {
	for _, base := range []string{"ftp://db1.wspr.live", "not a url at all", "://"} {
		if _, err := New(config.WSPRLive{BaseURL: base}, testClient(t), testLogger()); err == nil {
			t.Errorf("New accepted base URL %q", base)
		}
	}
}

func TestAnUnsetBaseURLDefaultsToTheLiveService(t *testing.T) {
	src, err := New(config.WSPRLive{}, testClient(t), testLogger())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if !strings.HasPrefix(src.requestURL(), defaultBaseURL+"/?query=") {
		t.Errorf("request URL %q does not start at %s", src.requestURL(), defaultBaseURL)
	}
}
