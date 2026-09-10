package nmdb

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
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

// referenceNow is the instant every test pretends it is. The fixtures carry
// fixed timestamps and metric.SourceDataAge is computed against them.
var referenceNow = time.Date(2026, 9, 9, 15, 48, 0, 0, time.UTC)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// capturingLogger records every log record so a test can assert that the
// acknowledgement reached the log, which is this package's licence obligation
// rather than decoration.
type capturingLogger struct {
	mu      sync.Mutex
	records []slog.Record
}

func (c *capturingLogger) Enabled(context.Context, slog.Level) bool { return true }

func (c *capturingLogger) Handle(_ context.Context, r slog.Record) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.records = append(c.records, r.Clone())
	return nil
}

func (c *capturingLogger) WithAttrs([]slog.Attr) slog.Handler { return c }
func (c *capturingLogger) WithGroup(string) slog.Handler      { return c }

// attr returns the named attribute of the first record whose message contains
// substr, and how many records matched.
func (c *capturingLogger) attr(substr, key string) (string, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var (
		value string
		count int
	)
	for _, r := range c.records {
		if !strings.Contains(r.Message, substr) {
			continue
		}
		count++
		if count == 1 {
			r.Attrs(func(a slog.Attr) bool {
				if a.Key == key {
					value = a.Value.String()
					return false
				}
				return true
			})
		}
	}
	return value, count
}

// testClient is the shared httpx client with the politeness policy relaxed.
// The real policy spaces www.nmdb.eu by 30 seconds with a burst of one; the
// limiter has its own tests in httpx.
func testClient(t *testing.T) *httpx.Client {
	t.Helper()
	return httpx.New(httpx.Options{
		Timeout:  5 * time.Second,
		Fallback: httpx.HostPolicy{MinInterval: time.Nanosecond, Burst: 1000},
	}, testLogger())
}

// recordingServer serves one handler and records every request it saw.
type recordingServer struct {
	*httptest.Server
	hits atomic.Int64

	mu   sync.Mutex
	urls []*url.URL
}

func newServer(t *testing.T, handler http.HandlerFunc) *recordingServer {
	t.Helper()
	rs := &recordingServer{}
	rs.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rs.hits.Add(1)
		rs.mu.Lock()
		u := *r.URL
		rs.urls = append(rs.urls, &u)
		rs.mu.Unlock()
		handler(w, r)
	}))
	t.Cleanup(rs.Close)
	return rs
}

func (rs *recordingServer) requests() []*url.URL {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	out := make([]*url.URL, len(rs.urls))
	copy(out, rs.urls)
	return out
}

func fixture(t *testing.T, name string) []byte {
	t.Helper()
	body, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatalf("reading fixture %s: %v", name, err)
	}
	return body
}

func serveBody(body []byte) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write(body)
	}
}

func newSourceWithLog(t *testing.T, base string, cfg config.NMDB, log *slog.Logger) *Source {
	t.Helper()
	cfg.BaseURL = base
	src, err := New(cfg, testClient(t), log)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	src.now = func() time.Time { return referenceNow }
	return src
}

func newSource(t *testing.T, base string, cfg config.NMDB) *Source {
	t.Helper()
	return newSourceWithLog(t, base, cfg, testLogger())
}

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

func stationRates(batch metric.Batch) map[string]float64 {
	out := map[string]float64{}
	for _, s := range batch.Samples {
		if s.Desc == metric.NeutronMonitorRate {
			out[s.Labels[0]] = s.Value
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// The extraction hazard
// ---------------------------------------------------------------------------

// TestTheDataBlockIsSlicedOutOfTheHTMLWithoutTouchingTheRestOfThePage is the
// central test of this package.
//
// The captured page embeds a complete nested HTML document — <html>, <head>,
// <body>, </body></html> — inside its acknowledgement section, and that nested
// document appears BEFORE the <pre><code> block holding the data. Any parser
// that strips tags across the whole page, or slices to the first </html>, ends
// up either with navigation furniture interleaved with the numbers or with
// nothing at all.
func TestTheDataBlockIsSlicedOutOfTheHTMLWithoutTouchingTheRestOfThePage(t *testing.T) {
	page := string(fixture(t, "draw_graph.html"))

	// The fixture must actually contain the hazard, or this test proves
	// nothing. Guard the premise.
	preAt := strings.Index(page, preOpen)
	if preAt < 0 {
		t.Fatalf("the fixture has no %s block", preOpen)
	}
	if closing := strings.Index(page, "</html>"); closing < 0 || closing > preAt {
		t.Fatalf("the fixture no longer contains a nested closing </html> before the data block; "+
			"it can no longer prove the extraction is safe (found at %d, block at %d)", closing, preAt)
	}

	block, err := extractBlock(page)
	if err != nil {
		t.Fatalf("extractBlock: %v", err)
	}

	// Nothing from the surrounding page may survive the slice.
	for _, forbidden := range []string{
		"<html", "</html>", "<body", "</body>", "<div", "<h1>", "href=", "<title",
		"Total Running Time",
	} {
		if strings.Contains(block, forbidden) {
			t.Errorf("the extracted block contains %q from the surrounding page", forbidden)
		}
	}
	if !strings.Contains(block, "QUERY RESULTS SUMMARY") {
		t.Errorf("the extracted block is missing the table header")
	}
}

// TestTheNestedDocumentsDecoyRowsAreNotParsedAsData. The handcrafted fixture
// puts plausible-looking rows inside the nested <table> and <pre> so that a
// parser reading the whole page would pick up 999.999 and 111.111. Those
// values must never appear.
func TestTheNestedDocumentsDecoyRowsAreNotParsedAsData(t *testing.T) {
	page := fixture(t, "draw_graph_null_station.html")
	if !strings.Contains(string(page), "999.999") {
		t.Fatalf("the fixture no longer carries decoy values; it can no longer prove the extraction is safe")
	}

	ts := newServer(t, serveBody(page))
	src := newSource(t, ts.URL, config.NMDB{Stations: []string{"OULU", "KIEL2", "SOPO"}})

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	for _, s := range batch.Samples {
		if s.Value == 999.999 || s.Value == 111.111 {
			t.Errorf("a decoy value from the nested document was published as %s%v = %v",
				s.Desc.FullName(), s.Labels, s.Value)
		}
		if s.Desc == metric.NeutronMonitorRate && s.Time.Year() == 1111 {
			t.Errorf("a decoy timestamp from the nested document was published: %s", s.Time)
		}
	}
}

// TestAPageWithNoDataBlockIsAnError, which is what NEST returns when formchk
// or force is missing and it renders the form instead.
func TestAPageWithNoDataBlockIsAnError(t *testing.T) {
	ts := newServer(t, serveBody([]byte("<html><body><form>the form, not the data</form></body></html>")))
	src := newSource(t, ts.URL, config.NMDB{})

	_, err := src.Poll(context.Background())
	if err == nil {
		t.Fatalf("Poll accepted a page with no data block")
	}
	if !strings.Contains(err.Error(), preOpen) {
		t.Errorf("error %q does not say what was missing", err)
	}
}

// TestAnUnclosedDataBlockIsAnError rather than a parse of whatever happened to
// follow it, which is what a truncated response looks like.
func TestAnUnclosedDataBlockIsAnError(t *testing.T) {
	ts := newServer(t, serveBody([]byte("<html><body>"+preOpen+"  OULU\n2026-09-09 15:44:00;99.5\n")))
	src := newSource(t, ts.URL, config.NMDB{})

	if _, err := src.Poll(context.Background()); err == nil {
		t.Fatalf("Poll accepted an unclosed data block")
	}
}

// ---------------------------------------------------------------------------
// One request, not N
// ---------------------------------------------------------------------------

// TestAllStationsGoIntoOneRequest. NEST issues a MySQL query per request
// against a shared academic host, so three stations must cost one query rather
// than three.
func TestAllStationsGoIntoOneRequest(t *testing.T) {
	ts := newServer(t, serveBody(fixture(t, "draw_graph.html")))
	src := newSource(t, ts.URL, config.NMDB{Stations: []string{"OULU", "KIEL2", "SOPO"}})

	if _, err := src.Poll(context.Background()); err != nil {
		t.Fatalf("Poll: %v", err)
	}

	if got := ts.hits.Load(); got != 1 {
		t.Fatalf("three stations produced %d requests, want exactly 1", got)
	}
	reqs := ts.requests()
	got := reqs[0].Query()["stations[]"]
	if len(got) != 3 {
		t.Errorf("the single request carried %d stations %v, want all 3", len(got), got)
	}
	for _, want := range []string{"OULU", "KIEL2", "SOPO"} {
		found := false
		for _, g := range got {
			if g == want {
				found = true
			}
		}
		if !found {
			t.Errorf("station %s is missing from the request", want)
		}
	}
}

// TestTheRequestCarriesTheParametersNESTRequires. Without formchk and force
// the page renders the form rather than the data, and output=ascii is what
// produces the <pre> block at all.
func TestTheRequestCarriesTheParametersNESTRequires(t *testing.T) {
	ts := newServer(t, serveBody(fixture(t, "draw_graph.html")))
	src := newSource(t, ts.URL, config.NMDB{})

	if _, err := src.Poll(context.Background()); err != nil {
		t.Fatalf("Poll: %v", err)
	}

	q := ts.requests()[0].Query()
	for key, want := range map[string]string{
		"formchk":     "1",
		"force":       "1",
		"output":      "ascii",
		"tabchoice":   "revori",
		"dtype":       "corr_for_efficiency",
		"date_choice": "last",
	} {
		if got := q.Get(key); got != want {
			t.Errorf("%s = %q, want %q", key, got, want)
		}
	}
}

// TestUnsetStationsDefaultToASpreadOfCutoffRigidities. Three monitors at the
// same cutoff tell you about the weather; a spread tells you about the
// particle spectrum.
func TestUnsetStationsDefaultToASpreadOfCutoffRigidities(t *testing.T) {
	src := newSource(t, "https://example.invalid", config.NMDB{})
	got := src.Stations()
	if len(got) != len(defaultStations) {
		t.Fatalf("default stations = %v, want %v", got, defaultStations)
	}
	for i := range got {
		if got[i] != defaultStations[i] {
			t.Errorf("default station %d = %q, want %q", i, got[i], defaultStations[i])
		}
	}
}

func TestConfiguredStationsAreUpperCasedAndDeduplicated(t *testing.T) {
	src := newSource(t, "https://example.invalid",
		config.NMDB{Stations: []string{"oulu", " OULU ", "kiel2", ""}})
	if got := src.Stations(); len(got) != 2 || got[0] != "OULU" || got[1] != "KIEL2" {
		t.Errorf("stations = %v, want [OULU KIEL2]", got)
	}
}

// ---------------------------------------------------------------------------
// Parsing
// ---------------------------------------------------------------------------

// TestTheColumnOrderIsReadFromTheHeaderRatherThanAssumed. A request for OULU,
// KIEL2, SOPO comes back with the columns in NMDB's own order — KIEL2, OULU,
// SOPO — so assuming the requested order would attribute each monitor's counts
// to a different station.
func TestTheColumnOrderIsReadFromTheHeaderRatherThanAssumed(t *testing.T) {
	ts := newServer(t, serveBody(fixture(t, "draw_graph.html")))
	src := newSource(t, ts.URL, config.NMDB{Stations: []string{"OULU", "KIEL2", "SOPO"}})

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}

	// The last rows of testdata/draw_graph.html:
	//   2026-09-09 15:43:00;179.159; 99.590;   null
	//   2026-09-09 15:44:00;168.567; 99.970;   null
	// with the header "KIEL2 OULU SOPO". Oulu sits around 99 counts/min and
	// Kiel around 170, so a swapped mapping is immediately visible.
	rates := stationRates(batch)
	if got := rates["OULU"]; got != 99.970 {
		t.Errorf("OULU rate = %v, want 99.970 from the second column", got)
	}
	if got := rates["KIEL2"]; got != 168.567 {
		t.Errorf("KIEL2 rate = %v, want 168.567 from the first column", got)
	}
}

// TestAHeaderWithALeadingTimestampLabelIsHandled. NEST has been observed
// emitting the header both with and without a name for the timestamp column,
// and a leading label would shift every station one column to the left.
func TestAHeaderWithALeadingTimestampLabelIsHandled(t *testing.T) {
	cases := map[string][]string{
		"  start_date_time   OULU     KIEL2   SOPO":   {"OULU", "KIEL2", "SOPO"},
		"                      KIEL2    OULU    SOPO": {"KIEL2", "OULU", "SOPO"},
		"  datetime OULU": {"OULU"},
	}
	for line, want := range cases {
		got := parseHeader(line)
		if len(got) != len(want) {
			t.Errorf("parseHeader(%q) = %v, want %v", line, got, want)
			continue
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("parseHeader(%q)[%d] = %q, want %q", line, i, got[i], want[i])
			}
		}
	}
}

// TestANullValueProducesNoSample. A neutron monitor reading zero counts per
// minute would be a broken detector, so a missing value must not become a zero.
func TestANullValueProducesNoSample(t *testing.T) {
	ts := newServer(t, serveBody(fixture(t, "draw_graph_null_station.html")))
	src := newSource(t, ts.URL, config.NMDB{Stations: []string{"OULU", "KIEL2", "SOPO"}})

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}

	rates := stationRates(batch)
	if _, published := rates["SOPO"]; published {
		t.Errorf("SOPO is null throughout the fixture but was published as %v", rates["SOPO"])
	}
	for station, v := range rates {
		if v == 0 {
			t.Errorf("station %s was published as zero counts per minute", station)
		}
	}

	// The last non-null value per station, not the last row's value:
	//   15:43:00;   null; 99.046;   null
	//   15:44:00;170.500;   null;   null
	if got := rates["KIEL2"]; got != 170.500 {
		t.Errorf("KIEL2 rate = %v, want its newest non-null value 170.500", got)
	}
	if got := rates["OULU"]; got != 99.046 {
		t.Errorf("OULU rate = %v, want its newest non-null value 99.046", got)
	}
}

// TestAStationThatIsNullThroughoutIsCounted, so an operator staring at an
// empty graph can tell "we filtered it" from "the upstream is down".
func TestAStationThatIsNullThroughoutIsCounted(t *testing.T) {
	ts := newServer(t, serveBody(fixture(t, "draw_graph_null_station.html")))
	src := newSource(t, ts.URL, config.NMDB{Stations: []string{"OULU", "KIEL2", "SOPO"}})

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if got := sampleFor(t, batch, metric.StationsFiltered, Name, reasonAllNull).Value; got != 1 {
		t.Errorf("all-null station count = %v, want 1 (SOPO)", got)
	}
}

// TestAConfiguredStationMissingFromTheResponseIsCountedSeparately. A typo in a
// station code and a station that is down are different problems and must not
// share a series.
func TestAConfiguredStationMissingFromTheResponseIsCountedSeparately(t *testing.T) {
	ts := newServer(t, serveBody(fixture(t, "draw_graph_null_station.html")))
	src := newSource(t, ts.URL, config.NMDB{Stations: []string{"OULU", "NOSUCHSTATION"}})

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if got := sampleFor(t, batch, metric.StationsFiltered, Name, reasonAbsent).Value; got != 1 {
		t.Errorf("absent station count = %v, want 1", got)
	}
	if got := sampleFor(t, batch, metric.StationsFiltered, Name, reasonAllNull).Value; got != 0 {
		t.Errorf("all-null count = %v, want 0; a missing station is not a null station", got)
	}
}

// TestTheFilterCountsArePublishedEvenWhenZero. A series that only appears when
// something is wrong cannot be alerted on.
func TestTheFilterCountsArePublishedEvenWhenZero(t *testing.T) {
	ts := newServer(t, serveBody(fixture(t, "draw_graph.html")))
	src := newSource(t, ts.URL, config.NMDB{Stations: []string{"OULU"}})

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	for _, reason := range []string{reasonAllNull, reasonAbsent} {
		if !hasSample(batch, metric.StationsFiltered, Name, reason) {
			t.Errorf("no stations-filtered series for reason %q", reason)
		}
	}
}

// TestSourceDataAgeComesFromTheNewestReadingsOwnTimestamp. At one-minute
// resolution this normally sits around NMDB's four-minute ingest latency; a
// value climbing past an hour means the network has gone quiet while NEST
// keeps serving a well-formed page.
func TestSourceDataAgeComesFromTheNewestReadingsOwnTimestamp(t *testing.T) {
	ts := newServer(t, serveBody(fixture(t, "draw_graph.html")))
	src := newSource(t, ts.URL, config.NMDB{Stations: []string{"OULU", "KIEL2"}})

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	// referenceNow is 15:48:00; the fixture's newest row is 15:44:00.
	if got := sampleFor(t, batch, metric.SourceDataAge, Name).Value; got != 240 {
		t.Errorf("source data age = %v s, want 240 (the fixture's four-minute latency)", got)
	}
}

// TestABlockWithAHeaderButNoRowsIsAFailure, because that is what a query
// matching nothing looks like and it must not be reported as a quiet network.
func TestABlockWithAHeaderButNoRowsIsAFailure(t *testing.T) {
	page := "<html><body>" + preOpen + "#  summary\n   OULU   KIEL2\n" + preClose + "</pre></body></html>"
	ts := newServer(t, serveBody([]byte(page)))
	src := newSource(t, ts.URL, config.NMDB{})

	if _, err := src.Poll(context.Background()); err == nil {
		t.Errorf("Poll accepted a block with no data rows")
	}
}

// TestRowsBeforeAnyHeaderAreRefusedRatherThanGuessed. Guessing which column is
// which station would silently attribute one monitor's counts to another,
// which is worse than failing.
func TestRowsBeforeAnyHeaderAreRefusedRatherThanGuessed(t *testing.T) {
	page := "<html><body>" + preOpen + "2026-09-09 15:44:00;100.0;200.0\n" + preClose + "</pre></body></html>"
	ts := newServer(t, serveBody([]byte(page)))
	src := newSource(t, ts.URL, config.NMDB{})

	_, err := src.Poll(context.Background())
	if err == nil {
		t.Fatalf("Poll accepted data rows with no column header")
	}
	if !strings.Contains(err.Error(), "column") {
		t.Errorf("error %q does not explain that the mapping could not be established", err)
	}
}

// TestAMalformedBodyErrorsRatherThanPanicking.
func TestAMalformedBodyErrorsRatherThanPanicking(t *testing.T) {
	for name, body := range map[string]string{
		"empty":                     "",
		"json":                      `{"error":"no"}`,
		"block with only comments":  "<html>" + preOpen + "#\n#\n#\n" + preClose,
		"block with garbage rows":   "<html>" + preOpen + "  OULU\nnot;a;row\n" + preClose,
		"truncated in mid-tag":      "<html><body><pre><cod",
		"nul bytes inside the page": "<html>\x00\x00" + preOpen + "\x00" + preClose,
	} {
		t.Run(name, func(t *testing.T) {
			ts := newServer(t, serveBody([]byte(body)))
			src := newSource(t, ts.URL, config.NMDB{})
			if _, err := src.Poll(context.Background()); err == nil {
				t.Errorf("Poll accepted a malformed body")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// The acknowledgement
// ---------------------------------------------------------------------------

// TestTheGeneratedAcknowledgementIsExtractedAndLoggedOnce.
//
// The required attribution names the PI institute of each station in the
// result, so it is generated per query and cannot be hardcoded here. It is
// logged so that the sentence an operator is obliged to reproduce lands in
// their logs — once, because a licence notice repeated every ten minutes is
// noise that gets filtered out.
func TestTheGeneratedAcknowledgementIsExtractedAndLoggedOnce(t *testing.T) {
	logs := &capturingLogger{}
	ts := newServer(t, serveBody(fixture(t, "draw_graph.html")))
	src := newSourceWithLog(t, ts.URL, config.NMDB{}, slog.New(logs))

	for range 3 {
		if _, err := src.Poll(context.Background()); err != nil {
			t.Fatalf("Poll: %v", err)
		}
	}

	ack, count := logs.attr("acknowledgement", "acknowledgement")
	if count != 1 {
		t.Errorf("the acknowledgement was logged %d times across three polls, want 1", count)
	}
	for _, want := range []string{
		"Data retrieved via NMDB are the property of the individual data providers",
		"www.nmdb.eu",
		"non commercial use",
		"Kiel",
		"Oulu",
		"South Pole",
		"University of Wisconsin",
	} {
		if !strings.Contains(ack, want) {
			t.Errorf("the logged acknowledgement does not mention %q; it was %q", want, ack)
		}
	}
}

// TestTheAcknowledgementCarriesNoBoxDrawingCharacters, so that what lands in
// the log is the sentence rather than the ascii frame around it.
func TestTheAcknowledgementCarriesNoBoxDrawingCharacters(t *testing.T) {
	block, err := extractBlock(string(fixture(t, "draw_graph.html")))
	if err != nil {
		t.Fatalf("extractBlock: %v", err)
	}
	ack := extractAcknowledgement(block)
	if ack == "" {
		t.Fatalf("no acknowledgement extracted")
	}
	for _, forbidden := range []string{"#|", "|", "___"} {
		if strings.Contains(ack, forbidden) {
			t.Errorf("the acknowledgement contains box drawing %q: %q", forbidden, ack)
		}
	}
}

// ---------------------------------------------------------------------------
// Schedule
// ---------------------------------------------------------------------------

// TestAnUnsetIntervalDefaultsToTenMinutes. Native resolution is one minute,
// but cosmic ray intensity does not change meaningfully minute to minute
// outside a ground-level enhancement, which ten-minute sampling resolves.
func TestAnUnsetIntervalDefaultsToTenMinutes(t *testing.T) {
	src := newSource(t, "https://example.invalid", config.NMDB{})
	if got := src.Interval(); got != defaultInterval {
		t.Errorf("Interval() = %s, want %s", got, defaultInterval)
	}
}

// TestAConfiguredIntervalBelowFiveMinutesIsFloored.
func TestAConfiguredIntervalBelowFiveMinutesIsFloored(t *testing.T) {
	for _, configured := range []time.Duration{time.Second, time.Minute, 4 * time.Minute} {
		src := newSource(t, "https://example.invalid", config.NMDB{Interval: configured})
		if got := src.Interval(); got != minInterval {
			t.Errorf("interval %s produced Interval() = %s, want the %s floor",
				configured, got, minInterval)
		}
		next := src.Schedule().NextAfter(referenceNow)
		if gap := next.Sub(referenceNow); gap < minInterval {
			t.Errorf("interval %s schedules the next poll %s later, inside the floor", configured, gap)
		}
	}
}

func TestAConfiguredIntervalAboveTheFloorIsHonoured(t *testing.T) {
	src := newSource(t, "https://example.invalid", config.NMDB{Interval: time.Hour})
	if got := src.Interval(); got != time.Hour {
		t.Errorf("Interval() = %s, want 1h", got)
	}
}

// ---------------------------------------------------------------------------
// Transport behaviour
// ---------------------------------------------------------------------------

// TestANotFoundIsNotRetried. Each attempt is a fresh MySQL query on a shared
// academic host, so repeating a permanent failure is a database load problem.
func TestANotFoundIsNotRetried(t *testing.T) {
	ts := newServer(t, func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "not found", http.StatusNotFound)
	})
	src := newSource(t, ts.URL, config.NMDB{Retries: 3})

	if _, err := src.Poll(context.Background()); err == nil {
		t.Fatalf("Poll succeeded against a 404")
	}
	if got := ts.hits.Load(); got != 1 {
		t.Errorf("a 404 was requested %d times, want 1", got)
	}
}

func TestATooManyRequestsIsReportedAsRateLimited(t *testing.T) {
	ts := newServer(t, func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "slow down", http.StatusTooManyRequests)
	})
	src := newSource(t, ts.URL, config.NMDB{Retries: 3})

	_, err := src.Poll(context.Background())
	if !errors.Is(err, source.ErrRateLimited) {
		t.Errorf("error = %v, want source.ErrRateLimited", err)
	}
	if got := ts.hits.Load(); got != 1 {
		t.Errorf("a 429 was requested %d times, want 1", got)
	}
}

func TestACancelledContextAbortsThePollWithoutRequesting(t *testing.T) {
	ts := newServer(t, serveBody(fixture(t, "draw_graph.html")))
	src := newSource(t, ts.URL, config.NMDB{})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := src.Poll(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("error = %v, want context.Canceled", err)
	}
	if got := ts.hits.Load(); got != 0 {
		t.Errorf("a cancelled poll still made %d requests", got)
	}
}

func TestAContextCancelledMidFlightAbortsThePoll(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	ts := newServer(t, func(w http.ResponseWriter, r *http.Request) {
		cancel()
		<-r.Context().Done()
	})
	src := newSource(t, ts.URL, config.NMDB{})

	if _, err := src.Poll(ctx); err == nil {
		t.Errorf("Poll succeeded against a cancelled request")
	}
}

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

func TestNewRequiresTheSharedClient(t *testing.T) {
	if _, err := New(config.NMDB{}, nil, testLogger()); err == nil {
		t.Errorf("New accepted a nil client; politeness has to be enforced across sources")
	}
}

func TestNewRejectsABaseURLThatIsNotHTTP(t *testing.T) {
	for _, base := range []string{"ftp://www.nmdb.eu", "://", "mysql://nmdb"} {
		if _, err := New(config.NMDB{BaseURL: base}, testClient(t), testLogger()); err == nil {
			t.Errorf("New accepted base URL %q", base)
		}
	}
}

func TestAnUnsetBaseURLDefaultsToTheLiveService(t *testing.T) {
	src, err := New(config.NMDB{}, testClient(t), testLogger())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if !strings.HasPrefix(src.requestURL(), defaultBaseURL+path) {
		t.Errorf("request URL %q does not start at %s%s", src.requestURL(), defaultBaseURL, path)
	}
}
