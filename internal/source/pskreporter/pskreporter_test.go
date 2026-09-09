package pskreporter

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

// referenceNow is the instant every test pretends it is. The response carries
// no timestamp of its own, so the observation time is derived from this and has
// to be pinned.
var referenceNow = time.Date(2026, 9, 9, 15, 50, 0, 0, time.UTC)

// testContact is a syntactically real address. New refuses an empty one, so
// every test that builds a source has to supply something.
const testContact = "operator@example.org"

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// capturingLogger records every log record so a test can assert that a warning
// was emitted, which for this source is part of the contract rather than
// decoration.
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

// warnings returns the messages of every record at warn or above.
func (c *capturingLogger) warnings() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []string
	for _, r := range c.records {
		if r.Level >= slog.LevelWarn {
			out = append(out, r.Message)
		}
	}
	return out
}

// testClient is the shared httpx client with the politeness policy relaxed.
//
// The real policy spaces pskreporter.info by five minutes, and an httptest
// server on 127.0.0.1 falls to the strict 30-second fallback. Waiting either
// out would make the suite unusable; the limiter has its own tests in httpx.
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

func fixture(t *testing.T) []byte {
	t.Helper()
	body, err := os.ReadFile("testdata/psk-freq-fm05.txt")
	if err != nil {
		t.Fatalf("reading fixture: %v", err)
	}
	return body
}

func serveBody(body []byte) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write(body)
	}
}

func newSourceWithLog(t *testing.T, base string, cfg config.PSKReporter, log *slog.Logger) *Source {
	t.Helper()
	cfg.BaseURL = base
	if cfg.Contact == "" {
		cfg.Contact = testContact
	}
	src, err := New(cfg, testClient(t), log)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	src.now = func() time.Time { return referenceNow }
	return src
}

func newSource(t *testing.T, base string, cfg config.PSKReporter) *Source {
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

func countFor(batch metric.Batch, desc *metric.Descriptor) int {
	n := 0
	for _, s := range batch.Samples {
		if s.Desc == desc {
			n++
		}
	}
	return n
}

// ---------------------------------------------------------------------------
// The contact refusal
// ---------------------------------------------------------------------------

// TestNewRefusesAnEmptyContact is the substance of this package's terms
// handling. The operator of pskreporter.info asks in writing for an appcontact
// address so he can reach a misbehaving client before blocking it, and running
// a poller against a personal server while withholding it is not acceptable —
// so this is a refusal, not a warning.
func TestNewRefusesAnEmptyContact(t *testing.T) {
	for name, contact := range map[string]string{
		"empty":            "",
		"whitespace only":  "   ",
		"tab and newline":  "\t\n",
		"not an address":   "please-dont-block-me",
		"placeholder word": "none",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := New(config.PSKReporter{Contact: contact}, testClient(t), testLogger())
			if err == nil {
				t.Fatalf("New accepted contact %q", contact)
			}
			if !errors.Is(err, ErrContactRequired) {
				t.Errorf("error = %v, want it to wrap ErrContactRequired", err)
			}
			// The message has to tell an operator which setting to fill in,
			// otherwise a hard refusal is just a dead end.
			if !strings.Contains(err.Error(), "contact") {
				t.Errorf("error %q does not name the contact setting", err)
			}
			if !strings.Contains(err.Error(), "appcontact") {
				t.Errorf("error %q does not explain that the upstream asks for appcontact", err)
			}
		})
	}
}

// TestNewAcceptsAContactAddress is the other half: the refusal must not be
// blanket.
func TestNewAcceptsAContactAddress(t *testing.T) {
	src, err := New(config.PSKReporter{Contact: testContact}, testClient(t), testLogger())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if src.contact != testContact {
		t.Errorf("contact = %q, want %q", src.contact, testContact)
	}
}

// TestAContactIsTrimmedBeforeUse guards against a config file with a trailing
// newline turning into a malformed query parameter.
func TestAContactIsTrimmedBeforeUse(t *testing.T) {
	src, err := New(config.PSKReporter{Contact: "  " + testContact + "\n"}, testClient(t), testLogger())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if src.contact != testContact {
		t.Errorf("contact = %q, want it trimmed to %q", src.contact, testContact)
	}
}

// ---------------------------------------------------------------------------
// Request shape
// ---------------------------------------------------------------------------

// TestAppcontactIsSentOnEveryRequest, not just the first. The point of the
// parameter is that the operator can identify the traffic he is looking at,
// and an address that appears on one request in twelve does not do that.
func TestAppcontactIsSentOnEveryRequest(t *testing.T) {
	ts := newServer(t, serveBody(fixture(t)))
	src := newSource(t, ts.URL, config.PSKReporter{Grids: []string{"FM05", "IO91", "JN45"}})

	for range 2 {
		if _, err := src.Poll(context.Background()); err != nil {
			t.Fatalf("Poll: %v", err)
		}
	}

	reqs := ts.requests()
	if len(reqs) != 6 {
		t.Fatalf("two polls of three grids made %d requests, want 6", len(reqs))
	}
	for i, u := range reqs {
		if got := u.Query().Get("appcontact"); got != testContact {
			t.Errorf("request %d appcontact = %q, want %q", i, got, testContact)
		}
	}
}

// TestTheGridFilterIsSentAndLabelled.
func TestTheGridFilterIsSentAndLabelled(t *testing.T) {
	ts := newServer(t, serveBody(fixture(t)))
	src := newSource(t, ts.URL, config.PSKReporter{Grids: []string{"fm05"}})

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}

	reqs := ts.requests()
	if len(reqs) != 1 {
		t.Fatalf("made %d requests, want 1", len(reqs))
	}
	if got := reqs[0].Query().Get("grid"); got != "FM05" {
		t.Errorf("grid parameter = %q, want it upper-cased to FM05", got)
	}
	if got := reqs[0].Path; got != path {
		t.Errorf("request path = %q, want %q", got, path)
	}
	if !sampleHasGrid(batch, "FM05") {
		t.Errorf("no sample carries grid=\"FM05\"")
	}
}

// TestNoConfiguredGridAsksForTheGlobalFigure. Omitting grid is how the
// endpoint is asked for the worldwide numbers, and the label has to say so
// rather than being empty, which metric.Sample.Validate rejects anyway.
func TestNoConfiguredGridAsksForTheGlobalFigure(t *testing.T) {
	ts := newServer(t, serveBody(fixture(t)))
	src := newSource(t, ts.URL, config.PSKReporter{})

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}

	reqs := ts.requests()
	if len(reqs) != 1 {
		t.Fatalf("made %d requests, want 1", len(reqs))
	}
	if _, present := reqs[0].Query()["grid"]; present {
		t.Errorf("a grid parameter was sent when none was configured")
	}
	if !sampleHasGrid(batch, globalGrid) {
		t.Errorf("no sample carries grid=%q", globalGrid)
	}
}

func sampleHasGrid(batch metric.Batch, grid string) bool {
	for _, s := range batch.Samples {
		if s.Desc != metric.BandActivityScore {
			continue
		}
		if v, ok := s.LabelFor("grid"); ok && v == grid {
			return true
		}
	}
	return false
}

// TestDuplicateGridsAreCollapsed, because each grid is a request against a
// five-minute budget and asking twice for the same square wastes half of it.
func TestDuplicateGridsAreCollapsed(t *testing.T) {
	src := newSource(t, "https://example.invalid",
		config.PSKReporter{Grids: []string{"FM05", "fm05", " FM05 ", "IO91"}})
	if got := src.Grids(); len(got) != 2 {
		t.Errorf("grids = %v, want FM05 and IO91 only", got)
	}
}

// ---------------------------------------------------------------------------
// The five-minute floor
// ---------------------------------------------------------------------------

// TestTheFiveMinuteFloorCannotBeLoweredFromConfiguration. The operator's
// stated limit lives in the schedule rather than in validation, because a
// limit an operator can override by editing a setting is not a limit.
func TestTheFiveMinuteFloorCannotBeLoweredFromConfiguration(t *testing.T) {
	for _, configured := range []time.Duration{
		time.Second, 30 * time.Second, time.Minute, 4*time.Minute + 59*time.Second,
	} {
		src := newSource(t, "https://example.invalid", config.PSKReporter{Interval: configured})

		if got := src.Interval(); got != minInterval {
			t.Errorf("interval %s produced Interval() = %s, want the %s floor",
				configured, got, minInterval)
		}
		next := src.Schedule().NextAfter(referenceNow)
		if gap := next.Sub(referenceNow); gap < minInterval {
			t.Errorf("interval %s schedules the next poll %s later, inside the %s floor",
				configured, gap, minInterval)
		}
	}
}

// TestLoweringTheIntervalWarnsSoTheOperatorKnowsItWasIgnored. Silently
// clamping a setting is how an operator concludes the exporter is broken.
func TestLoweringTheIntervalWarnsSoTheOperatorKnowsItWasIgnored(t *testing.T) {
	logs := &capturingLogger{}
	newSourceWithLog(t, "https://example.invalid",
		config.PSKReporter{Interval: time.Minute}, slog.New(logs))

	if !containsSubstring(logs.warnings(), "raised to the operator's stated limit") {
		t.Errorf("no warning about the interval being raised; got %v", logs.warnings())
	}
}

// TestAnIntervalAboveTheFloorIsHonoured makes sure the clamp is a floor rather
// than a fixed value: an operator who wants to be gentler than asked may be.
func TestAnIntervalAboveTheFloorIsHonoured(t *testing.T) {
	src := newSource(t, "https://example.invalid", config.PSKReporter{Interval: time.Hour})
	if got := src.Interval(); got != time.Hour {
		t.Errorf("Interval() = %s, want 1h", got)
	}
}

// TestAnUnsetIntervalDefaultsToTheStatedFiveMinutes.
func TestAnUnsetIntervalDefaultsToTheStatedFiveMinutes(t *testing.T) {
	src := newSource(t, "https://example.invalid", config.PSKReporter{})
	if got := src.Interval(); got != defaultInterval {
		t.Errorf("Interval() = %s, want %s", got, defaultInterval)
	}
}

// TestMoreThanThreeGridsWarns. Each grid is one request against a five-minute
// budget, so a fourth grid's data is stale before it arrives.
func TestMoreThanThreeGridsWarns(t *testing.T) {
	logs := &capturingLogger{}
	newSourceWithLog(t, "https://example.invalid",
		config.PSKReporter{Grids: []string{"FM05", "IO91", "JN45", "PM95"}}, slog.New(logs))

	if !containsSubstring(logs.warnings(), "more grids configured than fits its request budget") {
		t.Errorf("four grids produced no warning; got %v", logs.warnings())
	}
}

// TestThreeGridsDoesNotWarn: the threshold has to be a threshold, not a
// blanket complaint about configuring grids at all.
func TestThreeGridsDoesNotWarn(t *testing.T) {
	logs := &capturingLogger{}
	newSourceWithLog(t, "https://example.invalid",
		config.PSKReporter{Grids: []string{"FM05", "IO91", "JN45"}}, slog.New(logs))

	if containsSubstring(logs.warnings(), "more grids configured") {
		t.Errorf("three grids produced a budget warning; got %v", logs.warnings())
	}
}

func containsSubstring(haystack []string, needle string) bool {
	for _, h := range haystack {
		if strings.Contains(h, needle) {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// Band derivation
// ---------------------------------------------------------------------------

// TestBandDerivationCoversTheAllocationBoundaries walks each allocation's two
// edges and the hertz either side of them. Off-by-one at a band edge is the
// failure this table exists to prevent, and it is invisible in normal traffic
// because digital sub-segments sit in the middle of a band.
func TestBandDerivationCoversTheAllocationBoundaries(t *testing.T) {
	for _, b := range bandRanges {
		if got := bandForHz(b.loHz); got != b.label {
			t.Errorf("%.0f Hz (lower edge of %s) mapped to %q", b.loHz, b.label, got)
		}
		if got := bandForHz(b.hiHz); got != b.label {
			t.Errorf("%.0f Hz (upper edge of %s) mapped to %q", b.hiHz, b.label, got)
		}
		// One hertz outside each edge must not still be the band, unless an
		// adjacent allocation happens to abut it. None of ours do.
		if got := bandForHz(b.loHz - 1); got == b.label {
			t.Errorf("%.0f Hz is one hertz below %s but still mapped to it", b.loHz-1, b.label)
		}
		if got := bandForHz(b.hiHz + 1); got == b.label {
			t.Errorf("%.0f Hz is one hertz above %s but still mapped to it", b.hiHz+1, b.label)
		}
	}
}

// TestBandDerivationPlacesTheFrequenciesTheFeedActuallyReports uses the
// frequencies from the captured fixture plus the common digital watering holes.
func TestBandDerivationPlacesTheFrequenciesTheFeedActuallyReports(t *testing.T) {
	cases := []struct {
		hz   float64
		want string
	}{
		{7_080_000, "40m"},
		{7_070_000, "40m"},
		{10_140_000, "30m"},
		{14_070_000, "20m"},
		{14_080_000, "20m"},
		{14_090_000, "20m"},
		{14_110_000, "20m"},
		{18_100_000, "17m"},
		{28_110_000, "10m"},
		{50_313_000, "6m"},
		{144_174_000, "2m"},
		{474_200, "630m"},
		{136_000, "2200m"},
	}
	for _, tc := range cases {
		if got := bandForHz(tc.hz); got != tc.want {
			t.Errorf("%.0f Hz mapped to %q, want %q", tc.hz, got, tc.want)
		}
	}
}

// TestAFrequencyOutsideEveryAllocationIsNotGivenABand. A report outside every
// amateur band is a broken receiver readout, and inventing a series for it
// would leave permanent junk in the label space.
func TestAFrequencyOutsideEveryAllocationIsNotGivenABand(t *testing.T) {
	for _, hz := range []float64{0, 1_000_000, 9_000_000, 100_000_000, 999_999_999_999} {
		if got := bandForHz(hz); got != "" {
			t.Errorf("%.0f Hz is in no allocation but mapped to %q", hz, got)
		}
	}
}

// TestAFrequencyOutsideEveryAllocationProducesNoSample, end to end.
func TestAFrequencyOutsideEveryAllocationProducesNoSample(t *testing.T) {
	body := "9000000 100 50 1 2\n14070000 200 60 3 4\n# frequency score #spots #tx #rx\n# grid FM%, 5 mins\n"
	ts := newServer(t, serveBody([]byte(body)))
	src := newSource(t, ts.URL, config.PSKReporter{})

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if got := countFor(batch, metric.BandActivityScore); got != 1 {
		t.Errorf("%d activity-score samples, want only the 20 m row", got)
	}
	if got := sampleFor(t, batch, metric.BandActivityScore, network, "20m", globalGrid).Value; got != 200 {
		t.Errorf("20m activity score = %v, want 200", got)
	}
}

// ---------------------------------------------------------------------------
// Parsing
// ---------------------------------------------------------------------------

// TestTheTrailingCommentLinesAreNotParsedAsData. A naive whitespace split of
// "# frequency score #spots #tx #rx" yields five fields, and a parser that
// skips unreadable numbers rather than skipping comment lines would happily
// invent a band from the legend.
func TestTheTrailingCommentLinesAreNotParsedAsData(t *testing.T) {
	ts := newServer(t, serveBody(fixture(t)))
	src := newSource(t, ts.URL, config.PSKReporter{Grids: []string{"FM05"}})

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}

	// testdata/psk-freq-fm05.txt holds nine data rows across 40 m, 30 m, 20 m,
	// 17 m and 10 m — five bands — plus the two legend lines.
	if got := countFor(batch, metric.BandActivityScore); got != 5 {
		t.Errorf("%d activity-score samples, want 5 (one per band in the fixture)", got)
	}
	for _, s := range batch.Samples {
		band, ok := s.LabelFor("band")
		if !ok {
			// metric.SourceDataAge has no band label; it is not a band series.
			continue
		}
		if band == "" || strings.ContainsAny(band, "#") {
			t.Errorf("a comment line produced a sample with band=%q", band)
		}
	}
	for _, s := range batch.Samples {
		if err := s.Validate(); err != nil {
			t.Errorf("Poll emitted an invalid sample: %v", err)
		}
	}
}

// TestTheCapturedFixtureFoldsMultipleDialFrequenciesIntoOneBand. 14070000,
// 14080000, 14090000 and 14110000 are all 20 m; four series named 20 m would
// collapse arbitrarily in the store, so they are summed here where the
// arithmetic is visible.
func TestTheCapturedFixtureFoldsMultipleDialFrequenciesIntoOneBand(t *testing.T) {
	ts := newServer(t, serveBody(fixture(t)))
	src := newSource(t, ts.URL, config.PSKReporter{Grids: []string{"FM05"}})

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}

	// The 20 m rows of the fixture:
	//   14070000 591 100 1 4
	//   14080000 131  71 0 4
	//   14110000  31   4 0 0
	//   14090000  10   1 1 0
	if got := sampleFor(t, batch, metric.BandActivityScore, network, "20m", "FM05").Value; got != 763 {
		t.Errorf("20m activity score = %v, want 591+131+31+10 = 763", got)
	}
	if got := sampleFor(t, batch, metric.SpotCount, network, "20m").Value; got != 176 {
		t.Errorf("20m spot count = %v, want 100+71+4+1 = 176", got)
	}
	if got := sampleFor(t, batch, metric.SpotStationCount, network, "20m", "tx").Value; got != 2 {
		t.Errorf("20m tx stations = %v, want 1+0+0+1 = 2", got)
	}
	if got := sampleFor(t, batch, metric.SpotStationCount, network, "20m", "rx").Value; got != 8 {
		t.Errorf("20m rx stations = %v, want 4+4+0+0 = 8", got)
	}

	// 40 m: 7080000 825 97 0 2 and 7070000 7 5 0 1
	if got := sampleFor(t, batch, metric.BandActivityScore, network, "40m", "FM05").Value; got != 832 {
		t.Errorf("40m activity score = %v, want 832", got)
	}
}

// TestTheObservationTimeIsTheWindowMidpointRatherThanReceipt. The figures are
// a five-minute aggregate; stamping them at receipt would date the average as
// an instant reading taken after the window closed.
func TestTheObservationTimeIsTheWindowMidpointRatherThanReceipt(t *testing.T) {
	ts := newServer(t, serveBody(fixture(t)))
	src := newSource(t, ts.URL, config.PSKReporter{Grids: []string{"FM05"}})

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	want := referenceNow.Add(-150 * time.Second)
	got := sampleFor(t, batch, metric.SpotCount, network, "20m").Time
	if !got.Equal(want) {
		t.Errorf("sample time = %s, want the midpoint of the five-minute window %s", got, want)
	}
}

// TestTheWindowIsReadFromTheLegendWhenItSaysSomethingElse. The endpoint has
// always answered five minutes, but the value is in the response and reading it
// is cheaper than assuming it.
func TestTheWindowIsReadFromTheLegendWhenItSaysSomethingElse(t *testing.T) {
	body := "14070000 200 60 3 4\n# frequency score #spots #tx #rx\n# grid FM%, 20 mins\n"
	ts := newServer(t, serveBody([]byte(body)))
	src := newSource(t, ts.URL, config.PSKReporter{})

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	want := referenceNow.Add(-10 * time.Minute)
	if got := sampleFor(t, batch, metric.SpotCount, network, "20m").Time; !got.Equal(want) {
		t.Errorf("sample time = %s, want the midpoint of a twenty-minute window %s", got, want)
	}
}

// TestSourceDataAgeIsPublishedFromReceiptTimeAndOnlyThat. Documented honestly
// rather than fudged: the response carries no timestamp, so this series
// measures our polling, not the publisher's freshness.
func TestSourceDataAgeIsPublishedFromReceiptTimeAndOnlyThat(t *testing.T) {
	ts := newServer(t, serveBody(fixture(t)))
	src := newSource(t, ts.URL, config.PSKReporter{Grids: []string{"FM05"}})

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	// Half of the five-minute window, since that is the observation time.
	if got := sampleFor(t, batch, metric.SourceDataAge, Name).Value; got != 150 {
		t.Errorf("source data age = %v s, want 150 (half the five-minute window)", got)
	}
}

// TestAQuietGridWithOnlyTheLegendIsASuccessWithNoSamples. A grid square with
// nobody on the air is an ordinary condition, not a failure.
func TestAQuietGridWithOnlyTheLegendIsASuccessWithNoSamples(t *testing.T) {
	body := "# frequency score #spots #tx #rx\n# grid AA00, 5 mins\n"
	ts := newServer(t, serveBody([]byte(body)))
	src := newSource(t, ts.URL, config.PSKReporter{Grids: []string{"AA00"}})

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if countFor(batch, metric.BandActivityScore) != 0 {
		t.Errorf("a quiet grid produced activity samples")
	}
}

// TestABodyWithNeitherDataNorLegendIsAFailure, because that is what an error
// page or a redirect body looks like and it must not be mistaken for a quiet
// band.
func TestABodyWithNeitherDataNorLegendIsAFailure(t *testing.T) {
	for name, body := range map[string]string{
		"empty":       "",
		"html":        "<html><body>Service unavailable</body></html>",
		"prose":       "The database is being rebuilt, please try later.",
		"binary junk": "\x00\x01\x02\x03",
	} {
		t.Run(name, func(t *testing.T) {
			ts := newServer(t, serveBody([]byte(body)))
			src := newSource(t, ts.URL, config.PSKReporter{})
			if _, err := src.Poll(context.Background()); err == nil {
				t.Errorf("Poll accepted a body that is not the CGI's output")
			}
		})
	}
}

// TestAMalformedDataLineIsSkippedRatherThanFailingThePoll.
func TestAMalformedDataLineIsSkippedRatherThanFailingThePoll(t *testing.T) {
	body := "14070000 200 60 3 4\n" +
		"garbage garbage garbage garbage garbage\n" +
		"18100000\n" +
		"7080000 100 20 1 2\n" +
		"# frequency score #spots #tx #rx\n# grid FM%, 5 mins\n"
	ts := newServer(t, serveBody([]byte(body)))
	src := newSource(t, ts.URL, config.PSKReporter{})

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if got := countFor(batch, metric.BandActivityScore); got != 2 {
		t.Errorf("%d activity-score samples, want the two readable rows", got)
	}
}

// ---------------------------------------------------------------------------
// Transport behaviour
// ---------------------------------------------------------------------------

// TestANotFoundIsNotRetried. Repeating a 404 against a personal server whose
// operator reserves the right to block clients that load it is abuse rather
// than resilience.
func TestANotFoundIsNotRetried(t *testing.T) {
	ts := newServer(t, func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "not found", http.StatusNotFound)
	})
	src := newSource(t, ts.URL, config.PSKReporter{Retries: 3})

	if _, err := src.Poll(context.Background()); err == nil {
		t.Fatalf("Poll succeeded against a 404")
	}
	if got := ts.hits.Load(); got != 1 {
		t.Errorf("a 404 was requested %d times, want 1", got)
	}
}

// TestATooManyRequestsIsReportedAsRateLimited. This is the response the
// operator's stated policy predicts, so it has to back the source off rather
// than being treated as an ordinary failure.
func TestATooManyRequestsIsReportedAsRateLimited(t *testing.T) {
	ts := newServer(t, func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "slow down", http.StatusTooManyRequests)
	})
	src := newSource(t, ts.URL, config.PSKReporter{Retries: 3})

	_, err := src.Poll(context.Background())
	if !errors.Is(err, source.ErrRateLimited) {
		t.Errorf("error = %v, want source.ErrRateLimited", err)
	}
	if got := ts.hits.Load(); got != 1 {
		t.Errorf("a 429 was requested %d times, want 1", got)
	}
}

// TestOneFailingGridDoesNotLoseTheOthers.
func TestOneFailingGridDoesNotLoseTheOthers(t *testing.T) {
	ts := newServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("grid") == "IO91" {
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write(fixture(t))
	})
	src := newSource(t, ts.URL, config.PSKReporter{Grids: []string{"FM05", "IO91"}, Retries: 0})

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if !sampleHasGrid(batch, "FM05") {
		t.Errorf("the healthy grid's samples were lost with the failing one")
	}
	if sampleHasGrid(batch, "IO91") {
		t.Errorf("the failing grid produced samples")
	}
}

// TestACancelledContextAbortsThePoll.
func TestACancelledContextAbortsThePoll(t *testing.T) {
	ts := newServer(t, serveBody(fixture(t)))
	src := newSource(t, ts.URL, config.PSKReporter{Grids: []string{"FM05", "IO91"}})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := src.Poll(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("error = %v, want context.Canceled", err)
	}
	if got := ts.hits.Load(); got != 0 {
		t.Errorf("a cancelled poll still made %d requests", got)
	}
}

// TestAContextCancelledMidFlightAbortsThePoll.
func TestAContextCancelledMidFlightAbortsThePoll(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	ts := newServer(t, func(w http.ResponseWriter, r *http.Request) {
		cancel()
		<-r.Context().Done()
	})
	src := newSource(t, ts.URL, config.PSKReporter{})

	if _, err := src.Poll(ctx); err == nil {
		t.Errorf("Poll succeeded against a cancelled request")
	}
}

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

func TestNewRequiresTheSharedClient(t *testing.T) {
	if _, err := New(config.PSKReporter{Contact: testContact}, nil, testLogger()); err == nil {
		t.Errorf("New accepted a nil client; politeness has to be enforced across sources")
	}
}

func TestNewRejectsABaseURLThatIsNotHTTP(t *testing.T) {
	for _, base := range []string{"ftp://pskreporter.info", "gopher://x", "://"} {
		_, err := New(config.PSKReporter{BaseURL: base, Contact: testContact}, testClient(t), testLogger())
		if err == nil {
			t.Errorf("New accepted base URL %q", base)
		}
	}
}

func TestAnUnsetBaseURLDefaultsToTheLiveService(t *testing.T) {
	src, err := New(config.PSKReporter{Contact: testContact}, testClient(t), testLogger())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if !strings.HasPrefix(src.requestURL(""), defaultBaseURL+path) {
		t.Errorf("request URL %q does not start at %s%s", src.requestURL(""), defaultBaseURL, path)
	}
}
