package intermagnet

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math"
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

// The recorded Boulder fixture is a 70-minute slice of the live 1,440-entry
// day, ending with the trailing nulls the service really serves: samples run
// from 14:40 to 15:49 UT on 2026-09-09, X, Y and Z stop at 15:45 and S stops
// two minutes earlier at 15:43. Truncating the day keeps the file readable
// while still spanning more than an hour, so the range computation is
// exercised against real numbers rather than a synthetic ramp.
var (
	// bouNewest is the timestamp of Boulder's newest X, Y and Z values.
	bouNewest = time.Date(2026, 9, 9, 15, 45, 0, 0, time.UTC)

	// bouNewestS is the timestamp of its newest S value, deliberately older.
	bouNewestS = time.Date(2026, 9, 9, 15, 43, 0, 0, time.UTC)

	// pollTime is the injected wall clock: a quarter of an hour after the
	// fixture's newest sample, so a correct StationDataAge is 900 seconds and
	// anything derived from the real clock is off by years. It is late enough
	// in the UT day that the hourly window does not cross midnight.
	pollTime = time.Date(2026, 9, 9, 16, 0, 0, 0, time.UTC)

	// handCheckedPollTime suits the hand-checked range fixture, whose samples
	// end at 14:30 UT.
	handCheckedPollTime = time.Date(2026, 9, 9, 15, 0, 0, 0, time.UTC)
)

// Boulder's expected newest values, read off the recorded fixture.
const (
	bouX = 20453.9
	bouY = 2816.47
	bouZ = 46628.38
	bouS = 50996.22

	// bouRange is the peak-to-peak horizontal range over the hour ending
	// 15:45: 61 minutes of samples, min hypot 20646.771862877256, max
	// 20668.303568800704.
	bouRange = 21.53170592344759
)

// quietLogger discards output so a test exercising the warning paths does not
// bury the failure it is looking for.
func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// recordingLogger captures log output for the tests that assert an operator is
// actually told which observatory was dropped and why.
type recordingLogger struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (r *recordingLogger) Write(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.buf.Write(p)
}

func (r *recordingLogger) String() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.buf.String()
}

func (r *recordingLogger) logger() *slog.Logger {
	return slog.New(slog.NewTextHandler(r, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// testClient returns an httpx client with the loopback host's politeness
// spacing removed. The spacing is the right default against a real publisher
// and pure latency against an httptest server two goroutines away — and with
// it in place the bounded-concurrency test would measure the limiter instead of
// the bound.
func testClient(t *testing.T) *httpx.Client {
	t.Helper()
	return httpx.New(httpx.Options{
		Timeout:  5 * time.Second,
		Policies: map[string]httpx.HostPolicy{"127.0.0.1": {MinInterval: time.Microsecond, Burst: 256}},
		Fallback: httpx.HostPolicy{MinInterval: time.Microsecond, Burst: 256},
	}, quietLogger())
}

// fixtureServer serves one recorded document per IAGA code and counts requests
// per code, so a test can assert that an excluded observatory stops being
// fetched and that a 404 is not retried.
func fixtureServer(t *testing.T, byCode map[string]string) (*httptest.Server, func(string) int) {
	t.Helper()

	var (
		mu     sync.Mutex
		counts = map[string]int{}
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		code := r.URL.Query().Get("observatoryIagaCode")

		mu.Lock()
		counts[code]++
		mu.Unlock()

		name, ok := byCode[code]
		if !ok {
			http.NotFound(w, r)
			return
		}
		body, err := os.ReadFile(filepath.Join("testdata", name))
		if err != nil {
			t.Errorf("reading fixture %s: %v", name, err)
			http.Error(w, "fixture unreadable", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)

	count := func(code string) int {
		mu.Lock()
		defer mu.Unlock()
		return counts[code]
	}
	return srv, count
}

// newSource builds a source aimed at a test server with an injected clock.
func newSource(t *testing.T, baseURL string, codes []string, now time.Time, log *slog.Logger) *Source {
	t.Helper()

	src, err := New(config.INTERMAGNET{
		Enabled:       true,
		BaseURL:       baseURL,
		Observatories: codes,
		Interval:      5 * time.Minute,
		Timeout:       5 * time.Second,
	}, testClient(t), log)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	src.now = func() time.Time { return now }
	return src
}

// find returns the value of the first sample matching a descriptor and labels.
func find(t *testing.T, batch metric.Batch, desc *metric.Descriptor, labels ...string) (float64, time.Time, bool) {
	t.Helper()

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
			return s.Value, s.Time, true
		}
	}
	return 0, time.Time{}, false
}

// mustFind fails the test when a series a test is about is missing entirely.
func mustFind(t *testing.T, batch metric.Batch, desc *metric.Descriptor, labels ...string) (float64, time.Time) {
	t.Helper()

	value, at, ok := find(t, batch, desc, labels...)
	if !ok {
		t.Fatalf("no %s sample for labels %v", desc.Name, labels)
	}
	return value, at
}

func closeTo(got, want, tolerance float64) bool {
	return math.Abs(got-want) <= tolerance
}

func TestNewRejectsAMissingHTTPClient(t *testing.T) {
	if _, err := New(config.INTERMAGNET{Enabled: true}, nil, quietLogger()); err == nil {
		t.Error("New accepted a nil HTTP client; the shared client is not optional")
	}
}

func TestNewDefaultsToTheShortVerifiedFreshObservatoryList(t *testing.T) {
	src, err := New(config.INTERMAGNET{Enabled: true}, testClient(t), quietLogger())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	want := []string{"BOU", "FRD", "CLF", "HRN", "BEL"}
	if len(src.observatories) != len(want) {
		t.Fatalf("default observatories = %v, want %v", src.observatories, want)
	}
	for i := range want {
		if src.observatories[i] != want[i] {
			t.Errorf("default observatory %d = %q, want %q", i, src.observatories[i], want[i])
		}
	}
	if src.interval != defaultInterval {
		t.Errorf("default interval = %s, want %s", src.interval, defaultInterval)
	}
}

func TestNewNormalisesAndDeduplicatesConfiguredCodes(t *testing.T) {
	src, err := New(config.INTERMAGNET{
		Enabled:       true,
		Observatories: []string{" bou ", "BOU", "", "frd"},
	}, testClient(t), quietLogger())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if got := strings.Join(src.observatories, ","); got != "BOU,FRD" {
		t.Errorf("observatories = %q, want %q", got, "BOU,FRD")
	}
}

func TestPollParsesTheRecordedDocumentIntoTheExpectedSeries(t *testing.T) {
	srv, _ := fixtureServer(t, map[string]string{"BOU": "bou_minute.json"})
	src := newSource(t, srv.URL, []string{"BOU"}, pollTime, quietLogger())

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}

	components := map[string]struct {
		value float64
		at    time.Time
	}{
		"X": {bouX, bouNewest},
		"Y": {bouY, bouNewest},
		"Z": {bouZ, bouNewest},
		"S": {bouS, bouNewestS},
	}
	for component, want := range components {
		value, at := mustFind(t, batch, metric.GeomagneticFieldComponent, "BOU", component)
		if !closeTo(value, want.value, 1e-9) {
			t.Errorf("%s component = %v, want %v", component, value, want.value)
		}
		if !at.Equal(want.at) {
			t.Errorf("%s component stamped %s, want the observation's own time %s",
				component, at.Format(time.RFC3339), want.at.Format(time.RFC3339))
		}
	}

	value, at := mustFind(t, batch, metric.GeomagneticFieldRange, "BOU")
	if !closeTo(value, bouRange, 1e-6) {
		t.Errorf("hourly range = %v, want %v", value, bouRange)
	}
	if !at.Equal(bouNewest) {
		t.Errorf("range stamped %s, want %s", at.Format(time.RFC3339), bouNewest.Format(time.RFC3339))
	}

	if age, _ := mustFind(t, batch, metric.SourceDataAge, Name); !closeTo(age, 900, 1e-9) {
		t.Errorf("SourceDataAge = %v, want 900 from the fixture's own newest timestamp", age)
	}
	if batch.Source != Name {
		t.Errorf("batch source = %q, want %q", batch.Source, Name)
	}
}

// This is the test the package exists for: an embargoed observatory answers
// HTTP 200 with a full day of timestamps and every value null, so nothing about
// the transport says anything is wrong. It must be recognised, named in a
// warning, dropped from the polling set, and never published as an empty or
// zero series.
func TestAnAllNullEmbargoedObservatoryIsDetectedLoggedAndExcludedRatherThanPublished(t *testing.T) {
	srv, count := fixtureServer(t, map[string]string{
		"BOU": "bou_minute.json",
		"ESK": "esk_embargoed.json",
	})
	rec := &recordingLogger{}
	src := newSource(t, srv.URL, []string{"BOU", "ESK"}, pollTime, rec.logger())

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v; one embargoed observatory must not fail the poll", err)
	}

	// Nothing at all is published for the embargoed station: no component, no
	// range, and above all no age series implying it is reporting.
	for _, component := range publishedComponents {
		if _, _, ok := find(t, batch, metric.GeomagneticFieldComponent, "ESK", component); ok {
			t.Errorf("published a %s series for an embargoed observatory", component)
		}
	}
	if _, _, ok := find(t, batch, metric.GeomagneticFieldRange, "ESK"); ok {
		t.Error("published an hourly range for an embargoed observatory")
	}
	if _, _, ok := find(t, batch, metric.StationDataAge, Name, "ESK"); ok {
		t.Error("published a data age for an embargoed observatory")
	}

	// The healthy observatory in the same poll is unaffected.
	if value, _ := mustFind(t, batch, metric.GeomagneticFieldComponent, "BOU", "X"); !closeTo(value, bouX, 1e-9) {
		t.Errorf("BOU X = %v, want %v", value, bouX)
	}

	if n, _ := mustFind(t, batch, metric.StationsFiltered, Name, reasonEmbargoed); n != 1 {
		t.Errorf("StationsFiltered[embargoed] = %v, want 1", n)
	}
	if n, _ := mustFind(t, batch, metric.StationsFiltered, Name, reasonNoData); n != 0 {
		t.Errorf("StationsFiltered[no_data] = %v, want 0; the reason must be the embargo", n)
	}

	logged := rec.String()
	for _, want := range []string{"ESK", reasonEmbargoed, "excluded"} {
		if !strings.Contains(logged, want) {
			t.Errorf("the exclusion warning does not mention %q; logged:\n%s", want, logged)
		}
	}

	// And it stops being fetched: the point of the exclusion is not to pull 87
	// KB every five minutes forever to publish nothing.
	before := count("ESK")
	if _, err := src.Poll(context.Background()); err != nil {
		t.Fatalf("second Poll: %v", err)
	}
	if after := count("ESK"); after != before {
		t.Errorf("the excluded observatory was fetched again: %d requests, then %d", before, after)
	}
	if got := count("BOU"); got != 2 {
		t.Errorf("BOU fetch count = %d, want 2; a healthy station must keep being polled", got)
	}
}

func TestTheEmbargoFlagIsHonouredEvenWhenValuesArePresent(t *testing.T) {
	// A response that carries readings but sets embargo_applied is still an
	// observatory saying it does not release current data. The flag wins: the
	// values could be a stale or partial release, and republishing data an
	// institute has asked us not to is a licence problem, not a data problem.
	doc := `{"datetime":["2026-09-09T15:44:00.000Z","2026-09-09T15:45:00.000Z"],
	  "@info":{"institute":"British Geological Survey","station_name":"Hartland",
	           "iaga_code":"HAD","sample_period":60,"embargo_applied":true},
	  "S":[50000.0,50001.0],"X":[20000.0,20001.0],"Y":[2000.0,2001.0],"Z":[46000.0,46001.0]}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, doc)
	}))
	t.Cleanup(srv.Close)

	src := newSource(t, srv.URL, []string{"HAD"}, pollTime, quietLogger())
	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if _, _, ok := find(t, batch, metric.GeomagneticFieldComponent, "HAD", "X"); ok {
		t.Error("published a reading from a response that set embargo_applied")
	}
	if n, _ := mustFind(t, batch, metric.StationsFiltered, Name, reasonEmbargoed); n != 1 {
		t.Errorf("StationsFiltered[embargoed] = %v, want 1", n)
	}
}

func TestAnObservatoryWithNoDataTodayIsFilteredAsNoDataRatherThanEmbargoed(t *testing.T) {
	// The two states are reported separately because they mean different things
	// to an operator: an embargo is a policy, and an empty day is a station
	// that has not reported yet.
	srv, _ := fixtureServer(t, map[string]string{"NAQ": "naq_no_data.json"})
	rec := &recordingLogger{}
	src := newSource(t, srv.URL, []string{"NAQ"}, pollTime, rec.logger())

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if n, _ := mustFind(t, batch, metric.StationsFiltered, Name, reasonNoData); n != 1 {
		t.Errorf("StationsFiltered[no_data] = %v, want 1", n)
	}
	if n, _ := mustFind(t, batch, metric.StationsFiltered, Name, reasonEmbargoed); n != 0 {
		t.Errorf("StationsFiltered[embargoed] = %v, want 0", n)
	}
	if !strings.Contains(rec.String(), "NAQ") {
		t.Errorf("the exclusion warning does not name the observatory; logged:\n%s", rec.String())
	}
}

func TestAnExcludedObservatoryIsProbedAgainOnceTheReprobeIntervalHasPassed(t *testing.T) {
	// "No data today" is a statement about today. A station silent this morning
	// may be reporting this afternoon, so an exclusion has to expire.
	srv, count := fixtureServer(t, map[string]string{"NAQ": "naq_no_data.json"})
	now := pollTime
	src := newSource(t, srv.URL, []string{"NAQ"}, now, quietLogger())
	src.now = func() time.Time { return now }

	if _, err := src.Poll(context.Background()); err != nil {
		t.Fatalf("first Poll: %v", err)
	}
	if got := count("NAQ"); got != 1 {
		t.Fatalf("first poll made %d requests, want 1", got)
	}

	now = now.Add(reprobeInterval - time.Minute)
	if _, err := src.Poll(context.Background()); err != nil {
		t.Fatalf("second Poll: %v", err)
	}
	if got := count("NAQ"); got != 1 {
		t.Errorf("an exclusion younger than %s was re-probed: %d requests", reprobeInterval, got)
	}

	now = now.Add(2 * time.Minute)
	if _, err := src.Poll(context.Background()); err != nil {
		t.Fatalf("third Poll: %v", err)
	}
	if got := count("NAQ"); got != 2 {
		t.Errorf("an exclusion older than %s was not re-probed: %d requests", reprobeInterval, got)
	}
}

func TestNullValuesAreSkippedAndTheNewestNonNullIsTaken(t *testing.T) {
	// The recorded fixture ends with four minutes of trailing nulls, and S runs
	// two minutes shorter than the vector channels. Both are the normal state
	// of a live observatory, and neither may become a zero or a gap.
	srv, _ := fixtureServer(t, map[string]string{"BOU": "bou_minute.json"})
	src := newSource(t, srv.URL, []string{"BOU"}, pollTime, quietLogger())

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}

	_, at := mustFind(t, batch, metric.GeomagneticFieldComponent, "BOU", "X")
	if !at.Equal(bouNewest) {
		t.Errorf("X came from %s, want the newest non-null sample at %s",
			at.Format(time.RFC3339), bouNewest.Format(time.RFC3339))
	}
	value, atS := mustFind(t, batch, metric.GeomagneticFieldComponent, "BOU", "S")
	if !atS.Equal(bouNewestS) {
		t.Errorf("S came from %s, want %s: each component is searched separately",
			atS.Format(time.RFC3339), bouNewestS.Format(time.RFC3339))
	}
	if value == 0 {
		t.Error("S is zero, which is what decoding a null into a float64 would produce")
	}
}

func TestTheHourlyRangeIsThePeakToPeakHorizontalVariationOverTheLastHour(t *testing.T) {
	// The hand-checked fixture has horizontal magnitudes of exactly 500 at
	// 13:00, 50 at 13:45, 100 at 14:15 and 75 at 14:30, plus a null pair. The
	// newest instant with both X and Y is 14:30, so the window is 13:30 to
	// 14:30: the 500 falls outside it and the answer is 100 - 50 = 50. Were the
	// window ignored the answer would be 450, and were the anchor the request
	// time it would be empty.
	srv, _ := fixtureServer(t, map[string]string{"HND": "range_handchecked.json"})
	src := newSource(t, srv.URL, []string{"HND"}, handCheckedPollTime, quietLogger())

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}

	value, at := mustFind(t, batch, metric.GeomagneticFieldRange, "HND")
	if !closeTo(value, 50, 1e-9) {
		t.Errorf("hourly range = %v, want 50", value)
	}
	want := time.Date(2026, 9, 9, 14, 30, 0, 0, time.UTC)
	if !at.Equal(want) {
		t.Errorf("range stamped %s, want the newest contributing instant %s",
			at.Format(time.RFC3339), want.Format(time.RFC3339))
	}
}

func TestASingleUsableSampleProducesNoRangeRatherThanAZero(t *testing.T) {
	// Zero nT of variation reads as a perfectly quiet hour, which is the
	// opposite of "we have one sample".
	doc := `{"datetime":["2026-09-09T15:44:00.000Z","2026-09-09T15:45:00.000Z"],
	  "@info":{"institute":"Test","iaga_code":"ONE","sample_period":60,"embargo_applied":false},
	  "S":[null,50000.0],"X":[null,20000.0],"Y":[null,2000.0],"Z":[null,46000.0]}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, doc)
	}))
	t.Cleanup(srv.Close)

	src := newSource(t, srv.URL, []string{"ONE"}, pollTime, quietLogger())
	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if _, _, ok := find(t, batch, metric.GeomagneticFieldRange, "ONE"); ok {
		t.Error("published a range computed from one sample")
	}
	if _, _, ok := find(t, batch, metric.GeomagneticFieldComponent, "ONE", "X"); !ok {
		t.Error("the single usable component was dropped along with the range")
	}
}

func TestTheIAGAMissingValueSentinelIsSkippedRatherThanPublished(t *testing.T) {
	// 99999.00 is IAGA-2002's missing-value marker. No real component comes
	// near it — the total intensity peaks around 68,000 nT — so publishing it
	// would put a wildly wrong number on a dashboard. The hand-checked fixture
	// carries 99999.0 as Z's newest value and 46000.0 before it.
	srv, _ := fixtureServer(t, map[string]string{"HND": "range_handchecked.json"})
	src := newSource(t, srv.URL, []string{"HND"}, handCheckedPollTime, quietLogger())

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	value, at := mustFind(t, batch, metric.GeomagneticFieldComponent, "HND", "Z")
	if !closeTo(value, 46000, 1e-9) {
		t.Errorf("Z = %v, want 46000: the 99999.0 sentinel must be skipped", value)
	}
	want := time.Date(2026, 9, 9, 14, 15, 0, 0, time.UTC)
	if !at.Equal(want) {
		t.Errorf("Z stamped %s, want %s", at.Format(time.RFC3339), want.Format(time.RFC3339))
	}
}

func TestStationDataAgeIsStampedAtPollTimeFromTheObservationsOwnTimestamp(t *testing.T) {
	// "How old is this" is a fact about now, so the sample carries the poll
	// time while its value comes from the observation's timestamp. Stamping it
	// at the observation instead would freeze the age at zero.
	srv, _ := fixtureServer(t, map[string]string{"BOU": "bou_minute.json"})
	src := newSource(t, srv.URL, []string{"BOU"}, pollTime, quietLogger())

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}

	value, at := mustFind(t, batch, metric.StationDataAge, Name, "BOU")
	if !closeTo(value, 900, 1e-9) {
		t.Errorf("StationDataAge = %v, want 900 seconds (16:00 UT minus 15:45 UT)", value)
	}
	if !at.Equal(pollTime) {
		t.Errorf("StationDataAge stamped %s, want the poll time %s",
			at.Format(time.RFC3339), pollTime.Format(time.RFC3339))
	}
}

func TestOneObservatoryFailingDoesNotLoseTheOthers(t *testing.T) {
	srv, _ := fixtureServer(t, map[string]string{"BOU": "bou_minute.json"})
	src := newSource(t, srv.URL, []string{"BOU", "NOPE"}, pollTime, quietLogger())

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v; one dead observatory must not fail the poll", err)
	}
	if value, _ := mustFind(t, batch, metric.GeomagneticFieldComponent, "BOU", "X"); !closeTo(value, bouX, 1e-9) {
		t.Errorf("BOU X = %v, want %v", value, bouX)
	}
	if n, _ := mustFind(t, batch, metric.StationsFiltered, Name, reasonFetchFailed); n != 1 {
		t.Errorf("StationsFiltered[fetch_failed] = %v, want 1", n)
	}
}

func TestEveryObservatoryFailingReturnsAnError(t *testing.T) {
	srv, _ := fixtureServer(t, map[string]string{})
	src := newSource(t, srv.URL, []string{"BOU", "FRD"}, pollTime, quietLogger())

	batch, err := src.Poll(context.Background())
	if err == nil {
		t.Fatal("Poll succeeded with every observatory failing")
	}
	if len(batch.Samples) != 0 {
		t.Errorf("a wholly failed poll returned %d samples, want none", len(batch.Samples))
	}
}

func TestAFetchFailureLeavesTheObservatoryInThePollingSet(t *testing.T) {
	// A failure is transient — a magnetometer down for maintenance, a bad
	// afternoon at the GIN — and must not permanently exclude a station the way
	// an embargo does.
	srv, count := fixtureServer(t, map[string]string{})
	src := newSource(t, srv.URL, []string{"BOU"}, pollTime, quietLogger())

	for range 2 {
		if _, err := src.Poll(context.Background()); err == nil {
			t.Fatal("Poll succeeded against a server with no documents")
		}
	}
	if got := count("BOU"); got != 2 {
		t.Errorf("BOU fetch count = %d, want 2: a fetch failure is not an exclusion", got)
	}
}

func TestConcurrentFetchesAreBoundedToThreeObservatories(t *testing.T) {
	var (
		mu       sync.Mutex
		inFlight int
		peak     int
	)
	body, err := os.ReadFile(filepath.Join("testdata", "bou_minute.json"))
	if err != nil {
		t.Fatalf("reading fixture: %v", err)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		inFlight++
		if inFlight > peak {
			peak = inFlight
		}
		mu.Unlock()

		// Hold the request open long enough that any unbounded fan-out would
		// overlap visibly.
		time.Sleep(25 * time.Millisecond)

		mu.Lock()
		inFlight--
		mu.Unlock()

		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)

	codes := []string{"AAA", "BBB", "CCC", "DDD", "EEE", "FFF", "GGG", "HHH"}
	src := newSource(t, srv.URL, codes, pollTime, quietLogger())
	if _, err := src.Poll(context.Background()); err != nil {
		t.Fatalf("Poll: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if peak > maxConcurrentFetches {
		t.Errorf("peak concurrent requests = %d, want at most %d", peak, maxConcurrentFetches)
	}
	if peak < 2 {
		t.Errorf("peak concurrent requests = %d; the fetches never overlapped, so the "+
			"bound was not exercised", peak)
	}
}

func TestTheRequestUsesTheParameterNamesTheServiceActuallyRequires(t *testing.T) {
	// The widely copied testObsList/Start_Date/Duration form returns HTTP 400
	// "Missing observatory code", and samplesPerDay=1 is rejected outright.
	// Getting these names wrong is the single easiest way to break this source,
	// so they are pinned.
	var got map[string][]string
	body, err := os.ReadFile(filepath.Join("testdata", "bou_minute.json"))
	if err != nil {
		t.Fatalf("reading fixture: %v", err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != pathServices {
			t.Errorf("request path = %q, want %q", r.URL.Path, pathServices)
		}
		got = r.URL.Query()
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)

	src := newSource(t, srv.URL, []string{"BOU"}, pollTime, quietLogger())
	if _, err := src.Poll(context.Background()); err != nil {
		t.Fatalf("Poll: %v", err)
	}

	want := map[string]string{
		"Request":             "GetData",
		"format":              "json",
		"observatoryIagaCode": "BOU",
		"samplesPerDay":       "minute",
		"publicationState":    "Best available",
		"dataStartDate":       "2026-09-09",
		"dataDuration":        "1",
	}
	for key, value := range want {
		if len(got[key]) != 1 || got[key][0] != value {
			t.Errorf("query %s = %v, want %q", key, got[key], value)
		}
	}
}

func TestAMissingDocumentIsNotRetried(t *testing.T) {
	// Repeating a 404 against a publicly funded observatory service adds load
	// and learns nothing.
	var requests int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		http.NotFound(w, r)
	}))
	t.Cleanup(srv.Close)

	src, err := New(config.INTERMAGNET{
		Enabled:       true,
		BaseURL:       srv.URL,
		Observatories: []string{"BOU"},
		Timeout:       5 * time.Second,
		Retries:       3,
	}, testClient(t), quietLogger())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	src.now = func() time.Time { return pollTime }

	if _, err := src.Poll(context.Background()); err == nil {
		t.Fatal("Poll succeeded against a 404")
	}
	if requests != 1 {
		t.Errorf("a 404 was requested %d times, want 1", requests)
	}
}

func TestARefusalForBeingTooFrequentReportsRateLimited(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "120")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	t.Cleanup(srv.Close)

	src := newSource(t, srv.URL, []string{"BOU"}, pollTime, quietLogger())
	batch, err := src.Poll(context.Background())
	if !errors.Is(err, source.ErrRateLimited) {
		t.Errorf("Poll error = %v, want source.ErrRateLimited", err)
	}
	if len(batch.Samples) != 0 {
		t.Errorf("a rate-limited poll returned %d samples, want none", len(batch.Samples))
	}
}

func TestMalformedJSONErrorsRatherThanPanicking(t *testing.T) {
	// Three observatories were observed answering 200 with a non-JSON error
	// page, so this is a live path.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "<html><body>Service temporarily unavailable</body></html>")
	}))
	t.Cleanup(srv.Close)

	src := newSource(t, srv.URL, []string{"BOU"}, pollTime, quietLogger())
	if _, err := src.Poll(context.Background()); err == nil {
		t.Error("Poll accepted a non-JSON body")
	}
}

func TestATimestamplessDocumentIsReportedRatherThanPublishedAsEmpty(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"datetime":[],"@info":{"iaga_code":"BOU"}}`)
	}))
	t.Cleanup(srv.Close)

	src := newSource(t, srv.URL, []string{"BOU"}, pollTime, quietLogger())
	if _, err := src.Poll(context.Background()); err == nil {
		t.Error("Poll accepted a document with no timestamps")
	}
}

func TestPollHonoursContextCancellation(t *testing.T) {
	srv, _ := fixtureServer(t, map[string]string{"BOU": "bou_minute.json"})
	src := newSource(t, srv.URL, []string{"BOU"}, pollTime, quietLogger())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := src.Poll(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("Poll error = %v, want context.Canceled", err)
	}
}

func TestScheduleAgreesWithTheConfiguredInterval(t *testing.T) {
	src := newSource(t, "https://example.invalid", []string{"BOU"}, pollTime, quietLogger())
	sched := src.Schedule()
	if sched.Interval() != src.Interval() {
		t.Errorf("schedule interval = %s, source interval = %s", sched.Interval(), src.Interval())
	}
	if next := sched.NextAfter(pollTime); !next.Equal(pollTime.Add(src.Interval())) {
		t.Errorf("next poll = %s, want %s", next, pollTime.Add(src.Interval()))
	}
}

func TestTheOperatingInstitutesAreLoggedOnceForTheAcknowledgement(t *testing.T) {
	// The licence's acknowledgement is owed to each contributing institute, and
	// an operator cannot honour that without being told which ones this
	// deployment fetches from.
	srv, _ := fixtureServer(t, map[string]string{"BOU": "bou_minute.json"})
	rec := &recordingLogger{}
	src := newSource(t, srv.URL, []string{"BOU"}, pollTime, rec.logger())

	for range 2 {
		if _, err := src.Poll(context.Background()); err != nil {
			t.Fatalf("Poll: %v", err)
		}
	}

	logged := rec.String()
	if !strings.Contains(logged, "United States Geological Survey") {
		t.Errorf("the institute from @info was not logged; logged:\n%s", logged)
	}
	if n := strings.Count(logged, "must be acknowledged"); n != 1 {
		t.Errorf("the institute list was logged %d times, want exactly 1", n)
	}
}

func TestFixturesAreServedFromTestdataRatherThanTheNetwork(t *testing.T) {
	// A guard against a fixture quietly disappearing and a test passing for the
	// wrong reason, and against anything in this package reaching the live
	// service during a test run.
	for _, name := range []string{
		"bou_minute.json", "esk_embargoed.json", "naq_no_data.json", "range_handchecked.json",
	} {
		body, err := os.ReadFile(filepath.Join("testdata", name))
		if err != nil {
			t.Errorf("fixture %s: %v", name, err)
			continue
		}
		var doc series
		if err := json.Unmarshal(body, &doc); err != nil {
			t.Errorf("fixture %s is not a GetData document: %v", name, err)
			continue
		}
		if len(doc.Times) == 0 {
			t.Errorf("fixture %s carries no timestamps", name)
		}
	}
}
