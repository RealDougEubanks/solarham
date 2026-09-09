package celestrak

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

// Facts about testdata/gp-amateur.json, captured live from
// /NORAD/elements/gp.php?GROUP=amateur&FORMAT=json.
const fixtureObjects = 97

var (
	// fixtureNewestEpoch is MOZHAETS 4 (RS22)'s epoch, the newest of the 97.
	fixtureNewestEpoch = time.Date(2026, 9, 9, 8, 34, 16, 254912000, time.UTC)

	// fixtureOldestEpoch is six days earlier, which is what a rolling
	// per-object refresh looks like and is why the newest is what matters.
	fixtureOldestEpoch = time.Date(2026, 9, 3, 1, 15, 10, 727136000, time.UTC)

	// referenceNow is the instant every test pretends it is: a shade over four
	// hours after the newest epoch in the fixture.
	referenceNow = time.Date(2026, 9, 9, 12, 34, 16, 254912000, time.UTC)
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// testClient relaxes the politeness table, which gives an unlisted host —
// 127.0.0.1 — a thirty-second floor. One per test, so the conditional-GET
// validator cache and the 429 cooldown cannot leak between tests.
func testClient(t *testing.T) *httpx.Client {
	t.Helper()
	return httpx.New(httpx.Options{
		Timeout:  5 * time.Second,
		Fallback: httpx.HostPolicy{MinInterval: time.Microsecond, Burst: 1000},
	}, testLogger())
}

func fixture(t *testing.T, name string) []byte {
	t.Helper()
	body, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatalf("reading fixture: %v", err)
	}
	return body
}

// server serves one handler at the GP path and records what was asked for.
type server struct {
	*httptest.Server
	hits  atomic.Int64
	query atomic.Value // url.Values, as a string
}

func newServer(t *testing.T, h http.HandlerFunc) *server {
	t.Helper()
	s := &server{}
	mux := http.NewServeMux()
	mux.HandleFunc(Path, func(w http.ResponseWriter, r *http.Request) {
		s.hits.Add(1)
		s.query.Store(r.URL.RawQuery)
		h(w, r)
	})
	s.Server = httptest.NewServer(mux)
	t.Cleanup(s.Close)
	return s
}

func serveJSON(body []byte) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("ETag", `"gp-amateur-1"`)
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

func newSource(t *testing.T, ts *server) *Source {
	t.Helper()
	src, err := New(config.Celestrak{Enabled: true, BaseURL: ts.URL}, testClient(t), testLogger())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	src.now = func() time.Time { return referenceNow }
	return src
}

func valueFor(t *testing.T, batch metric.Batch, desc *metric.Descriptor, labels ...string) float64 {
	t.Helper()
	for _, s := range batch.Samples {
		if s.Desc != desc || len(s.Labels) != len(labels) {
			continue
		}
		match := true
		for i, l := range labels {
			if s.Labels[i] != l {
				match = false
				break
			}
		}
		if match {
			return s.Value
		}
	}
	t.Fatalf("no sample for %s%v in %d samples", desc.FullName(), labels, len(batch.Samples))
	return 0
}

// ---------------------------------------------------------------------------
// The published numbers
// ---------------------------------------------------------------------------

func TestTLEAgeIsMeasuredFromTheNewestEpochAgainstAFixedClock(t *testing.T) {
	ts := newServer(t, serveJSON(fixture(t, "gp-amateur.json")))
	batch, err := newSource(t, ts).Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}

	want := referenceNow.Sub(fixtureNewestEpoch).Seconds()
	if want != (4 * time.Hour).Seconds() {
		t.Fatalf("the test's own arithmetic is wrong: %v", want)
	}
	if got := valueFor(t, batch, metric.TLEAge, DefaultGroup); got != want {
		t.Errorf("TLE age = %v, want %v", got, want)
	}

	// The oldest epoch is six days back. Picking it, or averaging, would report
	// a catalogue that is refreshing normally as badly stale.
	if stale := referenceNow.Sub(fixtureOldestEpoch).Seconds(); valueFor(t, batch, metric.TLEAge, DefaultGroup) == stale {
		t.Errorf("TLE age was computed from the oldest epoch, not the newest")
	}
}

func TestTLEAgeIsPositiveForAPastEpochAndZeroForAFutureOne(t *testing.T) {
	// The sign convention matters: this metric answers "how stale", so a
	// catalogue refreshed an hour ago must read 3600 and not -3600. Some
	// catalogues propagate epochs slightly into the future, which is not
	// staleness of any kind.
	for _, tc := range []struct {
		name  string
		epoch string
		want  float64
	}{
		{"an hour old", referenceNow.Add(-time.Hour).Format("2006-01-02T15:04:05.000000"), 3600},
		{"four days old", referenceNow.Add(-96 * time.Hour).Format("2006-01-02T15:04:05.000000"), 345600},
		{"an hour in the future", referenceNow.Add(time.Hour).Format("2006-01-02T15:04:05.000000"), 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := []byte(`[{"OBJECT_NAME":"TEST","NORAD_CAT_ID":1,"EPOCH":"` + tc.epoch + `"}]`)
			ts := newServer(t, serveJSON(body))
			batch, err := newSource(t, ts).Poll(context.Background())
			if err != nil {
				t.Fatalf("Poll: %v", err)
			}
			if got := valueFor(t, batch, metric.TLEAge, DefaultGroup); got != tc.want {
				t.Errorf("TLE age = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestTheObjectCountIsTheLengthOfTheArray(t *testing.T) {
	ts := newServer(t, serveJSON(fixture(t, "gp-amateur.json")))
	batch, err := newSource(t, ts).Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if got := valueFor(t, batch, metric.TLECount, DefaultGroup); got != fixtureObjects {
		t.Errorf("object count = %v, want %v", got, float64(fixtureObjects))
	}
}

func TestObjectsWithAnUnreadableEpochStillCountButDoNotSetTheAge(t *testing.T) {
	// A single bad epoch must not lose the document, and must not be treated as
	// the newest, which is what a zero time would do if it were sorted naively.
	body := []byte(`[
	  {"OBJECT_NAME":"GOOD","NORAD_CAT_ID":1,"EPOCH":"2026-09-09T11:34:16.254912"},
	  {"OBJECT_NAME":"BAD","NORAD_CAT_ID":2,"EPOCH":"not a timestamp"},
	  {"OBJECT_NAME":"BLANK","NORAD_CAT_ID":"3","EPOCH":""}
	]`)
	ts := newServer(t, serveJSON(body))
	batch, err := newSource(t, ts).Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if got := valueFor(t, batch, metric.TLECount, DefaultGroup); got != 3 {
		t.Errorf("object count = %v, want 3: an unreadable epoch is still an object "+
			"in the catalogue", got)
	}
	if got := valueFor(t, batch, metric.TLEAge, DefaultGroup); got != 3600 {
		t.Errorf("TLE age = %v, want 3600 from the one readable epoch", got)
	}
}

func TestADocumentWithNoReadableEpochAtAllIsAnError(t *testing.T) {
	// A format change must not publish an age of zero, which would read as a
	// perfectly fresh catalogue.
	body := []byte(`[{"OBJECT_NAME":"X","NORAD_CAT_ID":1,"EPOCH":"09/09/26 08:34"}]`)
	ts := newServer(t, serveJSON(body))
	batch, err := newSource(t, ts).Poll(context.Background())
	if err == nil {
		t.Fatalf("Poll succeeded with %d samples, want an error", batch.Len())
	}
	if batch.Len() != 0 {
		t.Errorf("a failed poll returned %d samples, want none", batch.Len())
	}
}

func TestAnEmptyArrayIsAnErrorRatherThanACountOfZero(t *testing.T) {
	// An unknown group name is answered with [] rather than a 404, and
	// publishing zero would read as every amateur satellite having decayed.
	ts := newServer(t, serveJSON([]byte(`[]`)))
	if _, err := newSource(t, ts).Poll(context.Background()); err == nil {
		t.Fatal("Poll accepted an empty group, want an error naming the group")
	}
}

func TestSourceDataAgeMatchesTheElementSetAgeAndEverySampleIsValid(t *testing.T) {
	ts := newServer(t, serveJSON(fixture(t, "gp-amateur.json")))
	batch, err := newSource(t, ts).Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}

	if batch.Len() != 3 {
		t.Errorf("batch has %d samples, want 3", batch.Len())
	}
	tle := valueFor(t, batch, metric.TLEAge, DefaultGroup)
	if got := valueFor(t, batch, metric.SourceDataAge, Name); got != tle {
		t.Errorf("source data age = %v but TLE age = %v; there is only one "+
			"timestamp in this document, so they must agree", got, tle)
	}
	for _, s := range batch.Samples {
		if err := s.Validate(); err != nil {
			t.Errorf("invalid sample: %v", err)
		}
	}
}

func TestTheAgeIsStampedNowAndTheCountAtTheNewestEpoch(t *testing.T) {
	ts := newServer(t, serveJSON(fixture(t, "gp-amateur.json")))
	batch, err := newSource(t, ts).Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	for _, s := range batch.Samples {
		want := referenceNow
		if s.Desc == metric.TLECount {
			want = fixtureNewestEpoch
		}
		if !s.Time.Equal(want) {
			t.Errorf("%s%v is stamped %s, want %s", s.Desc.FullName(), s.Labels,
				s.Time.Format(time.RFC3339Nano), want.Format(time.RFC3339Nano))
		}
	}
}

func TestNoPassPredictionOrPerObjectSeriesIsPublished(t *testing.T) {
	// The scope decision, asserted. 97 objects with names and catalogue numbers
	// go in; three series come out, none of them naming an object.
	ts := newServer(t, serveJSON(fixture(t, "gp-amateur.json")))
	batch, err := newSource(t, ts).Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}

	forbidden := map[string]bool{
		"satellite": true, "object": true, "object_name": true, "norad_cat_id": true,
		"norad": true, "pass": true, "elevation": true, "azimuth": true,
	}
	for _, s := range batch.Samples {
		for _, name := range s.Desc.Labels {
			if forbidden[strings.ToLower(name)] {
				t.Errorf("%s declares the label %q", s.Desc.FullName(), name)
			}
		}
		for i, v := range s.Labels {
			if strings.Contains(v, "OSCAR") || strings.Contains(v, "MOZHAETS") {
				t.Errorf("%s label %q = %q, which is an object name",
					s.Desc.FullName(), s.Desc.Labels[i], v)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Epoch parsing
// ---------------------------------------------------------------------------

func TestTheEpochIsParsedTolerantlyAndAlwaysAsUTC(t *testing.T) {
	want := time.Date(2026, 9, 8, 14, 58, 28, 28928000, time.UTC)
	for _, raw := range []string{
		"2026-09-08T14:58:28.028928",  // the live JSON form
		"2026-09-08T14:58:28.028928Z", // with an explicit zone
		" 2026-09-08T14:58:28.028928 ",
		"2026-09-08 14:58:28.028928", // through a spreadsheet
	} {
		got, ok := ParseEpoch(raw)
		if !ok {
			t.Errorf("ParseEpoch(%q) failed", raw)
			continue
		}
		if !got.Equal(want) {
			t.Errorf("ParseEpoch(%q) = %s, want %s", raw,
				got.Format(time.RFC3339Nano), want.Format(time.RFC3339Nano))
		}
		if got.Location() != time.UTC {
			t.Errorf("ParseEpoch(%q) returned location %s, want UTC", raw, got.Location())
		}
	}

	// Coarser forms parse to the start of their resolution rather than failing.
	for _, tc := range []struct {
		raw  string
		want time.Time
	}{
		{"2026-09-08T14:58:28", time.Date(2026, 9, 8, 14, 58, 28, 0, time.UTC)},
		{"2026-09-08", time.Date(2026, 9, 8, 0, 0, 0, 0, time.UTC)},
		{"2026-09-08T14:58:28.5", time.Date(2026, 9, 8, 14, 58, 28, 500000000, time.UTC)},
	} {
		got, ok := ParseEpoch(tc.raw)
		if !ok || !got.Equal(tc.want) {
			t.Errorf("ParseEpoch(%q) = (%s, %v), want %s", tc.raw,
				got.Format(time.RFC3339Nano), ok, tc.want.Format(time.RFC3339Nano))
		}
	}

	// A non-UTC offset is honoured rather than ignored.
	if got, ok := ParseEpoch("2026-09-08T14:58:28.028928+02:00"); !ok {
		t.Errorf("ParseEpoch with an offset failed")
	} else if !got.Equal(time.Date(2026, 9, 8, 12, 58, 28, 28928000, time.UTC)) {
		t.Errorf("ParseEpoch with a +02:00 offset = %s, want it converted to UTC",
			got.Format(time.RFC3339Nano))
	}

	for _, raw := range []string{
		"", "   ", "not a timestamp", "09/09/26 08:34", "26251.35714286", "null",
	} {
		if got, ok := ParseEpoch(raw); ok {
			t.Errorf("ParseEpoch(%q) = %s, want failure", raw, got)
		}
	}
}

// ---------------------------------------------------------------------------
// Request shape
// ---------------------------------------------------------------------------

func TestTheRequestAsksForTheConfiguredGroupAsJSON(t *testing.T) {
	ts := newServer(t, serveJSON(fixture(t, "gp-amateur.json")))
	src, err := New(config.Celestrak{Enabled: true, BaseURL: ts.URL, Group: "cubesat"},
		testClient(t), testLogger())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	src.now = func() time.Time { return referenceNow }
	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}

	query, _ := ts.query.Load().(string)
	for _, want := range []string{"GROUP=cubesat", "FORMAT=json"} {
		if !strings.Contains(query, want) {
			t.Errorf("query %q does not contain %q", query, want)
		}
	}
	// The group is the catalogue label, so it must follow the configuration.
	if got := valueFor(t, batch, metric.TLECount, "cubesat"); got != fixtureObjects {
		t.Errorf("object count under the cubesat label = %v, want %v", got, float64(fixtureObjects))
	}
}

func TestTheGroupIsEscapedForTheQueryString(t *testing.T) {
	src, err := New(config.Celestrak{Enabled: true, Group: "special&interest"},
		testClient(t), testLogger())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if strings.Contains(src.url, "special&interest") {
		t.Errorf("url = %q, want the ampersand escaped so it cannot inject a "+
			"second query parameter", src.url)
	}
	if !strings.Contains(src.url, "special%26interest") {
		t.Errorf("url = %q, want the group percent-encoded", src.url)
	}
}

// ---------------------------------------------------------------------------
// HTTP behaviour
// ---------------------------------------------------------------------------

func TestAConditionalRequestAnsweredNotModifiedReportsErrNotModified(t *testing.T) {
	ts := newServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotModified)
	})
	batch, err := newSource(t, ts).Poll(context.Background())
	if !errors.Is(err, source.ErrNotModified) {
		t.Fatalf("Poll error = %v, want source.ErrNotModified", err)
	}
	if batch.Len() != 0 {
		t.Errorf("an unchanged catalogue produced %d samples, want none", batch.Len())
	}
}

func TestTheRequestReplaysTheValidatorsFromThePreviousResponse(t *testing.T) {
	var conditional atomic.Int64
	ts := newServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("If-None-Match") != "" {
			conditional.Add(1)
		}
		serveJSON(fixture(t, "gp-amateur.json"))(w, r)
	})

	src := newSource(t, ts)
	for range 2 {
		if _, err := src.Poll(context.Background()); err != nil {
			t.Fatalf("Poll: %v", err)
		}
	}
	if conditional.Load() != 1 {
		t.Errorf("%d of 2 requests carried If-None-Match, want 1; without it every "+
			"poll transfers 40 KB from a host that blocks aggressive clients",
			conditional.Load())
	}
}

func TestNotFoundIsPermanentAndIsNotRetried(t *testing.T) {
	ts := newServer(t, serveStatus(http.StatusNotFound, nil))
	src := newSource(t, ts)
	src.retries = 3

	if _, err := src.Poll(context.Background()); !errors.Is(err, httpx.ErrPermanent) {
		t.Fatalf("Poll error = %v, want httpx.ErrPermanent", err)
	}
	if ts.hits.Load() != 1 {
		t.Errorf("the server was hit %d times for a 404, want 1: retrying against a "+
			"host that blocks aggressive clients is how you get blocked", ts.hits.Load())
	}
}

func TestTooManyRequestsIsReportedAsRateLimitedAndIsNotRetried(t *testing.T) {
	ts := newServer(t, serveStatus(http.StatusTooManyRequests,
		map[string]string{"Retry-After": "3600"}))
	src := newSource(t, ts)
	src.retries = 3

	_, err := src.Poll(context.Background())
	if !errors.Is(err, source.ErrRateLimited) {
		t.Fatalf("Poll error = %v, want source.ErrRateLimited", err)
	}
	if !errors.Is(err, httpx.ErrRateLimited) {
		t.Errorf("Poll error = %v, want it to wrap httpx.ErrRateLimited too", err)
	}
	if ts.hits.Load() != 1 {
		t.Errorf("the server was hit %d times for a 429, want 1", ts.hits.Load())
	}
}

func TestMalformedBodiesAreErrorsRatherThanPanics(t *testing.T) {
	for _, body := range []string{
		`{"error":"no such group"}`, // an object where an array belongs
		`[{"EPOCH":`,                // truncated
		`<html>maintenance</html>`,
		``,
		"1 25544U 98067A   26252.35714286  .00016717  00000-0  10270-3 0  9004", // TLE text
	} {
		ts := newServer(t, serveJSON([]byte(body)))
		batch, err := newSource(t, ts).Poll(context.Background())
		if err == nil {
			t.Errorf("Poll accepted %q with %d samples, want an error", body, batch.Len())
		}
	}
}

func TestPollHonoursContextCancellation(t *testing.T) {
	ts := newServer(t, serveJSON(fixture(t, "gp-amateur.json")))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	batch, err := newSource(t, ts).Poll(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Poll error = %v, want context.Canceled", err)
	}
	if batch.Len() != 0 {
		t.Errorf("a cancelled poll returned %d samples, want none", batch.Len())
	}
	if ts.hits.Load() != 0 {
		t.Errorf("a cancelled poll made %d requests, want none", ts.hits.Load())
	}
}

// ---------------------------------------------------------------------------
// Schedule and construction
// ---------------------------------------------------------------------------

func TestTheScheduleIsTwiceDailyAndNeverHourly(t *testing.T) {
	src, err := New(config.Celestrak{Enabled: true}, testClient(t), testLogger())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	sched := src.Schedule()

	if got, want := sched.Interval(), 12*time.Hour; got != want {
		t.Errorf("schedule interval = %s, want %s", got, want)
	}
	if got, want := src.Interval(), sched.Interval(); got != want {
		t.Errorf("Interval() = %s but the schedule says %s; the two must not disagree", got, want)
	}

	// Twenty consecutive slots, checked for the two expected times of day and
	// for a gap that is never shorter than eleven hours. An hourly schedule, or
	// one that had drifted to four times a day, fails both.
	at := time.Date(2026, 9, 9, 0, 0, 0, 0, time.UTC)
	hours := map[int]int{}
	prev := time.Time{}
	for range 20 {
		at = sched.NextAfter(at)
		hours[at.UTC().Hour()]++

		if h, m := at.UTC().Hour(), at.UTC().Minute(); (h != 4 && h != 16) || m < 20 || m > 22 {
			t.Errorf("poll scheduled for %s, want 04:20 or 16:20 UTC plus a little spread",
				at.UTC().Format(time.RFC3339))
		}
		if !prev.IsZero() {
			if gap := at.Sub(prev); gap < 11*time.Hour {
				t.Errorf("consecutive polls %s apart, want about 12 hours", gap)
			}
		}
		prev = at
	}
	if len(hours) != 2 {
		t.Errorf("polls landed at %d distinct hours of the day, want 2: %v", len(hours), hours)
	}
}

func TestNewFillsInTheDefaultsAndRejectsConfigurationItCannotWorkWith(t *testing.T) {
	src, err := New(config.Celestrak{Enabled: true}, testClient(t), testLogger())
	if err != nil {
		t.Fatalf("New with empty configuration: %v", err)
	}
	if src.group != DefaultGroup {
		t.Errorf("group = %q, want the default %q", src.group, DefaultGroup)
	}
	if !strings.HasPrefix(src.url, DefaultBaseURL+Path) {
		t.Errorf("url = %q, want it built from the default base URL", src.url)
	}

	for _, tc := range []struct {
		name   string
		cfg    config.Celestrak
		client *httpx.Client
	}{
		{"no client", config.Celestrak{Enabled: true}, nil},
		{"a base URL with no host", config.Celestrak{BaseURL: "https://"}, testClient(t)},
		{"a non-HTTP scheme", config.Celestrak{BaseURL: "ftp://celestrak.org"}, testClient(t)},
		{"an unparseable base URL", config.Celestrak{BaseURL: "http://[::1"}, testClient(t)},
		{"a group with whitespace in it", config.Celestrak{Group: "amateur radio"}, testClient(t)},
		{"a group with a quote in it", config.Celestrak{Group: `am"ateur`}, testClient(t)},
	} {
		if _, err := New(tc.cfg, tc.client, testLogger()); err == nil {
			t.Errorf("New accepted %s", tc.name)
		}
	}
}

func TestTheSourceIdentifiesItselfConsistently(t *testing.T) {
	src, err := New(config.Celestrak{Enabled: true}, testClient(t), testLogger())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if src.Name() != Name || Name != "celestrak" {
		t.Errorf("Name() = %q, const Name = %q, want both %q", src.Name(), Name, "celestrak")
	}
}
