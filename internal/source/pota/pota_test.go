package pota

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
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

// referenceNow is the instant every test pretends it is.
//
// The captured spot fixture's newest spotTime is 2026-09-09T15:45:38Z and the
// activation fixture's spans are relative to the same day, so the clock has to
// be pinned rather than left as time.Now.
var referenceNow = time.Date(2026, 9, 9, 15, 46, 0, 0, time.UTC)

// Facts about the captured fixtures, verified against the live responses.
const (
	fixtureSpots = 77

	// Every spot in the captured feed carried a frequency inside an allocation
	// and a non-empty mode, so all 77 are also in the band breakdown across 12
	// band-and-mode pairs.
	fixtureBandModePairs = 12

	// Scheduled activations whose startDate/endDate span 2026-09-09, out of 176
	// records.
	fixtureUnderway = 63
)

var fixtureNewestSpot = time.Date(2026, 9, 9, 15, 45, 38, 0, time.UTC)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// testClient relaxes the politeness table, which would otherwise give
// 127.0.0.1 a thirty-second floor. One per test, so the 429 cooldown that lives
// on the client cannot leak between tests.
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

// server serves the two endpoints and counts hits on each.
type server struct {
	*httptest.Server
	spotHits atomic.Int64
	actHits  atomic.Int64
}

func newServer(t *testing.T, spots, activations http.HandlerFunc) *server {
	t.Helper()
	s := &server{}
	mux := http.NewServeMux()
	mux.HandleFunc(PathSpots, func(w http.ResponseWriter, r *http.Request) {
		s.spotHits.Add(1)
		spots(w, r)
	})
	mux.HandleFunc(PathActivations, func(w http.ResponseWriter, r *http.Request) {
		s.actHits.Add(1)
		if activations == nil {
			// Not every test cares about the calendar, and the source is
			// supposed to survive it failing.
			http.Error(w, "no handler", http.StatusInternalServerError)
			return
		}
		activations(w, r)
	})
	s.Server = httptest.NewServer(mux)
	t.Cleanup(s.Close)
	return s
}

func serveJSON(body []byte) http.HandlerFunc {
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

func newSource(t *testing.T, ts *server) *Source {
	t.Helper()
	src, err := New(config.POTA{Enabled: true, BaseURL: ts.URL}, testClient(t), testLogger())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	src.now = func() time.Time { return referenceNow }
	return src
}

// pollFixtures polls with both captured fixtures served.
func pollFixtures(t *testing.T) metric.Batch {
	t.Helper()
	ts := newServer(t,
		serveJSON(fixture(t, "spot-activator.json")),
		serveJSON(fixture(t, "activation.json")))
	batch, err := newSource(t, ts).Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	return batch
}

func valueFor(t *testing.T, batch metric.Batch, desc *metric.Descriptor, labels ...string) float64 {
	t.Helper()
	v, ok := lookup(batch, desc, labels...)
	if !ok {
		t.Fatalf("no sample for %s%v in %d samples", desc.FullName(), labels, len(batch.Samples))
	}
	return v
}

func lookup(batch metric.Batch, desc *metric.Descriptor, labels ...string) (float64, bool) {
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
			return s.Value, true
		}
	}
	return 0, false
}

// ---------------------------------------------------------------------------
// Band derivation
// ---------------------------------------------------------------------------

func TestBandIsDerivedFromTheFrequencyAtEveryBandBoundary(t *testing.T) {
	// Both edges of every allocation, plus one step outside each, plus the gaps
	// between adjacent bands. The edges matter because band edges are exactly
	// where contest and DX stations sit.
	for _, tc := range []struct {
		frequency string
		want      string
		ok        bool
	}{
		// Below everything.
		{"0", "", false},
		{"135.6", "", false},
		{"135.7", "2200m", true},
		{"137.8", "2200m", true},
		{"137.9", "", false},

		{"471.9", "", false},
		{"472", "630m", true},
		{"479", "630m", true},
		{"479.1", "", false},

		{"1799.9", "", false},
		{"1800", "160m", true},
		{"1810.5", "160m", true},
		{"2000", "160m", true},
		{"2000.1", "", false},

		{"3499.9", "", false},
		{"3500", "80m", true},
		{"3573", "80m", true},
		{"4000", "80m", true},
		{"4000.1", "", false},

		{"5249.9", "", false},
		{"5250", "60m", true},
		{"5357", "60m", true},
		{"5450", "60m", true},
		{"5450.1", "", false},

		{"6999.9", "", false},
		{"7000", "40m", true},
		{"7060.2", "40m", true},
		{"7300", "40m", true},
		{"7300.1", "", false},

		{"10099.9", "", false},
		{"10100", "30m", true},
		{"10136", "30m", true},
		{"10150", "30m", true},
		{"10150.1", "", false},

		{"13999.9", "", false},
		{"14000", "20m", true},
		{"14074", "20m", true},
		{"14350", "20m", true},
		{"14350.1", "", false},

		{"18067.9", "", false},
		{"18068", "17m", true},
		{"18100", "17m", true},
		{"18168", "17m", true},
		{"18168.1", "", false},

		{"20999.9", "", false},
		{"21000", "15m", true},
		{"21074", "15m", true},
		{"21450", "15m", true},
		{"21450.1", "", false},

		{"24889.9", "", false},
		{"24890", "12m", true},
		{"24915", "12m", true},
		{"24990", "12m", true},
		{"24990.1", "", false},

		{"27999.9", "", false},
		{"28000", "10m", true},
		{"28074", "10m", true},
		{"29700", "10m", true},
		{"29700.1", "", false},

		{"49999.9", "", false},
		{"50000", "6m", true},
		{"50313", "6m", true},
		{"54000", "6m", true},
		{"54000.1", "", false},

		{"69999.9", "", false},
		{"70000", "4m", true},
		{"70200", "4m", true},
		{"70500", "4m", true},
		{"70500.1", "", false},

		{"143999.9", "", false},
		{"144000", "2m", true},
		{"144174", "2m", true},
		{"148000", "2m", true},
		{"148000.1", "", false},

		{"218999.9", "", false},
		{"219000", "1.25m", true},
		{"223500", "1.25m", true},
		{"225000", "1.25m", true},
		{"225000.1", "", false},

		{"419999.9", "", false},
		{"420000", "70cm", true},
		{"432300", "70cm", true},
		{"450000", "70cm", true},
		{"450000.1", "", false},

		{"901999.9", "", false},
		{"902000", "33cm", true},
		{"928000", "33cm", true},
		{"928000.1", "", false},

		{"1239999.9", "", false},
		{"1240000", "23cm", true},
		{"1296100", "23cm", true},
		{"1300000", "23cm", true},
		{"1300000.1", "", false},

		{"2299999.9", "", false},
		{"2300000", "13cm", true},
		{"2450000", "13cm", true},
		{"2450000.1", "", false},

		// Above everything the table covers.
		{"10500000", "", false},
	} {
		got, ok := BandFor(tc.frequency)
		if ok != tc.ok || got != tc.want {
			t.Errorf("BandFor(%q) = (%q, %v), want (%q, %v)",
				tc.frequency, got, ok, tc.want, tc.ok)
		}
	}
}

func TestAnUnreadableFrequencyProducesNoBandRatherThanAGuess(t *testing.T) {
	// All of these arrive from real spotting software, because people type
	// frequencies by hand. "7.060" is the common one: megahertz in a kilohertz
	// field.
	for _, frequency := range []string{
		"", "   ", "unknown", "7.060", "-14074", "NaN", "14074kHz", "14,074", "0",
	} {
		if band, ok := BandFor(frequency); ok {
			t.Errorf("BandFor(%q) = %q, want no band", frequency, band)
		}
	}
}

func TestASpotWithAnUnusableFrequencyOrModeIsCountedInTheTotalButNotByBand(t *testing.T) {
	body := []byte(`[
	  {"frequency":"14074","mode":"FT8","spotTime":"2026-09-09T15:45:00"},
	  {"frequency":"7.060","mode":"SSB","spotTime":"2026-09-09T15:44:00"},
	  {"frequency":"14200","mode":"","spotTime":"2026-09-09T15:43:00"},
	  {"frequency":"","mode":"","spotTime":"2026-09-09T15:42:00"}
	]`)
	ts := newServer(t, serveJSON(body), serveJSON([]byte(`[]`)))
	batch, err := newSource(t, ts).Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}

	if got := valueFor(t, batch, metric.ActivationSpotsTotal, ProgramLabel); got != 4 {
		t.Errorf("total spots = %v, want 4: an unreadable frequency is still a "+
			"station on the air", got)
	}
	if got := valueFor(t, batch, metric.ActivationSpots, ProgramLabel, "20m", "FT8"); got != 1 {
		t.Errorf("20m FT8 = %v, want 1", got)
	}

	// Exactly one band-labelled sample, and no "unknown" bucket anywhere.
	var banded int
	for _, s := range batch.Samples {
		if s.Desc != metric.ActivationSpots {
			continue
		}
		banded++
		for _, l := range s.Labels {
			if strings.EqualFold(l, "unknown") || l == "" {
				t.Errorf("activation_spots carries the label value %q", l)
			}
		}
	}
	if banded != 1 {
		t.Errorf("%d band-labelled samples, want 1", banded)
	}
}

// ---------------------------------------------------------------------------
// Counts from the captured feed
// ---------------------------------------------------------------------------

func TestTheSpotTotalMatchesTheLengthOfTheArray(t *testing.T) {
	batch := pollFixtures(t)
	if got := valueFor(t, batch, metric.ActivationSpotsTotal, ProgramLabel); got != fixtureSpots {
		t.Errorf("total spots = %v, want %v", got, float64(fixtureSpots))
	}
}

func TestTheBandAndModeBreakdownSumsToTheTotalForTheCapturedFeed(t *testing.T) {
	batch := pollFixtures(t)

	var sum, pairs float64
	for _, s := range batch.Samples {
		if s.Desc == metric.ActivationSpots {
			sum += s.Value
			pairs++
		}
	}
	if pairs != fixtureBandModePairs {
		t.Errorf("%v band-and-mode pairs, want %v", pairs, float64(fixtureBandModePairs))
	}
	// Only true because every spot in this capture had a usable frequency and
	// mode; it is the equality that would break if the band table regressed.
	if sum != fixtureSpots {
		t.Errorf("band breakdown sums to %v, want %v", sum, float64(fixtureSpots))
	}
}

func TestTheBandAndModeBreakdownMatchesTheRecordedFigures(t *testing.T) {
	batch := pollFixtures(t)
	for _, tc := range []struct {
		band, mode string
		want       float64
	}{
		{"15m", "FT8", 1},
		{"17m", "CW", 2},
		{"17m", "FT8", 1},
		{"20m", "CW", 16},
		{"20m", "FT4", 1},
		{"20m", "FT8", 10},
		{"20m", "SSB", 24},
		{"30m", "CW", 3},
		{"30m", "FT8", 1},
		{"40m", "CW", 9},
		{"40m", "FT8", 1},
		{"40m", "SSB", 8},
	} {
		if got := valueFor(t, batch, metric.ActivationSpots, ProgramLabel, tc.band, tc.mode); got != tc.want {
			t.Errorf("%s %s = %v, want %v", tc.band, tc.mode, got, tc.want)
		}
	}
}

func TestAnEmptyFeedPublishesZeroRatherThanNoSample(t *testing.T) {
	// The small hours really do produce an empty array, and a gauge that
	// disappears rather than reading zero breaks every alert written on it.
	ts := newServer(t, serveJSON([]byte(`[]`)), serveJSON([]byte(`[]`)))
	batch, err := newSource(t, ts).Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if got := valueFor(t, batch, metric.ActivationSpotsTotal, ProgramLabel); got != 0 {
		t.Errorf("total spots = %v, want 0", got)
	}
	if got := valueFor(t, batch, metric.ScheduledActivations, ProgramLabel); got != 0 {
		t.Errorf("scheduled activations = %v, want 0", got)
	}
	// No spots means no upstream timestamp, so there is nothing honest to
	// report an age from.
	if _, ok := lookup(batch, metric.SourceDataAge, Name); ok {
		t.Errorf("an empty feed published a data age, which it cannot know")
	}
}

func TestSourceDataAgeComesFromTheNewestSpotTimeReadAsUTC(t *testing.T) {
	batch := pollFixtures(t)
	want := referenceNow.Sub(fixtureNewestSpot).Seconds()
	if got := valueFor(t, batch, metric.SourceDataAge, Name); got != want {
		t.Errorf("source data age = %v, want %v; a spotTime read as local time "+
			"rather than UTC would be hours out", got, want)
	}
	// The snapshot is stamped at the newest spot, not at the fetch.
	for _, s := range batch.Samples {
		if s.Desc == metric.ActivationSpotsTotal && !s.Time.Equal(fixtureNewestSpot) {
			t.Errorf("the spot total is stamped %s, want %s",
				s.Time.Format(time.RFC3339), fixtureNewestSpot.Format(time.RFC3339))
		}
	}
}

func TestASpotStampedInTheFutureDoesNotPublishANegativeAge(t *testing.T) {
	body := []byte(`[{"frequency":"14074","mode":"FT8","spotTime":"2026-09-09T23:00:00"}]`)
	ts := newServer(t, serveJSON(body), serveJSON([]byte(`[]`)))
	batch, err := newSource(t, ts).Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if got := valueFor(t, batch, metric.SourceDataAge, Name); got != 0 {
		t.Errorf("source data age = %v, want it clamped to 0", got)
	}
}

// ---------------------------------------------------------------------------
// Cardinality
// ---------------------------------------------------------------------------

func TestNoParkReferenceCallsignSpotterOrGridReachesAnyLabel(t *testing.T) {
	// The whole cardinality argument in one assertion. The captured feed
	// carries 77 references, activators, spotters and grids; a single one
	// reaching a label would be an unbounded, high-churn series set.
	batch := pollFixtures(t)

	forbiddenNames := map[string]bool{
		"reference": true, "activator": true, "spotter": true, "grid": true,
		"grid4": true, "grid6": true, "park": true, "callsign": true,
		"latitude": true, "longitude": true, "spot_id": true, "spotid": true,
	}
	// Values sampled from the captured fixtures.
	forbiddenValues := map[string]bool{
		"KD8OBA": true, "US-1999": true, "N0RC": true, "EN91": true,
		"EN91kd": true, "West Branch State Park": true, "US-OH": true,
	}

	if batch.Len() == 0 {
		t.Fatal("no samples to inspect")
	}
	for _, s := range batch.Samples {
		for _, name := range s.Desc.Labels {
			if forbiddenNames[strings.ToLower(name)] {
				t.Errorf("%s declares the label %q", s.Desc.FullName(), name)
			}
		}
		for i, v := range s.Labels {
			if forbiddenValues[v] {
				t.Errorf("%s label %q = %q, which came from a spot record",
					s.Desc.FullName(), s.Desc.Labels[i], v)
			}
		}
	}
}

func TestTheEmittedSeriesSetStaysSmallAndEverySampleIsValid(t *testing.T) {
	batch := pollFixtures(t)

	// 1 total + 12 band-and-mode pairs + 1 scheduled + 1 age.
	if want := fixtureBandModePairs + 3; batch.Len() != want {
		t.Errorf("batch has %d samples, want %d", batch.Len(), want)
	}
	for _, s := range batch.Samples {
		if err := s.Validate(); err != nil {
			t.Errorf("invalid sample: %v", err)
		}
	}
}

// ---------------------------------------------------------------------------
// Scheduled activations
// ---------------------------------------------------------------------------

func TestScheduledActivationsCountOnlyThoseWhoseSpanContainsToday(t *testing.T) {
	batch := pollFixtures(t)
	if got := valueFor(t, batch, metric.ScheduledActivations, ProgramLabel); got != fixtureUnderway {
		t.Errorf("scheduled activations = %v, want %v of 176", got, float64(fixtureUnderway))
	}
}

func TestTheScheduledActivationSpanIsInclusiveAtBothEnds(t *testing.T) {
	// Both edges, and one day outside each. The first-day and last-day cases
	// are the ones an exclusive comparison silently drops, and a single-day
	// activation — which is the common case — is both edges at once.
	body := []byte(`[
	  {"startDate":"2026-09-09","endDate":"2026-09-09"},
	  {"startDate":"2026-09-09","endDate":"2026-09-30"},
	  {"startDate":"2026-08-01","endDate":"2026-09-09"},
	  {"startDate":"2026-08-01","endDate":"2026-09-08"},
	  {"startDate":"2026-09-10","endDate":"2026-09-20"},
	  {"startDate":"2026-02-15","endDate":"2027-10-03"},
	  {"startDate":"","endDate":"2026-09-09"},
	  {"startDate":"2026-09-09","endDate":"not-a-date"},
	  {"startDate":"2026-09-20","endDate":"2026-09-10"}
	]`)
	ts := newServer(t, serveJSON([]byte(`[]`)), serveJSON(body))
	batch, err := newSource(t, ts).Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}

	// Four span today: the single day, the one starting today, the one ending
	// today, and the multi-month one. The two with unreadable dates are
	// skipped, and the inverted span matches nothing.
	if got := valueFor(t, batch, metric.ScheduledActivations, ProgramLabel); got != 4 {
		t.Errorf("scheduled activations = %v, want 4", got)
	}
}

func TestScheduledActivationsAreEvaluatedAgainstTheWholeUTCDay(t *testing.T) {
	// One minute past midnight and one minute before it must give the same
	// answer, because the calendar has date resolution and pretending otherwise
	// would make the count flicker at the day boundary.
	body := []byte(`[{"startDate":"2026-09-09","endDate":"2026-09-09"}]`)
	for _, at := range []time.Time{
		time.Date(2026, 9, 9, 0, 1, 0, 0, time.UTC),
		time.Date(2026, 9, 9, 23, 59, 0, 0, time.UTC),
	} {
		ts := newServer(t, serveJSON([]byte(`[]`)), serveJSON(body))
		src := newSource(t, ts)
		src.now = func() time.Time { return at }
		batch, err := src.Poll(context.Background())
		if err != nil {
			t.Fatalf("Poll at %s: %v", at, err)
		}
		if got := valueFor(t, batch, metric.ScheduledActivations, ProgramLabel); got != 1 {
			t.Errorf("at %s scheduled activations = %v, want 1", at.Format(time.RFC3339), got)
		}
	}
}

func TestTheCalendarFailingDoesNotCostUsTheSpotCount(t *testing.T) {
	ts := newServer(t, serveJSON(fixture(t, "spot-activator.json")), nil)
	batch, err := newSource(t, ts).Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v; the spot feed is the headline and it succeeded", err)
	}
	if got := valueFor(t, batch, metric.ActivationSpotsTotal, ProgramLabel); got != fixtureSpots {
		t.Errorf("total spots = %v, want %v", got, float64(fixtureSpots))
	}
	if _, ok := lookup(batch, metric.ScheduledActivations, ProgramLabel); ok {
		t.Errorf("a failed calendar published a scheduled-activation count anyway")
	}
}

func TestTheSpotFeedFailingFailsThePoll(t *testing.T) {
	ts := newServer(t, serveStatus(http.StatusInternalServerError, nil),
		serveJSON(fixture(t, "activation.json")))
	batch, err := newSource(t, ts).Poll(context.Background())
	if err == nil {
		t.Fatalf("Poll succeeded with %d samples, want an error", batch.Len())
	}
	if batch.Len() != 0 {
		t.Errorf("a failed poll returned %d samples, want none", batch.Len())
	}
}

// ---------------------------------------------------------------------------
// HTTP behaviour
// ---------------------------------------------------------------------------

func TestNotFoundIsPermanentAndIsNotRetried(t *testing.T) {
	ts := newServer(t, serveStatus(http.StatusNotFound, nil), serveJSON([]byte(`[]`)))
	src := newSource(t, ts)
	src.retries = 3

	if _, err := src.Poll(context.Background()); !errors.Is(err, httpx.ErrPermanent) {
		t.Fatalf("Poll error = %v, want httpx.ErrPermanent", err)
	}
	if ts.spotHits.Load() != 1 {
		t.Errorf("the spot endpoint was hit %d times for a 404, want 1", ts.spotHits.Load())
	}
}

func TestTheForbiddenStatsEndpointsAreNeverRequested(t *testing.T) {
	// /stats, /program/stats, /spot/comments, /spot/activator/latest and the
	// API root all answer 403. A source that asks anyway just makes load and
	// log noise, so this asserts nothing but the two known-good paths are hit.
	var (
		mu    sync.Mutex
		paths []string
	)
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths = append(paths, r.URL.Path)
		mu.Unlock()
		http.Error(w, `{"message":"Missing Authentication Token"}`, http.StatusForbidden)
	})
	mux.HandleFunc(PathSpots, serveJSON(fixture(t, "spot-activator.json")))
	mux.HandleFunc(PathActivations, serveJSON(fixture(t, "activation.json")))
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)

	src, err := New(config.POTA{Enabled: true, BaseURL: ts.URL}, testClient(t), testLogger())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	src.now = func() time.Time { return referenceNow }
	if _, err := src.Poll(context.Background()); err != nil {
		t.Fatalf("Poll: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(paths) != 0 {
		t.Errorf("the source requested %d paths beyond the two it should: %v", len(paths), paths)
	}
}

func TestTooManyRequestsIsReportedAsRateLimited(t *testing.T) {
	ts := newServer(t,
		serveStatus(http.StatusTooManyRequests, map[string]string{"Retry-After": "120"}),
		serveJSON([]byte(`[]`)))
	src := newSource(t, ts)
	src.retries = 3

	_, err := src.Poll(context.Background())
	if !errors.Is(err, source.ErrRateLimited) {
		t.Fatalf("Poll error = %v, want source.ErrRateLimited", err)
	}
	if ts.spotHits.Load() != 1 {
		t.Errorf("the spot endpoint was hit %d times for a 429, want 1", ts.spotHits.Load())
	}
}

func TestMalformedJSONIsAnErrorRatherThanAPanic(t *testing.T) {
	for _, body := range []string{
		`{"message":"Missing Authentication Token"}`, // an object where an array belongs
		`[{"frequency":`, // truncated
		`<html>maintenance</html>`,
		``,
		`[{"frequency":7060.2,"mode":"CW","spotTime":"2026-09-09T15:45:00"}]`, // frequency as a number
	} {
		ts := newServer(t, serveJSON([]byte(body)), serveJSON([]byte(`[]`)))
		batch, err := newSource(t, ts).Poll(context.Background())
		if err == nil {
			t.Errorf("Poll accepted %q with %d samples, want an error", body, batch.Len())
		}
	}
}

func TestPollHonoursContextCancellation(t *testing.T) {
	ts := newServer(t, serveJSON(fixture(t, "spot-activator.json")),
		serveJSON(fixture(t, "activation.json")))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	batch, err := newSource(t, ts).Poll(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Poll error = %v, want context.Canceled", err)
	}
	if batch.Len() != 0 {
		t.Errorf("a cancelled poll returned %d samples, want none", batch.Len())
	}
	if ts.spotHits.Load() != 0 || ts.actHits.Load() != 0 {
		t.Errorf("a cancelled poll made requests: %d spot, %d activation",
			ts.spotHits.Load(), ts.actHits.Load())
	}
}

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

func TestTheIntervalDefaultsToAMinuteAndIsClampedToTheCourtesyFloor(t *testing.T) {
	for _, tc := range []struct {
		configured, want time.Duration
	}{
		{0, DefaultInterval},
		{-time.Second, DefaultInterval},
		{time.Second, minInterval},
		{5 * time.Minute, 5 * time.Minute},
	} {
		src, err := New(config.POTA{Enabled: true, Interval: tc.configured},
			testClient(t), testLogger())
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		if got := src.Interval(); got != tc.want {
			t.Errorf("Interval() with %s configured = %s, want %s", tc.configured, got, tc.want)
		}
	}
}

func TestTheSourcePollsOnAnIntervalRatherThanDeclaringAPublicationClock(t *testing.T) {
	src, err := New(config.POTA{Enabled: true}, testClient(t), testLogger())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// The feed genuinely changes continuously; declaring a daily clock for it
	// would be a fiction, so Scheduled is deliberately not implemented.
	if _, ok := any(src).(source.Scheduled); ok {
		t.Errorf("the source implements source.Scheduled; this upstream has no publication clock")
	}
}

func TestNewFillsInTheDefaultBaseURLAndRejectsConfigurationItCannotWorkWith(t *testing.T) {
	src, err := New(config.POTA{Enabled: true}, testClient(t), testLogger())
	if err != nil {
		t.Fatalf("New with an empty base URL: %v", err)
	}
	if src.base != DefaultBaseURL {
		t.Errorf("base = %q, want the default %q", src.base, DefaultBaseURL)
	}

	for _, tc := range []struct {
		name   string
		cfg    config.POTA
		client *httpx.Client
	}{
		{"no client", config.POTA{Enabled: true}, nil},
		{"a base URL with no host", config.POTA{BaseURL: "https://"}, testClient(t)},
		{"a non-HTTP scheme", config.POTA{BaseURL: "ftp://api.pota.app"}, testClient(t)},
		{"an unparseable base URL", config.POTA{BaseURL: "http://[::1"}, testClient(t)},
	} {
		if _, err := New(tc.cfg, tc.client, testLogger()); err == nil {
			t.Errorf("New accepted %s", tc.name)
		}
	}
}

func TestTheSourceIdentifiesItselfConsistently(t *testing.T) {
	src, err := New(config.POTA{Enabled: true}, testClient(t), testLogger())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if src.Name() != Name || Name != "pota" {
		t.Errorf("Name() = %q, const Name = %q, want both %q", src.Name(), Name, "pota")
	}
}
