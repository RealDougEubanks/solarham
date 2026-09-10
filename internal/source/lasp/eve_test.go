package lasp

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/RealDougEubanks/solarham/internal/config"
	"github.com/RealDougEubanks/solarham/internal/metric"
	"github.com/RealDougEubanks/solarham/internal/source"
)

// The last row of testdata/eve_l0cs_tail.txt, captured live from a
// "Range: bytes=-3000" request, with the date from the header fixture:
//
//	2026 252 09 09
//	1544  4.29e-07  3.75e-09  1.56e-04  1.916e-04  7.326e-04  3.446e-04  7.029e-04 -1.000e+00  1.94e+02  6.7982e-03  1.52e+02  3.361e-01  2.021e-01  2.617e-01  2.001e-01  -6.3 -25.0  1.16e+03  7.77e-09
var (
	eveWantAt   = time.Date(2026, 9, 9, 15, 44, 0, 0, time.UTC)
	eveWantXRSB = 4.29e-07
	eveWant0107 = 1.916e-04
	eveWant171  = 7.326e-04
	eveWant257  = 3.446e-04
	eveWant304  = 7.029e-04
	eveWantLat  = -6.3
	eveWantLon  = -25.0
)

// serveEVE answers the header range with headFixture and the tail range with
// tailFixture, mimicking the real server's suffix and prefix range handling.
// It records every Range header it saw.
type eveServer struct {
	ranges  chan string
	headHit atomic.Int64
	tailHit atomic.Int64
}

func serveEVE(t *testing.T, head, tail []byte) (*httptest.Server, *eveServer) {
	t.Helper()
	rec := &eveServer{ranges: make(chan string, 64)}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hdr := r.Header.Get("Range")
		select {
		case rec.ranges <- hdr:
		default:
		}

		var n int
		switch {
		case func() bool { _, err := fmt.Sscanf(hdr, "bytes=-%d", &n); return err == nil }():
			rec.tailHit.Add(1)
			body := tail
			if n < len(body) {
				body = body[len(body)-n:]
			}
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(body)

		case strings.HasPrefix(hdr, "bytes=0-"):
			rec.headHit.Add(1)
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(head)

		default:
			// No usable Range header: the whole file.
			_, _ = w.Write(append(append([]byte(nil), head...), tail...))
		}
	}))
	t.Cleanup(ts.Close)
	return ts, rec
}

func (e *eveServer) seenRanges() []string {
	var out []string
	for {
		select {
		case r := <-e.ranges:
			out = append(out, r)
		default:
			return out
		}
	}
}

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

func TestNewRequiresASharedHTTPClient(t *testing.T) {
	if _, err := New(config.LASP{EVE: true}, nil, discardLogger()); err == nil {
		t.Fatal("wanted an error when no httpx client was supplied, got nil")
	}
}

func TestNewRejectsAConfigurationWithNeitherProductEnabled(t *testing.T) {
	if _, err := New(config.LASP{Enabled: true}, testClient(t), discardLogger()); err == nil {
		t.Fatal("a source that would fetch nothing was accepted; wanted an error")
	}
}

func TestNewRejectsABaseURLThatIsNotAbsolute(t *testing.T) {
	for _, bad := range []string{"lasp.colorado.edu", "/eve", "://nope"} {
		if _, err := New(config.LASP{EVE: true, BaseURL: bad}, testClient(t), discardLogger()); err == nil {
			t.Errorf("base URL %q was accepted; wanted an error", bad)
		}
	}
}

func TestNewAppliesTheDocumentedDefaults(t *testing.T) {
	s, err := New(config.LASP{Enabled: true, EVE: true}, testClient(t), discardLogger())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if s.Name() != Name || Name != "lasp" {
		t.Errorf("Name is %q, want %q", s.Name(), "lasp")
	}
	if s.baseURL != defaultBaseURL {
		t.Errorf("base URL is %q, want %q", s.baseURL, defaultBaseURL)
	}
	if s.retries != defaultRetries {
		t.Errorf("retries is %d, want %d", s.retries, defaultRetries)
	}
}

// ---------------------------------------------------------------------------
// Fixture parsing, with exact expected values
// ---------------------------------------------------------------------------

func TestTheCapturedEVERangeResponsesParseIntoTheExpectedSamples(t *testing.T) {
	ts, _ := serveEVE(t, fixture(t, "eve_l0cs_head.txt"), fixture(t, "eve_l0cs_tail.txt"))
	s := newTestSource(t, config.LASP{Enabled: true, EVE: true}, ts.URL, eveWantAt.Add(time.Minute))

	batch, err := s.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}

	for _, want := range []struct {
		band  string
		value float64
	}{
		{"0.1-7nm", eveWant0107},
		{"17.1nm", eveWant171},
		{"25.7nm", eveWant257},
		{"30.4nm", eveWant304},
	} {
		got := sampleFor(t, batch, metric.EUVIrradiance, want.band)
		if got.Value != want.value {
			t.Errorf("the %s irradiance is %g, want %g", want.band, got.Value, want.value)
		}
		if !got.Time.Equal(eveWantAt) {
			t.Errorf("the %s irradiance is stamped %s, want %s", want.band, got.Time, eveWantAt)
		}
	}

	if got := sampleFor(t, batch, metric.EUVSourceLatitude); got.Value != eveWantLat {
		t.Errorf("CMLat is %g, want %g", got.Value, eveWantLat)
	}
	if got := sampleFor(t, batch, metric.EUVSourceLongitude); got.Value != eveWantLon {
		t.Errorf("CMLon is %g, want %g", got.Value, eveWantLon)
	}
	if got := sampleFor(t, batch, metric.XRayFlux, xrayBandLabel); got.Value != eveWantXRSB {
		t.Errorf("the XRS-B proxy is %g, want %g", got.Value, eveWantXRSB)
	}
}

func TestTheWholeFileAlsoParsesWhenTheRangeHeaderIsIgnored(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(fixture(t, "eve_l0cs_full.txt"))
	}))
	t.Cleanup(ts.Close)

	s := newTestSource(t, config.LASP{Enabled: true, EVE: true}, ts.URL, eveWantAt)
	batch, err := s.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}

	// eve_l0cs_full.txt ends on the same row as the tail fixture.
	if got := sampleFor(t, batch, metric.EUVSourceLatitude); got.Value != eveWantLat {
		t.Errorf("CMLat is %g, want %g", got.Value, eveWantLat)
	}
	if got := sampleFor(t, batch, metric.EUVIrradiance, "17.1nm"); !got.Time.Equal(eveWantAt) {
		t.Errorf("the sample is stamped %s, want %s from the header's date line", got.Time, eveWantAt)
	}
}

// ---------------------------------------------------------------------------
// The -1.00e+00 sentinel
// ---------------------------------------------------------------------------

func TestTheDegradedMEGSPChannelProducesNoSample(t *testing.T) {
	// 121.6 nm reports the sentinel permanently, so the band must not appear in
	// eveBands at all — a band label that never produces a sample is a series an
	// operator will spend an afternoon looking for.
	for _, band := range eveBands {
		if band.column == col1216MEGSP {
			t.Errorf("the permanently-sentinel 121.6 nm MEGS-P column is published as band %q", band.label)
		}
	}
}

func TestASentinelIrradianceColumnProducesNoSample(t *testing.T) {
	// The captured row has -1.000e+00 in the 36.6 nm column.
	ts, _ := serveEVE(t, fixture(t, "eve_l0cs_head.txt"), fixture(t, "eve_l0cs_tail.txt"))
	s := newTestSource(t, config.LASP{Enabled: true, EVE: true}, ts.URL, eveWantAt)

	batch, err := s.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	for _, smp := range batch.Samples {
		if smp.Desc != metric.EUVIrradiance {
			continue
		}
		if band, _ := smp.LabelFor("band"); band == "36.6nm" {
			t.Errorf("the sentinel 36.6 nm column produced a sample of %g", smp.Value)
		}
		if smp.Value <= 0 {
			t.Errorf("a non-positive irradiance reached a sample: %v = %g", smp.Labels, smp.Value)
		}
	}
}

func TestASentinelCentroidProducesNoSample(t *testing.T) {
	// A row that is otherwise good but whose CMLat and CMLon are the sentinel.
	// This is the one pair where the sentinel cannot be detected by sign, so it
	// is the case most likely to leak.
	row := eveRow{fields: strings.Fields(
		"1544  4.29e-07  3.75e-09  1.56e-04  1.916e-04  7.326e-04  3.446e-04  7.029e-04 -1.000e+00  1.94e+02  6.7982e-03  1.52e+02  3.361e-01  2.021e-01  2.617e-01  2.001e-01  -1.00e+00 -1.00e+00  1.16e+03  7.77e-09")}

	if v, ok := row.angle(colCMLat); ok {
		t.Errorf("the sentinel CMLat produced %g; wanted no sample", v)
	}
	if v, ok := row.angle(colCMLon); ok {
		t.Errorf("the sentinel CMLon produced %g; wanted no sample", v)
	}
}

func TestARealNegativeCentroidIsStillPublished(t *testing.T) {
	// The regression guard on the sentinel test above: a latitude of -6.3 is a
	// perfectly ordinary reading and must survive.
	row := eveRow{fields: strings.Fields(
		"1544  4.29e-07  3.75e-09  1.56e-04  1.916e-04  7.326e-04  3.446e-04  7.029e-04 -1.000e+00  1.94e+02  6.7982e-03  1.52e+02  3.361e-01  2.021e-01  2.617e-01  2.001e-01  -6.3 -25.0  1.16e+03  7.77e-09")}

	if v, ok := row.angle(colCMLat); !ok || v != -6.3 {
		t.Errorf("CMLat is (%g, %t), want (-6.3, true)", v, ok)
	}
	if v, ok := row.angle(colCMLon); !ok || v != -25.0 {
		t.Errorf("CMLon is (%g, %t), want (-25, true)", v, ok)
	}
}

func TestTheSentinelIsRecognisedAtEveryPrecisionTheFileWritesIt(t *testing.T) {
	// The same missing value appears as -1.00e+00, -1.000e+00 and -1.0000e+00
	// in different columns of the same row.
	for _, text := range []string{"-1.00e+00", "-1.000e+00", "-1.0000e+00", "-1.0", "-1"} {
		row := eveRow{fields: []string{"0000", text}}
		if _, ok := row.angle(1); ok {
			t.Errorf("%q was not recognised as the sentinel", text)
		}
		if _, ok := row.value(1); ok {
			t.Errorf("%q was accepted as a positive value", text)
		}
	}
}

func TestACentroidOutsideThePhysicalRangeProducesNoSample(t *testing.T) {
	for _, text := range []string{"-91", "91", "1000", "-1e6"} {
		row := eveRow{fields: []string{"0000", text}}
		if v, ok := row.angle(1); ok {
			t.Errorf("a centroid of %q was accepted as %g; the disc only spans +/-90 degrees", text, v)
		}
	}
}

func TestARowWhereEveryColumnIsASentinelYieldsNoSamplesButStillATimestamp(t *testing.T) {
	sentinelRow := "1600 " + strings.Repeat("-1.00e+00 ", eveColumns-1)
	tail := []byte("\n" + sentinelRow + "\n")

	ts, _ := serveEVE(t, fixture(t, "eve_l0cs_head.txt"), tail)
	s := newTestSource(t, config.LASP{Enabled: true, EVE: true}, ts.URL, eveWantAt.Add(time.Hour))

	batch, err := s.Poll(context.Background())
	if err != nil {
		t.Fatalf("an all-sentinel row is a real downlink gap, not an error, got: %v", err)
	}
	for _, smp := range batch.Samples {
		if smp.Desc != metric.SourceDataAge {
			t.Errorf("a sample survived an all-sentinel row: %s%v = %g",
				smp.Desc.FullName(), smp.Labels, smp.Value)
		}
	}
	// The age still reflects the row's own time, so a stuck instrument shows up
	// as a rising age rather than as silence.
	age := sampleFor(t, batch, metric.SourceDataAge, Name)
	if age.Value <= 0 {
		t.Errorf("age is %g, want a positive age from the sentinel row's own timestamp", age.Value)
	}
}

// ---------------------------------------------------------------------------
// Newest-row selection
// ---------------------------------------------------------------------------

func TestTheNewestRowWinsRatherThanTheFirstOne(t *testing.T) {
	body := strings.Join([]string{
		"1542 " + strings.Repeat("1.0 ", eveColumns-1),
		"1543 " + strings.Repeat("2.0 ", eveColumns-1),
		"1544 " + strings.Repeat("3.0 ", eveColumns-1),
	}, "\n") + "\n"

	row, err := newestEVERow(body)
	if err != nil {
		t.Fatalf("newestEVERow: %v", err)
	}
	if row.minute != 15*60+44 {
		t.Errorf("chose the row at minute %d, want %d (15:44)", row.minute, 15*60+44)
	}
}

func TestATruncatedFirstLineFromARangeResponseIsDiscardedRatherThanFatal(t *testing.T) {
	body := "01  2.617e-01  2.001e-01  -6.1 -25.1  1.16e+03  7.54e-09\n" +
		"1544 " + strings.Repeat("3.0 ", eveColumns-1) + "\n"

	row, err := newestEVERow(body)
	if err != nil {
		t.Fatalf("a truncated leading line should be skipped, got: %v", err)
	}
	if row.minute != 15*60+44 {
		t.Errorf("chose the row at minute %d, want %d", row.minute, 15*60+44)
	}
}

func TestTheHeaderCommentAndDateLineAreNotMistakenForData(t *testing.T) {
	body := ";END_OF_HEADER\n2026 252 09 09\n" +
		"1544 " + strings.Repeat("3.0 ", eveColumns-1) + "\n"

	row, err := newestEVERow(body)
	if err != nil {
		t.Fatalf("newestEVERow: %v", err)
	}
	if row.minute != 15*60+44 {
		t.Errorf("chose the row at minute %d, want %d", row.minute, 15*60+44)
	}
}

// ---------------------------------------------------------------------------
// The date line
// ---------------------------------------------------------------------------

func TestTheDateComesFromTheHeaderRatherThanTheWallClock(t *testing.T) {
	ts, _ := serveEVE(t, fixture(t, "eve_l0cs_head.txt"), fixture(t, "eve_l0cs_tail.txt"))

	// A wall clock deliberately set to the wrong day. The samples must still be
	// stamped with the date the file itself declares.
	wrongDay := time.Date(2026, 9, 9, 16, 0, 0, 0, time.UTC)
	s := newTestSource(t, config.LASP{Enabled: true, EVE: true}, ts.URL, wrongDay)

	batch, err := s.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	got := sampleFor(t, batch, metric.EUVIrradiance, "17.1nm")
	if !got.Time.Equal(eveWantAt) {
		t.Errorf("the sample is stamped %s, want %s from the file's own date line", got.Time, eveWantAt)
	}
}

func TestTheHeaderIsFetchedOncePerDayRatherThanOncePerMinute(t *testing.T) {
	ts, rec := serveEVE(t, fixture(t, "eve_l0cs_head.txt"), fixture(t, "eve_l0cs_tail.txt"))
	s := newTestSource(t, config.LASP{Enabled: true, EVE: true}, ts.URL, eveWantAt)

	for i := range 5 {
		if _, err := s.Poll(context.Background()); err != nil {
			t.Fatalf("poll %d: %v", i, err)
		}
	}

	if n := rec.headHit.Load(); n != 1 {
		t.Errorf("the header range was fetched %d times over five polls, want 1", n)
	}
	if n := rec.tailHit.Load(); n != 5 {
		t.Errorf("the tail range was fetched %d times over five polls, want 5", n)
	}
}

func TestTheHeaderIsRefetchedWhenTheTimeColumnGoesBackwards(t *testing.T) {
	// A day rollover, seen from the tail alone: HHMM drops from 1544 to 0001.
	// The date must be re-read rather than carried over, and this check must not
	// depend on this process's clock.
	head := fixture(t, "eve_l0cs_head.txt")
	late := []byte("\n1544 " + strings.Repeat("3.0 ", eveColumns-1) + "\n")
	early := []byte("\n0001 " + strings.Repeat("3.0 ", eveColumns-1) + "\n")

	var serveEarly atomic.Bool
	var headHits atomic.Int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hdr := r.Header.Get("Range")
		if strings.HasPrefix(hdr, "bytes=0-") {
			headHits.Add(1)
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(head)
			return
		}
		w.WriteHeader(http.StatusPartialContent)
		if serveEarly.Load() {
			_, _ = w.Write(early)
		} else {
			_, _ = w.Write(late)
		}
	}))
	t.Cleanup(ts.Close)

	s := newTestSource(t, config.LASP{Enabled: true, EVE: true}, ts.URL, eveWantAt)
	if _, err := s.Poll(context.Background()); err != nil {
		t.Fatalf("first Poll: %v", err)
	}
	if n := headHits.Load(); n != 1 {
		t.Fatalf("the header was fetched %d times on the first poll, want 1", n)
	}

	serveEarly.Store(true)
	if _, err := s.Poll(context.Background()); err != nil {
		t.Fatalf("second Poll: %v", err)
	}
	if n := headHits.Load(); n != 2 {
		t.Errorf("the header was fetched %d times after a rollover, want 2", n)
	}
}

func TestTheHeaderRangeRequestIsNotConditional(t *testing.T) {
	// The two ranges share a URL, so replaying the tail's validators on the
	// header request would earn a 304 and no date at all.
	ts, rec := serveEVE(t, fixture(t, "eve_l0cs_head.txt"), fixture(t, "eve_l0cs_tail.txt"))

	var headConditional atomic.Bool
	wrapped := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.Header.Get("Range"), "bytes=0-") &&
			(r.Header.Get("If-None-Match") != "" || r.Header.Get("If-Modified-Since") != "") {
			headConditional.Store(true)
		}
		w.Header().Set("ETag", `"tail"`)
		if strings.HasPrefix(r.Header.Get("Range"), "bytes=0-") {
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(fixture(t, "eve_l0cs_head.txt"))
			return
		}
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(fixture(t, "eve_l0cs_tail.txt"))
	}))
	t.Cleanup(wrapped.Close)
	_ = ts
	_ = rec

	s := newTestSource(t, config.LASP{Enabled: true, EVE: true}, wrapped.URL, eveWantAt)
	// Two polls, so the second has validators cached from the first.
	for i := range 2 {
		if _, err := s.Poll(context.Background()); err != nil {
			t.Fatalf("poll %d: %v", i, err)
		}
	}
	if headConditional.Load() {
		t.Error("the header request replayed the tail's validators, which would earn a 304 and no date")
	}
}

func TestTheDateLineIsCrossCheckedAgainstItsOwnDayOfYear(t *testing.T) {
	// 2026-09-09 is day 252. A file whose two halves disagree has been assembled
	// wrongly, and stamping a day of one-minute samples with the wrong date is
	// not something anybody spots on a graph.
	if _, err := parseEVEDate(";END_OF_HEADER\n2026 100 09 09\n"); err == nil {
		t.Error("a date line whose day-of-year disagrees with its calendar date was accepted")
	}
	got, err := parseEVEDate(";END_OF_HEADER\n2026 252 09 09\n")
	if err != nil {
		t.Fatalf("the correct date line was rejected: %v", err)
	}
	if want := time.Date(2026, 9, 9, 0, 0, 0, 0, time.UTC); !got.Equal(want) {
		t.Errorf("date is %s, want %s", got, want)
	}
}

func TestAMalformedDateLineIsAnErrorRatherThanAGuess(t *testing.T) {
	cases := map[string]string{
		"no header marker":  "2026 252 09 09\n",
		"no date line":      ";END_OF_HEADER\n",
		"three fields":      ";END_OF_HEADER\n2026 252 09\n",
		"non-numeric":       ";END_OF_HEADER\nyear doy mo dd\n",
		"an absurd year":    ";END_OF_HEADER\n1066 252 09 09\n",
		"an absurd month":   ";END_OF_HEADER\n2026 252 13 09\n",
		"a zero day-of-yea": ";END_OF_HEADER\n2026 000 09 09\n",
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			if got, err := parseEVEDate(body); err == nil {
				t.Errorf("%s was accepted as %s; wanted an error", name, got)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// The Range request
// ---------------------------------------------------------------------------

func TestPollAsksForOnlyTheTailOfTheQuicklookFile(t *testing.T) {
	ts, rec := serveEVE(t, fixture(t, "eve_l0cs_head.txt"), fixture(t, "eve_l0cs_tail.txt"))
	s := newTestSource(t, config.LASP{Enabled: true, EVE: true}, ts.URL, eveWantAt)

	if _, err := s.Poll(context.Background()); err != nil {
		t.Fatalf("Poll: %v", err)
	}

	want := fmt.Sprintf("bytes=-%d", eveTailBytes)
	var found bool
	for _, r := range rec.seenRanges() {
		if r == want {
			found = true
		}
	}
	if !found {
		t.Errorf("no request carried %q; without it every minute downloads the whole growing file", want)
	}
}

func TestA206PartialContentBodyIsAcceptedRatherThanTreatedAsAnError(t *testing.T) {
	ts, _ := serveEVE(t, fixture(t, "eve_l0cs_head.txt"), fixture(t, "eve_l0cs_tail.txt"))
	s := newTestSource(t, config.LASP{Enabled: true, EVE: true}, ts.URL, eveWantAt)

	body, err := s.fetchEVETail(context.Background())
	if err != nil {
		t.Fatalf("a 206 response was rejected: %v", err)
	}
	if len(body) == 0 {
		t.Fatal("a 206 response returned an empty body")
	}
}

func TestAResponseLargerThanTheBodyLimitFails(t *testing.T) {
	big := strings.Repeat("1544 "+strings.Repeat("3.0 ", eveColumns-1)+"\n", 2000)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(big))
	}))
	t.Cleanup(ts.Close)

	s := newTestSource(t, config.LASP{Enabled: true, EVE: true, Retries: -1}, ts.URL, eveWantAt)
	if _, err := s.Poll(context.Background()); err == nil {
		t.Fatalf("wanted an error from a %d-byte body over a %d-byte limit, got nil", len(big), eveMaxBody)
	}
}

// ---------------------------------------------------------------------------
// Schedule
// ---------------------------------------------------------------------------

func TestTheEVEScheduleIsOneMinute(t *testing.T) {
	s := newTestSource(t, config.LASP{Enabled: true, EVE: true}, "https://example.invalid", time.Time{})
	sched := s.Schedule()

	at := time.Date(2026, 9, 9, 0, 0, 0, 0, time.UTC)
	for i := range 60 {
		next := sched.NextAfter(at)
		if gap := next.Sub(at); gap != time.Minute {
			t.Fatalf("step %d: gap is %s, want 1m", i, gap)
		}
		at = next
	}
	if want := time.Date(2026, 9, 9, 1, 0, 0, 0, time.UTC); !at.Equal(want) {
		t.Errorf("sixty one-minute polls landed on %s, want %s", at, want)
	}
}

func TestTheSourceIsScheduledRatherThanIntervalOnly(t *testing.T) {
	s := newTestSource(t, config.LASP{Enabled: true, EVE: true}, "https://example.invalid", time.Time{})
	if _, ok := any(s).(source.Scheduled); !ok {
		t.Error("the source does not implement source.Scheduled")
	}
	if s.Schedule() == nil {
		t.Error("Schedule returned nil, which would make the scheduler fall back to Interval")
	}
}

// ---------------------------------------------------------------------------
// Data age
// ---------------------------------------------------------------------------

func TestSourceDataAgeIsMeasuredFromThePayloadTimestamp(t *testing.T) {
	head := fixture(t, "eve_l0cs_head.txt")
	tail := fixture(t, "eve_l0cs_tail.txt")

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// A Last-Modified of right now over a two-hour-old newest row. The EVE
		// file is rewritten every minute whether or not SDO downlinked
		// anything, so this header says nothing about the data.
		w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
		w.WriteHeader(http.StatusPartialContent)
		if strings.HasPrefix(r.Header.Get("Range"), "bytes=0-") {
			_, _ = w.Write(head)
			return
		}
		_, _ = w.Write(tail)
	}))
	t.Cleanup(ts.Close)

	now := eveWantAt.Add(2 * time.Hour)
	s := newTestSource(t, config.LASP{Enabled: true, EVE: true}, ts.URL, now)

	batch, err := s.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	age := sampleFor(t, batch, metric.SourceDataAge, Name)
	if want := (2 * time.Hour).Seconds(); age.Value != want {
		t.Errorf("age is %g seconds, want %g from the row's HHMM and the header's date", age.Value, want)
	}
}

// ---------------------------------------------------------------------------
// HTTP behaviour
// ---------------------------------------------------------------------------

func TestNotModifiedIsASuccessWithNoSamples(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotModified)
	}))
	t.Cleanup(ts.Close)

	s := newTestSource(t, config.LASP{Enabled: true, EVE: true}, ts.URL, eveWantAt)
	batch, err := s.Poll(context.Background())
	if !errors.Is(err, source.ErrNotModified) {
		t.Errorf("Poll returned %v, want source.ErrNotModified", err)
	}
	if len(batch.Samples) != 0 {
		t.Errorf("a 304 produced samples: %s", describe(batch))
	}
}

func TestNotFoundIsNotRetried(t *testing.T) {
	var requests atomic.Int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(ts.Close)

	// The commonly cited EVE paths all answer 404, so this is what a wrong path
	// looks like rather than a hypothetical.
	s := newTestSource(t, config.LASP{Enabled: true, EVE: true, Retries: 5}, ts.URL, eveWantAt)
	if _, err := s.Poll(context.Background()); err == nil {
		t.Fatal("wanted an error from a 404, got nil")
	}
	if n := requests.Load(); n != 1 {
		t.Errorf("a 404 was requested %d times, want 1", n)
	}
}

func TestTooManyRequestsReturnsErrRateLimited(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "30")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	t.Cleanup(ts.Close)

	s := newTestSource(t, config.LASP{Enabled: true, EVE: true}, ts.URL, eveWantAt)
	batch, err := s.Poll(context.Background())
	if !errors.Is(err, source.ErrRateLimited) {
		t.Errorf("Poll returned %v, want it to wrap source.ErrRateLimited", err)
	}
	if len(batch.Samples) != 0 {
		t.Errorf("a 429 produced samples: %s", describe(batch))
	}
}

func TestPollHonoursContextCancellation(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	t.Cleanup(ts.Close)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	s := newTestSource(t, config.LASP{Enabled: true, EVE: true}, ts.URL, eveWantAt)
	if _, err := s.Poll(ctx); err == nil {
		t.Fatal("wanted an error from a cancelled context, got nil")
	}
}

// ---------------------------------------------------------------------------
// Malformed input
// ---------------------------------------------------------------------------

func TestMalformedEVEInputReturnsAnErrorRatherThanPanicking(t *testing.T) {
	cases := map[string]string{
		"an empty body":               "",
		"header only":                 ";END_OF_HEADER\n",
		"HTML from an error page":     "<html><body>Not found</body></html>",
		"rows with too few columns":   "\n1544  4.29e-07  3.75e-09\n",
		"a non-numeric time column":   "\nHHMM " + strings.Repeat("3.0 ", eveColumns-1) + "\n",
		"an out-of-range hour":        "\n2544 " + strings.Repeat("3.0 ", eveColumns-1) + "\n",
		"an out-of-range minute":      "\n1099 " + strings.Repeat("3.0 ", eveColumns-1) + "\n",
		"a three-digit time column":   "\n154 " + strings.Repeat("3.0 ", eveColumns-1) + "\n",
		"a row of nothing but commas": "\n,,,,,,,,,,,,,,,,,,,,\n",
	}

	for name, tail := range cases {
		t.Run(name, func(t *testing.T) {
			ts, _ := serveEVE(t, fixture(t, "eve_l0cs_head.txt"), []byte(tail))
			s := newTestSource(t, config.LASP{Enabled: true, EVE: true, Retries: -1}, ts.URL, eveWantAt)

			batch, err := s.Poll(context.Background())
			if err == nil {
				t.Fatalf("wanted an error, got %s", describe(batch))
			}
			if len(batch.Samples) != 0 {
				t.Errorf("a malformed body produced samples: %s", describe(batch))
			}
		})
	}
}

func TestOneGarbledColumnDoesNotDiscardTheRestOfTheRow(t *testing.T) {
	// The published columns come from two independent instruments. One garbled
	// field should not lose a good EUV centroid.
	fields := strings.Fields("1544  4.29e-07  3.75e-09  1.56e-04  1.916e-04  garbled  3.446e-04  7.029e-04 -1.000e+00  1.94e+02  6.7982e-03  1.52e+02  3.361e-01  2.021e-01  2.617e-01  2.001e-01  -6.3 -25.0  1.16e+03  7.77e-09")
	row := eveRow{minute: 944, fields: fields}

	if _, ok := row.value(col171ESP); ok {
		t.Error("the garbled 17.1 nm column was accepted")
	}
	if v, ok := row.value(col257ESP); !ok || v != 3.446e-04 {
		t.Errorf("the 25.7 nm column is (%g, %t), want (3.446e-04, true)", v, ok)
	}
	if v, ok := row.angle(colCMLat); !ok || v != -6.3 {
		t.Errorf("CMLat is (%g, %t), want (-6.3, true)", v, ok)
	}
}

func TestParseHHMMRejectsAnythingThatIsNotFourDigits(t *testing.T) {
	for _, bad := range []string{"", "1", "123", "12345", "abcd", "24 0", "2400", "1060"} {
		if got, err := parseHHMM(bad); err == nil {
			t.Errorf("parseHHMM(%q) returned %d, want an error", bad, got)
		}
	}
	if got, err := parseHHMM("1544"); err != nil || got != 15*60+44 {
		t.Errorf("parseHHMM(\"1544\") = (%d, %v), want (%d, nil)", got, err, 15*60+44)
	}
}
