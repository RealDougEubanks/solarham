package kiwisdr

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/RealDougEubanks/solarham/internal/config"
	"github.com/RealDougEubanks/solarham/internal/httpx"
	"github.com/RealDougEubanks/solarham/internal/metric"
	"github.com/RealDougEubanks/solarham/internal/source"
)

// referenceNow is the instant every test pretends it is: thirty minutes after
// the newest record in the captured fixture, which is stamped
// "Thu Sep 10 00:43:05 2026".
var referenceNow = time.Date(2026, 9, 10, 1, 13, 5, 0, time.UTC)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// testClient is the shared client with its politeness spacing wound down. A
// KiwiSDR host is unlisted in the policy table and so falls under the
// thirty-second fallback in production, which a test cannot wait for.
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

// newReceiver starts a fake receiver serving /snr from a handler, and returns
// its base URL plus a hit counter.
func newReceiver(t *testing.T, handler http.HandlerFunc) (string, *atomic.Int64) {
	t.Helper()
	var hits atomic.Int64
	mux := http.NewServeMux()
	mux.HandleFunc(pathSNR, func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		handler(w, r)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv.URL, &hits
}

// serveFixture answers with a fixture as text/plain, which is what a real
// receiver does despite the body being JSON.
func serveFixture(t *testing.T, name string) http.HandlerFunc {
	t.Helper()
	body := fixture(t, name)
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write(body)
	}
}

func serveBody(body string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = io.WriteString(w, body)
	}
}

func newSource(t *testing.T, receivers map[string]string) *Source {
	t.Helper()
	src, err := New(config.KiwiSDR{
		Enabled:   true,
		Receivers: receivers,
		Timeout:   3 * time.Second,
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

func count(b metric.Batch, desc *metric.Descriptor) int {
	n := 0
	for _, s := range b.Samples {
		if s.Desc == desc {
			n++
		}
	}
	return n
}

func TestNewRefusesToRunWithNoReceiversConfigured(t *testing.T) {
	if _, err := New(config.KiwiSDR{Enabled: true}, testClient(t), testLogger()); err == nil {
		t.Error("New accepted an empty receiver list")
	}
	if _, err := New(config.KiwiSDR{
		Receivers: map[string]string{"home": "http://localhost:8073"},
	}, nil, testLogger()); err == nil {
		t.Error("New accepted a nil client")
	}
}

func TestNewRejectsAReceiverWithAnUnusableBaseURL(t *testing.T) {
	bad := map[string]string{
		"empty":     "",
		"no scheme": "192.168.1.50:8073",
		"ftp":       "ftp://192.168.1.50",
		"no host":   "http://",
	}
	for label, raw := range bad {
		_, err := New(config.KiwiSDR{
			Receivers: map[string]string{"rx": raw},
		}, testClient(t), testLogger())
		if err == nil {
			t.Errorf("New accepted a %s receiver URL %q", label, raw)
		}
	}
	if _, err := New(config.KiwiSDR{
		Receivers: map[string]string{"  ": "http://localhost:8073"},
	}, testClient(t), testLogger()); err == nil {
		t.Error("New accepted a receiver with a blank name")
	}
}

func TestNewDefaultsToHalfTheHourlyIntegrationLength(t *testing.T) {
	src, err := New(config.KiwiSDR{
		Receivers: map[string]string{"home": "http://localhost:8073"},
	}, testClient(t), testLogger())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if src.Interval() != 30*time.Minute {
		t.Errorf("default interval = %v, want 30m against hourly integrations", src.Interval())
	}
	if src.timeout != defaultTimeout || src.timeout > 15*time.Second {
		t.Errorf("default timeout = %v, want something short for a small machine", src.timeout)
	}
	if src.Name() != Name || Name != "kiwisdr" {
		t.Errorf("Name = %q, want kiwisdr", src.Name())
	}
	if src.Schedule().Interval() != 30*time.Minute {
		t.Errorf("schedule interval = %v, want 30m", src.Schedule().Interval())
	}
}

func TestPollParsesTheCapturedFixtureIntoExactNoiseFloorValues(t *testing.T) {
	base, _ := newReceiver(t, serveFixture(t, "snr_rolling.json"))
	src := newSource(t, map[string]string{"home": base})

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}

	// The newest record in the capture, "Thu Sep 10 00:43:05 2026".
	want := map[string]struct{ p50, p95, snr float64 }{
		"0_30000":     {-105, -64, 41},
		"1800_30000":  {-106, -70, 36},
		"0_1800":      {-64, -42, 22},
		"1800_10000":  {-89, -62, 27},
		"10000_20000": {-105, -81, 24},
		"20000_30000": {-107, -106, 1},
	}
	for band, w := range want {
		if got := find(t, batch, metric.NoiseFloor, "home", band).Value; got != w.p50 {
			t.Errorf("noise floor %s = %v dBm, want %v", band, got, w.p50)
		}
		if got := find(t, batch, metric.NoiseFloorPeak, "home", band).Value; got != w.p95 {
			t.Errorf("noise floor peak %s = %v dBm, want %v", band, got, w.p95)
		}
		if got := find(t, batch, metric.BandSignalToNoise, "home", band).Value; got != w.snr {
			t.Errorf("band SNR %s = %v dB, want %v", band, got, w.snr)
		}
	}
	if got := count(batch, metric.NoiseFloor); got != 6 {
		t.Errorf("published %d noise floor samples, want 6 sub-bands", got)
	}

	// Every sample carries the record's own timestamp.
	at := time.Date(2026, 9, 10, 0, 43, 5, 0, time.UTC)
	for _, s := range batch.Samples {
		if s.Desc == metric.SourceDataAge {
			continue
		}
		if !s.Time.Equal(at) {
			t.Errorf("%s%v stamped %v, want the record time %v",
				s.Desc.FullName(), s.Labels, s.Time.UTC(), at)
		}
	}
}

func TestTheNewestRecordIsSelectedByTimestampRatherThanArrayPosition(t *testing.T) {
	// The out-of-order fixture puts "Thu Sep 10 00:43:05 2026" first and two
	// older records after it. A source taking the last element would publish
	// the 23:43 record; one taking the first would happen to be right here but
	// wrong against the real rolling array, where the newest record sits in the
	// middle once seq has wrapped.
	base, _ := newReceiver(t, serveFixture(t, "snr_out_of_order.json"))
	src := newSource(t, map[string]string{"home": base})

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	at := time.Date(2026, 9, 10, 0, 43, 5, 0, time.UTC)
	s := find(t, batch, metric.NoiseFloor, "home", "0_30000")
	if !s.Time.Equal(at) {
		t.Errorf("selected the record stamped %v, want the newest %v", s.Time.UTC(), at)
	}
	if s.Value != -105 {
		t.Errorf("noise floor = %v, want -105 from the newest record", s.Value)
	}
}

func TestASingleDigitDayTimestampWithItsDoubleSpaceIsParsed(t *testing.T) {
	// "Mon Sep  7 13:36:58 2026" — two spaces, because asctime pads the day.
	// A layout with a literal single space fails for the first nine days of
	// every month, which is a bug that hides for three weeks at a time.
	cases := []struct {
		ts   string
		want time.Time
	}{
		{"Mon Sep  7 13:36:58 2026", time.Date(2026, 9, 7, 13, 36, 58, 0, time.UTC)},
		{"Wed Sep  9 22:42:55 2026", time.Date(2026, 9, 9, 22, 42, 55, 0, time.UTC)},
		{"Thu Sep 10 00:43:05 2026", time.Date(2026, 9, 10, 0, 43, 5, 0, time.UTC)},
		{"Thu Jan  1 00:00:00 2026", time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)},
	}
	for _, tc := range cases {
		got, err := parseTS(tc.ts)
		if err != nil {
			t.Errorf("parseTS(%q): %v", tc.ts, err)
			continue
		}
		if !got.Equal(tc.want) {
			t.Errorf("parseTS(%q) = %v, want %v", tc.ts, got, tc.want)
		}
		if got.Location() != time.UTC {
			t.Errorf("parseTS(%q) location = %v, want UTC", tc.ts, got.Location())
		}
	}

	// And end to end, from a fixture whose only record has a single-digit day.
	base, _ := newReceiver(t, serveFixture(t, "snr_single_digit_day.json"))
	src := newSource(t, map[string]string{"home": base})
	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	s := find(t, batch, metric.NoiseFloor, "home", "0_1800")
	if want := time.Date(2026, 9, 7, 13, 36, 58, 0, time.UTC); !s.Time.Equal(want) {
		t.Errorf("sample time = %v, want %v", s.Time.UTC(), want)
	}
	if s.Value != -86 {
		t.Errorf("noise floor 0_1800 = %v, want -86", s.Value)
	}
}

func TestBandLabelsAreDerivedFromTheFrequencyBoundsNotTheArrayOrder(t *testing.T) {
	cases := []struct {
		lo, hi float64
		want   string
		ok     bool
	}{
		{0, 30000, "0_30000", true},
		{1800, 30000, "1800_30000", true},
		{0, 1800, "0_1800", true},
		{1800, 10000, "1800_10000", true},
		{10000, 20000, "10000_20000", true},
		{20000, 30000, "20000_30000", true},
		{1800.5, 10000, "1800.5_10000", true},
		{30000, 30000, "", false},
		{30000, 0, "", false},
	}
	for _, tc := range cases {
		got, ok := bandLabel(tc.lo, tc.hi)
		if ok != tc.ok {
			t.Errorf("bandLabel(%v, %v) ok = %v, want %v", tc.lo, tc.hi, ok, tc.ok)
			continue
		}
		if got != tc.want {
			t.Errorf("bandLabel(%v, %v) = %q, want %q", tc.lo, tc.hi, got, tc.want)
		}
	}
}

func TestBandLabelsSurviveTheReceiverReorderingItsSubBands(t *testing.T) {
	// Firmware has changed the order of these entries before. A label derived
	// from array position would re-point every series at a different slice of
	// spectrum after such a change, without anything failing.
	var records []map[string]any
	if err := json.Unmarshal(fixture(t, "snr_single_digit_day.json"), &records); err != nil {
		t.Fatalf("decoding fixture: %v", err)
	}
	bands, _ := records[0]["snr"].([]any)
	if len(bands) != 6 {
		t.Fatalf("fixture has %d sub-bands, want 6", len(bands))
	}
	reversed := make([]any, 0, len(bands))
	for i := len(bands) - 1; i >= 0; i-- {
		reversed = append(reversed, bands[i])
	}

	labelValues := func(order []any) map[string]float64 {
		records[0]["snr"] = order
		body, err := json.Marshal(records)
		if err != nil {
			t.Fatalf("encoding: %v", err)
		}
		base, _ := newReceiver(t, serveBody(string(body)))
		src := newSource(t, map[string]string{"home": base})
		batch, err := src.Poll(context.Background())
		if err != nil {
			t.Fatalf("Poll: %v", err)
		}
		out := make(map[string]float64)
		for _, s := range batch.Samples {
			if s.Desc == metric.NoiseFloor {
				out[s.Labels[1]] = s.Value
			}
		}
		return out
	}

	forward := labelValues(bands)
	backward := labelValues(reversed)

	if len(forward) != 6 {
		t.Fatalf("got %d bands, want 6", len(forward))
	}
	for band, want := range forward {
		if got, ok := backward[band]; !ok || got != want {
			t.Errorf("band %s = %v after reordering, want %v", band, got, want)
		}
	}

	// And the labels themselves are the documented scheme.
	labels := make([]string, 0, len(forward))
	for band := range forward {
		labels = append(labels, band)
	}
	sort.Strings(labels)
	want := "0_1800,0_30000,10000_20000,1800_10000,1800_30000,20000_30000"
	if got := strings.Join(labels, ","); got != want {
		t.Errorf("band labels = %q, want %q", got, want)
	}
}

func TestSourceDataAgeComesFromTheRecordTimestamp(t *testing.T) {
	base, _ := newReceiver(t, serveFixture(t, "snr_rolling.json"))
	src := newSource(t, map[string]string{"home": base})

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	// referenceNow is thirty minutes after the newest record.
	if got, want := find(t, batch, metric.SourceDataAge, Name).Value, (30 * time.Minute).Seconds(); got != want {
		t.Errorf("source data age = %v s, want %v s", got, want)
	}
}

func TestOneReceiverDownWhileAnotherSucceedsStillPublishes(t *testing.T) {
	up, upHits := newReceiver(t, serveFixture(t, "snr_rolling.json"))
	down, _ := newReceiver(t, func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "rebooting", http.StatusServiceUnavailable)
	})
	src := newSource(t, map[string]string{"shack": up, "remote": down})

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll returned an error though one receiver answered: %v", err)
	}
	if got := count(batch, metric.NoiseFloor); got != 6 {
		t.Errorf("published %d noise floor samples, want the healthy receiver's 6", got)
	}
	for _, s := range batch.Samples {
		if s.Desc == metric.SourceDataAge {
			continue
		}
		if s.Labels[0] != "shack" {
			t.Errorf("published %s for receiver %q, which was down", s.Desc.FullName(), s.Labels[0])
		}
	}
	if upHits.Load() == 0 {
		t.Error("the healthy receiver was never asked")
	}
	if _, ok := lookupAge(batch); !ok {
		t.Error("no source data age published from the surviving receiver")
	}
}

func lookupAge(b metric.Batch) (metric.Sample, bool) {
	for _, s := range b.Samples {
		if s.Desc == metric.SourceDataAge {
			return s, true
		}
	}
	return metric.Sample{}, false
}

func TestEveryReceiverDownReturnsAnError(t *testing.T) {
	down := func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "rebooting", http.StatusServiceUnavailable)
	}
	a, _ := newReceiver(t, down)
	b, _ := newReceiver(t, down)
	src := newSource(t, map[string]string{"shack": a, "remote": b})

	batch, err := src.Poll(context.Background())
	if err == nil {
		t.Fatal("Poll succeeded with every receiver down")
	}
	if len(batch.Samples) != 0 {
		t.Errorf("failed poll carried %d samples", len(batch.Samples))
	}
	// Both receivers are named, so an operator can tell which are missing.
	for _, name := range []string{"shack", "remote"} {
		if !strings.Contains(err.Error(), name) {
			t.Errorf("error %q does not name receiver %q", err, name)
		}
	}
}

func TestReceiversAreFetchedConcurrentlyWithinABound(t *testing.T) {
	var (
		inFlight atomic.Int64
		peak     atomic.Int64
	)
	handler := func(w http.ResponseWriter, r *http.Request) {
		n := inFlight.Add(1)
		for {
			old := peak.Load()
			if n <= old || peak.CompareAndSwap(old, n) {
				break
			}
		}
		time.Sleep(30 * time.Millisecond)
		inFlight.Add(-1)
		serveFixture(t, "snr_single_digit_day.json")(w, r)
	}

	receivers := make(map[string]string, 8)
	for i := range 8 {
		base, _ := newReceiver(t, handler)
		receivers["rx"+string(rune('a'+i))] = base
	}
	src := newSource(t, receivers)

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if got := count(batch, metric.NoiseFloor); got != 8*6 {
		t.Errorf("published %d noise floor samples, want %d", got, 8*6)
	}
	if got := peak.Load(); got < 2 {
		t.Errorf("peak concurrency was %d; receivers were fetched serially", got)
	}
	if got := peak.Load(); got > maxConcurrent {
		t.Errorf("peak concurrency was %d, above the bound of %d", got, maxConcurrent)
	}
}

func TestA404IsNotRetried(t *testing.T) {
	base, hits := newReceiver(t, func(w http.ResponseWriter, _ *http.Request) {
		http.NotFound(w, nil)
	})
	src, err := New(config.KiwiSDR{
		Receivers: map[string]string{"home": base},
		Retries:   3,
		Timeout:   3 * time.Second,
	}, testClient(t), testLogger())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	src.now = func() time.Time { return referenceNow }

	if _, err := src.Poll(context.Background()); err == nil {
		t.Fatal("Poll succeeded against a 404")
	}
	if got := hits.Load(); got != 1 {
		t.Errorf("requested /snr %d times for a 404 with three retries configured, want 1", got)
	}
}

func TestA429IsReportedAsRateLimited(t *testing.T) {
	limited := func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "1")
		w.WriteHeader(http.StatusTooManyRequests)
	}
	base, _ := newReceiver(t, limited)
	src := newSource(t, map[string]string{"home": base})

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

func TestMalformedJSONReturnsAnErrorRatherThanPanicking(t *testing.T) {
	bodies := []string{
		`[{"ts":`,
		`not json`,
		`[]`,
		`{}`,
		`[{"ts":"Mon Sep  7 13:36:58 2026","snr":[]}]`,
		`[{"ts":"whenever","snr":[{"lo":0,"hi":30000,"p50":-96}]}]`,
		`[{"ts":"Mon Sep  7 13:36:58 2026","snr":[{"lo":30000,"hi":0,"p50":-96}]}]`,
		``,
	}
	for _, body := range bodies {
		base, _ := newReceiver(t, serveBody(body))
		src := newSource(t, map[string]string{"home": base})

		batch, err := src.Poll(context.Background())
		if err == nil {
			t.Errorf("body %q was accepted, producing %d samples", body, len(batch.Samples))
		}
		if len(batch.Samples) != 0 {
			t.Errorf("body %q produced %d samples", body, len(batch.Samples))
		}
	}
}

func TestOneReceiverServingRubbishDoesNotCostAHealthyOne(t *testing.T) {
	good, _ := newReceiver(t, serveFixture(t, "snr_rolling.json"))
	bad, _ := newReceiver(t, serveBody(`[{"ts": broken`))
	src := newSource(t, map[string]string{"shack": good, "attic": bad})

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if got := count(batch, metric.NoiseFloor); got != 6 {
		t.Errorf("published %d noise floor samples, want 6", got)
	}
}

func TestACancelledContextStopsThePollWithoutPublishing(t *testing.T) {
	base, hits := newReceiver(t, serveFixture(t, "snr_rolling.json"))
	src := newSource(t, map[string]string{"home": base})

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
	if hits.Load() != 0 {
		t.Errorf("made %d requests after cancellation", hits.Load())
	}
}

func TestACancelledContextMidPollAbandonsTheRemainingReceivers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var served atomic.Int64

	handler := func(w http.ResponseWriter, r *http.Request) {
		if served.Add(1) == 1 {
			cancel()
			time.Sleep(20 * time.Millisecond)
		}
		serveFixture(t, "snr_single_digit_day.json")(w, r)
	}
	receivers := make(map[string]string, 8)
	for i := range 8 {
		base, _ := newReceiver(t, handler)
		receivers["rx"+string(rune('a'+i))] = base
	}
	src := newSource(t, receivers)

	if _, err := src.Poll(ctx); err == nil {
		t.Fatal("Poll succeeded after cancellation")
	}
	if got := served.Load(); got > maxConcurrent {
		t.Errorf("served %d receivers after cancellation, want no more than the %d already in flight",
			got, maxConcurrent)
	}
}

func TestOversizedResponsesAreRefusedRatherThanRead(t *testing.T) {
	huge := strings.Repeat("x", maxBodyBytes+1024)
	base, _ := newReceiver(t, serveBody(huge))
	src := newSource(t, map[string]string{"home": base})

	if _, err := src.Poll(context.Background()); err == nil {
		t.Fatal("Poll accepted an oversized body")
	}
}

func TestReceiverOrderIsStableAcrossPolls(t *testing.T) {
	a, _ := newReceiver(t, serveFixture(t, "snr_single_digit_day.json"))
	b, _ := newReceiver(t, serveFixture(t, "snr_single_digit_day.json"))
	c, _ := newReceiver(t, serveFixture(t, "snr_single_digit_day.json"))
	src := newSource(t, map[string]string{"zulu": a, "alpha": b, "mike": c})

	if got := []string{src.receivers[0].name, src.receivers[1].name, src.receivers[2].name}; got[0] != "alpha" || got[1] != "mike" || got[2] != "zulu" {
		t.Errorf("receiver order = %v, want alpha, mike, zulu", got)
	}

	var first string
	for poll := range 3 {
		batch, err := src.Poll(context.Background())
		if err != nil {
			t.Fatalf("poll %d: %v", poll, err)
		}
		keys := make([]string, 0, len(batch.Samples))
		for _, s := range batch.Samples {
			keys = append(keys, s.Key())
		}
		joined := strings.Join(keys, "|")
		if poll == 0 {
			first = joined
			continue
		}
		if joined != first {
			t.Errorf("poll %d produced a different sample order; map iteration is leaking through", poll)
		}
	}
}
