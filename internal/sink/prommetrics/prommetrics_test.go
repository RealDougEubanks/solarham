package prommetrics

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/RealDougEubanks/solarham/internal/config"
	"github.com/RealDougEubanks/solarham/internal/metric"
	"github.com/RealDougEubanks/solarham/internal/sink"
	"github.com/RealDougEubanks/solarham/internal/source"
)

// observed is the observation time every test sample carries. It is fixed so
// that the store's out-of-order rule never surprises a test.
var observed = time.Unix(1700000000, 0).UTC()

// newTestSink builds a sink over a store with the given TTL. A TTL of zero
// disables expiry, which is what every test but the expiry one wants.
func newTestSink(t *testing.T, ttl time.Duration) (*Sink, *metric.Store) {
	t.Helper()

	store := metric.NewStore(ttl)
	s, err := New(config.Prometheus{Enabled: true, Path: "/metrics"}, store, discardLogger())
	if err != nil {
		t.Fatalf("New returned %v, want nil", err)
	}
	return s, store
}

// discardLogger keeps the deliberately-provoked error logs out of test output.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// sample builds one observation of a descriptor.
func sample(d *metric.Descriptor, value float64, labels ...string) metric.Sample {
	return metric.Sample{Desc: d, Labels: labels, Value: value, Time: observed}
}

// batch wraps samples as one poll's output.
func batch(samples ...metric.Sample) metric.Batch {
	return metric.Batch{Source: "test", Fetched: observed, Samples: samples}
}

// publish sends a batch and fails the test if the sink rejected it.
func publish(t *testing.T, s *Sink, b metric.Batch) {
	t.Helper()
	if err := s.Publish(context.Background(), b); err != nil {
		t.Fatalf("Publish returned %v, want nil", err)
	}
}

// gather flattens the registry to "name{label=\"value\",...}" against a value.
//
// A gauge and a counter contribute their value; a histogram contributes its
// sample count, since what an assertion cares about is that an observation was
// recorded against the right source or sink.
//
// The gather error is returned rather than fatal, because one test deliberately
// provokes it and still expects every other series to be intact.
func gather(t *testing.T, s *Sink) (map[string]float64, error) {
	t.Helper()

	families, err := s.Registry().Gather()
	out := make(map[string]float64)
	for _, family := range families {
		for _, m := range family.GetMetric() {
			parts := make([]string, 0, len(m.GetLabel()))
			for _, l := range m.GetLabel() {
				parts = append(parts, fmt.Sprintf("%s=%q", l.GetName(), l.GetValue()))
			}
			sort.Strings(parts)

			key := family.GetName()
			if len(parts) > 0 {
				key += "{" + strings.Join(parts, ",") + "}"
			}

			switch {
			case m.GetGauge() != nil:
				out[key] = m.GetGauge().GetValue()
			case m.GetCounter() != nil:
				out[key] = m.GetCounter().GetValue()
			case m.GetHistogram() != nil:
				out[key] = float64(m.GetHistogram().GetSampleCount())
			default:
				t.Fatalf("metric %s has an unexpected type", key)
			}
		}
	}
	return out, err
}

// mustGather gathers and fails on any collection error.
func mustGather(t *testing.T, s *Sink) map[string]float64 {
	t.Helper()
	got, err := gather(t, s)
	if err != nil {
		t.Fatalf("Gather returned %v, want nil", err)
	}
	return got
}

func assertSeries(t *testing.T, got map[string]float64, key string, want float64) {
	t.Helper()
	v, ok := got[key]
	if !ok {
		t.Errorf("series %s is absent, want value %v", key, want)
		return
	}
	if v != want {
		t.Errorf("series %s is %v, want %v", key, v, want)
	}
}

// assertAbsent checks no series exists with the given name, whatever its
// labels.
func assertAbsent(t *testing.T, got map[string]float64, name string) {
	t.Helper()
	for key := range got {
		if key == name || strings.HasPrefix(key, name+"{") {
			t.Errorf("series %s is present with value %v, want absent", key, got[key])
		}
	}
}

// scrape fetches the exposition text through a real HTTP server.
func scrape(t *testing.T, s *Sink) (int, string) {
	t.Helper()

	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL + s.Path())
	if err != nil {
		t.Fatalf("GET returned %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading body returned %v", err)
	}
	return resp.StatusCode, string(body)
}

func TestAPublishedSampleAppearsWithItsNameHelpLabelsAndValue(t *testing.T) {
	s, _ := newTestSink(t, 0)

	publish(t, s, batch(
		sample(metric.FluxSFU, 172.4),
		sample(metric.KIndex, 3, "boulder"),
	))

	got := mustGather(t, s)
	assertSeries(t, got, "solar_flux_sfu", 172.4)
	assertSeries(t, got, `solar_k_index{station="boulder"}`, 3)

	status, page := scrape(t, s)
	if status != http.StatusOK {
		t.Fatalf("status %d, want %d", status, http.StatusOK)
	}
	for _, line := range []string{
		"# HELP solar_flux_sfu " + metric.FluxSFU.Help,
		"# TYPE solar_flux_sfu gauge",
		"solar_flux_sfu 172.4",
		`solar_k_index{station="boulder"} 3`,
	} {
		if !strings.Contains(page, line) {
			t.Errorf("scrape output is missing %q\n%s", line, page)
		}
	}
}

func TestNothingIsExposedBeforeTheFirstPublish(t *testing.T) {
	s, _ := newTestSink(t, 0)

	got := mustGather(t, s)
	for _, d := range metric.All {
		assertAbsent(t, got, d.FullName())
	}
}

// This is the reason the sink is a custom collector rather than a set of
// registered gauges. A value the upstream has stopped supplying must leave the
// scrape output entirely; a stale solar wind speed presented as current is
// worse than no reading at all.
func TestAnExpiredSampleDisappearsFromMetrics(t *testing.T) {
	const ttl = 30 * time.Millisecond
	s, _ := newTestSink(t, ttl)

	publish(t, s, batch(sample(metric.WindSpeed, 421)))
	assertSeries(t, mustGather(t, s), "solar_wind_speed_kilometers_per_second", 421)

	time.Sleep(3 * ttl)

	assertAbsent(t, mustGather(t, s), "solar_wind_speed_kilometers_per_second")

	_, page := scrape(t, s)
	if strings.Contains(page, "solar_wind_speed_kilometers_per_second") {
		t.Errorf("scrape output still carries an expired sample\n%s", page)
	}
}

func TestASupersededSampleDoesNotLingerAlongsideItsReplacement(t *testing.T) {
	s, _ := newTestSink(t, 0)

	publish(t, s, batch(sample(metric.KIndexEstimated, 2)))
	newer := metric.Sample{
		Desc:  metric.KIndexEstimated,
		Value: 6,
		Time:  observed.Add(time.Minute),
	}
	publish(t, s, batch(newer))

	got := mustGather(t, s)
	assertSeries(t, got, "solar_k_index_estimated", 6)
	if len(got) == 0 {
		t.Fatal("nothing was gathered")
	}
}

func TestAnInfoSampleRendersAsOneWithItsCategoryLabel(t *testing.T) {
	s, _ := newTestSink(t, 0)

	publish(t, s, batch(
		sample(metric.XRayClassInfo, 1, "M1.2"),
		sample(metric.GeomagneticFieldInfo, 1, "STORM"),
	))

	got := mustGather(t, s)
	assertSeries(t, got, `solar_xray_class_info{class="M1.2"}`, 1)
	assertSeries(t, got, `solar_geomagnetic_field_info{state="STORM"}`, 1)

	_, page := scrape(t, s)
	// An info metric is a gauge whose only information is in its labels, so
	// the type line matters as much as the value.
	for _, line := range []string{
		"# TYPE solar_xray_class_info gauge",
		`solar_xray_class_info{class="M1.2"} 1`,
	} {
		if !strings.Contains(page, line) {
			t.Errorf("scrape output is missing %q\n%s", line, page)
		}
	}
}

// The sample carries label values positionally, so a collector that paired them
// with the wrong names would still produce a plausible-looking series. This
// checks each value landed under the name the descriptor gives its position.
func TestLabelValuesFollowTheDescriptorsLabelOrder(t *testing.T) {
	s, _ := newTestSink(t, 0)

	values := []string{"20m", "day", "Good"}
	publish(t, s, batch(sample(metric.BandConditionInfo, 1, values...)))

	families, err := s.Registry().Gather()
	if err != nil {
		t.Fatalf("Gather returned %v, want nil", err)
	}

	found := false
	for _, family := range families {
		if family.GetName() != metric.BandConditionInfo.FullName() {
			continue
		}
		found = true
		for _, m := range family.GetMetric() {
			byName := make(map[string]string, len(m.GetLabel()))
			for _, l := range m.GetLabel() {
				byName[l.GetName()] = l.GetValue()
			}
			for i, name := range metric.BandConditionInfo.Labels {
				if byName[name] != values[i] {
					t.Errorf("label %q is %q, want %q", name, byName[name], values[i])
				}
			}
		}
	}
	if !found {
		t.Fatalf("family %s is absent", metric.BandConditionInfo.FullName())
	}
}

// A sample the store accepted but Prometheus cannot encode — here a descriptor
// that names the same label twice — must cost only its own series. An operator
// watching a geomagnetic storm should not lose the whole scrape because one
// reading was malformed.
func TestAnInvalidSampleDoesNotBreakTheRestOfTheScrape(t *testing.T) {
	s, _ := newTestSink(t, 0)

	bad := &metric.Descriptor{
		Name:   "malformed_metric",
		Help:   "Deliberately malformed: a duplicated label name.",
		Kind:   metric.KindGauge,
		Labels: []string{"station", "station"},
	}
	publish(t, s, batch(sample(bad, 1, "boulder", "fredericksburg"), sample(metric.SunspotNumber, 88)))

	got, err := gather(t, s)
	if err == nil {
		t.Error("Gather returned nil, want an error naming the invalid metric")
	}
	assertSeries(t, got, "solar_sunspot_number", 88)

	// ContinueOnError means the endpoint still serves the good metrics rather
	// than 500ing the entire scrape.
	status, page := scrape(t, s)
	if status != http.StatusOK {
		t.Errorf("status %d, want %d\n%s", status, http.StatusOK, page)
	}
	if !strings.Contains(page, "solar_sunspot_number 88") {
		t.Errorf("scrape output is missing the valid metric\n%s", page)
	}
}

func TestAnEmptyBatchIsSkippedRatherThanPublished(t *testing.T) {
	s, _ := newTestSink(t, 0)

	err := s.Publish(context.Background(), batch())
	if !errors.Is(err, sink.ErrSkipped) {
		t.Errorf("Publish of an empty batch returned %v, want sink.ErrSkipped", err)
	}
}

func TestObservePublishCountsEachOutcomeUnderItsOwnResult(t *testing.T) {
	s, _ := newTestSink(t, 0)

	var observer sink.Observer = s // the fan-out only ever holds the interface
	observer.ObservePublish("influx2", 12*time.Millisecond, nil)
	observer.ObservePublish("influx2", 9*time.Millisecond, nil)
	observer.ObservePublish("influx2", 30*time.Millisecond, errors.New("connection refused"))
	observer.ObservePublish("mqtt", time.Millisecond, fmt.Errorf("nothing to send: %w", sink.ErrSkipped))

	got := mustGather(t, s)
	assertSeries(t, got, `solar_sink_publish_total{result="success",sink="influx2"}`, 2)
	assertSeries(t, got, `solar_sink_publish_total{result="failure",sink="influx2"}`, 1)
	assertSeries(t, got, `solar_sink_publish_total{result="skipped",sink="mqtt"}`, 1)

	// A skipped publish must never be counted as a failure: a sink with
	// nothing to send behaved correctly.
	if _, ok := got[`solar_sink_publish_total{result="failure",sink="mqtt"}`]; ok {
		t.Error("a skipped publish was counted as a failure")
	}

	assertSeries(t, got, `solar_sink_publish_duration_seconds{sink="influx2"}`, 3)
	assertSeries(t, got, `solar_sink_publish_duration_seconds{sink="mqtt"}`, 1)
}

func TestObservePollCountsEachOutcomeUnderItsOwnResult(t *testing.T) {
	s, _ := newTestSink(t, 0)

	var observer source.Observer = s // the scheduler only ever holds the interface
	observer.ObservePoll("swpc", 40*time.Millisecond, 12, nil)
	observer.ObservePoll("swpc", 50*time.Millisecond, 0, source.ErrNotModified)
	observer.ObservePoll("hamqsl", 900*time.Millisecond, 0, errors.New("dial tcp: timeout"))
	observer.ObservePoll("hamqsl", 20*time.Millisecond, 0, fmt.Errorf("backing off: %w", source.ErrRateLimited))

	got := mustGather(t, s)
	assertSeries(t, got, `solar_source_poll_total{result="success",source="swpc"}`, 1)
	assertSeries(t, got, `solar_source_poll_total{result="unchanged",source="swpc"}`, 1)
	assertSeries(t, got, `solar_source_poll_total{result="failure",source="hamqsl"}`, 2)
	assertSeries(t, got, `solar_source_poll_duration_seconds{source="swpc"}`, 2)
	assertSeries(t, got, `solar_source_poll_duration_seconds{source="hamqsl"}`, 2)
}

// A 304 means the data we hold is current and the source said so cheaply.
// Letting the last-success gauge go stale would fire the staleness alert on
// exactly the upstreams behaving best.
func TestNotModifiedCountsAsUnchangedAndStillAdvancesLastSuccess(t *testing.T) {
	s, _ := newTestSink(t, 0)

	before := float64(time.Now().Unix())
	s.ObservePoll("swpc", time.Millisecond, 0, source.ErrNotModified)
	after := float64(time.Now().Unix())

	got := mustGather(t, s)
	assertSeries(t, got, `solar_source_poll_total{result="unchanged",source="swpc"}`, 1)

	key := `solar_source_last_success_timestamp_seconds{source="swpc"}`
	v, ok := got[key]
	if !ok {
		t.Fatalf("series %s is absent after an unchanged poll", key)
	}
	if v < before || v > after {
		t.Errorf("series %s is %v, want between %v and %v", key, v, before, after)
	}
}

func TestAFailedPollLeavesTheLastSuccessGaugeAlone(t *testing.T) {
	s, _ := newTestSink(t, 0)

	s.ObservePoll("kc2g", time.Millisecond, 0, errors.New("503"))

	got := mustGather(t, s)
	assertSeries(t, got, `solar_source_poll_total{result="failure",source="kc2g"}`, 1)
	assertAbsent(t, got, "solar_source_last_success_timestamp_seconds")
}

func TestBuildInfoCarriesTheSuppliedVersion(t *testing.T) {
	s, _ := newTestSink(t, 0)
	s.SetBuildInfo("1.2.3", "abc1234")

	key := fmt.Sprintf(`solar_build_info{commit="abc1234",goversion=%q,version="1.2.3"}`, runtime.Version())
	assertSeries(t, mustGather(t, s), key, 1)

	// A second stamp must replace the first rather than leaving two series
	// claiming to describe the same binary.
	s.SetBuildInfo("1.3.0", "def5678")
	got := mustGather(t, s)
	if _, ok := got[key]; ok {
		t.Errorf("the previous build stamp survived SetBuildInfo")
	}
	assertSeries(t, got,
		fmt.Sprintf(`solar_build_info{commit="def5678",goversion=%q,version="1.3.0"}`, runtime.Version()), 1)
}

func TestBuildInfoFallsBackToUnknownWhenUnstamped(t *testing.T) {
	s, _ := newTestSink(t, 0)

	key := fmt.Sprintf(`solar_build_info{commit="unknown",goversion=%q,version="unknown"}`, runtime.Version())
	assertSeries(t, mustGather(t, s), key, 1)
}

// The registry is dedicated and carries only what this sink registered. The Go
// runtime and process collectors are deliberately not added: this process is a
// data path, not a service whose heap is interesting, and their forty-odd
// series would outnumber the space weather ones on a quiet scrape.
func TestTheRegistryCarriesNothingThisSinkDidNotRegister(t *testing.T) {
	s, _ := newTestSink(t, 0)
	got := mustGather(t, s)

	for _, name := range []string{
		"go_goroutines", "go_memstats_alloc_bytes", "process_open_fds",
		"promhttp_metric_handler_requests_total",
	} {
		assertAbsent(t, got, name)
	}
}

func TestPathDefaultsWhenTheConfigLeavesItEmpty(t *testing.T) {
	s, err := New(config.Prometheus{Enabled: true}, metric.NewStore(0), discardLogger())
	if err != nil {
		t.Fatalf("New returned %v, want nil", err)
	}
	if s.Path() != DefaultPath {
		t.Errorf("Path is %q, want %q", s.Path(), DefaultPath)
	}

	custom, err := New(config.Prometheus{Path: "/solar"}, metric.NewStore(0), discardLogger())
	if err != nil {
		t.Fatalf("New returned %v, want nil", err)
	}
	if custom.Path() != "/solar" {
		t.Errorf("Path is %q, want %q", custom.Path(), "/solar")
	}
}

func TestNewRejectsANilStore(t *testing.T) {
	if _, err := New(config.Prometheus{}, nil, discardLogger()); err == nil {
		t.Error("New with a nil store returned nil, want an error")
	}
}

func TestNameAndCloseAreSafeOnASinkThatNeverPublished(t *testing.T) {
	s, _ := newTestSink(t, 0)

	if s.Name() != Name {
		t.Errorf("Name is %q, want %q", s.Name(), Name)
	}
	if err := s.Close(); err != nil {
		t.Errorf("Close returned %v, want nil", err)
	}
	if err := s.Close(); err != nil {
		t.Errorf("a second Close returned %v, want nil", err)
	}
}

func TestConcurrentPublishObserveAndScrapeAreRaceFree(t *testing.T) {
	s, _ := newTestSink(t, 0)

	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	const workers = 8
	const iterations = 50

	var wg sync.WaitGroup
	for i := range workers {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for j := range iterations {
				b := batch(
					metric.Sample{
						Desc:  metric.FluxSFU,
						Value: float64(worker*iterations + j),
						Time:  observed.Add(time.Duration(j) * time.Second),
					},
					metric.Sample{
						Desc:   metric.KIndex,
						Labels: []string{fmt.Sprintf("station%d", worker)},
						Value:  float64(j % 9),
						Time:   observed.Add(time.Duration(j) * time.Second),
					},
				)
				if err := s.Publish(context.Background(), b); err != nil {
					t.Errorf("Publish returned %v, want nil", err)
					return
				}
				s.ObservePublish("influx2", time.Millisecond, nil)
				s.ObservePoll("swpc", time.Millisecond, b.Len(), nil)
			}
		}(i)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for range iterations {
			resp, err := http.Get(srv.URL + s.Path())
			if err != nil {
				t.Errorf("GET returned %v", err)
				return
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}
	}()

	wg.Wait()

	got := mustGather(t, s)
	assertSeries(t, got, `solar_sink_publish_total{result="success",sink="influx2"}`, workers*iterations)
	assertSeries(t, got, `solar_source_poll_total{result="success",source="swpc"}`, workers*iterations)
}
