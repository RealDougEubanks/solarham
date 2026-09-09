package otlpmetrics

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/RealDougEubanks/solarham/internal/config"
	"github.com/RealDougEubanks/solarham/internal/metric"
	"github.com/RealDougEubanks/solarham/internal/sink"
)

// unreachableEndpoint is a port nothing listens on. Port 1 is privileged and
// unused, so a connection there fails immediately rather than hanging.
const unreachableEndpoint = "127.0.0.1:1"

// observedAt is a fixed upstream observation time. Samples carry the
// upstream's own timestamp, never time.Now(), so the tests use a constant.
var observedAt = time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
}

// newTestSink builds a sink over a manual reader, which collects on demand and
// keeps everything in memory. No collector, no listener and no waiting for a
// periodic export to fire.
func newTestSink(t *testing.T, store *metric.Store, log *slog.Logger) (*Sink, *sdkmetric.ManualReader) {
	t.Helper()

	reader := sdkmetric.NewManualReader()
	s, err := newWithReader(config.OTLP{Timeout: time.Second}, store, log, reader, WithBuild("1.2.3", "abc1234"))
	if err != nil {
		t.Fatalf("newWithReader returned %v, want nil", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s, reader
}

// exported is one collected metric, flattened to what the assertions care
// about.
type exported struct {
	name        string
	description string
	unit        string
	points      []point
}

type point struct {
	value  float64
	labels map[string]string
}

// collect gathers from the manual reader and flattens the result by metric
// name.
func collect(t *testing.T, reader *sdkmetric.ManualReader) map[string]exported {
	t.Helper()

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect returned %v, want nil", err)
	}

	out := make(map[string]exported)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			e := exported{name: m.Name, description: m.Description, unit: m.Unit}
			gauge, ok := m.Data.(metricdata.Gauge[float64])
			if !ok {
				t.Errorf("metric %s is %T, want a float64 gauge", m.Name, m.Data)
				continue
			}
			for _, dp := range gauge.DataPoints {
				labels := make(map[string]string, dp.Attributes.Len())
				for _, kv := range dp.Attributes.ToSlice() {
					labels[string(kv.Key)] = kv.Value.Emit()
				}
				e.points = append(e.points, point{value: dp.Value, labels: labels})
			}
			out[m.Name] = e
		}
	}
	return out
}

// resourceAttributes flattens the resource attached to a collection.
func resourceAttributes(t *testing.T, reader *sdkmetric.ManualReader) map[string]string {
	t.Helper()

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect returned %v, want nil", err)
	}

	out := make(map[string]string)
	for _, kv := range rm.Resource.Attributes() {
		out[string(kv.Key)] = kv.Value.Emit()
	}
	return out
}

// sampleBatch is a batch touching a plain gauge, a labelled gauge and an info
// metric, which is every shape a sink has to handle.
func sampleBatch() metric.Batch {
	return metric.Batch{
		Source:  "swpc",
		Fetched: observedAt,
		Samples: []metric.Sample{
			{Desc: metric.FluxSFU, Value: 142.5, Time: observedAt},
			{Desc: metric.KIndex, Labels: []string{"planetary"}, Value: 4, Time: observedAt},
			{Desc: metric.XRayClassInfo, Labels: []string{"M1.2"}, Value: 1, Time: observedAt},
		},
	}
}

func TestNewSucceedsForBothProtocols(t *testing.T) {
	// Construction must not wait for a collector: one that is temporarily
	// unreachable is a routine condition, not a reason to refuse to start an
	// unattended exporter.
	for _, protocol := range []string{ProtocolGRPC, ProtocolHTTP} {
		t.Run(protocol, func(t *testing.T) {
			s, err := New(config.OTLP{
				Enabled:  true,
				Protocol: protocol,
				Endpoint: unreachableEndpoint,
				Insecure: true,
				Interval: time.Minute,
				Timeout:  time.Second,
			}, metric.NewStore(time.Minute), discardLogger())
			if err != nil {
				t.Fatalf("New returned %v, want nil", err)
			}
			defer func() { _ = s.Close() }()

			if s.Name() != Name {
				t.Errorf("Name is %q, want %q", s.Name(), Name)
			}

			// The fan-out holds this as a sink.Sink.
			var _ sink.Sink = s
		})
	}
}

func TestNewRejectsAnUnknownProtocol(t *testing.T) {
	s, err := New(config.OTLP{
		Protocol: "thrift",
		Endpoint: unreachableEndpoint,
	}, metric.NewStore(time.Minute), discardLogger())
	if err == nil {
		_ = s.Close()
		t.Fatal("New accepted an unknown protocol, want an error")
	}
	if !strings.Contains(err.Error(), "thrift") {
		t.Errorf("New returned %q, want it to name the rejected protocol", err)
	}
}

func TestNewRejectsAnEmptyEndpointAndANilStore(t *testing.T) {
	if s, err := New(config.OTLP{Protocol: ProtocolGRPC}, metric.NewStore(time.Minute), discardLogger()); err == nil {
		_ = s.Close()
		t.Error("New accepted an empty endpoint, want an error")
	}
	if s, err := New(config.OTLP{Protocol: ProtocolGRPC, Endpoint: unreachableEndpoint}, nil, discardLogger()); err == nil {
		_ = s.Close()
		t.Error("New accepted a nil store, want an error")
	}
}

// TestExportedNamesUnitsAndHelpMatchTheDescriptorTable is the point of the
// whole package: the same quantity must have the same name in every backend, so
// a dashboard written against Prometheus keeps working against OTLP.
func TestExportedNamesUnitsAndHelpMatchTheDescriptorTable(t *testing.T) {
	store := metric.NewStore(time.Minute)
	s, reader := newTestSink(t, store, discardLogger())

	if err := s.Publish(context.Background(), sampleBatch()); err != nil {
		t.Fatalf("Publish returned %v, want nil", err)
	}

	got := collect(t, reader)

	for _, d := range []*metric.Descriptor{metric.FluxSFU, metric.KIndex, metric.XRayClassInfo} {
		e, ok := got[d.FullName()]
		if !ok {
			t.Errorf("metric %s was not exported", d.FullName())
			continue
		}
		if !strings.HasPrefix(e.name, metric.Prefix) {
			t.Errorf("metric %s does not carry the %q prefix", e.name, metric.Prefix)
		}
		if e.unit != d.Unit {
			t.Errorf("metric %s has unit %q, want the descriptor's UCUM unit %q", e.name, e.unit, d.Unit)
		}
		if e.description != d.Help {
			t.Errorf("metric %s has description %q, want the descriptor's help text", e.name, e.description)
		}
	}

	flux := got[metric.FluxSFU.FullName()]
	if len(flux.points) != 1 || flux.points[0].value != 142.5 {
		t.Errorf("%s exported %+v, want a single point of 142.5", metric.FluxSFU.FullName(), flux.points)
	}

	k := got[metric.KIndex.FullName()]
	if len(k.points) != 1 || k.points[0].labels["station"] != "planetary" {
		t.Errorf("%s exported %+v, want one point labelled station=planetary", metric.KIndex.FullName(), k.points)
	}
}

// TestInfoSamplesExportAsOneWithTheCategoryAsALabel checks KindInfo, which is
// the only way a string can be carried by a numeric metric.
func TestInfoSamplesExportAsOneWithTheCategoryAsALabel(t *testing.T) {
	store := metric.NewStore(time.Minute)
	s, reader := newTestSink(t, store, discardLogger())

	if err := s.Publish(context.Background(), sampleBatch()); err != nil {
		t.Fatalf("Publish returned %v, want nil", err)
	}

	info, ok := collect(t, reader)[metric.XRayClassInfo.FullName()]
	if !ok {
		t.Fatalf("%s was not exported", metric.XRayClassInfo.FullName())
	}
	if len(info.points) != 1 {
		t.Fatalf("%s exported %d points, want 1", info.name, len(info.points))
	}
	if info.points[0].value != 1 {
		t.Errorf("%s is %v, want the constant 1", info.name, info.points[0].value)
	}
	if got := info.points[0].labels["class"]; got != "M1.2" {
		t.Errorf("%s carries class=%q, want %q", info.name, got, "M1.2")
	}
}

// TestAnExpiredSampleIsNoLongerExported is why the instruments are observable
// gauges read from the store rather than values written at publish time. A
// registered synchronous gauge would keep reporting its last value forever; a
// solar wind speed from six hours ago presented as current is worse than no
// reading at all.
func TestAnExpiredSampleIsNoLongerExported(t *testing.T) {
	const ttl = 20 * time.Millisecond

	store := metric.NewStore(ttl)
	s, reader := newTestSink(t, store, discardLogger())

	if err := s.Publish(context.Background(), sampleBatch()); err != nil {
		t.Fatalf("Publish returned %v, want nil", err)
	}
	if _, ok := collect(t, reader)[metric.FluxSFU.FullName()]; !ok {
		t.Fatalf("%s was not exported while fresh", metric.FluxSFU.FullName())
	}

	time.Sleep(4 * ttl)

	after := collect(t, reader)
	for name := range after {
		t.Errorf("metric %s was still exported after its sample expired", name)
	}
}

func TestPublishSkipsAnEmptyBatch(t *testing.T) {
	store := metric.NewStore(time.Minute)
	s, reader := newTestSink(t, store, discardLogger())

	// A 304 from SWPC, or a hamqsl document whose optional elements were all
	// empty, is a correct outcome rather than a failure.
	err := s.Publish(context.Background(), metric.Batch{Source: "swpc", Fetched: observedAt})
	if !errors.Is(err, sink.ErrSkipped) {
		t.Fatalf("Publish returned %v, want sink.ErrSkipped", err)
	}
	if got := collect(t, reader); len(got) != 0 {
		t.Errorf("an empty batch produced %d exported metrics, want none", len(got))
	}
}

func TestPublishReportsACancelledContext(t *testing.T) {
	store := metric.NewStore(time.Minute)
	s, _ := newTestSink(t, store, discardLogger())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := s.Publish(ctx, sampleBatch()); !errors.Is(err, context.Canceled) {
		t.Errorf("Publish returned %v, want context.Canceled", err)
	}
	if store.Len() != 0 {
		t.Error("a cancelled Publish still wrote to the store")
	}
}

func TestCloseIsIdempotentAndSafeOnASinkThatNeverExported(t *testing.T) {
	store := metric.NewStore(time.Minute)
	s, _ := newTestSink(t, store, discardLogger())

	// Nothing has ever been published, so nothing has ever been exported.
	if err := s.Close(); err != nil {
		t.Fatalf("Close returned %v, want nil", err)
	}
	// A second Close is what happens when the fan-out closes a sink the caller
	// already closed. It must not report a shutdown that already happened as a
	// failure.
	if err := s.Close(); err != nil {
		t.Errorf("second Close returned %v, want nil", err)
	}
}

func TestCloseIsBoundedWhenTheCollectorIsUnreachable(t *testing.T) {
	const timeout = time.Second

	store := metric.NewStore(time.Minute)
	s, err := New(config.OTLP{
		Protocol: ProtocolGRPC,
		Endpoint: unreachableEndpoint,
		Insecure: true,
		Interval: time.Minute,
		Timeout:  timeout,
	}, store, discardLogger())
	if err != nil {
		t.Fatalf("New returned %v, want nil", err)
	}
	if err := s.Publish(context.Background(), sampleBatch()); err != nil {
		t.Fatalf("Publish returned %v, want nil", err)
	}

	done := make(chan struct{})
	start := time.Now()
	go func() {
		defer close(done)
		// The error is not asserted on: a final export to a collector that is
		// not there may legitimately fail. What matters is that Close returns.
		_ = s.Close()
	}()

	select {
	case <-done:
	case <-time.After(10 * timeout):
		t.Fatal("Close did not return; shutdown is not bounded")
	}
	if elapsed := time.Since(start); elapsed > 5*timeout {
		t.Errorf("Close took %s, want it bounded near the %s timeout", elapsed, timeout)
	}
}

func TestResourceIdentifiesTheServiceAndBuild(t *testing.T) {
	store := metric.NewStore(time.Minute)
	s, reader := newTestSink(t, store, discardLogger())
	if err := s.Publish(context.Background(), sampleBatch()); err != nil {
		t.Fatalf("Publish returned %v, want nil", err)
	}

	attrs := resourceAttributes(t, reader)
	if attrs["service.name"] != ServiceName {
		t.Errorf("service.name is %q, want %q", attrs["service.name"], ServiceName)
	}
	if attrs["service.version"] != "1.2.3" {
		t.Errorf("service.version is %q, want %q", attrs["service.version"], "1.2.3")
	}
	if attrs["service.commit"] != "abc1234" {
		t.Errorf("service.commit is %q, want %q", attrs["service.commit"], "abc1234")
	}
}

// TestHeadersNeverReachALogOrAnError guards the one credential this sink is
// given. cfg.Headers routinely carries a bearer token or a tenant key, and a
// log line is copied into tickets and shipped to aggregators.
func TestHeadersNeverReachALogOrAnError(t *testing.T) {
	const canary = "canary-bearer-token-do-not-log"

	var logged bytes.Buffer
	log := slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelDebug}))

	cfg := config.OTLP{
		Protocol: ProtocolHTTP,
		Endpoint: unreachableEndpoint,
		Insecure: true,
		Headers:  map[string]string{"Authorization": "Bearer " + canary},
		Interval: 10 * time.Millisecond,
		Timeout:  100 * time.Millisecond,
	}

	s, err := New(cfg, metric.NewStore(time.Minute), log)
	if err != nil {
		t.Fatalf("New returned %v, want nil", err)
	}
	if err := s.Publish(context.Background(), sampleBatch()); err != nil {
		t.Fatalf("Publish returned %v, want nil", err)
	}

	// Give the periodic reader time to fail an export against a dead endpoint,
	// which is the path most likely to build an error out of the request.
	time.Sleep(150 * time.Millisecond)

	// Close's error is the other candidate, since a final flush fails here.
	closeErr := s.Close()

	// A sink formatted whole is how a struct dump reaches a log.
	for what, text := range map[string]string{
		"the log":          logged.String(),
		"the Close error":  fmt.Sprint(closeErr),
		"a formatted sink": fmt.Sprintf("%+v %#v", s, s),
	} {
		if strings.Contains(text, canary) {
			t.Errorf("%s contains the header credential:\n%s", what, text)
		}
	}

	// The rejected-protocol path builds an error from the configuration, so it
	// is checked with the same credential present.
	cfg.Protocol = "thrift"
	if _, err := New(cfg, metric.NewStore(time.Minute), log); err == nil {
		t.Fatal("New accepted an unknown protocol, want an error")
	} else if strings.Contains(err.Error(), canary) {
		t.Errorf("the protocol error contains the header credential: %v", err)
	}
}

// TestEveryDescriptorInTheTableGetsAnInstrument keeps the table and the sink
// from drifting: a descriptor added to metric.All must be exportable without
// anyone remembering to touch this package.
func TestEveryDescriptorInTheTableGetsAnInstrument(t *testing.T) {
	s, _ := newTestSink(t, metric.NewStore(time.Minute), discardLogger())

	for _, d := range metric.All {
		if _, ok := s.gauges[d]; !ok {
			t.Errorf("descriptor %s has no registered instrument", d.FullName())
		}
	}
}
