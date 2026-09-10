// Package otlpmetrics exports the sample store over OpenTelemetry.
//
// It exists because Grafana Alloy, the OpenTelemetry Collector and most modern
// agents are natively OTLP receivers, so speaking OTLP puts these readings into
// a pipeline with no bridge or side-car in between. From there the same data
// can be routed onward to Prometheus, Mimir, or anything else the collector is
// configured with, without this exporter needing to know about any of them.
//
// Nothing here blocks a poll. Publish only writes the batch into the shared
// metric.Store; the metric SDK's periodic reader does the exporting on its own
// goroutine, so a collector that is down or slow costs a background export
// attempt rather than a missed poll.
//
// The instruments are observable gauges read from the store at collection time,
// which is the same pull-shaped design as the Prometheus sink and for the same
// reason. A registered synchronous instrument keeps reporting its last value
// forever; a series that has expired out of the store must stop being reported
// instead. A solar wind speed from six hours ago, presented to a dashboard as
// current, is worse than no reading at all.
package otlpmetrics

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	otelmetric "go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"

	"github.com/RealDougEubanks/solarham/internal/config"
	"github.com/RealDougEubanks/solarham/internal/metric"
	"github.com/RealDougEubanks/solarham/internal/redact"
	"github.com/RealDougEubanks/solarham/internal/sink"
)

// Name is the sink's stable identifier, used in logs and as a metric label.
const Name = "otlp"

// ServiceName identifies this exporter in the resource attributes every
// exported metric carries.
const ServiceName = "solarham-exporter"

// ProtocolGRPC and ProtocolHTTP are the supported values of cfg.Protocol.
const (
	ProtocolGRPC = "grpc"
	ProtocolHTTP = "http"
)

// defaultInterval is how often the periodic reader ships what the store holds
// when the configuration does not say. Thirty seconds is shorter than the
// fastest source's cadence, so a reading reaches the collector in roughly the
// timeframe it was observed rather than being held until the next poll.
const defaultInterval = 30 * time.Second

// defaultTimeout bounds one export, and the shutdown, when the configuration
// gives no timeout of its own.
const defaultTimeout = 15 * time.Second

// unknown stands in for a build stamp the main package did not supply, so the
// resource carries an honest placeholder rather than an empty attribute.
const unknown = "unknown"

// Sink exports the sample store through the OpenTelemetry metric SDK.
type Sink struct {
	store           *metric.Store
	log             *slog.Logger
	provider        *sdkmetric.MeterProvider
	shutdownTimeout time.Duration

	// gauges is the instrument for each descriptor, keyed by pointer identity
	// exactly as the descriptor table is declared: one package-level
	// *Descriptor per metric.
	gauges map[*metric.Descriptor]otelmetric.Float64ObservableGauge

	// warnedOnce keeps an unregistered descriptor from logging on every
	// collection. The reader runs on a timer for the life of the process, so an
	// unbounded warning would be a log flood rather than a diagnostic.
	warnedOnce sync.Map

	closeOnce sync.Once
	closeErr  error
}

// The fan-out holds this as a sink.Sink, so a signature drift fails the build
// here rather than at the wiring call in main.
var _ sink.Sink = (*Sink)(nil)

// Option adjusts a Sink at construction.
type Option func(*options)

type options struct {
	version string
	commit  string
}

// WithBuild stamps the running binary's identity onto the exported resource.
//
// Build information has to be an option rather than a setter because the
// resource is fixed when the meter provider is built, and every exported point
// carries it. A setter called afterwards could only change what later exports
// claim, which would leave two versions attributed to one process.
func WithBuild(version, commit string) Option {
	return func(o *options) {
		o.version = version
		o.commit = commit
	}
}

// New builds the exporter, meter provider and instruments over an existing
// sample store.
//
// The store is owned by the caller because more than this sink reads it: the
// Prometheus sink serves from it, the health endpoints report its size, and a
// sweeper ticks over it. Publishing through the same store is what keeps an
// OTLP series and a Prometheus series describing the same quantity with the
// same name, expiring at the same moment.
//
// New does not wait for the collector. The gRPC client is created lazily and
// the HTTP client does not connect until it sends, so a collector that is down
// at startup delays nothing and fails nothing: the exporter comes up, the
// periodic reader retries in the background, and samples flow as soon as the
// collector returns. Refusing to start because a downstream is briefly
// unreachable would be the worse failure for a process meant to run unattended
// for months.
func New(cfg config.OTLP, store *metric.Store, log *slog.Logger, opts ...Option) (*Sink, error) {
	if store == nil {
		return nil, errors.New("otlpmetrics: store is nil")
	}
	if cfg.Endpoint == "" {
		return nil, errors.New("otlpmetrics: endpoint is required")
	}

	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	interval := cfg.Interval
	if interval <= 0 {
		interval = defaultInterval
	}

	exporter, err := newExporter(cfg, timeout)
	if err != nil {
		return nil, err
	}

	reader := sdkmetric.NewPeriodicReader(exporter,
		sdkmetric.WithInterval(interval),
		sdkmetric.WithTimeout(timeout),
	)

	return newWithReader(cfg, store, log, reader, opts...)
}

// newWithReader builds the sink over a caller-supplied reader.
//
// This is the seam the tests use. A manual reader collects on demand and keeps
// everything in memory, so the instrument names, units and expiry behaviour can
// be asserted exactly without a live collector, a network listener or a wait
// for a periodic export to fire.
func newWithReader(cfg config.OTLP, store *metric.Store, log *slog.Logger, reader sdkmetric.Reader, opts ...Option) (*Sink, error) {
	if store == nil {
		return nil, errors.New("otlpmetrics: store is nil")
	}
	if log == nil {
		log = slog.Default()
	}

	var o options
	for _, opt := range opts {
		opt(&o)
	}
	if o.version == "" {
		o.version = unknown
	}
	if o.commit == "" {
		o.commit = unknown
	}

	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = defaultTimeout
	}

	// The resource is built from an explicit attribute set rather than merged
	// with resource.Default(), because merging fails on a schema URL mismatch
	// and a resource detector disagreeing about semconv versions must not be
	// able to stop the exporter starting.
	res := resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName(ServiceName),
		semconv.ServiceVersion(o.version),
		attribute.String("service.commit", o.commit),
	)

	s := &Sink{
		store:           store,
		log:             log,
		shutdownTimeout: timeout,
		gauges:          make(map[*metric.Descriptor]otelmetric.Float64ObservableGauge, len(metric.All)),
	}

	s.provider = sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(reader),
	)

	if err := s.registerInstruments(); err != nil {
		// The provider owns the reader and, through it, the exporter. Shut it
		// down rather than leaking its goroutine and connection.
		_ = s.shutdown()
		return nil, err
	}

	return s, nil
}

// newExporter builds the protocol-specific exporter.
func newExporter(cfg config.OTLP, timeout time.Duration) (sdkmetric.Exporter, error) {
	// A configured endpoint may be written either as a bare host:port, which is
	// what OTLP/gRPC conventionally uses, or as a full URL, which is what
	// people copy out of a collector's HTTP receiver configuration. Both are
	// accepted rather than rejecting one and making the operator guess which.
	hasScheme := strings.Contains(cfg.Endpoint, "://")

	switch cfg.Protocol {
	case ProtocolHTTP:
		opts := []otlpmetrichttp.Option{otlpmetrichttp.WithTimeout(timeout)}
		if hasScheme {
			opts = append(opts, otlpmetrichttp.WithEndpointURL(cfg.Endpoint))
		} else {
			opts = append(opts, otlpmetrichttp.WithEndpoint(cfg.Endpoint))
		}
		if len(cfg.Headers) > 0 {
			opts = append(opts, otlpmetrichttp.WithHeaders(cfg.Headers))
		}
		if cfg.Insecure {
			opts = append(opts, otlpmetrichttp.WithInsecure())
		}
		exp, err := otlpmetrichttp.New(context.Background(), opts...)
		return exp, wrapErr("build the OTLP/HTTP exporter", err)

	case ProtocolGRPC, "":
		opts := []otlpmetricgrpc.Option{otlpmetricgrpc.WithTimeout(timeout)}
		if hasScheme {
			opts = append(opts, otlpmetricgrpc.WithEndpointURL(cfg.Endpoint))
		} else {
			opts = append(opts, otlpmetricgrpc.WithEndpoint(cfg.Endpoint))
		}
		if len(cfg.Headers) > 0 {
			opts = append(opts, otlpmetricgrpc.WithHeaders(cfg.Headers))
		}
		if cfg.Insecure {
			opts = append(opts, otlpmetricgrpc.WithInsecure())
		}
		exp, err := otlpmetricgrpc.New(context.Background(), opts...)
		return exp, wrapErr("build the OTLP/gRPC exporter", err)

	default:
		return nil, fmt.Errorf("otlpmetrics: unsupported protocol %q, want %q or %q",
			cfg.Protocol, ProtocolGRPC, ProtocolHTTP)
	}
}

// wrapErr scrubs an SDK error before it is returned.
//
// cfg.Headers routinely carries a bearer token or a tenant key, and the
// endpoint may itself be a URL with credentials in it. Every error leaving this
// package goes through redact.Error first, and no error message here is built
// from the header map at all: an error that is returned is an error that will
// be logged.
func wrapErr(what string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("otlpmetrics: %s: %w", what, redact.Error(err))
}

// registerInstruments creates one observable gauge per descriptor, plus the
// single callback that reports all of them.
//
// Names, help text and units come from the descriptor table in package metric,
// which is what makes solar_flux_sfu mean the same thing in Prometheus, in
// InfluxDB and here. The unit is the descriptor's UCUM string, which OTLP
// requires and the other sinks ignore.
func (s *Sink) registerInstruments() error {
	meter := s.provider.Meter(ServiceName)

	instruments := make([]otelmetric.Observable, 0, len(metric.All))
	for _, d := range metric.All {
		g, err := meter.Float64ObservableGauge(d.FullName(),
			otelmetric.WithDescription(d.Help),
			otelmetric.WithUnit(d.Unit),
		)
		if err != nil {
			return fmt.Errorf("otlpmetrics: instrument %s: %w", d.FullName(), err)
		}
		s.gauges[d] = g
		instruments = append(instruments, g)
	}

	if _, err := meter.RegisterCallback(s.observe, instruments...); err != nil {
		return fmt.Errorf("otlpmetrics: register callback: %w", err)
	}
	return nil
}

// observe reports the store's current contents.
//
// It runs on the reader's goroutine, not a poll goroutine, and reads the store
// through its own lock. Only unexpired samples come back from Samples(), so a
// series whose source has stopped publishing simply stops appearing in the
// export rather than being re-sent at a stale value.
//
// KindInfo needs no special handling: the descriptor already carries the
// category as a label and the sample already carries the constant 1, so both
// kinds are observed as a float over sample.Value with the label values in the
// descriptor's order.
func (s *Sink) observe(_ context.Context, o otelmetric.Observer) error {
	for _, sample := range s.store.Samples() {
		if sample.Desc == nil {
			continue
		}
		g, ok := s.gauges[sample.Desc]
		if !ok {
			// A descriptor outside metric.All cannot be registered from here:
			// registering an instrument during collection would deadlock
			// against the reader. It is dropped and reported once, which makes
			// a missing table entry a visible bug rather than a silent gap.
			s.warnUnregistered(sample.Desc)
			continue
		}
		o.ObserveFloat64(g, sample.Value, otelmetric.WithAttributes(attributesFor(sample)...))
	}
	return nil
}

// warnUnregistered logs an unknown descriptor at most once per descriptor.
func (s *Sink) warnUnregistered(d *metric.Descriptor) {
	if _, seen := s.warnedOnce.LoadOrStore(d, struct{}{}); seen {
		return
	}
	s.log.Error("a sample used a descriptor that is not in metric.All, so it will not be exported",
		"sink", Name, "metric", d.FullName())
}

// attributesFor pairs a sample's label values with its descriptor's label
// names. Sample.Validate has already rejected a mismatch, but the length is
// checked anyway rather than trusting it inside a callback that must never
// panic on the reader's goroutine.
func attributesFor(s metric.Sample) []attribute.KeyValue {
	if len(s.Labels) == 0 {
		return nil
	}
	attrs := make([]attribute.KeyValue, 0, len(s.Labels))
	for i, name := range s.Desc.Labels {
		if i >= len(s.Labels) {
			break
		}
		attrs = append(attrs, attribute.String(name, s.Labels[i]))
	}
	return attrs
}

// Name implements sink.Sink.
func (s *Sink) Name() string { return Name }

// Publish writes the batch into the store.
//
// It sends nothing over the wire. That is worth being explicit about, because
// the name says otherwise: the export is performed by the SDK's periodic
// reader on its own schedule, and its failures are the SDK's to retry. Publish
// is a map update under a mutex, so an unreachable collector cannot slow or
// fail a poll, and a nil return here says the batch was recorded, not that it
// reached a collector.
func (s *Sink) Publish(ctx context.Context, b metric.Batch) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if b.Len() == 0 {
		// An empty batch is a routine, correct outcome — a 304 from SWPC, a
		// hamqsl document whose optional elements were all empty — so it is
		// reported as skipped rather than as success or failure.
		return fmt.Errorf("no samples from %s: %w", b.Source, sink.ErrSkipped)
	}
	s.store.Replace(b)
	return nil
}

// Close shuts the meter provider down, flushing whatever has not been exported.
//
// The shutdown is bounded by its own timeout rather than inheriting an
// unbounded context: a collector that has stopped answering must not be able to
// hold the process open at exit. It is safe on a sink that never exported and
// safe to call twice, which is what happens when the fan-out closes a sink the
// caller already closed.
func (s *Sink) Close() error {
	s.closeOnce.Do(func() { s.closeErr = s.shutdown() })
	return s.closeErr
}

// shutdown stops the provider under a bounded context.
func (s *Sink) shutdown() error {
	if s == nil || s.provider == nil {
		return nil
	}

	timeout := s.shutdownTimeout
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if err := s.provider.Shutdown(ctx); err != nil {
		return wrapErr("shutdown", err)
	}
	return nil
}
