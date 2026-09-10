// Package prommetrics exposes the sample store and the exporter's own health
// on a Prometheus scrape endpoint.
//
// This is the most robust of the sinks and the one worth reaching for first. It
// needs no credentials, it performs no I/O on the publish path so it cannot
// stall or fail a poll, and it keeps working unchanged when every push-based
// backend is reconfigured. It is also the only sink that reports on the others:
// the fan-out hands it each publish outcome through sink.Observer and the
// scheduler hands it each poll outcome through source.Observer, so a silently
// failing InfluxDB sink or a source that stopped answering is visible in the
// same scrape as the readings themselves.
package prommetrics

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"runtime"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/RealDougEubanks/solarham/internal/config"
	"github.com/RealDougEubanks/solarham/internal/metric"
	"github.com/RealDougEubanks/solarham/internal/sink"
	"github.com/RealDougEubanks/solarham/internal/source"
)

// Name is the sink's stable identifier. It appears in logs and, because this
// sink observes the whole fan-out, as its own value of the sink label.
const Name = "prometheus"

// DefaultPath is used when the configuration leaves the scrape path empty.
const DefaultPath = "/metrics"

// unknown stands in for a build stamp the main package did not supply, so that
// solar_build_info is present with honest labels rather than absent.
const unknown = "unknown"

// Sink serves the sample store and this exporter's self-instrumentation from a
// dedicated Prometheus registry.
type Sink struct {
	path     string
	log      *slog.Logger
	registry *prometheus.Registry
	store    *metric.Store

	samples *storeCollector

	pollTotal       *prometheus.CounterVec
	pollDuration    *prometheus.HistogramVec
	pollLastSuccess *prometheus.GaugeVec
	staleWindow     *prometheus.GaugeVec
	publishTotal    *prometheus.CounterVec
	publishDuration *prometheus.HistogramVec
	buildInfo       *prometheus.GaugeVec
}

// This sink is three things at once: a destination for batches, the observer
// the fan-out reports publish outcomes to, and the observer the scheduler
// reports poll outcomes to. All three are asserted here so a signature drift in
// any of them fails the build rather than a wiring call in main.
var (
	_ sink.Sink       = (*Sink)(nil)
	_ sink.Observer   = (*Sink)(nil)
	_ source.Observer = (*Sink)(nil)
)

// New builds the sink over an existing sample store.
//
// The store is owned by the caller because more than this sink reads it: the
// health endpoints report its size, and a sweeper ticks over it. The registry,
// by contrast, is private and dedicated rather than prometheus.DefaultRegisterer
// so that tests are isolated from each other and so that no library linked into
// the binary can inject its own metrics into this exporter's scrape output.
//
// The Go runtime and process collectors are deliberately not registered. This
// process is a data path, not a service whose heap is interesting, and their
// forty-odd series would outnumber the space weather ones on a quiet scrape. A
// caller that wants them can register them on Registry() itself.
func New(cfg config.Prometheus, store *metric.Store, log *slog.Logger) (*Sink, error) {
	if store == nil {
		return nil, fmt.Errorf("prommetrics: store is nil")
	}
	if log == nil {
		log = slog.Default()
	}

	path := cfg.Path
	if path == "" {
		path = DefaultPath
	}

	s := &Sink{
		path:     path,
		log:      log,
		registry: prometheus.NewRegistry(),
		store:    store,
		samples:  newStoreCollector(store, log),
		pollTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: metric.Prefix + "source_poll_total",
			Help: "Polls of each upstream source, by outcome: success, unchanged or failure.",
		}, []string{"source", "result"}),
		pollDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: metric.Prefix + "source_poll_duration_seconds",
			Help: "Time each source took to complete one poll.",
			// Ten milliseconds to a few seconds: the range a public HTTP API
			// occupies between a warm CDN edge and a request about to hit the
			// source's own timeout.
			Buckets: prometheus.ExponentialBuckets(0.01, 2, 10),
		}, []string{"source"}),
		pollLastSuccess: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: metric.Prefix + "source_last_success_timestamp_seconds",
			Help: "Unix time of the most recent successful poll of each source.",
		}, []string{"source"}),
		staleWindow: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: metric.Prefix + "source_stale_window_seconds",
			Help: "How long each source may go without a successful poll before it is stale.",
		}, []string{"source"}),
		publishTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: metric.Prefix + "sink_publish_total",
			Help: "Publish attempts per sink, by outcome: success, skipped or failure.",
		}, []string{"sink", "result"}),
		publishDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: metric.Prefix + "sink_publish_duration_seconds",
			Help: "Time each sink took to publish one batch.",
			// Five milliseconds to a few seconds, which is where a network
			// sink sits between a healthy local backend and one about to hit
			// the fan-out timeout.
			Buckets: prometheus.ExponentialBuckets(0.005, 2, 10),
		}, []string{"sink"}),
		buildInfo: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: metric.Prefix + "build_info",
			Help: "Build information for the running binary, always 1.",
		}, []string{"version", "commit", "goversion"}),
	}

	// MustRegister would panic, and this process must never die over a metric.
	// Registration can only fail on a duplicate, which a fresh registry makes
	// impossible, so a failure here is a programming error: it is logged and
	// the collector left out rather than taking the exporter with it.
	for _, c := range []prometheus.Collector{
		s.staleWindow,
		s.samples, s.pollTotal, s.pollDuration, s.pollLastSuccess,
		s.publishTotal, s.publishDuration, s.buildInfo,
	} {
		if err := s.registry.Register(c); err != nil {
			s.log.Error("registering a collector failed; its metrics will be absent",
				"sink", Name, "error", err)
		}
	}

	s.SetBuildInfo("", "")
	return s, nil
}

// Name implements sink.Sink.
func (s *Sink) Name() string { return Name }

// Path is where the scrape endpoint should be mounted.
func (s *Sink) Path() string { return s.path }

// Registry exposes the dedicated registry, for a caller that wants to gather
// directly or register an additional collector alongside these.
func (s *Sink) Registry() *prometheus.Registry { return s.registry }

// SetBuildInfo stamps the running binary's identity onto solar_build_info.
//
// It is a method rather than a New parameter so that this package does not
// dictate how the main package obtains its version, and so the metric is
// trivially testable. Empty values become "unknown": a build_info series that
// is present with honest placeholders is more useful to a dashboard than one
// that is missing entirely on an unstamped local build.
func (s *Sink) SetBuildInfo(version, commit string) {
	if version == "" {
		version = unknown
	}
	if commit == "" {
		commit = unknown
	}
	// Reset first so a second call replaces the stamp rather than leaving two
	// build_info series claiming to describe the same binary.
	s.buildInfo.Reset()
	s.buildInfo.WithLabelValues(version, commit, runtime.Version()).Set(1)
}

// Handler serves the scrape endpoint from this sink's registry.
//
// ContinueOnError is deliberate. A collection error here can only come from one
// malformed sample, and the useful behaviour is then to serve every other
// metric and log the bad one: an operator watching a geomagnetic storm should
// not lose the whole scrape because one station reported a label the exporter
// could not encode. HTTPErrorOnError would convert one bad series into a total
// outage of the endpoint, and PanicOnError is never acceptable in a process
// whose entire purpose is to keep running unattended.
func (s *Sink) Handler() http.Handler {
	return promhttp.HandlerFor(s.registry, promhttp.HandlerOpts{
		ErrorHandling: promhttp.ContinueOnError,
		ErrorLog:      promLogger{log: s.log},
	})
}

// Publish writes the batch into the store.
//
// There is no I/O and no network here, only a map update under a mutex, so this
// sink cannot slow or fail a poll. Expiry, ordering and validation are all the
// store's job, which keeps the same rules applying however a sample arrived.
func (s *Sink) Publish(_ context.Context, b metric.Batch) error {
	if b.Len() == 0 {
		// An empty batch is a routine, correct outcome — a 304 from SWPC, a
		// hamqsl document whose optional elements were all empty — so it is
		// reported as skipped rather than as success or failure.
		return fmt.Errorf("no samples from %s: %w", b.Source, sink.ErrSkipped)
	}
	s.store.Replace(b)
	return nil
}

// Close implements sink.Sink. There is nothing to release, and the metrics stay
// available to a final scrape until the HTTP server itself stops. It is safe on
// a sink that never published.
func (s *Sink) Close() error { return nil }

// ObservePublish implements sink.Observer, recording one sink's outcome.
//
// A skipped publish is counted separately from a failure because it is not one:
// a sink handed a batch carrying nothing it publishes has behaved correctly,
// and folding that into the failure count would make a healthy exporter look
// broken.
func (s *Sink) ObservePublish(sinkName string, duration time.Duration, err error) {
	var result string
	switch {
	case err == nil:
		result = "success"
	case errors.Is(err, sink.ErrSkipped):
		result = "skipped"
	default:
		result = "failure"
	}

	s.publishTotal.WithLabelValues(sinkName, result).Inc()
	s.publishDuration.WithLabelValues(sinkName).Observe(duration.Seconds())
}

// ObservePoll implements source.Observer, recording one source's outcome.
//
// ErrNotModified is counted under its own result rather than as a failure, and
// it still advances the last-success gauge. A 304 means the data we hold is
// current and the source told us so cheaply; treating a source that behaves
// efficiently as broken would fire the staleness alert on exactly the upstreams
// that are working best.
//
// The sample count is not turned into a metric. How many series a poll produced
// is already visible as the series themselves, and a per-poll count would go
// stale the moment a sample expired out of the store.
func (s *Sink) ObservePoll(sourceName string, duration time.Duration, samples int, err error) {
	_ = samples

	var result string
	var succeeded bool
	switch {
	case err == nil:
		result, succeeded = "success", true
	case errors.Is(err, source.ErrNotModified):
		result, succeeded = "unchanged", true
	default:
		result = "failure"
	}

	s.pollTotal.WithLabelValues(sourceName, result).Inc()
	s.pollDuration.WithLabelValues(sourceName).Observe(duration.Seconds())
	if succeeded {
		s.pollLastSuccess.WithLabelValues(sourceName).Set(float64(time.Now().Unix()))
	}
}

// storeCollector emits the store's contents on every scrape.
//
// The reading gauges are collected from the store rather than held in
// registered prometheus.Gauge values, because a registered Gauge is always
// present in a scrape. A field the upstream did not supply must be genuinely
// absent, not zero and not the previous cycle's value: a solar wind speed from
// six hours ago, presented to a dashboard as current, is worse than no reading.
// Deriving the metric set from the store on each scrape is what lets a series
// expire and simply stop appearing.
type storeCollector struct {
	store *metric.Store
	log   *slog.Logger

	mu    sync.Mutex
	descs map[*metric.Descriptor]*prometheus.Desc
}

func newStoreCollector(store *metric.Store, log *slog.Logger) *storeCollector {
	c := &storeCollector{
		store: store,
		log:   log,
		descs: make(map[*metric.Descriptor]*prometheus.Desc, len(metric.All)),
	}
	// The known table is translated up front so the common path never allocates
	// a Desc under a scrape.
	for _, d := range metric.All {
		c.descs[d] = promDesc(d)
	}
	return c
}

// desc returns the Prometheus descriptor for one of ours, translating and
// caching an unrecognised descriptor rather than refusing it. Descriptors are
// package-level values in metric, so the map is keyed by pointer identity.
func (c *storeCollector) desc(d *metric.Descriptor) *prometheus.Desc {
	c.mu.Lock()
	defer c.mu.Unlock()
	if pd, ok := c.descs[d]; ok {
		return pd
	}
	pd := promDesc(d)
	c.descs[d] = pd
	return pd
}

// promDesc translates one of our descriptors into a Prometheus one. Name, help
// and label names all come from the table in package metric, so a Prometheus
// series and an InfluxDB field describe the same quantity by the same name.
func promDesc(d *metric.Descriptor) *prometheus.Desc {
	return prometheus.NewDesc(d.FullName(), d.Help, d.Labels, nil)
}

// Describe implements prometheus.Collector as an unchecked collector: it sends
// no descriptors at all.
//
// This is the documented way to declare a collector whose metric set is not
// known ahead of time, and this one's genuinely is not — which series exist
// depends on what the upstreams have supplied and what has since expired. An
// unchecked collector also gives up registration-time duplicate detection,
// which is the right trade here: the alternative is describing all thirty-odd
// descriptors, which would tell the registry to expect series that may
// legitimately never appear.
func (c *storeCollector) Describe(chan<- *prometheus.Desc) {}

// Collect implements prometheus.Collector, emitting one constant metric per
// unexpired sample. Samples that have expired out of the store are never seen
// here, so they leave the scrape output entirely.
func (c *storeCollector) Collect(ch chan<- prometheus.Metric) {
	for _, sample := range c.store.Samples() {
		if sample.Desc == nil {
			continue
		}

		// KindInfo needs no special handling: the descriptor already carries
		// the category as a label and the sample already carries the constant
		// 1, so both kinds emit as a gauge over sample.Value with the label
		// values in the descriptor's order.
		m, err := prometheus.NewConstMetric(
			c.desc(sample.Desc), prometheus.GaugeValue, sample.Value, sample.Labels...)
		if err != nil {
			// NewConstMetric returns an error where MustNewConstMetric would
			// panic; the invalid metric carries the reason to the scraper's
			// error handling and leaves every other series intact.
			c.log.Error("building a metric from a sample failed",
				"metric", sample.Desc.FullName(), "labels", sample.Labels, "error", err)
			m = prometheus.NewInvalidMetric(c.desc(sample.Desc), err)
		}
		ch <- m
	}
}

// promLogger adapts slog to promhttp.Logger so scrape-time errors land in the
// exporter's own structured log rather than on stderr.
type promLogger struct{ log *slog.Logger }

func (l promLogger) Println(v ...any) {
	l.log.Error("scrape error", "sink", Name, "error", fmt.Sprint(v...))
}

// SetStaleWindow publishes how long a source may go without a successful poll
// before it should be considered stale.
//
// This is the companion to source_last_success_timestamp_seconds, and the pair
// is what makes a correct staleness alert expressible without restating the
// exporter's schedules in the alerting rules:
//
//	time() - solar_source_last_success_timestamp_seconds
//	  > solar_source_stale_window_seconds
//
// The window differs by an order of magnitude across sources -- three minutes
// for the one-minute SWPC feeds, better than twenty hours for an observatory
// that publishes three times a day -- so a single hand-written threshold is
// either far too tight for the slow sources or useless for the fast ones. The
// exporter already knows each window because it derives it from the schedule;
// exporting it moves that knowledge to where the alert is written.
//
// It is a gauge rather than a constant because a source's schedule can change
// between releases, and an alert built on a stale hard-coded number is the
// failure this is meant to prevent.
func (s *Sink) SetStaleWindow(source string, window time.Duration) {
	if s == nil || window <= 0 {
		return
	}
	s.staleWindow.WithLabelValues(source).Set(window.Seconds())
}
