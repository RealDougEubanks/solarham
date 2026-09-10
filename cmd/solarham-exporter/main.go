// Command solarham-exporter publishes space weather and HF propagation data to
// Prometheus, InfluxDB, MQTT and OTLP.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/RealDougEubanks/solarham/internal/config"
	"github.com/RealDougEubanks/solarham/internal/httpserver"
	"github.com/RealDougEubanks/solarham/internal/metric"
	"github.com/RealDougEubanks/solarham/internal/sink"
	"github.com/RealDougEubanks/solarham/internal/sink/influxv1"
	"github.com/RealDougEubanks/solarham/internal/sink/influxv2"
	"github.com/RealDougEubanks/solarham/internal/sink/mqtt"
	"github.com/RealDougEubanks/solarham/internal/sink/otlpmetrics"
	"github.com/RealDougEubanks/solarham/internal/sink/prommetrics"
	"github.com/RealDougEubanks/solarham/internal/source"
	"github.com/RealDougEubanks/solarham/internal/source/hamqsl"
	"github.com/RealDougEubanks/solarham/internal/source/kc2g"
	"github.com/RealDougEubanks/solarham/internal/source/swpc"
)

// Stamped at build time by the Dockerfile's ldflags.
var (
	version   = "dev"
	commit    = "unknown"
	buildDate = "unknown"
)

// Exit codes. A clean SIGTERM exits zero: exiting non-zero after a deliberate
// stop makes a supervisor restart a container that was stopped on purpose.
const (
	exitOK      = 0
	exitFailure = 1
)

func main() {
	// run does the work so that deferred cleanup still executes; os.Exit in
	// the middle of it would skip every defer.
	os.Exit(run())
}

func run() int {
	// The signal context is established before anything is constructed, so a
	// SIGTERM arriving during a slow startup is still honoured.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	cfg, err := config.Load(os.Args[1:], os.LookupEnv)
	if err != nil {
		if errors.Is(err, config.ErrVersionRequested) {
			fmt.Println(versionLine(version, commit, buildDate))
			return exitOK
		}
		// The logger does not exist yet, and a configuration error is the one
		// failure an operator reads on stderr rather than in a log aggregator.
		fmt.Fprintln(os.Stderr, err)
		return exitFailure
	}

	log := newLogger(cfg)
	slog.SetDefault(log)

	log.Info("starting solarham-exporter",
		"version", version, "commit", commit, "built", buildDate, "go", runtime.Version())
	cfg.LogEffective(log)

	// Deprecation warnings and similar notes are collected during loading,
	// before a logger exists, and emitted here.
	for _, note := range cfg.Notes() {
		log.Warn(note)
	}

	sources, err := buildSources(cfg, log)
	if err != nil {
		log.Error("failed to build sources", "error", err)
		return exitFailure
	}

	// The store is the seam between push-based sources and the pull-based
	// sinks. Its TTL bounds how long a series survives after its source stops
	// reporting it.
	store := metric.NewStore(retention(cfg, sources))

	sinks, prom, err := buildSinks(cfg, store, log)
	if err != nil {
		log.Error("failed to build sinks", "error", err)
		return exitFailure
	}

	set := sink.NewSet(sinks, sinkTimeout(sources), log, sinkObserver(prom))
	defer func() {
		if err := set.Close(); err != nil {
			log.Error("error closing sinks", "error", err)
		}
	}()

	scheduler := source.NewScheduler(sources, set, log, sourceObserver(prom))

	srv, err := httpserver.New(cfg.HTTP, httpserver.Deps{
		MetricsHandler: metricsHandler(prom),
		MetricsPath:    metricsPath(prom),
		Statuses:       scheduler.Statuses,
		SinkNames:      set.Names,
		Build: httpserver.BuildInfo{
			Version:   version,
			Commit:    commit,
			BuildDate: buildDate,
		},
	}, log)
	if err != nil {
		log.Error("failed to build the http server", "error", err)
		return exitFailure
	}

	log.Info("sources enabled", "count", scheduler.Len(), "sources", scheduler.Names())
	log.Info("sinks enabled", "count", set.Len(), "sinks", set.Names())

	var wg sync.WaitGroup
	errCh := make(chan error, 1)

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := srv.Start(ctx); err != nil {
			select {
			case errCh <- err:
			default:
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		scheduler.Run(ctx)
	}()

	select {
	case <-ctx.Done():
		log.Info("shutting down")
	case err := <-errCh:
		log.Error("http server failed", "error", err)
		stop()
		wg.Wait()
		return exitFailure
	}

	wg.Wait()
	return exitOK
}

// versionLine is deliberately given its arguments rather than reading the
// package globals, so parallel tests can exercise it without racing.
func versionLine(version, commit, buildDate string) string {
	return fmt.Sprintf("solarham-exporter %s (commit %s, built %s, %s)",
		version, commit, buildDate, runtime.Version())
}

// newLogger builds the structured logger. Output goes to stdout, because logs
// are data rather than errors and a container runtime collects both anyway.
func newLogger(cfg *config.Config) *slog.Logger {
	opts := &slog.HandlerOptions{Level: cfg.Log.Level}
	if cfg.Log.Format == "text" {
		return slog.New(slog.NewTextHandler(os.Stdout, opts))
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, opts))
}

// buildSources constructs each enabled source.
//
// A construction error is fatal, because it means a setting is wrong and no
// amount of retrying will fix it. A poll error never is.
func buildSources(cfg *config.Config, log *slog.Logger) ([]source.Source, error) {
	var sources []source.Source

	if cfg.Hamqsl.Enabled {
		s, err := hamqsl.New(cfg.Hamqsl, log)
		if err != nil {
			return nil, fmt.Errorf("hamqsl: %w", err)
		}
		sources = append(sources, s)
	}

	if cfg.SWPC.Enabled {
		s, err := swpc.New(cfg.SWPC, log)
		if err != nil {
			return nil, fmt.Errorf("swpc: %w", err)
		}
		sources = append(sources, s.All()...)
	}

	if cfg.KC2G.Enabled {
		s, err := kc2g.New(cfg.KC2G, log)
		if err != nil {
			return nil, fmt.Errorf("kc2g: %w", err)
		}
		sources = append(sources, s)
	}

	return sources, nil
}

// buildSinks constructs each enabled sink.
//
// The Prometheus sink is returned separately because it is also the observer
// for both fan-outs and the owner of the /metrics handler.
func buildSinks(cfg *config.Config, store *metric.Store, log *slog.Logger) ([]sink.Sink, *prommetrics.Sink, error) {
	var sinks []sink.Sink
	var prom *prommetrics.Sink

	if cfg.Prometheus.Enabled {
		s, err := prommetrics.New(cfg.Prometheus, store, log)
		if err != nil {
			return nil, nil, fmt.Errorf("prometheus: %w", err)
		}
		s.SetBuildInfo(version, commit)
		prom = s
		sinks = append(sinks, s)
	}

	if cfg.InfluxV2.Enabled {
		s, err := influxv2.New(cfg.InfluxV2, log)
		if err != nil {
			return nil, nil, fmt.Errorf("influxv2: %w", err)
		}
		sinks = append(sinks, s)
	}

	if cfg.InfluxV1.Enabled {
		s, err := influxv1.New(cfg.InfluxV1, log)
		if err != nil {
			return nil, nil, fmt.Errorf("influxv1: %w", err)
		}
		sinks = append(sinks, s)
	}

	if cfg.OTLP.Enabled {
		s, err := otlpmetrics.New(cfg.OTLP, store, log, otlpmetrics.WithBuild(version, commit))
		if err != nil {
			return nil, nil, fmt.Errorf("otlp: %w", err)
		}
		sinks = append(sinks, s)
	}

	if cfg.MQTT.Enabled {
		mqtt.Version = version
		s, err := mqtt.New(cfg.MQTT, log)
		if err != nil {
			return nil, nil, fmt.Errorf("mqtt: %w", err)
		}
		sinks = append(sinks, s)
	}

	return sinks, prom, nil
}

// retention decides how long a sample stays available after it was last
// observed.
//
// It is derived from the slowest source rather than configured per metric,
// because the alternative is asking an operator to reason about five different
// cadences. Three times the slowest interval means a series survives two missed
// polls of even the slowest source before disappearing.
func retention(cfg *config.Config, sources []source.Source) time.Duration {
	if cfg.Prometheus.Retention > 0 {
		return cfg.Prometheus.Retention
	}

	slowest := time.Duration(0)
	for _, s := range sources {
		if s.Interval() > slowest {
			slowest = s.Interval()
		}
	}
	if slowest <= 0 {
		return time.Hour
	}
	return 3 * slowest
}

// sinkTimeout bounds each individual sink publish.
//
// It is derived from the fastest source, not configured: a sink allowed to take
// longer than the interval of the source feeding it would let one slow backend
// build an unbounded backlog.
func sinkTimeout(sources []source.Source) time.Duration {
	fastest := time.Duration(0)
	for _, s := range sources {
		if fastest == 0 || s.Interval() < fastest {
			fastest = s.Interval()
		}
	}

	timeout := fastest / 2
	if timeout > 30*time.Second {
		timeout = 30 * time.Second
	}
	if timeout < time.Second {
		timeout = time.Second
	}
	return timeout
}

// The four helpers below exist to avoid handing a typed nil to an interface
// parameter. A nil *prommetrics.Sink assigned to an interface produces a
// non-nil interface holding a nil pointer, which panics on first use rather
// than behaving like the absent value it was meant to represent. Each helper
// therefore returns early with an untyped nil rather than letting the
// conversion happen implicitly at the call site.

func sinkObserver(p *prommetrics.Sink) sink.Observer {
	if p == nil {
		return nil
	}
	return p
}

func sourceObserver(p *prommetrics.Sink) source.Observer {
	if p == nil {
		return nil
	}
	return p
}

func metricsHandler(p *prommetrics.Sink) http.Handler {
	if p == nil {
		return nil
	}
	return p.Handler()
}

func metricsPath(p *prommetrics.Sink) string {
	if p == nil {
		return ""
	}
	return p.Path()
}
