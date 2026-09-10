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
	"github.com/RealDougEubanks/solarham/internal/httpx"
	"github.com/RealDougEubanks/solarham/internal/metric"
	"github.com/RealDougEubanks/solarham/internal/sink"
	"github.com/RealDougEubanks/solarham/internal/sink/influxv1"
	"github.com/RealDougEubanks/solarham/internal/sink/influxv2"
	"github.com/RealDougEubanks/solarham/internal/sink/mqtt"
	"github.com/RealDougEubanks/solarham/internal/sink/otlpmetrics"
	"github.com/RealDougEubanks/solarham/internal/sink/prommetrics"
	"github.com/RealDougEubanks/solarham/internal/source"
	"github.com/RealDougEubanks/solarham/internal/source/celestrak"
	"github.com/RealDougEubanks/solarham/internal/source/donki"
	"github.com/RealDougEubanks/solarham/internal/source/drao"
	"github.com/RealDougEubanks/solarham/internal/source/fmi"
	"github.com/RealDougEubanks/solarham/internal/source/gfz"
	"github.com/RealDougEubanks/solarham/internal/source/glotec"
	"github.com/RealDougEubanks/solarham/internal/source/hamqsl"
	"github.com/RealDougEubanks/solarham/internal/source/intermagnet"
	"github.com/RealDougEubanks/solarham/internal/source/iswa"
	"github.com/RealDougEubanks/solarham/internal/source/kc2g"
	"github.com/RealDougEubanks/solarham/internal/source/kiwisdr"
	"github.com/RealDougEubanks/solarham/internal/source/lasp"
	"github.com/RealDougEubanks/solarham/internal/source/lotw"
	"github.com/RealDougEubanks/solarham/internal/source/nmdb"
	"github.com/RealDougEubanks/solarham/internal/source/pota"
	"github.com/RealDougEubanks/solarham/internal/source/pskreporter"
	"github.com/RealDougEubanks/solarham/internal/source/silso"
	"github.com/RealDougEubanks/solarham/internal/source/swpc"
	"github.com/RealDougEubanks/solarham/internal/source/swpcforecast"
	"github.com/RealDougEubanks/solarham/internal/source/usgsgeomag"
	"github.com/RealDougEubanks/solarham/internal/source/wsprlive"
)

// Stamped at build time by the Dockerfile's ldflags.
var (
	version   = "dev"
	commit    = "unknown"
	buildDate = "unknown"
)

// httpClientTimeout bounds every individual request made by any source.
//
// It has to clear the slowest legitimate fetch rather than the typical one:
// the ARRL activity file is six megabytes and DRAO's flux table is two, so a
// tighter bound would cut off the sources that need the most time. Individual
// sources narrow this with their own context deadline where they care.
const httpClientTimeout = 2 * time.Minute

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

	// One HTTP client, shared by every source. The rate limiting inside it is
	// per host and shared, which is the point: a per-source limiter would let
	// ten sources each politely make one request per second to one host and
	// collectively make ten.
	client := httpx.NewDefault(httpClientTimeout, log)

	// Restricted-licence sources are named at startup so the obligation is
	// visible in the logs rather than only in a config file nobody re-reads.
	for _, note := range cfg.RestrictedSources() {
		log.Info("restricted-licence source enabled", "terms", note)
	}

	sources, err := buildSources(cfg, client, log)
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
	scheduler.SetAuthority(sourceAuthority)

	// Publish each source's staleness window so an alerting rule can compare
	// it against the last-success timestamp without hard-coding a threshold
	// per source. The windows come from the schedules and do not change while
	// the process runs, so this is set once here.
	for _, st := range scheduler.Statuses() {
		prom.SetStaleWindow(st.Name, st.StaleAfter)
	}

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
func buildSources(cfg *config.Config, client *httpx.Client, log *slog.Logger) ([]source.Source, error) {
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

	if cfg.SolarProbabilities.Enabled {
		s, err := swpcforecast.New(cfg.SolarProbabilities, client, log)
		if err != nil {
			return nil, fmt.Errorf("swpc-forecast: %w", err)
		}
		sources = append(sources, s)
	}

	if cfg.GloTEC.Enabled {
		s, err := glotec.New(cfg.GloTEC, client, log)
		if err != nil {
			return nil, fmt.Errorf("glotec: %w", err)
		}
		sources = append(sources, s)
	}

	if cfg.ISWA.Enabled {
		s, err := iswa.New(cfg.ISWA, client, log)
		if err != nil {
			return nil, fmt.Errorf("iswa: %w", err)
		}
		sources = append(sources, s)
	}

	if cfg.DONKI.Enabled {
		s, err := donki.New(cfg.DONKI, client, log)
		if err != nil {
			return nil, fmt.Errorf("donki: %w", err)
		}
		sources = append(sources, s)
	}

	if cfg.USGSGeomag.Enabled {
		s, err := usgsgeomag.New(cfg.USGSGeomag, client, log)
		if err != nil {
			return nil, fmt.Errorf("usgs-geomag: %w", err)
		}
		sources = append(sources, s)
	}

	if cfg.GFZ.Enabled {
		s, err := gfz.New(cfg.GFZ, client, log)
		if err != nil {
			return nil, fmt.Errorf("gfz: %w", err)
		}
		sources = append(sources, s)
	}

	if cfg.DRAO.Enabled {
		s, err := drao.New(cfg.DRAO, client, log)
		if err != nil {
			return nil, fmt.Errorf("drao: %w", err)
		}
		sources = append(sources, s)
	}

	if cfg.LASP.Enabled {
		s, err := lasp.New(cfg.LASP, client, log)
		if err != nil {
			return nil, fmt.Errorf("lasp: %w", err)
		}
		// LISIRD carries two datasets that would write the same 10.7 cm flux
		// series DRAO owns. They are requested only when DRAO is switched off,
		// which avoids the collision rather than arbitrating it -- and saves
		// two requests.
		if !f107Owner(cfg.DRAO.Enabled) {
			s.EnableF107Datasets()
			log.Debug("lasp will publish the 10.7 cm flux because drao is disabled")
		}
		sources = append(sources, s)
	}

	if cfg.FMI.Enabled {
		s, err := fmi.New(cfg.FMI, client, log)
		if err != nil {
			return nil, fmt.Errorf("fmi: %w", err)
		}
		sources = append(sources, s)
	}

	if cfg.KiwiSDR.Enabled {
		s, err := kiwisdr.New(cfg.KiwiSDR, client, log)
		if err != nil {
			return nil, fmt.Errorf("kiwisdr: %w", err)
		}
		sources = append(sources, s)
	}

	if cfg.LoTW.Enabled {
		s, err := lotw.New(cfg.LoTW, client, log)
		if err != nil {
			return nil, fmt.Errorf("lotw: %w", err)
		}
		sources = append(sources, s)
	}

	if cfg.POTA.Enabled {
		s, err := pota.New(cfg.POTA, client, log)
		if err != nil {
			return nil, fmt.Errorf("pota: %w", err)
		}
		sources = append(sources, s)
	}

	if cfg.Celestrak.Enabled {
		s, err := celestrak.New(cfg.Celestrak, client, log)
		if err != nil {
			return nil, fmt.Errorf("celestrak: %w", err)
		}
		sources = append(sources, s)
	}

	if cfg.WSPRLive.Enabled {
		s, err := wsprlive.New(cfg.WSPRLive, client, log)
		if err != nil {
			return nil, fmt.Errorf("wspr-live: %w", err)
		}
		sources = append(sources, s)
	}

	if cfg.PSKReporter.Enabled {
		s, err := pskreporter.New(cfg.PSKReporter, client, log)
		if err != nil {
			return nil, fmt.Errorf("pskreporter: %w", err)
		}
		sources = append(sources, s)
	}

	if cfg.NMDB.Enabled {
		s, err := nmdb.New(cfg.NMDB, client, log)
		if err != nil {
			return nil, fmt.Errorf("nmdb: %w", err)
		}
		sources = append(sources, s)
	}

	if cfg.INTERMAGNET.Enabled {
		s, err := intermagnet.New(cfg.INTERMAGNET, client, log)
		if err != nil {
			return nil, fmt.Errorf("intermagnet: %w", err)
		}
		sources = append(sources, s)
	}

	if cfg.SILSO.Enabled {
		s, err := silso.New(cfg.SILSO, client, log)
		if err != nil {
			return nil, fmt.Errorf("silso: %w", err)
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
