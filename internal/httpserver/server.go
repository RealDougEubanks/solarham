// Package httpserver exposes the exporter's metrics and health endpoints.
//
// Health is split three ways deliberately, because orchestrators and external
// monitors ask different questions:
//
//   - /healthz  liveness: is the process alive? Used to decide whether to
//     restart the container. It must not fail because an upstream
//     or a backend is down, or an outage at nasa.gov would cause a
//     restart loop.
//   - /readyz   readiness: can this instance serve? Fails when no enabled
//     source has a fresh success, which is the condition an
//     orchestrator can actually act on by restarting or draining.
//     A single upstream outage is reported by /health and by
//     solar_source_stale, not here; see report.ready.
//   - /health   detail: a human- and monitor-readable breakdown of every
//     source and the configured sinks, returning 503 when unhealthy
//     so external monitors can alert on the status code alone.
//
// Freshness is judged per source and relative to that source's own schedule,
// because "recent" means something very different for a one-minute source than
// for one that publishes twice a day. A source that has never succeeded is not
// fresh, which is what makes a freshly started container correctly report
// itself unready until it has actually fetched something.
//
// Whether a stale source makes the instance unready is a separate question,
// answered by SOLARHAM_READY_REQUIRE_ALL and explained on report.ready.
package httpserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/RealDougEubanks/solarham/internal/config"
	"github.com/RealDougEubanks/solarham/internal/redact"
	"github.com/RealDougEubanks/solarham/internal/source"
)

// DefaultMetricsPath is used when a metrics handler is supplied without a path.
const DefaultMetricsPath = "/metrics"

// unknown stands in for a build stamp the main package did not supply, so
// /health reports honest placeholders rather than empty strings.
const unknown = "unknown"

// Overall health verdicts, reported by /health.
const (
	// StatusOK means every enabled source has a fresh success.
	StatusOK = "ok"
	// StatusDegraded means some sources are fresh and some are not. The
	// exporter is still producing data, just not all of it.
	StatusDegraded = "degraded"
	// StatusFail means no source is producing current data, including the
	// case where none is configured at all.
	StatusFail = "fail"
)

// BuildInfo identifies the running build.
type BuildInfo struct {
	Version   string `json:"version"`
	Commit    string `json:"commit"`
	BuildDate string `json:"buildDate"`
}

// Deps is everything the endpoints need from the rest of the process.
//
// It is deliberately made of plain types and functions rather than concrete
// packages. The Prometheus handler arrives as an http.Handler plus a path, so
// this package never imports prommetrics: that avoids an import cycle, keeps
// the sink swappable, and lets every route be tested with a two-line stub
// instead of a live registry.
//
// The two accessors are functions rather than snapshots because health is a
// live question. A struct captured at wiring time would report the state the
// process had at startup for the rest of its life.
type Deps struct {
	// MetricsHandler serves the scrape endpoint, or is nil when the Prometheus
	// sink is disabled. It must be a genuine nil interface: a typed nil
	// pointer stored in an interface is not nil and panics when called.
	MetricsHandler http.Handler

	// MetricsPath is where MetricsHandler is mounted. Empty means
	// DefaultMetricsPath.
	MetricsPath string

	// Statuses reports the current state of every enabled source. Nil means no
	// scheduler is running.
	Statuses func() []source.Status

	// SinkNames lists the configured sinks by name. /health publishes these so
	// an operator can confirm which backends this instance was built with —
	// names only, never endpoints.
	SinkNames func() []string

	// Build identifies the running binary.
	Build BuildInfo
}

// Server owns the HTTP listener and the routes.
type Server struct {
	cfg     config.HTTP
	deps    Deps
	log     *slog.Logger
	httpSrv *http.Server
}

// New builds the server and its routes.
//
// A nil MetricsHandler simply leaves the route unregistered, so an unknown path
// falls through to the 404 the index serves. That is the honest answer: the
// endpoint genuinely does not exist on an instance with Prometheus disabled,
// and registering a handler that always errors would make a disabled sink look
// like a broken one.
func New(cfg config.HTTP, deps Deps, log *slog.Logger) (*Server, error) {
	if cfg.Addr == "" {
		return nil, errors.New("httpserver: addr is required")
	}
	if log == nil {
		log = slog.Default()
	}

	if deps.Build.Version == "" {
		deps.Build.Version = unknown
	}
	if deps.Build.Commit == "" {
		deps.Build.Commit = unknown
	}
	if deps.Build.BuildDate == "" {
		deps.Build.BuildDate = unknown
	}

	s := &Server{cfg: cfg, deps: deps, log: log}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleLiveness)
	mux.HandleFunc("/readyz", s.handleReadiness)
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/", s.handleRoot)

	// The nil check is the whole reason MetricsHandler is an interface field
	// rather than a concrete sink: the caller must convert a nil *prommetrics
	// .Sink to a nil interface before it gets here, because a typed nil in an
	// interface is non-nil and panics on the first request.
	if deps.MetricsHandler != nil {
		mux.Handle(s.metricsPath(), deps.MetricsHandler)
	}

	s.httpSrv = &http.Server{
		Addr:    cfg.Addr,
		Handler: mux,
		// A slow-loris client holding a half-sent request header must not be
		// able to occupy a connection indefinitely on an endpoint that is
		// often exposed to a whole cluster network.
		ReadHeaderTimeout: cfg.ReadTimeout,
		ReadTimeout:       cfg.ReadTimeout,
	}
	return s, nil
}

// metricsPath is where the scrape endpoint is mounted.
//
// A path of "/" is refused in favour of the default, because registering the
// metrics handler at the root would shadow the index and turn every unknown
// path into a scrape.
func (s *Server) metricsPath() string {
	p := s.deps.MetricsPath
	if p == "" || p == "/" {
		return DefaultMetricsPath
	}
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	return p
}

// Handler exposes the routes for testing without binding a port.
func (s *Server) Handler() http.Handler { return s.httpSrv.Handler }

// Addr reports the configured listen address.
func (s *Server) Addr() string { return s.httpSrv.Addr }

// Start serves until ctx is cancelled, then shuts down gracefully.
//
// It returns nil on a cancelled context, so a SIGTERM produces a clean exit:
// exiting non-zero after a deliberate stop makes a supervisor restart a
// container that was stopped on purpose.
func (s *Server) Start(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() {
		s.log.Info("http server listening", "addr", s.httpSrv.Addr)
		if err := s.httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("httpserver: %w", err)
			return
		}
		errCh <- nil
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return s.Shutdown(context.Background())
	}
}

// Shutdown stops the server gracefully, bounded by cfg.ShutdownTimeout.
//
// The bound matters: an in-flight scrape from a monitor that has itself stalled
// must not be able to hold the process open past the supervisor's own kill
// timer, which would turn a clean stop into a SIGKILL.
func (s *Server) Shutdown(ctx context.Context) error {
	timeout := s.cfg.ShutdownTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	shutdownCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	if err := s.httpSrv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("httpserver: shutdown: %w", err)
	}
	return nil
}

// handleLiveness answers whether the process is running.
//
// It deliberately checks nothing else — not a source, not a sink, not the
// store. Liveness that depends on a backend turns an upstream outage into a
// restart loop, which is strictly worse than the outage: the data is already
// unavailable, and restarting the one process that could serve what it still
// holds makes it unavailable for longer.
func (s *Server) handleLiveness(w http.ResponseWriter, _ *http.Request) {
	writeText(w, http.StatusOK, "ok\n")
}

// handleReadiness answers whether this instance is serving current data.
//
// The body is plain text and names the stale sources, because the audience is
// whoever is looking at a failing probe: `kubectl describe` and `curl` both
// show it directly, and a name is all that is needed to know where to look
// next. Source names only — never a URL.
func (s *Server) handleReadiness(w http.ResponseWriter, _ *http.Request) {
	report := s.evaluate()
	if report.ready() {
		writeText(w, http.StatusOK, "ready\n")
		return
	}

	var b strings.Builder
	b.WriteString("not ready\n")
	switch {
	case len(report.sources) == 0:
		b.WriteString("no sources are enabled\n")
	case s.cfg.ReadyRequireAll:
		b.WriteString("policy: every source must be fresh\n")
	default:
		b.WriteString("policy: no source has a fresh success\n")
	}
	for _, sr := range report.sources {
		if sr.ready {
			continue
		}
		fmt.Fprintf(&b, "stale: %s (%s)\n", sr.status.Name, sr.reason)
	}
	writeText(w, http.StatusServiceUnavailable, b.String())
}

// sourceReport is one source's readiness verdict.
type sourceReport struct {
	status source.Status
	ready  bool
	reason string
}

// report is the whole readiness picture, shared by /readyz and /health so the
// two endpoints cannot drift apart. An external monitor alerting on /health's
// status code must reach the same verdict as an orchestrator probing /readyz.
type report struct {
	sources []sourceReport

	// requireAll selects the strict policy: every source must be fresh. It is
	// off by default; see ready for why.
	requireAll bool
}

// ready reports whether this instance can serve, under the configured policy.
//
// The useful question for a readiness probe is not "is everything perfect" but
// "is there an action the thing probing me can take". A readiness failure tells
// an orchestrator to stop routing here and send traffic to another instance.
//
// One upstream being down fails that test. Every replica polls the same public
// endpoints, so shifting traffic reaches an instance missing exactly the same
// source, and a restart does not bring celestrak.org back. Under ReadyAll a
// single third-party outage pins the endpoint at 503 with no remediation
// available -- and a probe that is red for reasons nobody can act on is one
// people learn to ignore.
//
// Every source stale is different. That points at something local and
// fixable: no egress, broken DNS, a clock so far off that every window looks
// expired. Restarting or draining plausibly helps, so that is what the default
// policy fails on.
//
// Per-source freshness has not been discarded, only moved to where it can be
// acted on: /health reports it per source, and solar_source_stale carries the
// exporter's own verdict into Prometheus, where an alert can name one source,
// one owner and one runbook.
func (r report) ready() bool {
	// No sources at all is not ready under any policy. An exporter with
	// nothing to poll can never produce data, and reporting it ready would
	// hide a misconfiguration behind a green probe.
	if len(r.sources) == 0 {
		return false
	}
	if r.requireAll {
		for _, sr := range r.sources {
			if !sr.ready {
				return false
			}
		}
		return true
	}
	for _, sr := range r.sources {
		if sr.ready {
			return true
		}
	}
	return false
}

// status is the overall verdict for /health.
func (r report) status() string {
	if len(r.sources) == 0 {
		return StatusFail
	}
	fresh := 0
	for _, sr := range r.sources {
		if sr.ready {
			fresh++
		}
	}
	switch {
	case fresh == len(r.sources):
		return StatusOK
	case fresh == 0:
		return StatusFail
	default:
		return StatusDegraded
	}
}

// evaluate judges every source's freshness.
//
// The scheduler has already computed Stale against three of each source's own
// intervals, which is almost always the right window given how widely the
// intervals differ. cfg.StaleAfter overrides it with one flat window for every
// source, for an operator who would rather have a single number to reason about
// than five.
func (s *Server) evaluate() report {
	if s.deps.Statuses == nil {
		return report{}
	}

	statuses := s.deps.Statuses()
	out := report{
		sources:    make([]sourceReport, 0, len(statuses)),
		requireAll: s.cfg.ReadyRequireAll,
	}
	for _, st := range statuses {
		out.sources = append(out.sources, s.judge(st))
	}
	return out
}

// judge decides whether one source counts as fresh, and says why not.
func (s *Server) judge(st source.Status) sourceReport {
	if st.LastSuccess.IsZero() {
		// Never having succeeded is its own case, distinct from having gone
		// stale, and it is the state a container is in for its first few
		// seconds. Reporting it as unready is the point.
		return sourceReport{status: st, reason: "no successful poll yet"}
	}

	window := st.StaleAfter
	if s.cfg.StaleAfter > 0 {
		window = s.cfg.StaleAfter
	}
	if window <= 0 {
		// Neither the scheduler nor the configuration supplied a window, so
		// fall back to whatever the scheduler already decided.
		if st.Stale {
			return sourceReport{status: st, reason: "reported stale by the scheduler"}
		}
		return sourceReport{status: st, ready: true}
	}

	if age := time.Since(st.LastSuccess); age > window {
		return sourceReport{
			status: st,
			reason: fmt.Sprintf("last success %s ago, window is %s", age.Round(time.Second), window),
		}
	}
	return sourceReport{status: st, ready: true}
}

// sourceHealth is one source as reported by /health.
//
// It is a projection of source.Status rather than the status itself, because
// LastError is free text from an upstream failure and must be scrubbed before
// it is published on an unauthenticated endpoint.
type sourceHealth struct {
	Name        string `json:"name"`
	Interval    string `json:"interval"`
	Status      string `json:"status"`
	Detail      string `json:"detail,omitempty"`
	LastSuccess string `json:"lastSuccess,omitempty"`
	LastFailure string `json:"lastFailure,omitempty"`
	LastError   string `json:"lastError,omitempty"`
	Samples     int    `json:"samples"`
	Successes   uint64 `json:"successes"`
	Failures    uint64 `json:"failures"`
}

// healthResponse is the detailed health document.
//
// It reports which backends are configured but deliberately carries no URLs,
// hostnames, tokens, connection strings or broker addresses. This endpoint is
// unauthenticated so external monitors can reach it, which means it must not
// become a reconnaissance tool: an attacker who learns the internal hostname of
// the InfluxDB this writes to has been handed the next target for free.
type healthResponse struct {
	Status  string         `json:"status"`
	Build   BuildInfo      `json:"build"`
	Sources []sourceHealth `json:"sources"`
	Sinks   []string       `json:"configuredSinks"`
}

// handleHealth reports the detail behind readiness.
func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	rep := s.evaluate()

	resp := healthResponse{
		Status:  rep.status(),
		Build:   s.deps.Build,
		Sources: make([]sourceHealth, 0, len(rep.sources)),
		Sinks:   s.sinkNames(),
	}

	for _, sr := range rep.sources {
		sh := sourceHealth{
			Name:      sr.status.Name,
			Interval:  sr.status.Interval,
			Status:    StatusOK,
			Samples:   sr.status.Samples,
			Successes: sr.status.Successes,
			Failures:  sr.status.Failures,
			LastError: scrub(sr.status.LastError),
		}
		if !sr.status.LastSuccess.IsZero() {
			sh.LastSuccess = sr.status.LastSuccess.UTC().Format(time.RFC3339)
		}
		if !sr.status.LastFailure.IsZero() {
			sh.LastFailure = sr.status.LastFailure.UTC().Format(time.RFC3339)
		}
		if !sr.ready {
			sh.Status = StatusFail
			sh.Detail = sr.reason
		}
		resp.Sources = append(resp.Sources, sh)
	}

	// /health answers on the strict rule regardless of the readiness policy:
	// anything short of every source fresh is 503.
	//
	// This is the deliberate division of labour between the two endpoints. An
	// orchestrator probing /readyz should only act when acting would help, so
	// it is lenient. An external uptime monitor wants to know the moment the
	// dataset stops being complete, and a human reads the body to see which
	// source it was, so /health stays strict. Loosening readiness therefore
	// costs no monitoring coverage -- it moves it to the endpoint whose job
	// it already was.
	code := http.StatusOK
	if rep.status() != StatusOK {
		code = http.StatusServiceUnavailable
	}
	writeJSON(w, code, resp)
}

// sinkNames returns the configured sink names, never nil so the JSON field is
// an empty array rather than null.
func (s *Server) sinkNames() []string {
	if s.deps.SinkNames == nil {
		return []string{}
	}
	names := s.deps.SinkNames()
	if names == nil {
		return []string{}
	}
	return names
}

// scrub removes anything URL- or credential-shaped from free text before it is
// published.
//
// The sources already pass their errors through redact.Error, which handles the
// *url.Error case precisely. This is the second line: LastError is free text
// that reached the status struct from anywhere in the process, and a single
// careless fmt.Errorf upstream is all it would take to put an endpoint on an
// unauthenticated page. Any whitespace-delimited token carrying a scheme or an
// "@" is replaced wholesale rather than parsed, because a token that cannot be
// parsed as a URL is exactly the sort of malformed value most likely to carry
// something sensitive.
func scrub(text string) string {
	if text == "" {
		return ""
	}
	fields := strings.Fields(text)
	for i, f := range fields {
		if strings.Contains(f, "://") || strings.Contains(f, "@") {
			fields[i] = redact.Placeholder
		}
	}
	return strings.Join(fields, " ")
}

// handleRoot serves a small index so someone who opens the port in a browser
// can find their way, and returns 404 for anything unrecognised.
//
// The metrics path is listed only when it is actually mounted, so the index
// never advertises an endpoint that would answer 404.
func (s *Server) handleRoot(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}

	endpoints := make([]string, 0, 4)
	if s.deps.MetricsHandler != nil {
		endpoints = append(endpoints, s.metricsPath())
	}
	endpoints = append(endpoints, "/healthz", "/readyz", "/health")

	writeJSON(w, http.StatusOK, map[string]any{
		"service":   "solarham-exporter",
		"build":     s.deps.Build,
		"endpoints": endpoints,
	})
}

// writeJSON emits a JSON response, and never caches: every one of these
// endpoints reports a live, instance-specific state that must not be served
// from an intermediary. A cached readiness answer is a wrong answer.
func writeJSON(w http.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		// The status line is already written, so this can only be logged.
		slog.Default().Debug("writing a health response failed", "error", err)
	}
}

// writeText emits a plain-text response under the same no-store rule.
func writeText(w http.ResponseWriter, code int, body string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	_, _ = w.Write([]byte(body))
}
