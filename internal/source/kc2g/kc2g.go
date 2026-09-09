// Package kc2g polls prop.kc2g.com for ionosonde measurements: foF2, the
// 3000 km MUF and its M(3000)F2 factor, the F2 peak height, the autoscaling
// confidence score behind each of those, and the effective sunspot number and
// flux derived from the whole network.
//
// # Licence
//
// The data prop.kc2g.com serves is licensed CC BY-NC-SA 4.0 and is
// NON-COMMERCIAL ONLY. The underlying measurements come from ionosonde
// operators worldwide, distributed through GIRO (Global Ionosphere Radio
// Observatory) and INGV, with the aggregation funded by WWROF. Attribution must
// be preserved, derivative works must carry the same licence, and the data must
// not be used commercially. Enabling this source is an acceptance of those
// terms, which is why it is disabled by default: a licence restriction should
// be something an operator opts into knowingly rather than inherits.
//
// # Polling policy
//
// prop.kc2g.com publishes no rate limit and sends neither Cache-Control nor
// ETag nor Last-Modified, so a conditional GET is impossible and there is no
// server-supplied cadence to obey. Self-throttling is the only control
// available, and Interval therefore clamps to a five-minute floor: the site
// states that a new map is generated every 15 minutes from measurements 5 to 20
// minutes old, so polling faster fetches the same document again and gains
// nothing.
//
// # Freshness
//
// The stations document is served fresh even when individual stations inside it
// have been silent for months, carrying each station's last known reading with
// its original timestamp. A station last heard from in March is still present in
// a September document. Every station is therefore checked against its own
// timestamp and dropped when it exceeds the configured maximum age. Publishing a
// six-month-old foF2 as the current one would put a confidently wrong number on
// a propagation dashboard, which is worse than publishing nothing.
package kc2g

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/RealDougEubanks/solarham/internal/config"
	"github.com/RealDougEubanks/solarham/internal/metric"
	"github.com/RealDougEubanks/solarham/internal/redact"
	"github.com/RealDougEubanks/solarham/internal/source"
)

// Name identifies this source in logs, metrics and health output.
const Name = "kc2g"

const (
	// defaultBaseURL is the public site. It is a field on Source rather than
	// used directly so tests can aim at an httptest server; nothing outside
	// this package can change it, so production traffic can only go here.
	defaultBaseURL = "https://prop.kc2g.com"

	// pathStations carries one object per ionosonde.
	pathStations = "/api/stations.json"

	// pathESSN carries the effective sunspot number and flux series.
	pathESSN = "/api/essn.json"

	// essnDays is how much history to ask for. Only the most recent point is
	// published, but the endpoint requires a range and the smallest useful one
	// leaves room for the newest bucket to be incomplete.
	essnDays = "3"

	// defaultInterval matches the site's own 15-minute map generation.
	defaultInterval = 15 * time.Minute

	// minInterval is the self-imposed floor described in the package comment.
	// A courtesy limit an operator can casually override is not a limit.
	minInterval = 5 * time.Minute

	defaultTimeout = 30 * time.Second

	// maxBodyBytes bounds each response read. The live stations document is
	// about 42 KB; 8 MB leaves generous room for the network to grow while
	// still stopping a broken or hostile upstream from exhausting memory.
	maxBodyBytes = 8 << 20

	// userAgent names the project and its repository so the site's operator can
	// identify this traffic and get in touch rather than having to block it.
	userAgent = "solarham/1.0 (+https://github.com/RealDougEubanks/solarham)"

	// baseRetryDelay is the first backoff step; it doubles per attempt.
	baseRetryDelay = 500 * time.Millisecond

	// bodyExcerptRunes bounds how much of an unexpected body reaches an error
	// message, since that message is destined for a log.
	bodyExcerptRunes = 160
)

// errPermanent marks a failure that retrying cannot fix: a wrong base URL, a
// withdrawn endpoint, or a refusal. Repeating those against a small
// volunteer-run site is abuse rather than resilience.
var errPermanent = errors.New("kc2g: permanent failure")

// Source polls prop.kc2g.com.
type Source struct {
	baseURL          string
	client           *http.Client
	interval         time.Duration
	retries          int
	minConfidence    float64
	maxAge           time.Duration
	stations         map[string]struct{}
	effectiveIndices bool
	log              *slog.Logger

	// now is injectable so the per-station freshness filter, which is the whole
	// point of this source, can be tested against a fixed fixture.
	now func() time.Time

	// mu guards the rate-limit cooldown. Poll fetches both endpoints
	// concurrently, so this state is shared.
	mu            sync.Mutex
	cooldown      time.Duration
	cooldownUntil time.Time
}

var _ source.Source = (*Source)(nil)

// New builds a source from validated configuration.
func New(cfg config.KC2G, log *slog.Logger) (*Source, error) {
	if log == nil {
		log = slog.Default()
	}

	base := strings.TrimRight(strings.TrimSpace(cfg.BaseURL), "/")
	if base == "" {
		base = defaultBaseURL
	}
	u, err := url.Parse(base)
	if err != nil {
		return nil, fmt.Errorf("kc2g: base URL %q is not a URL: %w", redact.URL(base), err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("kc2g: base URL must be http or https, got %q", u.Scheme)
	}
	if u.Host == "" {
		return nil, errors.New("kc2g: base URL has no host")
	}

	interval := cfg.Interval
	if interval <= 0 {
		interval = defaultInterval
	}
	if interval < minInterval {
		log.Warn("kc2g interval raised to the courtesy floor",
			"configured", interval, "using", minInterval)
		interval = minInterval
	}

	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	retries := cfg.Retries
	if retries < 0 {
		retries = 0
	}

	maxAge := cfg.MaxAge
	if maxAge <= 0 {
		return nil, errors.New("kc2g: MaxAge must be positive; " +
			"without it, stations silent for months would publish as current")
	}

	allow := make(map[string]struct{}, len(cfg.Stations))
	for _, code := range cfg.Stations {
		code = strings.ToUpper(strings.TrimSpace(code))
		if code != "" {
			allow[code] = struct{}{}
		}
	}

	return &Source{
		baseURL:          base,
		client:           &http.Client{Timeout: timeout},
		interval:         interval,
		retries:          retries,
		minConfidence:    cfg.MinConfidence,
		maxAge:           maxAge,
		stations:         allow,
		effectiveIndices: cfg.EffectiveIndices,
		log:              log.With("source", Name),
		now:              time.Now,
	}, nil
}

// Name identifies the source.
func (s *Source) Name() string { return Name }

// Interval is how often Poll should be called, never below the floor.
func (s *Source) Interval() time.Duration { return s.interval }

// Poll fetches both endpoints and converts what they return into samples.
//
// The two are fetched concurrently and independently: the ionosonde readings and
// the effective indices are unrelated quantities, and losing both because one
// endpoint is having a bad afternoon would be a self-inflicted outage. A poll
// that produced any samples at all returns them with a nil error and logs what
// failed, since the scheduler discards the batch of a failed poll.
func (s *Source) Poll(ctx context.Context) (metric.Batch, error) {
	batch := metric.Batch{Source: Name}

	if remaining, cooling := s.coolingDown(); cooling {
		return batch, fmt.Errorf("%w: cooling down, next attempt in %s",
			source.ErrRateLimited, remaining.Round(time.Second))
	}

	if err := ctx.Err(); err != nil {
		return batch, fmt.Errorf("kc2g: poll cancelled: %w", err)
	}

	var (
		wg                     sync.WaitGroup
		stationSamples         []metric.Sample
		essnSamples            []metric.Sample
		stationErr, essnErrVal error
	)

	wg.Add(1)
	go func() {
		defer wg.Done()
		stationSamples, stationErr = s.pollStations(ctx)
	}()

	if s.effectiveIndices {
		wg.Add(1)
		go func() {
			defer wg.Done()
			essnSamples, essnErrVal = s.pollESSN(ctx)
		}()
	}
	wg.Wait()

	batch.Fetched = s.now().UTC()
	batch.Samples = append(batch.Samples, stationSamples...)
	batch.Samples = append(batch.Samples, essnSamples...)

	err := errors.Join(stationErr, essnErrVal)
	if err == nil {
		return batch, nil
	}

	// Rate limiting is reported upwards even when something was salvaged, so
	// the scheduler backs off rather than continuing to knock on a door the
	// site has just closed.
	if errors.Is(err, source.ErrRateLimited) {
		return metric.Batch{Source: Name, Fetched: batch.Fetched}, err
	}

	if len(batch.Samples) > 0 {
		s.log.Warn("kc2g partially succeeded; publishing what was fetched",
			"samples", len(batch.Samples), "error", err)
		return batch, nil
	}
	return batch, err
}

// coolingDown reports whether requests are currently suppressed by a 429.
func (s *Source) coolingDown() (time.Duration, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.cooldownUntil.IsZero() {
		return 0, false
	}
	remaining := s.cooldownUntil.Sub(s.now())
	if remaining <= 0 {
		return 0, false
	}
	return remaining, true
}

// Rate-limit cooldown bounds. The site publishes no limit, so if it ever starts
// refusing requests the workable cadence has to be discovered at runtime.
const (
	minRateLimitCooldown = 5 * time.Minute
	maxRateLimitCooldown = 1 * time.Hour
)

// noteRateLimited records a refusal, preferring the server's own Retry-After
// over the locally doubled cooldown.
func (s *Source) noteRateLimited(retryAfter time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch {
	case retryAfter > 0:
		s.cooldown = retryAfter
	case s.cooldown < minRateLimitCooldown:
		s.cooldown = minRateLimitCooldown
	default:
		s.cooldown *= 2
	}
	if s.cooldown > maxRateLimitCooldown {
		s.cooldown = maxRateLimitCooldown
	}
	s.cooldownUntil = s.now().Add(s.cooldown)
	s.log.Warn("prop.kc2g.com refused a request as too frequent; pausing this source",
		"cooldown", s.cooldown)
}

// noteAccepted clears the cooldown after a request lands.
func (s *Source) noteAccepted() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cooldown = 0
	s.cooldownUntil = time.Time{}
}

// fetch performs one GET with bounded retries and returns the response body.
func (s *Source) fetch(ctx context.Context, endpoint string) ([]byte, error) {
	attempts := s.retries + 1
	var lastErr error

	for attempt := 1; attempt <= attempts; attempt++ {
		if err := ctx.Err(); err != nil {
			if lastErr != nil {
				return nil, fmt.Errorf("kc2g: %s abandoned after %d attempt(s): %w",
					redact.URL(endpoint), attempt-1, lastErr)
			}
			return nil, fmt.Errorf("kc2g: %s cancelled: %w", redact.URL(endpoint), err)
		}

		body, err := s.attempt(ctx, endpoint)
		if err == nil {
			s.noteAccepted()
			return body, nil
		}
		lastErr = err

		// Neither a refusal nor a missing endpoint improves on a second try.
		if errors.Is(err, errPermanent) || errors.Is(err, source.ErrRateLimited) {
			return nil, err
		}
		if attempt == attempts {
			break
		}

		delay := baseRetryDelay << (attempt - 1)
		s.log.Debug("kc2g request failed, retrying",
			"url", redact.URL(endpoint), "attempt", attempt, "of", attempts,
			"delay", delay, "error", err)

		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, fmt.Errorf("kc2g: %s abandoned after %d attempt(s): %w",
				redact.URL(endpoint), attempt, lastErr)
		case <-timer.C:
		}
	}

	return nil, fmt.Errorf("kc2g: %s failed after %d attempt(s): %w",
		redact.URL(endpoint), attempts, lastErr)
}

// attempt performs a single request.
func (s *Source) attempt(ctx context.Context, endpoint string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, redact.Error(err)
	}
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Accept", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		// http.Client returns *url.Error, which carries the request URL in its
		// message. Every error from the client goes through redact.Error.
		return nil, redact.Error(err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxBodyBytes))
		_ = resp.Body.Close()
	}()

	switch resp.StatusCode {
	case http.StatusTooManyRequests:
		retryAfter := parseRetryAfter(resp.Header.Get("Retry-After"), s.now())
		s.noteRateLimited(retryAfter)
		return nil, fmt.Errorf("%w: status 429 from %s", source.ErrRateLimited, redact.URL(endpoint))
	case http.StatusUnauthorized, http.StatusForbidden:
		return nil, fmt.Errorf("%w: status %d from %s, the endpoint refused this client",
			errPermanent, resp.StatusCode, redact.URL(endpoint))
	case http.StatusNotFound:
		return nil, fmt.Errorf("%w: status 404 from %s, check the configured base URL",
			errPermanent, redact.URL(endpoint))
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes))
		return nil, fmt.Errorf("status %d from %s: %s",
			resp.StatusCode, redact.URL(endpoint), excerpt(string(body)))
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes))
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", redact.URL(endpoint), redact.Error(err))
	}
	if len(body) == 0 {
		return nil, fmt.Errorf("empty body from %s", redact.URL(endpoint))
	}
	return body, nil
}

// parseRetryAfter reads the header in either of its two documented forms, the
// delay in seconds or an HTTP date.
func parseRetryAfter(value string, now time.Time) time.Duration {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	if secs, err := strconv.Atoi(value); err == nil {
		if secs <= 0 {
			return 0
		}
		return time.Duration(secs) * time.Second
	}
	if when, err := http.ParseTime(value); err == nil {
		if d := when.Sub(now); d > 0 {
			return d
		}
	}
	return 0
}

// excerpt trims a response body down to something loggable. The body is remote
// input on its way into a log line, so it is bounded and quoted rather than
// interpolated raw.
func excerpt(body string) string {
	body = strings.TrimSpace(body)
	if body == "" {
		return `""`
	}
	runes := []rune(body)
	if len(runes) > bodyExcerptRunes {
		body = string(runes[:bodyExcerptRunes]) + "..."
	}
	return strconv.Quote(body)
}

// emit appends a sample after checking it against its descriptor.
//
// A sample with a label count or an empty value that does not match its
// descriptor would be rejected by Prometheus at collection time and would
// silently corrupt tag sets elsewhere, so it is caught here rather than four
// layers away.
func (s *Source) emit(out []metric.Sample, sample metric.Sample) []metric.Sample {
	if err := sample.Validate(); err != nil {
		s.log.Warn("kc2g discarded an invalid sample", "error", err)
		return out
	}
	return append(out, sample)
}
