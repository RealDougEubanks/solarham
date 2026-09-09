// Package hamqsl polls the solar XML feed published by N0NBH at hamqsl.com.
//
// The feed is one amateur operator's server sitting behind a CDN, and it is the
// only place two of this exporter's metrics come from: the HF band ratings in
// <calculatedconditions> and the VHF phenomena in <calculatedvhfconditions>.
// Those are N0NBH's own model output and have no authoritative equivalent
// anywhere. Everything else the document carries — flux, K, A, sunspots, X-ray,
// particle flux, solar wind, aurora — NOAA SWPC publishes first-hand, in the
// public domain, at far better cadence, so the swpc source owns those series
// and this one deliberately does not emit them. Two sources publishing the same
// series from two upstreams that disagree is worse than one source publishing
// it once.
//
// Three things about this feed shape the code:
//
// It sends no ETag and no Last-Modified, so a conditional GET is impossible and
// self-throttling is the only way to be a good citizen. N0NBH's FAQ asks for
// hourly polling and records that his ISP shut him down once over exactly this
// load, so Interval is clamped to a floor rather than being freely
// configurable downwards.
//
// Several element names are misspelled or abbreviated in ways that are easy to
// guess wrong: the feed says "electonflux" (no 'r'), "geomagfield" (not
// "geomagneticfield"), "muffactor" (two f's) and "muf" (not "muff"). The Python
// exporter this replaces guessed all four wrong, and because ElementTree's
// findall returns an empty list for a name that does not exist, it never raised
// and those four fields were silently absent for the life of the container. The
// struct tags below match the wire exactly; corrected names appear only in the
// published metric vocabulary.
//
// Numeric values carry leading whitespace, and several elements are routinely
// empty or carry a literal placeholder such as "NoRpt" or "No Report". Absent
// is not zero: those produce no sample at all.
package hamqsl

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/RealDougEubanks/solarham/internal/config"
	"github.com/RealDougEubanks/solarham/internal/metric"
	"github.com/RealDougEubanks/solarham/internal/redact"
	"github.com/RealDougEubanks/solarham/internal/source"
)

// Name identifies this source in logs, metrics and the health endpoints.
const Name = "hamqsl"

const (
	// defaultURL is the feed itself. It is a field on Source rather than used
	// directly so tests can aim at an httptest server, but nothing outside this
	// package can change it.
	defaultURL = "https://www.hamqsl.com/solarxml.php"

	// defaultInterval matches what N0NBH asks for in his FAQ.
	defaultInterval = time.Hour

	// MinInterval is the courtesy floor. N0NBH has publicly stated he was shut
	// down by his ISP once over polling load, and his own stated regeneration
	// cadences are three hours for flux/A/K/sunspots, one hour for X-ray and
	// particle flux, and thirty minutes for VHF conditions. Polling faster than
	// this cannot produce a new observation; it only produces load.
	//
	// A courtesy limit an operator can casually override is not a limit, so
	// this is enforced in New rather than documented and hoped for.
	MinInterval = 15 * time.Minute

	// defaultTimeout bounds a single request. The document is under 2 KB, so a
	// request still running after this is a hung connection, not a slow one.
	defaultTimeout = 15 * time.Second

	// defaultRetries is the number of retries after the first attempt, so the
	// default is three attempts in total.
	defaultRetries = 2

	// maxBodyBytes bounds the response read. The real document is about 1.6 KB;
	// a megabyte is three orders of magnitude of headroom and still small
	// enough that a misbehaving or hijacked origin cannot exhaust memory.
	maxBodyBytes = 1 << 20

	// userAgent identifies this client to the feed's operator. This is a
	// courtesy feed run by one person who has had to police its load before, so
	// he should be able to tell who is calling and where to complain.
	userAgent = "solarham-exporter (+https://github.com/RealDougEubanks/solarham)"

	// baseRetryDelay is the first backoff step; it doubles per attempt.
	baseRetryDelay = 500 * time.Millisecond
)

// ErrPermanent marks a failure retrying cannot fix — a wrong URL, a removed
// endpoint, a refusal. Repeating such a request against a small volunteer-run
// server is abuse rather than resilience.
var ErrPermanent = errors.New("hamqsl: permanent failure")

// Source polls the hamqsl.com solar XML feed.
type Source struct {
	url      string
	interval time.Duration
	retries  int
	client   *http.Client
	log      *slog.Logger

	// warnedBadTimestamp makes the unparseable-<updated> warning fire once for
	// the life of the process. The feed regenerates hourly at best, so a
	// malformed timestamp would otherwise repeat the same warning every poll
	// for as long as the fault persists.
	warnedBadTimestamp sync.Once

	// mu guards the rate-limit cooldown. Poll is called from the scheduler's
	// per-source goroutine, but /health may read the source concurrently, so
	// this state is shared.
	mu            sync.Mutex
	cooldownUntil time.Time
}

// compile-time proof the source satisfies the interface the scheduler polls.
var _ source.Source = (*Source)(nil)

// New builds a source from validated configuration.
//
// An interval below MinInterval is clamped with a warning rather than rejected
// as a configuration error. Both are defensible, and the choice is deliberate:
// refusing to start would let one over-eager number in an environment file
// take down an exporter that also serves four other sources, whereas clamping
// keeps the process up, keeps the feed protected, and tells the operator
// exactly what happened in a line they will see at startup.
func New(cfg config.Hamqsl, log *slog.Logger) (*Source, error) {
	if log == nil {
		log = slog.Default()
	}

	url := strings.TrimSpace(cfg.URL)
	if url == "" {
		url = defaultURL
	}

	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = defaultTimeout
	}

	// Zero is "unset", not "never retry", so a zero-valued config gets the
	// documented default. An operator who genuinely wants a single attempt sets
	// a negative value, which is the only way to distinguish that intent from
	// an absent setting on an int field.
	retries := cfg.Retries
	switch {
	case retries < 0:
		retries = 0
	case retries == 0:
		retries = defaultRetries
	}

	interval := cfg.Interval
	if interval <= 0 {
		interval = defaultInterval
	}
	if interval < MinInterval {
		log.Warn("hamqsl poll interval raised to the courtesy floor; "+
			"the feed's operator asks for hourly polling and has been shut down by his ISP over this load",
			"configured", interval, "floor", MinInterval)
		interval = MinInterval
	}

	return &Source{
		url:      url,
		interval: interval,
		retries:  retries,
		client:   &http.Client{Timeout: timeout},
		log:      log.With("source", Name),
	}, nil
}

// Name identifies the source. It is stable because it becomes a metric label.
func (s *Source) Name() string { return Name }

// Interval is how often the scheduler should call Poll. It is never below
// MinInterval, whatever the configuration said.
func (s *Source) Interval() time.Duration { return s.interval }

// Poll fetches the feed once and converts it into samples.
//
// A document whose optional elements are all empty yields a batch with fewer
// samples, and in the limit none at all. That is a success: the feed genuinely
// had nothing to say, and inventing zeros would be a lie with a timestamp on
// it.
func (s *Source) Poll(ctx context.Context) (metric.Batch, error) {
	if remaining, cooling := s.coolingDown(); cooling {
		// The server has already said it does not want this request yet, so
		// making it anyway is a refusal we asked for.
		return metric.Batch{Source: Name}, fmt.Errorf("%w: next attempt in %s",
			source.ErrRateLimited, remaining.Round(time.Second))
	}

	body, fetched, err := s.fetch(ctx)
	if err != nil {
		return metric.Batch{Source: Name}, err
	}

	doc, err := parse(body)
	if err != nil {
		return metric.Batch{Source: Name}, err
	}

	return s.batch(doc, fetched), nil
}

// batch converts a parsed document into the samples this source publishes.
func (s *Source) batch(doc *solarData, fetched time.Time) metric.Batch {
	observed, ok := parseUpdated(doc.Updated)
	if !ok {
		// A missing or malformed timestamp is not worth failing a poll over:
		// the band conditions in the same document are still perfectly usable.
		// Falling back to receipt time is the closest honest approximation
		// available, and it is logged once so the discrepancy is not silent.
		s.warnedBadTimestamp.Do(func() {
			s.log.Warn("hamqsl <updated> could not be parsed; stamping samples with the receipt time",
				"updated", strings.TrimSpace(doc.Updated))
		})
		observed = fetched
	}

	b := metric.Batch{Source: Name, Fetched: fetched}
	b.Samples = make([]metric.Sample, 0, 32)

	add := func(desc *metric.Descriptor, value float64, labels ...string) {
		sample := metric.Sample{Desc: desc, Labels: labels, Value: value, Time: observed}
		if err := sample.Validate(); err != nil {
			// A source that emits a malformed sample would corrupt tag sets in
			// InfluxDB and be rejected by Prometheus at collection time. Drop
			// it here, loudly, rather than let it travel.
			s.log.Warn("hamqsl produced an invalid sample; dropping it", "error", err)
			return
		}
		b.Samples = append(b.Samples, sample)
	}

	// HF band conditions. The ordinal exists so the value can be graphed and
	// alerted on; the paired _info sample preserves N0NBH's own wording, which
	// is what an operator actually recognises.
	for _, band := range doc.Bands {
		name := strings.TrimSpace(band.Name)
		period := strings.TrimSpace(band.Time)
		condition := strings.TrimSpace(band.Condition)
		if name == "" || period == "" || condition == "" {
			continue
		}
		add(metric.BandConditionInfo, 1, name, period, condition)
		if ordinal, known := bandOrdinal(condition); known {
			add(metric.BandCondition, ordinal, name, period)
		} else {
			// Inventing an ordinal for a word we do not recognise would put a
			// wrong number on a graph, which is worse than putting none there.
			s.log.Debug("hamqsl band condition not recognised; publishing the info sample only",
				"band", name, "period", period, "condition", condition)
		}
	}

	// VHF phenomena: aurora and sporadic-E openings, per region.
	for _, ph := range doc.VHF {
		name := strings.TrimSpace(ph.Name)
		location := strings.TrimSpace(ph.Location)
		condition := strings.TrimSpace(ph.Condition)
		if name == "" || location == "" || condition == "" {
			continue
		}
		add(metric.VHFConditionInfo, 1, name, location, condition)
		if ordinal, known := vhfOrdinal(condition); known {
			add(metric.VHFCondition, ordinal, name, location)
		} else {
			s.log.Debug("hamqsl VHF condition not recognised; publishing the info sample only",
				"phenomenon", name, "location", location, "condition", condition)
		}
	}

	// Background noise, reported as an S-meter range such as "S2-S3".
	if lower, upper, ok := parseSignalNoise(doc.SignalNoise); ok {
		add(metric.SignalNoise, lower, "min")
		add(metric.SignalNoise, upper, "max")
	}

	// Geomagnetic field state is genuinely categorical — QUIET, UNSETTLD,
	// STORM — so it is info only. The numeric equivalent is SWPC's G-scale,
	// which the swpc source publishes.
	if state := strings.TrimSpace(doc.GeomagField); state != "" && !isPlaceholder(state) {
		add(metric.GeomagneticFieldInfo, 1, state)
	}

	return b
}

// fetch retrieves the document with bounded retries, returning the body and the
// time the response was received.
//
// Backoff doubles from a short base. There is no point retrying for long: the
// feed regenerates at most hourly, so a fetch that cannot succeed within a few
// seconds may as well wait for the next scheduled poll.
func (s *Source) fetch(ctx context.Context) ([]byte, time.Time, error) {
	attempts := s.retries + 1
	var lastErr error

	for attempt := 1; attempt <= attempts; attempt++ {
		if err := ctx.Err(); err != nil {
			if lastErr != nil {
				return nil, time.Time{}, fmt.Errorf("hamqsl: abandoned after %d attempt(s): %w", attempt-1, lastErr)
			}
			return nil, time.Time{}, fmt.Errorf("hamqsl: cancelled: %w", err)
		}

		body, received, err := s.attempt(ctx)
		if err == nil {
			return body, received, nil
		}
		lastErr = err

		// A permanent failure and a rate limit are both unretryable, for the
		// same reason from opposite directions: one will fail identically
		// every time, the other has explicitly been told "not yet".
		if errors.Is(err, ErrPermanent) || errors.Is(err, source.ErrRateLimited) {
			return nil, time.Time{}, err
		}
		if attempt == attempts {
			break
		}

		delay := baseRetryDelay << (attempt - 1)
		s.log.Debug("hamqsl request failed, retrying",
			"attempt", attempt, "of", attempts, "delay", delay, "error", err)

		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, time.Time{}, fmt.Errorf("hamqsl: abandoned after %d attempt(s): %w", attempt, lastErr)
		case <-timer.C:
		}
	}

	return nil, time.Time{}, fmt.Errorf("hamqsl: failed after %d attempt(s): %w", attempts, lastErr)
}

// attempt performs a single request.
//
// There is deliberately no conditional GET here. The feed sends neither ETag
// nor Last-Modified — it is served through Sucuri, which answers most requests
// from cache with a byte-identical document but no validators — so there is
// nothing to replay as If-None-Match or If-Modified-Since and no way to earn a
// 304. Interval clamping is the only load control available.
func (s *Source) attempt(ctx context.Context) ([]byte, time.Time, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.url, nil)
	if err != nil {
		return nil, time.Time{}, redact.Error(err)
	}
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Accept", "application/xml, text/xml;q=0.9, */*;q=0.1")

	resp, err := s.client.Do(req)
	if err != nil {
		// http.Client returns *url.Error, which quotes the full request URL.
		// Every error from the client goes through redact.Error before it is
		// wrapped, returned or logged.
		return nil, time.Time{}, redact.Error(err)
	}
	received := time.Now().UTC()
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxBodyBytes))
		_ = resp.Body.Close()
	}()

	if err := s.classify(resp); err != nil {
		return nil, time.Time{}, err
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes))
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("hamqsl: reading response: %w", redact.Error(err))
	}
	if len(body) == 0 {
		return nil, time.Time{}, errors.New("hamqsl: empty response body")
	}
	return body, received, nil
}

// classify turns a status code into the right kind of error, or nil.
func (s *Source) classify(resp *http.Response) error {
	code := resp.StatusCode

	switch {
	case code == http.StatusTooManyRequests:
		wait := retryAfter(resp.Header.Get("Retry-After"))
		s.noteRateLimited(wait)
		return fmt.Errorf("%w: status 429, pausing for %s",
			source.ErrRateLimited, wait.Round(time.Second))

	case code == http.StatusUnauthorized, code == http.StatusForbidden, code == http.StatusNotFound:
		// These name a durable problem: the feed moved, or it is refusing this
		// client. Retrying cannot help and only adds load to a server whose
		// operator has already had to police it once.
		return fmt.Errorf("%w: status %d for %s", ErrPermanent, code, redact.URL(s.url))

	case code == http.StatusRequestTimeout:
		// 408 is the server saying it gave up waiting, which the next attempt
		// may well survive.
		return fmt.Errorf("hamqsl: status %d", code)

	case code >= 400 && code < 500:
		// Every other 4xx is a problem with the request we are making, and the
		// request will be identical next time.
		return fmt.Errorf("%w: status %d", ErrPermanent, code)

	case code >= 500:
		// A CDN hiccup or an origin restart. Worth another try.
		return fmt.Errorf("hamqsl: status %d", code)

	case code < 200 || code > 299:
		return fmt.Errorf("hamqsl: unexpected status %d", code)
	}
	return nil
}

// Rate-limit cooldown bounds. The feed publishes no rate limit, so a refusal
// without a Retry-After header gives no guidance at all; one interval's worth
// of silence is the conservative reading of "you are asking too often".
const (
	defaultRateLimitCooldown = time.Hour
	maxRateLimitCooldown     = 6 * time.Hour
)

// retryAfter interprets a Retry-After header, which may be delta-seconds or an
// HTTP date, falling back to the default cooldown when it is absent or
// unreadable.
func retryAfter(header string) time.Duration {
	header = strings.TrimSpace(header)
	if header == "" {
		return defaultRateLimitCooldown
	}
	if secs, err := strconv.Atoi(header); err == nil {
		if secs <= 0 {
			return defaultRateLimitCooldown
		}
		return capCooldown(time.Duration(secs) * time.Second)
	}
	if when, err := http.ParseTime(header); err == nil {
		if d := time.Until(when); d > 0 {
			return capCooldown(d)
		}
	}
	return defaultRateLimitCooldown
}

// capCooldown bounds a cooldown so a hostile or mistaken Retry-After cannot
// silence the source for days.
func capCooldown(d time.Duration) time.Duration {
	if d > maxRateLimitCooldown {
		return maxRateLimitCooldown
	}
	return d
}

// coolingDown reports whether requests are currently suppressed.
func (s *Source) coolingDown() (time.Duration, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.cooldownUntil.IsZero() {
		return 0, false
	}
	remaining := time.Until(s.cooldownUntil)
	if remaining <= 0 {
		return 0, false
	}
	return remaining, true
}

// noteRateLimited suppresses requests until the upstream is willing again.
func (s *Source) noteRateLimited(wait time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.cooldownUntil = time.Now().Add(wait)
	s.log.Warn("hamqsl refused the request as too frequent; pausing this source", "cooldown", wait)
}

// Close releases resources. The source holds only an http.Client, so there is
// nothing to release, but idle connections are dropped so a stopped exporter
// leaves no sockets behind.
func (s *Source) Close() error {
	s.client.CloseIdleConnections()
	return nil
}
