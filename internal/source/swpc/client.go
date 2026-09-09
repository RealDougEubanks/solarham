package swpc

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

	"github.com/RealDougEubanks/solarham/internal/redact"
	"github.com/RealDougEubanks/solarham/internal/source"
)

const (
	// userAgent identifies this exporter to SWPC. A descriptive agent naming
	// the project and its repository is what lets an upstream operator find
	// whoever is generating traffic they are unhappy about, instead of blocking
	// a Go default string that identifies nobody.
	userAgent = "solarham-exporter (+https://github.com/RealDougEubanks/solarham)"

	// maxBodyBytes bounds every response read. Most SWPC documents are a few
	// kilobytes; the D-RAP grid and the OVATION products are the large ones at
	// a few hundred kilobytes. 8 MiB is far above anything observed and far
	// below anything that could exhaust a container, which is the only property
	// that matters: reading an unbounded body from a third party is how a
	// broken upstream or a hijacked DNS answer turns into an OOM kill.
	maxBodyBytes = 8 << 20

	// retryBaseDelay is the first backoff step; it doubles per attempt.
	retryBaseDelay = 500 * time.Millisecond

	// maxRateLimitCooldown caps how long a 429 with an absurd Retry-After can
	// silence the client.
	maxRateLimitCooldown = 15 * time.Minute
)

// errNotModified reports a 304. It is internal: the tier converts an
// all-endpoint 304 into source.ErrNotModified, because a single unchanged
// document among several changed ones is not a fact the scheduler needs.
var errNotModified = errors.New("swpc: not modified")

// errPermanent marks a failure that retrying cannot fix — 401, 403 and 404.
// A path that does not exist will not exist on the second attempt either, and
// SWPC has a history of retiring whole directories, so this is a real case and
// not a theoretical one.
var errPermanent = errors.New("swpc: permanent failure")

// validators is what a previous response told us to replay on the next
// conditional request.
type validators struct {
	etag         string
	lastModified string
}

// client is the shared HTTP client and conditional-GET cache.
//
// All three tiers share one instance. They never request the same path as each
// other, but they do share a rate-limit cooldown, which is the point: if SWPC
// says "too many requests" the whole exporter should back off, not just the
// tier that happened to ask.
type client struct {
	http    *http.Client
	base    string
	retries int
	log     *slog.Logger

	mu sync.Mutex
	// cache is keyed by path, holding the validators from the last 200.
	cache map[string]validators
	// cooldownUntil suppresses requests after a 429.
	cooldownUntil time.Time
}

func newClient(base string, timeout time.Duration, retries int, log *slog.Logger) *client {
	return &client{
		http:    &http.Client{Timeout: timeout},
		base:    base,
		retries: retries,
		log:     log,
		cache:   make(map[string]validators),
	}
}

func (c *client) close() { c.http.CloseIdleConnections() }

// get fetches one path with bounded retries, returning errNotModified when the
// upstream answers a conditional request with 304.
func (c *client) get(ctx context.Context, path string) ([]byte, error) {
	if remaining, cooling := c.coolingDown(); cooling {
		return nil, fmt.Errorf("%w: next attempt in %s",
			source.ErrRateLimited, remaining.Round(time.Second))
	}

	attempts := c.retries + 1
	var lastErr error

	for attempt := 1; attempt <= attempts; attempt++ {
		if err := ctx.Err(); err != nil {
			if lastErr != nil {
				return nil, fmt.Errorf("swpc: %s abandoned after %d attempt(s): %w", path, attempt-1, lastErr)
			}
			return nil, fmt.Errorf("swpc: %s cancelled: %w", path, err)
		}

		body, err := c.attempt(ctx, path)
		if err == nil {
			return body, nil
		}
		lastErr = err

		// 304 is an answer, not a failure. Permanent failures and rate limits
		// are answers too: repeating either is load the upstream has already
		// declined to serve.
		if errors.Is(err, errNotModified) || errors.Is(err, errPermanent) ||
			errors.Is(err, source.ErrRateLimited) {
			return nil, err
		}
		if attempt == attempts {
			break
		}

		delay := retryBaseDelay << (attempt - 1)
		c.log.Debug("swpc request failed, retrying",
			"path", path, "attempt", attempt, "of", attempts, "delay", delay, "error", err)

		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, fmt.Errorf("swpc: %s abandoned after %d attempt(s): %w", path, attempt, lastErr)
		case <-timer.C:
		}
	}

	return nil, fmt.Errorf("swpc: %s failed after %d attempt(s): %w", path, attempts, lastErr)
}

// attempt performs one conditional request.
func (c *client) attempt(ctx context.Context, path string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+path, nil)
	if err != nil {
		return nil, redact.Error(err)
	}
	// Accept-Encoding is deliberately left to the transport, which adds gzip and
	// decompresses transparently. Setting it here would hand back a compressed
	// body every parser would then have to decode itself.
	req.Header.Set("User-Agent", userAgent)

	if v, ok := c.validatorsFor(path); ok {
		if v.etag != "" {
			req.Header.Set("If-None-Match", v.etag)
		}
		if v.lastModified != "" {
			req.Header.Set("If-Modified-Since", v.lastModified)
		}
	}

	resp, err := c.http.Do(req)
	if err != nil {
		// Every transport error goes through redact.Error before it is
		// returned, wrapped or logged. SWPC needs no credentials, so nothing
		// sensitive should be in this URL — but "should" is not a property
		// worth betting a log file on, and the rule is cheaper to apply
		// everywhere than to reason about per call site.
		return nil, redact.Error(err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxBodyBytes))
		_ = resp.Body.Close()
	}()

	switch resp.StatusCode {
	case http.StatusNotModified:
		return nil, errNotModified

	case http.StatusTooManyRequests:
		c.noteRateLimited(retryAfter(resp.Header.Get("Retry-After"), time.Now()))
		return nil, fmt.Errorf("%w: %s answered 429", source.ErrRateLimited, path)

	case http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound:
		return nil, fmt.Errorf("%w: %s answered %d", errPermanent, path, resp.StatusCode)
	}

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return nil, fmt.Errorf("%s answered %d", path, resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes))
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", path, redact.Error(err))
	}
	if len(body) == maxBodyBytes {
		// Truncated at the cap. Parsing half a document would produce
		// plausible-looking samples from an incomplete grid, which is worse
		// than no samples.
		return nil, fmt.Errorf("%s exceeded the %d byte response cap", path, maxBodyBytes)
	}

	c.storeValidators(path, validators{
		etag:         resp.Header.Get("ETag"),
		lastModified: resp.Header.Get("Last-Modified"),
	})
	return body, nil
}

// validatorsFor returns the cached validators for a path.
func (c *client) validatorsFor(path string) (validators, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	v, ok := c.cache[path]
	return v, ok
}

// storeValidators records what to replay next time. A response carrying neither
// validator drops the cache entry rather than keeping a stale one, so a
// conditional request is never made against a validator the upstream has
// stopped issuing.
func (c *client) storeValidators(path string, v validators) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if v.etag == "" && v.lastModified == "" {
		delete(c.cache, path)
		return
	}
	c.cache[path] = v
}

// coolingDown reports whether a 429 is still in effect.
func (c *client) coolingDown() (time.Duration, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cooldownUntil.IsZero() {
		return 0, false
	}
	remaining := time.Until(c.cooldownUntil)
	if remaining <= 0 {
		return 0, false
	}
	return remaining, true
}

// noteRateLimited suppresses further requests for d.
func (c *client) noteRateLimited(d time.Duration) {
	if d <= 0 {
		d = time.Minute
	}
	if d > maxRateLimitCooldown {
		d = maxRateLimitCooldown
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cooldownUntil = time.Now().Add(d)
	c.log.Warn("SWPC answered 429; pausing all SWPC polling", "cooldown", d)
}

// retryAfter interprets the Retry-After header, which is either a count of
// seconds or an HTTP date. An unparseable or absent value yields zero and the
// caller applies its own default.
func retryAfter(value string, now time.Time) time.Duration {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	if secs, err := strconv.Atoi(value); err == nil {
		if secs < 0 {
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
