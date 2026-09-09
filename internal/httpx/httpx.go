// Package httpx is the shared HTTP client every source fetches through.
//
// It exists because politeness has to be enforced somewhere central. Nineteen
// sources spread across a dozen hosts, each implementing its own retry and its
// own rate limiting, produces nineteen chances to get it wrong — and the
// failure mode is not a broken dashboard, it is a volunteer's server being
// hammered by our exporter. Several of these publishers have asked, in writing,
// to be polled gently; one has been shut down by his ISP over exactly this.
//
// So this package owns: per-host request spacing shared across all sources,
// conditional GETs, bounded retries with permanent-versus-transient
// classification, honouring Retry-After, response size limits, and keeping
// credentials out of errors.
package httpx

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/RealDougEubanks/solarham/internal/redact"
)

// UserAgent identifies this exporter to every upstream.
//
// Two of the hosts we fetch from return 403 to clients that do not look like a
// browser, and several publishers explicitly ask to be able to identify and
// contact automated clients. A bare Go default user agent is both rude and, in
// places, non-functional.
const UserAgent = "solarham-exporter/1.0 (+https://github.com/RealDougEubanks/solarham)"

// DefaultMaxBody bounds a response body. A misbehaving or hostile origin must
// not be able to exhaust memory.
const DefaultMaxBody = 8 << 20 // 8 MiB

// Sentinel errors for classifying a failed fetch. Every source can branch on
// these rather than re-deriving the rules from status codes.
var (
	// ErrPermanent means retrying will not help: a 401, 403, 404 or a
	// malformed request. The caller should surface it loudly rather than
	// quietly retrying forever.
	ErrPermanent = errors.New("httpx: permanent failure")

	// ErrRateLimited means the upstream refused because we asked too often.
	ErrRateLimited = errors.New("httpx: rate limited")

	// ErrNotModified means a conditional request was answered 304. The
	// upstream data is current and we already have it.
	ErrNotModified = errors.New("httpx: not modified")
)

// HostPolicy is the politeness contract for one host.
type HostPolicy struct {
	// MinInterval is the minimum spacing between requests to this host,
	// across every source that shares it. Zero means unlimited.
	MinInterval time.Duration

	// Burst allows this many requests to be made back to back before the
	// spacing applies. A startup that fetches four documents from one host at
	// once is reasonable; doing so every minute is not.
	Burst int
}

// Client is a rate-limited, retrying, conditional-GET HTTP client.
//
// It is safe for concurrent use and is intended to be shared by every source:
// that sharing is the point, because a per-source limiter would let ten
// sources each politely make one request per second to the same host and
// collectively make ten.
type Client struct {
	http    *http.Client
	log     *slog.Logger
	maxBody int64

	mu       sync.Mutex
	limiters map[string]*hostLimiter
	policies map[string]HostPolicy
	fallback HostPolicy

	cacheMu sync.RWMutex
	cache   map[string]validators

	clock func() time.Time
	sleep func(context.Context, time.Duration) error
}

// validators are the conditional-GET tokens from a previous response.
type validators struct {
	etag         string
	lastModified string
}

// Options configure a Client.
type Options struct {
	// Timeout bounds each individual request.
	Timeout time.Duration

	// MaxBody bounds each response body. Zero uses DefaultMaxBody.
	MaxBody int64

	// Policies sets per-host politeness, keyed by hostname.
	Policies map[string]HostPolicy

	// Fallback applies to any host with no explicit policy. A conservative
	// default is deliberate: an unlisted host is one nobody has thought about
	// yet, and the safe assumption is that it is somebody's hobby server.
	Fallback HostPolicy
}

// New returns a client. A nil logger falls back to the default.
func New(opts Options, log *slog.Logger) *Client {
	if log == nil {
		log = slog.Default()
	}
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	maxBody := opts.MaxBody
	if maxBody <= 0 {
		maxBody = DefaultMaxBody
	}
	fallback := opts.Fallback
	if fallback.MinInterval <= 0 {
		fallback.MinInterval = time.Second
	}
	if fallback.Burst <= 0 {
		fallback.Burst = 2
	}

	policies := make(map[string]HostPolicy, len(opts.Policies))
	for host, p := range opts.Policies {
		policies[host] = p
	}

	return &Client{
		http:     &http.Client{Timeout: timeout},
		log:      log,
		maxBody:  maxBody,
		limiters: make(map[string]*hostLimiter),
		policies: policies,
		fallback: fallback,
		cache:    make(map[string]validators),
		clock:    time.Now,
		sleep:    sleepCtx,
	}
}

// Request describes one fetch.
type Request struct {
	// URL to fetch.
	URL string

	// Conditional enables ETag and If-Modified-Since replay for this URL.
	// Leave it off for upstreams that send no validators; sending
	// If-None-Match to a server that ignores it costs a header and buys
	// nothing, but tracking a validator that never arrives is confusing.
	Conditional bool

	// Headers are added to the request. Values here may be credentials, so
	// they are never logged.
	Headers map[string]string

	// Retries is the number of retries after the first attempt. Zero means a
	// single attempt.
	Retries int

	// MaxBody overrides the client default for this request.
	MaxBody int64
}

// Response is a completed fetch.
type Response struct {
	Body         []byte
	StatusCode   int
	Header       http.Header
	LastModified time.Time
}

// Get fetches a URL, respecting the host's rate limit, retrying transient
// failures, and returning ErrNotModified when a conditional request is
// answered 304.
func (c *Client) Get(ctx context.Context, req Request) (*Response, error) {
	parsed, err := url.Parse(req.URL)
	if err != nil {
		return nil, fmt.Errorf("%w: unparseable url", ErrPermanent)
	}
	host := parsed.Hostname()
	if host == "" {
		return nil, fmt.Errorf("%w: url has no host", ErrPermanent)
	}

	attempts := req.Retries + 1
	if attempts < 1 {
		attempts = 1
	}

	const baseBackoff = 500 * time.Millisecond
	var lastErr error

	for attempt := 1; attempt <= attempts; attempt++ {
		if attempt > 1 {
			delay := baseBackoff << (attempt - 2)
			if delay > 30*time.Second {
				delay = 30 * time.Second
			}
			if err := c.sleep(ctx, delay); err != nil {
				return nil, err
			}
		}

		// The rate limiter is waited on inside the retry loop, so a retry is
		// spaced by the host policy as well as the backoff.
		if err := c.limiterFor(host).wait(ctx, c.clock, c.sleep); err != nil {
			return nil, err
		}

		resp, err := c.attempt(ctx, req, parsed)
		if err == nil {
			return resp, nil
		}
		lastErr = err

		// Permanent failures, rate limits and 304s are all final: retrying a
		// 404 just repeats the load, and retrying a 429 is precisely the
		// behaviour the upstream asked us to stop.
		if errors.Is(err, ErrPermanent) || errors.Is(err, ErrRateLimited) || errors.Is(err, ErrNotModified) {
			return nil, err
		}
	}

	return nil, lastErr
}

// attempt performs one HTTP request.
func (c *Client) attempt(ctx context.Context, req Request, parsed *url.URL) (*Response, error) {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, req.URL, nil)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrPermanent, err)
	}
	httpReq.Header.Set("User-Agent", UserAgent)

	// Accept-Encoding is deliberately NOT set here.
	//
	// Go's transport adds "Accept-Encoding: gzip" itself and transparently
	// decompresses the response — but only while the caller has not set that
	// header. Setting it manually silently transfers responsibility for
	// decoding to us, so every body arrives still compressed and every parser
	// fails on the gzip magic byte 0x1f. Requests are still compressed; the
	// transport simply owns both halves of the bargain.
	//
	// A source that genuinely wants to handle its own encoding can still set
	// the header through Headers below, and then owns the decoding too.
	for k, v := range req.Headers {
		httpReq.Header.Set(k, v)
	}

	if req.Conditional {
		if v, ok := c.validatorsFor(req.URL); ok {
			if v.etag != "" {
				httpReq.Header.Set("If-None-Match", v.etag)
			}
			if v.lastModified != "" {
				httpReq.Header.Set("If-Modified-Since", v.lastModified)
			}
		}
	}

	resp, err := c.http.Do(httpReq)
	if err != nil {
		return nil, redact.Error(err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
		_ = resp.Body.Close()
	}()

	switch {
	case resp.StatusCode == http.StatusNotModified:
		return nil, ErrNotModified

	case resp.StatusCode == http.StatusTooManyRequests:
		c.noteRateLimit(parsed.Hostname(), resp)
		return nil, fmt.Errorf("%w: %s answered 429", ErrRateLimited, parsed.Hostname())

	case resp.StatusCode == http.StatusUnauthorized,
		resp.StatusCode == http.StatusForbidden,
		resp.StatusCode == http.StatusNotFound,
		resp.StatusCode == http.StatusGone:
		return nil, fmt.Errorf("%w: %s returned %d", ErrPermanent, redact.URL(req.URL), resp.StatusCode)

	case resp.StatusCode >= 400 && resp.StatusCode < 500 &&
		resp.StatusCode != http.StatusRequestTimeout:
		return nil, fmt.Errorf("%w: %s returned %d", ErrPermanent, redact.URL(req.URL), resp.StatusCode)

	case resp.StatusCode >= 300:
		return nil, fmt.Errorf("%s returned %d", redact.URL(req.URL), resp.StatusCode)
	}

	maxBody := req.MaxBody
	if maxBody <= 0 {
		maxBody = c.maxBody
	}

	// Read one byte past the limit so an oversized body is detected rather
	// than silently truncated into a parse error.
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBody+1))
	if err != nil {
		return nil, redact.Error(err)
	}
	if int64(len(body)) > maxBody {
		return nil, fmt.Errorf("%s returned more than %d bytes", redact.URL(req.URL), maxBody)
	}

	if req.Conditional {
		c.storeValidators(req.URL, resp.Header)
	}

	out := &Response{
		Body:       body,
		StatusCode: resp.StatusCode,
		Header:     resp.Header.Clone(),
	}
	if lm := resp.Header.Get("Last-Modified"); lm != "" {
		if t, err := http.ParseTime(lm); err == nil {
			out.LastModified = t
		}
	}
	return out, nil
}

// noteRateLimit extends the host's spacing when it tells us to slow down.
//
// Honouring Retry-After matters more than it looks: at least one upstream here
// sends 429 with no Retry-After and needs several minutes of complete silence
// before it recovers, so a client that retries on its own schedule stays
// locked out indefinitely.
func (c *Client) noteRateLimit(host string, resp *http.Response) {
	cooldown := 5 * time.Minute

	if ra := resp.Header.Get("Retry-After"); ra != "" {
		if secs, err := strconv.Atoi(ra); err == nil && secs > 0 {
			cooldown = time.Duration(secs) * time.Second
		} else if t, err := http.ParseTime(ra); err == nil {
			if d := time.Until(t); d > 0 {
				cooldown = d
			}
		}
	}
	if cooldown > time.Hour {
		cooldown = time.Hour
	}

	c.limiterFor(host).penalise(c.clock().Add(cooldown))
	c.log.Warn("upstream rate limited us; pausing requests to this host",
		"host", host, "cooldown", cooldown)
}

// limiterFor returns the shared limiter for a host, creating it on first use.
func (c *Client) limiterFor(host string) *hostLimiter {
	c.mu.Lock()
	defer c.mu.Unlock()

	if l, ok := c.limiters[host]; ok {
		return l
	}
	policy, ok := c.policies[host]
	if !ok {
		policy = c.fallback
	}
	if policy.Burst <= 0 {
		policy.Burst = 1
	}
	l := &hostLimiter{policy: policy, tokens: policy.Burst}
	c.limiters[host] = l
	return l
}

func (c *Client) validatorsFor(u string) (validators, bool) {
	c.cacheMu.RLock()
	defer c.cacheMu.RUnlock()
	v, ok := c.cache[u]
	return v, ok
}

func (c *Client) storeValidators(u string, h http.Header) {
	etag := h.Get("ETag")
	lm := h.Get("Last-Modified")
	if etag == "" && lm == "" {
		return
	}
	c.cacheMu.Lock()
	defer c.cacheMu.Unlock()
	c.cache[u] = validators{etag: etag, lastModified: lm}
}

// hostLimiter spaces requests to one host.
//
// This is a token bucket rather than a plain minimum gap so that a burst at
// startup is allowed — fetching a tier's four documents at once is reasonable
// — while the sustained rate stays within policy.
type hostLimiter struct {
	mu     sync.Mutex
	policy HostPolicy

	tokens   int
	lastFill time.Time

	// until is a hard pause, set when the host returns 429.
	until time.Time
}

// wait blocks until this host may be called again.
func (l *hostLimiter) wait(ctx context.Context, clock func() time.Time, sleep func(context.Context, time.Duration) error) error {
	for {
		delay := l.reserve(clock())
		if delay <= 0 {
			return nil
		}
		if err := sleep(ctx, delay); err != nil {
			return err
		}
	}
}

// reserve takes a token if one is available, or reports how long to wait.
func (l *hostLimiter) reserve(now time.Time) time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()

	if now.Before(l.until) {
		return l.until.Sub(now)
	}
	if l.policy.MinInterval <= 0 {
		return 0
	}

	if l.lastFill.IsZero() {
		l.lastFill = now
	}

	// Refill by elapsed time.
	elapsed := now.Sub(l.lastFill)
	if gained := int(elapsed / l.policy.MinInterval); gained > 0 {
		l.tokens = min(l.tokens+gained, l.policy.Burst)
		l.lastFill = l.lastFill.Add(time.Duration(gained) * l.policy.MinInterval)
	}

	if l.tokens > 0 {
		l.tokens--
		return 0
	}
	return l.policy.MinInterval - (now.Sub(l.lastFill) % l.policy.MinInterval)
}

// penalise pauses this host until t.
func (l *hostLimiter) penalise(t time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if t.After(l.until) {
		l.until = t
	}
}

// sleepCtx sleeps unless the context is cancelled first.
func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
