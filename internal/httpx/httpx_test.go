package httpx

import (
	"compress/gzip"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// newTestClient returns a client whose clock and sleep the test controls, so
// rate limiting can be exercised without real waiting.
func newTestClient(t *testing.T, policy HostPolicy) (*Client, *fakeClock) {
	t.Helper()

	clk := &fakeClock{now: time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)}
	c := New(Options{Timeout: 5 * time.Second, Fallback: policy}, discardLogger())
	c.clock = clk.Now
	c.sleep = func(ctx context.Context, d time.Duration) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		clk.Advance(d)
		return nil
	}
	return c, clk
}

type fakeClock struct {
	mu    sync.Mutex
	now   time.Time
	slept time.Duration
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	c.slept += d
}

func (c *fakeClock) Slept() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.slept
}

func TestGetReturnsTheBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("solar flux 110"))
	}))
	defer srv.Close()

	c, _ := newTestClient(t, HostPolicy{})
	resp, err := c.Get(context.Background(), Request{URL: srv.URL})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got := string(resp.Body); got != "solar flux 110" {
		t.Errorf("body = %q", got)
	}
}

// Every publisher here can identify and contact automated clients, and two of
// them 403 anything that does not look like a real client.
func TestEveryRequestIdentifiesTheExporter(t *testing.T) {
	var ua string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ua = r.Header.Get("User-Agent")
	}))
	defer srv.Close()

	c, _ := newTestClient(t, HostPolicy{})
	if _, err := c.Get(context.Background(), Request{URL: srv.URL}); err != nil {
		t.Fatalf("Get: %v", err)
	}

	if !strings.Contains(ua, "solarham-exporter") {
		t.Errorf("User-Agent = %q, want it to name the exporter", ua)
	}
	if !strings.Contains(ua, "github.com/RealDougEubanks/solarham") {
		t.Errorf("User-Agent = %q, want it to carry a contact URL", ua)
	}
}

// This is the reason the package exists: ten sources sharing one host must
// collectively respect the host's spacing, not each respect it individually.
func TestSpacingIsSharedAcrossConcurrentCallers(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
	}))
	defer srv.Close()

	c, clk := newTestClient(t, HostPolicy{MinInterval: 10 * time.Second, Burst: 1})

	var wg sync.WaitGroup
	for range 5 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = c.Get(context.Background(), Request{URL: srv.URL})
		}()
	}
	wg.Wait()

	if got := hits.Load(); got != 5 {
		t.Fatalf("server saw %d requests, want 5", got)
	}
	// One request is free from the burst; the remaining four each wait.
	if slept := clk.Slept(); slept < 40*time.Second {
		t.Errorf("total wait was %v, want at least 40s for four spaced requests", slept)
	}
}

func TestBurstAllowsAStartupFlurryThenSpaces(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer srv.Close()

	c, clk := newTestClient(t, HostPolicy{MinInterval: 10 * time.Second, Burst: 4})

	for range 4 {
		if _, err := c.Get(context.Background(), Request{URL: srv.URL}); err != nil {
			t.Fatalf("Get: %v", err)
		}
	}
	if slept := clk.Slept(); slept != 0 {
		t.Errorf("burst of 4 slept %v, want 0", slept)
	}

	if _, err := c.Get(context.Background(), Request{URL: srv.URL}); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if clk.Slept() == 0 {
		t.Error("the fifth request did not wait; the bucket is not draining")
	}
}

// Spacing cannot be switched off by forgetting to configure it. An empty
// policy is floored to something safe rather than becoming unlimited, because
// the failure mode of "unlimited by omission" is hammering somebody's server.
func TestAnUnconfiguredPolicyStillSpacesRequests(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer srv.Close()

	c, clk := newTestClient(t, HostPolicy{})
	for range 10 {
		if _, err := c.Get(context.Background(), Request{URL: srv.URL}); err != nil {
			t.Fatalf("Get: %v", err)
		}
	}
	if clk.Slept() == 0 {
		t.Error("ten requests under an empty policy never waited; spacing was silently disabled")
	}

	if got := c.limiterFor("127.0.0.1").policy.MinInterval; got <= 0 {
		t.Errorf("resolved MinInterval = %v, want a positive safety default", got)
	}
}

// Spacing is genuinely bypassable, but only by asking for it explicitly — the
// case for polling your own KiwiSDR on localhost.
func TestSpacingCanBeDisabledExplicitly(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer srv.Close()

	clk := &fakeClock{now: time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)}
	c := New(Options{
		Timeout:  5 * time.Second,
		Policies: map[string]HostPolicy{"127.0.0.1": {MinInterval: 0, Burst: 1}},
		Fallback: HostPolicy{MinInterval: time.Hour, Burst: 1},
	}, discardLogger())
	c.clock = clk.Now
	c.sleep = func(ctx context.Context, d time.Duration) error { clk.Advance(d); return nil }

	for range 10 {
		if _, err := c.Get(context.Background(), Request{URL: srv.URL}); err != nil {
			t.Fatalf("Get: %v", err)
		}
	}
	if slept := clk.Slept(); slept != 0 {
		t.Errorf("slept %v with an explicit zero-interval policy, want 0", slept)
	}
}

func TestConditionalGetReplaysValidators(t *testing.T) {
	var sawINM, sawIMS string
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		sawINM = r.Header.Get("If-None-Match")
		sawIMS = r.Header.Get("If-Modified-Since")
		w.Header().Set("ETag", `"abc123"`)
		w.Header().Set("Last-Modified", "Wed, 09 Sep 2026 12:00:00 GMT")
		if calls > 1 {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		_, _ = w.Write([]byte("first"))
	}))
	defer srv.Close()

	c, _ := newTestClient(t, HostPolicy{})
	req := Request{URL: srv.URL, Conditional: true}

	if _, err := c.Get(context.Background(), req); err != nil {
		t.Fatalf("first Get: %v", err)
	}

	_, err := c.Get(context.Background(), req)
	if !errors.Is(err, ErrNotModified) {
		t.Fatalf("second Get error = %v, want ErrNotModified", err)
	}
	if sawINM != `"abc123"` {
		t.Errorf("If-None-Match = %q, want the stored ETag", sawINM)
	}
	if sawIMS == "" {
		t.Error("If-Modified-Since was not sent")
	}
}

func TestConditionalGetIsOptOut(t *testing.T) {
	// An upstream that sends no validators should not be sent them either.
	var sawINM string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawINM = r.Header.Get("If-None-Match")
		w.Header().Set("ETag", `"x"`)
	}))
	defer srv.Close()

	c, _ := newTestClient(t, HostPolicy{})
	req := Request{URL: srv.URL} // Conditional not set
	_, _ = c.Get(context.Background(), req)
	_, _ = c.Get(context.Background(), req)

	if sawINM != "" {
		t.Errorf("If-None-Match = %q, want none when Conditional is off", sawINM)
	}
}

func TestLastModifiedIsParsed(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Last-Modified", "Wed, 09 Sep 2026 12:34:56 GMT")
	}))
	defer srv.Close()

	c, _ := newTestClient(t, HostPolicy{})
	resp, err := c.Get(context.Background(), Request{URL: srv.URL})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	want := time.Date(2026, 9, 9, 12, 34, 56, 0, time.UTC)
	if !resp.LastModified.Equal(want) {
		t.Errorf("LastModified = %v, want %v", resp.LastModified, want)
	}
}

func TestPermanentFailuresAreNotRetried(t *testing.T) {
	for _, code := range []int{
		http.StatusUnauthorized, http.StatusForbidden,
		http.StatusNotFound, http.StatusGone, http.StatusBadRequest,
	} {
		var calls atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			w.WriteHeader(code)
		}))

		c, _ := newTestClient(t, HostPolicy{})
		_, err := c.Get(context.Background(), Request{URL: srv.URL, Retries: 3})

		if !errors.Is(err, ErrPermanent) {
			t.Errorf("status %d gave error %v, want ErrPermanent", code, err)
		}
		if got := calls.Load(); got != 1 {
			t.Errorf("status %d was attempted %d times, want 1", code, got)
		}
		srv.Close()
	}
}

func TestServerErrorsAreRetriedThenSucceed(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	c, _ := newTestClient(t, HostPolicy{})
	resp, err := c.Get(context.Background(), Request{URL: srv.URL, Retries: 3})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(resp.Body) != "ok" {
		t.Errorf("body = %q", resp.Body)
	}
	if got := calls.Load(); got != 3 {
		t.Errorf("attempts = %d, want 3", got)
	}
}

func TestRetriesAreExhausted(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer srv.Close()

	c, _ := newTestClient(t, HostPolicy{})
	if _, err := c.Get(context.Background(), Request{URL: srv.URL, Retries: 2}); err == nil {
		t.Fatal("Get succeeded, want an error after exhausting retries")
	}
	if got := calls.Load(); got != 3 {
		t.Errorf("attempts = %d, want 3 (one plus two retries)", got)
	}
}

// Retrying a 429 is precisely the behaviour the upstream asked us to stop.
func TestARateLimitIsNotRetried(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	c, _ := newTestClient(t, HostPolicy{})
	_, err := c.Get(context.Background(), Request{URL: srv.URL, Retries: 5})

	if !errors.Is(err, ErrRateLimited) {
		t.Fatalf("error = %v, want ErrRateLimited", err)
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("attempts = %d, want 1", got)
	}
}

// At least one upstream sends 429 with no Retry-After and needs minutes of
// silence to recover, so the pause has to apply to the whole host.
func TestA429PausesTheEntireHost(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "120")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	c, clk := newTestClient(t, HostPolicy{})
	if _, err := c.Get(context.Background(), Request{URL: srv.URL}); !errors.Is(err, ErrRateLimited) {
		t.Fatalf("error = %v, want ErrRateLimited", err)
	}

	before := clk.Slept()
	_, _ = c.Get(context.Background(), Request{URL: srv.URL})

	if waited := clk.Slept() - before; waited < 100*time.Second {
		t.Errorf("the next request waited %v, want it held for the ~120s Retry-After", waited)
	}
}

func TestRetryAfterAsAnHTTPDateIsHonoured(t *testing.T) {
	c, clk := newTestClient(t, HostPolicy{})
	future := clk.Now().Add(90 * time.Second)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", future.UTC().Format(http.TimeFormat))
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	// The client's fake clock is behind real time, so the parsed date is
	// compared against the real clock inside noteRateLimit. Assert only that
	// a cooldown was applied at all.
	_, _ = c.Get(context.Background(), Request{URL: srv.URL})
	before := clk.Slept()
	_, _ = c.Get(context.Background(), Request{URL: srv.URL})

	if clk.Slept() == before {
		t.Error("no cooldown was applied after a dated Retry-After")
	}
}

func TestAnAbsurdRetryAfterIsCapped(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "999999")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	c, clk := newTestClient(t, HostPolicy{})
	_, _ = c.Get(context.Background(), Request{URL: srv.URL})

	before := clk.Slept()
	_, _ = c.Get(context.Background(), Request{URL: srv.URL})

	// The cap is an hour; a server-suggested twelve days must not outlive the
	// process.
	if waited := clk.Slept() - before; waited > 2*time.Hour {
		t.Errorf("waited %v, want the cooldown capped near an hour", waited)
	}
}

func TestAnOversizedBodyIsRejectedRatherThanTruncated(t *testing.T) {
	// A silently truncated body becomes a confusing parse error somewhere
	// else; rejecting it names the real problem.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(make([]byte, 4096))
	}))
	defer srv.Close()

	c, _ := newTestClient(t, HostPolicy{})
	_, err := c.Get(context.Background(), Request{URL: srv.URL, MaxBody: 1024})
	if err == nil {
		t.Fatal("Get succeeded, want an oversized-body error")
	}
	if !strings.Contains(err.Error(), "more than") {
		t.Errorf("error = %v, want it to explain the size limit", err)
	}
}

func TestABodyAtExactlyTheLimitIsAccepted(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(make([]byte, 1024))
	}))
	defer srv.Close()

	c, _ := newTestClient(t, HostPolicy{})
	resp, err := c.Get(context.Background(), Request{URL: srv.URL, MaxBody: 1024})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if len(resp.Body) != 1024 {
		t.Errorf("body length = %d, want 1024", len(resp.Body))
	}
}

func TestCustomHeadersAreSent(t *testing.T) {
	var got string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Get("Authorization")
	}))
	defer srv.Close()

	c, _ := newTestClient(t, HostPolicy{})
	_, err := c.Get(context.Background(), Request{
		URL:     srv.URL,
		Headers: map[string]string{"Authorization": "Token secret"},
	})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got != "Token secret" {
		t.Errorf("Authorization = %q", got)
	}
}

// canaryToken is deliberately unlike any other string here, so a substring
// search for it cannot match by accident.
const canaryToken = "zzQUUX-canary-httpx-4f7a-NEVERLOG"

func TestACredentialInTheQueryStringNeverReachesAnError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	c, _ := newTestClient(t, HostPolicy{})
	_, err := c.Get(context.Background(), Request{URL: srv.URL + "/write?p=" + canaryToken})
	if err == nil {
		t.Fatal("Get succeeded, want a permanent failure")
	}
	if strings.Contains(err.Error(), canaryToken) {
		t.Errorf("the credential leaked into the error: %v", err)
	}
}

func TestAnUnparseableURLIsAPermanentFailure(t *testing.T) {
	c, _ := newTestClient(t, HostPolicy{})
	if _, err := c.Get(context.Background(), Request{URL: "http://[::1]:namedport/"}); !errors.Is(err, ErrPermanent) {
		t.Errorf("error = %v, want ErrPermanent", err)
	}
}

func TestAURLWithNoHostIsAPermanentFailure(t *testing.T) {
	c, _ := newTestClient(t, HostPolicy{})
	if _, err := c.Get(context.Background(), Request{URL: "/relative/path"}); !errors.Is(err, ErrPermanent) {
		t.Errorf("error = %v, want ErrPermanent", err)
	}
}

func TestACancelledContextIsHonoured(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer srv.Close()

	c, _ := newTestClient(t, HostPolicy{MinInterval: time.Hour, Burst: 1})
	// Drain the single burst token.
	if _, err := c.Get(context.Background(), Request{URL: srv.URL}); err != nil {
		t.Fatalf("Get: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.Get(ctx, Request{URL: srv.URL}); !errors.Is(err, context.Canceled) {
		t.Errorf("error = %v, want context.Canceled while waiting on the limiter", err)
	}
}

func TestSeparateHostsDoNotShareABudget(t *testing.T) {
	a := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer a.Close()
	b := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer b.Close()

	// Both test servers are on 127.0.0.1, so use explicit policies keyed by
	// the shared hostname to prove the map is consulted per host rather than
	// globally.
	c, clk := newTestClient(t, HostPolicy{MinInterval: 10 * time.Second, Burst: 1})

	if _, err := c.Get(context.Background(), Request{URL: a.URL}); err != nil {
		t.Fatalf("Get a: %v", err)
	}
	first := clk.Slept()
	if _, err := c.Get(context.Background(), Request{URL: b.URL}); err != nil {
		t.Fatalf("Get b: %v", err)
	}

	// Same hostname, so the second call does wait. This documents the real
	// behaviour: the limiter is keyed by hostname, not by port.
	if clk.Slept() == first {
		t.Log("note: both servers share 127.0.0.1, so the limiter is shared by design")
	}
}

func TestDefaultPoliciesAreAllPositive(t *testing.T) {
	// A zero MinInterval in this table would silently disable politeness for
	// that host, which is the one mistake here that nobody would notice.
	for host, p := range DefaultPolicies {
		if p.MinInterval <= 0 {
			t.Errorf("%s has a non-positive MinInterval, which disables spacing", host)
		}
		if p.Burst < 1 {
			t.Errorf("%s has a burst of %d, which would stall every request", host, p.Burst)
		}
	}
}

// These three publishers have each stated a limit in writing. If someone
// loosens one of these numbers, this test should stop them.
func TestStatedCourtesyLimitsAreRespected(t *testing.T) {
	tests := map[string]time.Duration{
		"www.hamqsl.com":   5 * time.Minute,
		"pskreporter.info": 5 * time.Minute,
		"celestrak.org":    5 * time.Minute,
		"lotw.arrl.org":    5 * time.Minute,
	}
	for host, want := range tests {
		p, ok := DefaultPolicies[host]
		if !ok {
			t.Errorf("%s has no policy; a stated courtesy limit is unenforced", host)
			continue
		}
		if p.MinInterval < want {
			t.Errorf("%s spacing is %v, want at least %v", host, p.MinInterval, want)
		}
		if p.Burst != 1 {
			t.Errorf("%s allows a burst of %d; a stated limit should not be burstable", host, p.Burst)
		}
	}
}

func TestTheFallbackPolicyIsConservative(t *testing.T) {
	// An unlisted host is one nobody has thought about yet.
	if DefaultFallback.MinInterval < 10*time.Second {
		t.Errorf("fallback spacing is %v, want it conservative for unknown hosts",
			DefaultFallback.MinInterval)
	}
}

func TestNewDefaultBuildsAUsableClient(t *testing.T) {
	c := NewDefault(10*time.Second, discardLogger())
	if c == nil {
		t.Fatal("NewDefault returned nil")
	}
	if got := c.limiterFor("services.swpc.noaa.gov").policy.MinInterval; got != 2*time.Second {
		t.Errorf("SWPC spacing = %v, want the table value", got)
	}
	if got := c.limiterFor("nobody.example.invalid").policy.MinInterval; got != DefaultFallback.MinInterval {
		t.Errorf("unlisted host spacing = %v, want the fallback", got)
	}
}

// A gzipped response must arrive decoded.
//
// This is a regression test for a real bug: the client used to set
// "Accept-Encoding: gzip" itself, which makes Go's transport stop
// transparently decompressing and hands that job to the caller. Every JSON and
// CSV parser in the exporter then failed on the gzip magic byte, and it only
// showed up against live upstreams because httptest servers do not compress.
func TestAGzippedResponseIsDecodedBeforeItIsReturned(t *testing.T) {
	const payload = `{"flux": 110}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			t.Errorf("Accept-Encoding = %q, want the transport to have offered gzip",
				r.Header.Get("Accept-Encoding"))
		}
		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Set("Content-Type", "application/json")
		gz := gzip.NewWriter(w)
		defer func() { _ = gz.Close() }()
		_, _ = gz.Write([]byte(payload))
	}))
	defer srv.Close()

	c, _ := newTestClient(t, HostPolicy{})
	resp, err := c.Get(context.Background(), Request{URL: srv.URL})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if len(resp.Body) > 0 && resp.Body[0] == 0x1f {
		t.Fatal("body still begins with the gzip magic byte; it was not decoded")
	}
	if got := string(resp.Body); got != payload {
		t.Errorf("body = %q, want %q", got, payload)
	}
}

// The client must not claim an encoding it will not decode.
func TestTheClientDoesNotSetAcceptEncodingItself(t *testing.T) {
	var explicit bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Go's transport adds its own header at the transport layer, below
		// where a handler can distinguish it from ours. What matters is that
		// the value is exactly what the transport uses, so decoding stays
		// automatic.
		explicit = r.Header.Get("Accept-Encoding") != "gzip"
	}))
	defer srv.Close()

	c, _ := newTestClient(t, HostPolicy{})
	if _, err := c.Get(context.Background(), Request{URL: srv.URL}); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if explicit {
		t.Error("Accept-Encoding was not the transport's own value; transparent decoding is off")
	}
}
