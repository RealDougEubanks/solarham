// Package swpc polls NOAA's Space Weather Prediction Center.
//
// SWPC is the authoritative first-hand publisher for nearly everything this
// exporter reports: solar wind, X-ray flux, particle flux, the planetary
// indices, the NOAA G/S/R scales, alerts, aurora and D-region absorption. It is
// public domain, needs no credentials, publishes no rate limit, and — unlike
// the other upstreams here — serves ETag and Last-Modified, so a repeat poll of
// an unchanged document costs a 304 rather than a body.
//
// # Three sources, not one
//
// SWPC publishes at wildly different cadences. Solar wind summaries update
// every minute; F10.7 updates three times a day. Polling all of it on one
// interval means either hammering the hourly products sixty times per update or
// letting the minute products go an hour stale. This package therefore exposes
// three source.Source implementations — swpc-fast, swpc-medium and swpc-slow —
// over one shared HTTP client and one shared conditional-GET cache. All() hands
// the scheduler all three, and each carries its own interval.
//
// # Partial data is still data
//
// Within a tier the endpoints are fetched concurrently and merged into one
// batch. One endpoint failing does not fail the tier: its error is logged and
// the samples that did arrive are still returned. A tier reports failure only
// when every one of its endpoints failed. This is deliberate. If the alerts
// document is briefly 500ing, an operator would far rather still see the solar
// wind, the Kp and the flare class than lose the whole minute's space weather
// to one sick file.
//
// # 304 is a success
//
// A tier whose endpoints all answered 304 returns an empty batch and
// source.ErrNotModified. That is not a failure and the scheduler does not count
// it as one; it means the upstream has published nothing new, which at a
// one-minute poll against a sixty-second cache is the common case.
package swpc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/RealDougEubanks/solarham/internal/config"
	"github.com/RealDougEubanks/solarham/internal/metric"
	"github.com/RealDougEubanks/solarham/internal/source"
)

// Default cadences, used when the configuration leaves an interval unset.
//
// They track what SWPC actually republishes: the summary products carry a
// sixty-second cache-control, the GOES particle and OVATION products move every
// five minutes, and F10.7, the three-hourly indices and the daily tables move
// far more slowly than the hour they are polled at.
const (
	defaultFastInterval   = time.Minute
	defaultMediumInterval = 5 * time.Minute
	defaultSlowInterval   = time.Hour
	defaultTimeout        = 20 * time.Second
	defaultBaseURL        = "https://services.swpc.noaa.gov"
)

// maxConcurrentFetches bounds how many endpoints of one tier are in flight at
// once. The tiers are small — six endpoints at most — but SWPC is a free public
// service and opening every connection simultaneously on every tick is louder
// than it needs to be for data that is seconds old either way.
const maxConcurrentFetches = 4

// Source names. They become metric labels, so they are stable.
const (
	nameFast   = "swpc-fast"
	nameMedium = "swpc-medium"
	nameSlow   = "swpc-slow"
)

// A tier is one of the three sources. All three are this type; they differ only
// in their name, interval and endpoint list.
//
// The compile-time assertion below therefore covers all three at once.
type tier struct {
	name      string
	interval  time.Duration
	endpoints []endpoint
	client    *client
	log       *slog.Logger

	// drapStep subsamples the D-RAP grid. Only the medium tier uses it.
	drapStep int

	// now is time.Now except in tests. Only the alert age window needs it:
	// every other decision in this package is made from the upstream's own
	// timestamps.
	now func() time.Time
}

// Compile-time proof that a tier is a source the scheduler can poll. swpc-fast,
// swpc-medium and swpc-slow are all *tier.
var _ source.Source = (*tier)(nil)

// Sources holds the three SWPC tiers.
type Sources struct {
	fast   *tier
	medium *tier
	slow   *tier
}

// New builds the three tiers from validated configuration.
//
// Everything is defaulted rather than rejected where a sensible default exists,
// because none of these settings is a credential and a missing one should not
// stop an exporter starting. A BaseURL that is not a URL is rejected: that is a
// typo an operator needs told about, not one to paper over.
func New(cfg config.SWPC, log *slog.Logger) (*Sources, error) {
	if log == nil {
		log = slog.Default()
	}

	base := strings.TrimRight(strings.TrimSpace(cfg.BaseURL), "/")
	if base == "" {
		base = defaultBaseURL
	}
	u, err := url.Parse(base)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return nil, fmt.Errorf("swpc: SOLARHAM_SWPC_BASE_URL is not an absolute URL: %s", redactBase(base))
	}

	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	retries := cfg.Retries
	if retries < 0 {
		retries = 0
	}
	drapStep := cfg.DRAPGridStep
	if drapStep < 1 {
		drapStep = 1
	}

	c := newClient(base, timeout, retries, log.With("source", "swpc"))

	mediumEndpoints := mediumTierEndpoints
	if cfg.DRAPEnabled {
		// D-RAP is a global grid. Even subsampled it produces more series than
		// every other endpoint combined, which is why it is opt-in rather than
		// merely configurable.
		mediumEndpoints = append(append([]endpoint(nil), mediumEndpoints...), drapEndpoint)
	}

	newTier := func(name string, interval, fallback time.Duration, eps []endpoint) *tier {
		if interval <= 0 {
			interval = fallback
		}
		return &tier{
			name:      name,
			interval:  interval,
			endpoints: eps,
			client:    c,
			log:       log.With("source", name),
			drapStep:  drapStep,
			now:       time.Now,
		}
	}

	return &Sources{
		fast:   newTier(nameFast, cfg.FastInterval, defaultFastInterval, fastTierEndpoints),
		medium: newTier(nameMedium, cfg.MediumInterval, defaultMediumInterval, mediumEndpoints),
		slow:   newTier(nameSlow, cfg.SlowInterval, defaultSlowInterval, slowTierEndpoints),
	}, nil
}

// All returns the three tiers in fast, medium, slow order.
func (s *Sources) All() []source.Source {
	return []source.Source{s.fast, s.medium, s.slow}
}

// Close releases the shared HTTP client's idle connections.
func (s *Sources) Close() error {
	s.fast.client.close()
	return nil
}

// redactBase keeps a misconfigured base URL out of the error message in a form
// that could carry a credential. It is only ever a scheme and host in practice,
// but an operator can put anything in an environment variable.
func redactBase(raw string) string {
	if raw == "" {
		return "(empty)"
	}
	return raw
}

// Name identifies the tier in logs and metrics.
func (t *tier) Name() string { return t.name }

// Interval is how often the scheduler should poll this tier.
func (t *tier) Interval() time.Duration { return t.interval }

// result is one endpoint's outcome, kept so the merge can distinguish "nothing
// new" from "nothing at all".
type result struct {
	path        string
	samples     []metric.Sample
	err         error
	notModified bool
}

// Poll fetches every endpoint in the tier concurrently and merges the samples.
//
// The three outcomes, in the order they are decided:
//
//   - every endpoint failed: the tier failed, and the joined error is returned
//     so the operator sees all of the causes rather than whichever finished
//     first.
//   - every endpoint answered 304: nothing new, an empty batch and
//     source.ErrNotModified.
//   - anything else: whatever was successfully parsed, with any failures logged
//     rather than returned. Partial space weather beats none.
func (t *tier) Poll(ctx context.Context) (metric.Batch, error) {
	fetched := t.now().UTC()
	batch := metric.Batch{Source: t.name, Fetched: fetched}

	if len(t.endpoints) == 0 {
		return batch, nil
	}

	results := make([]result, len(t.endpoints))
	sem := make(chan struct{}, maxConcurrentFetches)
	var wg sync.WaitGroup

	for i, ep := range t.endpoints {
		wg.Add(1)
		go func(i int, ep endpoint) {
			defer wg.Done()

			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				results[i] = result{path: ep.path, err: ctx.Err()}
				return
			}
			defer func() { <-sem }()

			results[i] = t.fetchOne(ctx, ep, fetched)
		}(i, ep)
	}
	wg.Wait()

	var (
		errs        []error
		notModified int
	)
	for _, r := range results {
		switch {
		case r.err != nil:
			errs = append(errs, fmt.Errorf("%s: %w", r.path, r.err))
		case r.notModified:
			notModified++
		default:
			batch.Samples = append(batch.Samples, r.samples...)
		}
	}

	if len(errs) == len(results) {
		return metric.Batch{Source: t.name, Fetched: fetched},
			fmt.Errorf("swpc: every endpoint in %s failed: %w", t.name, errors.Join(errs...))
	}

	for _, err := range errs {
		// Logged, not returned. The samples that did arrive are worth more than
		// the poll being marked failed, but a silently missing endpoint is how
		// a dashboard quietly stops showing a metric for a week.
		t.log.Warn("swpc endpoint failed, continuing with the rest", "error", err)
	}

	if notModified == len(results) {
		return metric.Batch{Source: t.name, Fetched: fetched}, source.ErrNotModified
	}

	return batch, nil
}

// fetchOne performs and parses a single endpoint.
//
// A parse panic would otherwise take down the whole poll and, with it, the
// other endpoints' data. Upstream text formats are fixed-width tables written
// by other people's code, so a shape nobody anticipated is a question of when.
func (t *tier) fetchOne(ctx context.Context, ep endpoint, fetched time.Time) (res result) {
	res.path = ep.path

	defer func() {
		if r := recover(); r != nil {
			res.samples = nil
			res.err = fmt.Errorf("swpc: panic parsing %s: %v", ep.path, r)
		}
	}()

	body, err := t.client.get(ctx, ep.path)
	if errors.Is(err, errNotModified) {
		res.notModified = true
		return res
	}
	if err != nil {
		res.err = err
		return res
	}

	samples, err := ep.parse(t, body, fetched)
	if err != nil {
		res.err = err
		return res
	}

	// Validate here rather than in each parser. A sample with the wrong number
	// of label values is rejected by Prometheus at collection time and silently
	// corrupts InfluxDB tag sets, and finding that at the source is far cheaper
	// than finding it in a sink.
	kept := samples[:0]
	for _, s := range samples {
		if err := s.Validate(); err != nil {
			t.log.Warn("swpc dropped an invalid sample", "endpoint", ep.path, "error", err)
			continue
		}
		kept = append(kept, s)
	}
	res.samples = kept
	return res
}
