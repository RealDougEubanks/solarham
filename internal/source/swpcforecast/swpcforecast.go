// Package swpcforecast polls NOAA SWPC's forecast products: the flare and
// particle-event probabilities, the solar region summary, the planetary
// K-index forecast, and the F10.7 and Fredericksburg A-index predictions.
//
// # Why this is a separate source from swpc
//
// The swpc package polls observations — solar wind, X-ray flux, the current
// indices — and it polls them on interval, because they genuinely change
// continuously. Everything here is a forecast: a human forecaster at Boulder
// runs a model, writes a number, and the file is regenerated once, or a few
// times, per UTC day. The probabilities file carries one record per day. The
// region summary is issued at about 00:30 UT. Polling either of those every
// five minutes fetches an identical document 288 times to learn nothing, which
// is rude to the publisher and pointless for us.
//
// So this source declares a publication clock rather than an interval. See
// Schedule for the times and the reasoning.
//
// # Forecasts as labels, not future points
//
// A value predicted for tomorrow cannot be stored as a point stamped
// tomorrow — Prometheus stamps at scrape time and every other sink here would
// have to invent a retention policy for the future. The horizon is therefore a
// label and the sample time is when the forecast was issued, which is the
// convention metric.FlareProbability and friends document.
//
// # Partial data is still data
//
// The five documents are fetched concurrently and merged into one batch. One
// document failing does not fail the poll: its error is logged and whatever
// else arrived is returned. A poll fails only when every document failed, and
// returns source.ErrNotModified only when every document answered 304.
package swpcforecast

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/RealDougEubanks/solarham/internal/config"
	"github.com/RealDougEubanks/solarham/internal/httpx"
	"github.com/RealDougEubanks/solarham/internal/metric"
	"github.com/RealDougEubanks/solarham/internal/source"
)

// Name identifies this source in logs, metrics and the health endpoints. It is
// stable because it becomes a metric label.
const Name = "swpc-forecast"

// defaultBaseURL is hardcoded rather than configurable.
//
// The existing swpc source already exposes SOLARHAM_SWPC_BASE_URL for
// operators who need to point at a mirror or a recording proxy, and a second
// knob for the same host would only be a second thing to get out of step. The
// field on Source exists so tests can aim at an httptest server; nothing
// outside this package can change it.
const defaultBaseURL = "https://services.swpc.noaa.gov"

// publishLag is added to each scheduled time before polling.
//
// A publisher's stated issue time is when it starts writing, not when the file
// is readable: the region summary is nominally 00:30 UT and has been observed
// arriving several minutes later. Polling at exactly the stated minute fetches
// yesterday's copy and then waits for the next slot to notice.
const publishLag = 30 * time.Minute

// maxConcurrentFetches bounds how many of the five documents are in flight at
// once. The shared httpx client rate-limits by host anyway; this simply avoids
// opening five connections in the same instant on every poll.
const maxConcurrentFetches = 3

// maxBodyBytes bounds each response. The largest of these documents is the
// solar region summary at about 130 KB; two megabytes is an order of magnitude
// of headroom and still far too small for a misbehaving origin to hurt us.
const maxBodyBytes = 2 << 20

// retries is the number of retries after the first attempt. These are forecast
// documents polled a handful of times a day, so a transient 5xx is worth two
// more tries rather than waiting six hours for the next slot.
const retries = 2

// Horizon labels. The three-day forecasts all use the same set, so it is
// declared once.
var horizons = [3]string{"1d", "2d", "3d"}

// Source polls the SWPC forecast products.
type Source struct {
	base    string
	regions bool
	client  *httpx.Client
	log     *slog.Logger

	// now is time.Now except in tests. It is used only to compute
	// metric.SourceDataAge and to stamp Batch.Fetched; every published value's
	// timestamp comes from the upstream's own date field.
	now func() time.Time
}

// Compile-time proof the source is both pollable and schedulable.
var (
	_ source.Source    = (*Source)(nil)
	_ source.Scheduled = (*Source)(nil)
)

// New builds the source from validated configuration.
//
// The client is passed in rather than constructed here because politeness has
// to be enforced across sources: a per-source limiter would let this source and
// the three swpc tiers each politely space their own requests to
// services.swpc.noaa.gov and collectively hammer it.
func New(cfg config.SolarProbabilities, client *httpx.Client, log *slog.Logger) (*Source, error) {
	if client == nil {
		return nil, errors.New("swpcforecast: a shared httpx client is required")
	}
	if log == nil {
		log = slog.Default()
	}
	return &Source{
		base:    defaultBaseURL,
		regions: cfg.Regions,
		client:  client,
		log:     log.With("source", Name),
		now:     time.Now,
	}, nil
}

// Name identifies the source.
func (s *Source) Name() string { return Name }

// Interval is the nominal spacing between polls, used for staleness windows and
// backoff sizing. It reports whatever the schedule reports, so the two can never
// disagree.
func (s *Source) Interval() time.Duration { return s.Schedule().Interval() }

// Schedule polls four times per UTC day, at 00:30, 06:30, 12:30 and 18:30 plus
// a thirty-minute lag: 01:00, 07:00, 13:00 and 19:00 UTC.
//
// The reasoning, in order:
//
// The region summary is the earliest-issued of the five and is nominally 00:30
// UT, so the first slot sits on it with the lag applied. That is the poll that
// actually collects the day's data.
//
// The other three exist because "issued daily" is a plan, not a guarantee.
// SWPC regenerates the probabilities and index forecasts when the forecaster
// updates them, which in practice is at least once more during the Boulder
// working day and occasionally late. A source that polls only at 01:00 shows
// yesterday's numbers for a full day whenever a regeneration slips. Four evenly
// spaced slots pick up a late or revised issue within six hours.
//
// Four and not more, because these are the same bytes each time. With
// Conditional set on every request an unchanged document costs a 304, but a 304
// is still a request, and there is nothing a fifth poll could learn that the
// next slot will not.
func (s *Source) Schedule() source.Schedule {
	return source.DailyAt(publishLag,
		source.TimeOfDay{Hour: 0, Minute: 30},
		source.TimeOfDay{Hour: 6, Minute: 30},
		source.TimeOfDay{Hour: 12, Minute: 30},
		source.TimeOfDay{Hour: 18, Minute: 30},
	)
}

// endpoint is one forecast document and the parser that reads it.
type endpoint struct {
	path  string
	parse func(s *Source, body []byte) ([]metric.Sample, time.Time, error)
}

// endpoints is the full document set. Order is stable so that a merged batch is
// deterministic, which makes the tests readable and diffs against a recorded
// batch meaningful.
var endpoints = []endpoint{
	{pathProbabilities, parseProbabilities},
	{pathRegions, parseRegions},
	{pathKpForecast, parseKpForecast},
	{pathFluxForecast, parseFluxForecast},
	{pathAIndexForecast, parseAIndexForecast},
}

// result is one document's outcome, kept so the merge can distinguish "nothing
// new" from "nothing at all".
type result struct {
	path        string
	samples     []metric.Sample
	newest      time.Time
	err         error
	notModified bool
}

// Poll fetches every document concurrently and merges the samples.
func (s *Source) Poll(ctx context.Context) (metric.Batch, error) {
	fetched := s.now().UTC()
	empty := metric.Batch{Source: Name, Fetched: fetched}

	results := make([]result, len(endpoints))
	sem := make(chan struct{}, maxConcurrentFetches)
	var wg sync.WaitGroup

	for i, ep := range endpoints {
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

			results[i] = s.fetchOne(ctx, ep)
		}(i, ep)
	}
	wg.Wait()

	batch := metric.Batch{Source: Name, Fetched: fetched}
	var (
		errs        []error
		notModified int
		newest      time.Time
		rateLimited bool
	)
	for _, r := range results {
		switch {
		case r.err != nil:
			errs = append(errs, fmt.Errorf("%s: %w", r.path, r.err))
			if errors.Is(r.err, httpx.ErrRateLimited) {
				rateLimited = true
			}
		case r.notModified:
			notModified++
		default:
			batch.Samples = append(batch.Samples, r.samples...)
			if r.newest.After(newest) {
				newest = r.newest
			}
		}
	}

	if len(errs) == len(results) {
		joined := errors.Join(errs...)
		if rateLimited {
			// The upstream has told us to stop asking. The scheduler backs off
			// rather than treating this as an ordinary failure.
			return empty, fmt.Errorf("%w: %w", source.ErrRateLimited, joined)
		}
		if err := ctx.Err(); err != nil {
			return empty, fmt.Errorf("swpcforecast: cancelled: %w", err)
		}
		return empty, fmt.Errorf("swpcforecast: every forecast document failed: %w", joined)
	}

	for _, err := range errs {
		// Logged, not returned. The forecasts that did arrive are worth more
		// than the poll being marked failed, but a silently missing document is
		// how a dashboard quietly stops showing a metric for a week.
		s.log.Warn("swpc forecast document failed, continuing with the rest", "error", err)
	}

	if notModified == len(results) {
		return empty, source.ErrNotModified
	}

	// Freshness, from the newest timestamp any document carried. This exists
	// because HTTP 200 with stale content is the dominant failure mode across
	// every upstream here: SWPC will happily serve a well-formed probabilities
	// file whose newest record is three days old.
	if !newest.IsZero() {
		age := fetched.Sub(newest).Seconds()
		if age < 0 {
			// A forecast dated in the future is normal for the horizon fields
			// but not for the issue date; clamp rather than publish a negative
			// age, which would look like a broken exporter rather than a
			// surprising upstream.
			age = 0
		}
		batch.Samples = append(batch.Samples, metric.Sample{
			Desc: metric.SourceDataAge, Labels: []string{Name},
			Value: age,
			// The age is a property of this moment, not of the observation, so
			// unlike every other sample here it is stamped with the fetch time.
			Time: fetched,
		})
	}

	return batch, nil
}

// fetchOne performs and parses a single document.
//
// A parse panic would otherwise take down the whole poll and with it the other
// four documents' data. These are other people's JSON files whose fields have
// been observed to change type, so a shape nobody anticipated is a question of
// when.
func (s *Source) fetchOne(ctx context.Context, ep endpoint) (res result) {
	res.path = ep.path

	defer func() {
		if r := recover(); r != nil {
			res.samples = nil
			res.err = fmt.Errorf("swpcforecast: panic parsing %s: %v", ep.path, r)
		}
	}()

	resp, err := s.client.Get(ctx, httpx.Request{
		URL: s.base + ep.path,
		// SWPC serves ETag and Last-Modified through CloudFront, so an
		// unchanged document costs a 304 rather than a body. At four polls a
		// day against files regenerated once a day, most polls should be 304s.
		Conditional: true,
		Retries:     retries,
		MaxBody:     maxBodyBytes,
	})
	switch {
	case errors.Is(err, httpx.ErrNotModified):
		res.notModified = true
		return res
	case err != nil:
		res.err = err
		return res
	}

	samples, newest, err := ep.parse(s, resp.Body)
	if err != nil {
		res.err = err
		return res
	}

	// Validate here rather than in each parser. A sample with the wrong number
	// of label values is rejected by Prometheus at collection time and silently
	// corrupts InfluxDB tag sets, and finding that at the source is far cheaper
	// than finding it in a sink.
	kept := samples[:0]
	for _, sample := range samples {
		if err := sample.Validate(); err != nil {
			s.log.Warn("swpcforecast dropped an invalid sample", "document", ep.path, "error", err)
			continue
		}
		kept = append(kept, sample)
	}
	res.samples = kept
	res.newest = newest
	return res
}
