// Package donki polls NASA CCMC's DONKI space weather event catalogue for
// coronal mass ejections, their analyses, and solar flares.
//
// # Why the CCMC host rather than api.nasa.gov
//
// DONKI is usually reached through api.nasa.gov, which requires an API key.
// Both hosts were fetched and compared: webtools.ccmc.gsfc.nasa.gov serves
// byte-identical JSON with no API key at all, sends no rate-limit headers, and
// is the origin api.nasa.gov proxies. The api.nasa.gov demo key, by contrast,
// measured x-ratelimit-limit: 10 per hour — which at a thirty-minute poll of
// three endpoints would exhaust the budget in under an hour and leave the
// exporter permanently throttled.
//
// So this package talks to the CCMC host directly. That is the whole reason it
// exists in this form: it removes a credential, removes a rate limit, and
// removes a dependency on a gateway, in exchange for nothing. The host is in
// the shared politeness table at five seconds between requests, which is
// generous for a government origin serving a hand-curated catalogue.
//
// # Event catalogues are a poor fit for gauges, so they are counted
//
// Everything else this exporter publishes is a measurement with a current
// value: a wind speed, a flux, an index. A flare catalogue is not that. It is a
// list of discrete things that happened, each with a start and an end, and
// there is no honest answer to "what is the flare right now".
//
// Making a gauge out of one anyway produces two distinct lies. A gauge holding
// the last flare's class says X9 for the six quiet days after an X9, because
// nothing has come along to overwrite it. A gauge holding zero between events
// says the Sun is quiet during the gap between two flares of a violent
// afternoon.
//
// These metrics are therefore counts over a rolling window — how many CMEs and
// how many flares of each class DONKI catalogued in the last few days — which
// is a question with a real answer at every instant, and is the shape
// metric.CMEEventCount and metric.FlareEventCount were defined for. The
// per-CME quantities that genuinely are instantaneous, speed and half angle and
// source position, are published from the most recent analysis and carry that
// analysis's own timestamp, so a stale one shows up as a rising
// metric.SourceDataAge rather than as a fresh-looking number.
//
// # Curation lag
//
// DONKI records are written by human analysts hours after the event. Nothing
// here is real time and polling it as though it were only fetches the same
// answer repeatedly, so the interval defaults to thirty minutes and is floored
// at fifteen.
//
// # What is deliberately not fetched
//
// The notifications endpoint carries the same alert, watch and warning text
// SWPC publishes and the swpc source already reports. Fetching it here would
// duplicate those series under a second source name, giving two entries per
// alert that disagree whenever the two poll cycles land differently. GST, SEP
// and IPS are not fetched either: this exporter has no descriptor for them, and
// a source that fetches data it cannot publish is just load.
package donki

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/RealDougEubanks/solarham/internal/config"
	"github.com/RealDougEubanks/solarham/internal/httpx"
	"github.com/RealDougEubanks/solarham/internal/metric"
	"github.com/RealDougEubanks/solarham/internal/redact"
	"github.com/RealDougEubanks/solarham/internal/source"
)

// Name identifies this source in logs, metrics and health output. It becomes a
// metric label, so it is stable.
const Name = "donki"

const (
	// defaultBaseURL is the key-free CCMC origin. See the package comment.
	defaultBaseURL = "https://webtools.ccmc.gsfc.nasa.gov/DONKI/WS/get"

	// Endpoint names, which are path segments under the base URL.
	pathCME         = "/CME"
	pathCMEAnalysis = "/CMEAnalysis"
	pathFLR         = "/FLR"

	// defaultInterval reflects curation lag rather than data cadence. Records
	// appear hours after the event they describe.
	defaultInterval = 30 * time.Minute

	// minInterval is the floor. Below a quarter of an hour a poll is fetching a
	// hand-curated catalogue faster than a human could possibly have added to
	// it. The floor lives in the schedule rather than in validation, because a
	// courtesy limit an operator can override by editing a setting is not a
	// limit.
	minInterval = 15 * time.Minute

	// defaultWindow is how far back events are counted. Three days spans the
	// transit time of a slow CME, so a launch counted today is still counted
	// when its shock arrives.
	defaultWindow = 72 * time.Hour

	// minWindow keeps a misconfiguration from producing a window shorter than
	// the curation lag, which would count zero events forever.
	minWindow = 6 * time.Hour

	// maxWindow bounds the request. DONKI will happily return months of CMEs,
	// and a window that long is a different question from "what is happening
	// now".
	maxWindow = 30 * 24 * time.Hour

	// maxBodyBytes bounds each response. The CME endpoint over a 72-hour window
	// measured about 14 KB; the same endpoint over 30 days measured 265 KB, so
	// 4 MB covers the widest allowed window with a wide margin and still stops
	// a broken origin from exhausting memory.
	maxBodyBytes = 4 << 20

	// maxConcurrentFetches bounds how many endpoints are in flight at once.
	// The shared client already spaces requests to this host; this bounds the
	// goroutines waiting on that spacing.
	maxConcurrentFetches = 3
)

// flareClasses are the class letters a count is always published for, so that a
// quiet window reports zero X-class flares rather than dropping the series and
// leaving a dashboard with a gap that reads as "the exporter is broken".
//
// A-class is absent: it sits below the background level of an even mildly
// active Sun and DONKI does not catalogue it. An A-class record that did appear
// would still be published; see summariseFlares.
var flareClasses = []string{"B", "C", "M", "X"}

// Source polls the DONKI catalogue.
type Source struct {
	baseURL  string
	client   *httpx.Client
	interval time.Duration
	window   time.Duration
	retries  int
	log      *slog.Logger

	// now is injectable so the window boundaries and the freshness metric can
	// be tested against fixtures captured at a known time.
	now func() time.Time
}

var _ source.Source = (*Source)(nil)
var _ source.Scheduled = (*Source)(nil)

// New builds a source from validated configuration.
func New(cfg config.DONKI, client *httpx.Client, log *slog.Logger) (*Source, error) {
	if client == nil {
		return nil, errors.New("donki: a shared httpx client is required")
	}
	if log == nil {
		log = slog.Default()
	}

	base := strings.TrimRight(strings.TrimSpace(cfg.BaseURL), "/")
	if base == "" {
		base = defaultBaseURL
	}
	u, err := url.Parse(base)
	if err != nil {
		return nil, fmt.Errorf("donki: base URL %q is not a URL: %w", redact.URL(base), err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("donki: base URL must be http or https, got %q", u.Scheme)
	}
	if u.Host == "" {
		return nil, errors.New("donki: base URL has no host")
	}

	interval := cfg.Interval
	if interval <= 0 {
		interval = defaultInterval
	}
	if interval < minInterval {
		log.Warn("donki interval raised to the floor; the catalogue is curated by hand and does not change faster",
			"configured", interval, "using", minInterval)
		interval = minInterval
	}

	window := cfg.Window
	if window <= 0 {
		window = defaultWindow
	}
	if window < minWindow {
		log.Warn("donki window widened to the minimum; a window shorter than the curation lag counts nothing",
			"configured", window, "using", minWindow)
		window = minWindow
	}
	if window > maxWindow {
		log.Warn("donki window narrowed to the maximum",
			"configured", window, "using", maxWindow)
		window = maxWindow
	}

	// cfg.Timeout is deliberately not read here. Request timeouts belong to the
	// shared httpx client, which is constructed once for the whole exporter; a
	// per-source override would mean a second client and a second, unshared
	// rate limiter for this host, which is the exact failure the shared client
	// exists to prevent.
	retries := cfg.Retries
	if retries < 0 {
		retries = 0
	}

	return &Source{
		baseURL:  base,
		client:   client,
		interval: interval,
		window:   window,
		retries:  retries,
		log:      log.With("source", Name),
		now:      time.Now,
	}, nil
}

// Name identifies the source.
func (s *Source) Name() string { return Name }

// Interval is the nominal spacing between polls.
func (s *Source) Interval() time.Duration { return s.interval }

// Schedule polls on a fixed interval, floored at minInterval.
//
// There is no publication clock to align to: an analyst adds a record whenever
// they get to it, so a fixed interval is honest here in a way it is not for
// NOAA's daily indices.
func (s *Source) Schedule() source.Schedule {
	return source.AtLeast(minInterval, source.Every(s.interval))
}

// Window is how far back events are counted.
func (s *Source) Window() time.Duration { return s.window }

// Poll fetches the three endpoints and converts them into counts and
// most-recent-analysis gauges.
//
// The endpoints are fetched concurrently and independently: losing the flare
// counts because the CME endpoint is having a bad afternoon would be a
// self-inflicted outage. A poll reports failure only when every endpoint
// failed.
func (s *Source) Poll(ctx context.Context) (metric.Batch, error) {
	batch := metric.Batch{Source: Name}

	if err := ctx.Err(); err != nil {
		return batch, fmt.Errorf("donki: poll cancelled: %w", err)
	}

	now := s.now().UTC()
	start, end := s.windowFor(now)

	var (
		wg        sync.WaitGroup
		cmes      []cmeRecord
		analyses  []cmeAnalysisRecord
		flares    []flareRecord
		cmeErr    error
		analysErr error
		flareErr  error
	)

	sem := make(chan struct{}, maxConcurrentFetches)
	run := func(fn func()) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			fn()
		}()
	}

	run(func() {
		body, err := s.fetch(ctx, pathCME, start, end)
		if err != nil {
			cmeErr = err
			return
		}
		cmeErr = decodeJSON("the CME endpoint", body, &cmes)
	})
	run(func() {
		body, err := s.fetch(ctx, pathCMEAnalysis, start, end)
		if err != nil {
			analysErr = err
			return
		}
		analysErr = decodeJSON("the CMEAnalysis endpoint", body, &analyses)
	})
	run(func() {
		body, err := s.fetch(ctx, pathFLR, start, end)
		if err != nil {
			flareErr = err
			return
		}
		flareErr = decodeJSON("the FLR endpoint", body, &flares)
	})
	wg.Wait()

	batch.Fetched = now

	var newest time.Time
	var samples []metric.Sample

	if cmeErr == nil {
		count, at := s.summariseCMEs(cmes, start, now)
		samples = s.emit(samples, metric.Sample{
			Desc: metric.CMEEventCount, Value: float64(count), Time: now,
		})
		newest = later(newest, at)
	}

	if analysErr == nil {
		analysisSamples, at := s.summariseAnalyses(analyses, start, now)
		samples = append(samples, analysisSamples...)
		newest = later(newest, at)
	}

	if flareErr == nil {
		flareSamples, at := s.summariseFlares(flares, start, now)
		samples = append(samples, flareSamples...)
		newest = later(newest, at)
	}

	// Freshness comes from the newest event in the payload, never from
	// Last-Modified. A catalogue that has stopped being updated still serves a
	// current-looking 200, and this is the only signal that distinguishes "the
	// Sun is quiet" from "the analysts have gone home".
	if !newest.IsZero() {
		age := now.Sub(newest).Seconds()
		if age < 0 {
			age = 0
		}
		samples = s.emit(samples, metric.Sample{
			Desc: metric.SourceDataAge, Labels: []string{Name}, Value: age, Time: newest,
		})
	}

	batch.Samples = samples

	joined := errors.Join(cmeErr, analysErr, flareErr)
	if joined == nil {
		return batch, nil
	}
	if errors.Is(joined, source.ErrRateLimited) {
		return metric.Batch{Source: Name, Fetched: now}, joined
	}

	failed := 0
	for _, err := range []error{cmeErr, analysErr, flareErr} {
		if err != nil {
			failed++
		}
	}
	if failed < 3 {
		s.log.Warn("donki partially succeeded; publishing what was fetched",
			"samples", len(batch.Samples), "failed", failed, "of", 3, "error", joined)
		return batch, nil
	}
	return metric.Batch{Source: Name, Fetched: now},
		fmt.Errorf("donki: every endpoint failed: %w", joined)
}

// windowFor returns the startDate and endDate to request.
//
// DONKI takes whole dates, not timestamps, so startDate is the calendar day the
// window opens on and endDate is tomorrow. Tomorrow rather than today because
// the endpoint's range appears to be exclusive of a same-day boundary in some
// records' handling, and an event catalogued a few minutes ago must not be
// missed by an off-by-one on the last day.
//
// The consequence is that the fetched range is wider than the configured
// window; events outside the window are filtered out by timestamp after
// parsing, so the counts are exact even though the request is coarse.
func (s *Source) windowFor(now time.Time) (start, end time.Time) {
	now = now.UTC()
	start = now.Add(-s.window)
	end = now.AddDate(0, 0, 1)
	return start, end
}

// fetch requests one endpoint for the window.
func (s *Source) fetch(ctx context.Context, path string, start, end time.Time) ([]byte, error) {
	q := url.Values{}
	q.Set("startDate", start.UTC().Format(time.DateOnly))
	q.Set("endDate", end.UTC().Format(time.DateOnly))
	endpoint := s.baseURL + path + "?" + q.Encode()

	resp, err := s.client.Get(ctx, httpx.Request{
		URL: endpoint,
		// The CCMC origin sends Cache-Control: no-store and no validators at
		// all, so a conditional request would buy nothing.
		Conditional: false,
		Headers:     map[string]string{"Accept": "application/json"},
		Retries:     s.retries,
		MaxBody:     maxBodyBytes,
	})
	if err != nil {
		if errors.Is(err, httpx.ErrRateLimited) {
			return nil, fmt.Errorf("%w: %w", source.ErrRateLimited, err)
		}
		return nil, fmt.Errorf("%s: %w", strings.TrimPrefix(path, "/"), err)
	}
	return resp.Body, nil
}

// summariseCMEs counts the CMEs inside the window and reports the newest start
// time seen.
func (s *Source) summariseCMEs(records []cmeRecord, start, now time.Time) (int, time.Time) {
	count := 0
	var newest time.Time
	for _, r := range records {
		at, err := parseDONKITime(r.StartTime)
		if err != nil {
			s.log.Debug("donki skipped a CME with an unreadable start time",
				"activityID", r.ActivityID, "startTime", r.StartTime, "error", err)
			continue
		}
		if at.Before(start) || at.After(now) {
			continue
		}
		count++
		newest = later(newest, at)
	}
	return count, newest
}

// summariseAnalyses publishes the most recent CME analysis in the window.
//
// "Most recent" is by time21_5, the model's own reference time, not by
// submission time or by position in the response: DONKI's ordering is not
// documented and an analyst revising an old event would otherwise move a
// week-old CME to the front.
//
// The accuracy label carries DONKI's own isMostAccurate flag, so a preliminary
// measurement and the analyst's final one occupy different series rather than
// overwriting each other.
func (s *Source) summariseAnalyses(records []cmeAnalysisRecord, start, now time.Time) ([]metric.Sample, time.Time) {
	var best *cmeAnalysisRecord
	var bestAt time.Time

	for i := range records {
		r := &records[i]
		at, err := parseDONKITime(r.Time215)
		if err != nil {
			s.log.Debug("donki skipped a CME analysis with an unreadable time21_5",
				"time21_5", r.Time215, "error", err)
			continue
		}
		if at.Before(start) || at.After(now) {
			continue
		}
		if best == nil || at.After(bestAt) {
			best, bestAt = r, at
		}
	}
	if best == nil {
		return nil, time.Time{}
	}

	accuracy := "other"
	if best.IsMostAccurate {
		accuracy = "most_accurate"
	}

	var out []metric.Sample
	if best.Speed != nil {
		out = s.emit(out, metric.Sample{
			Desc: metric.CMESpeed, Labels: []string{accuracy}, Value: *best.Speed, Time: bestAt,
		})
	}
	if best.HalfAngle != nil {
		out = s.emit(out, metric.Sample{
			Desc: metric.CMEHalfAngle, Labels: []string{accuracy}, Value: *best.HalfAngle, Time: bestAt,
		})
	}
	if best.Latitude != nil {
		out = s.emit(out, metric.Sample{
			Desc: metric.CMESourceLatitude, Value: *best.Latitude, Time: bestAt,
		})
	}
	if best.Longitude != nil {
		out = s.emit(out, metric.Sample{
			Desc: metric.CMESourceLongitude, Value: *best.Longitude, Time: bestAt,
		})
	}
	return out, bestAt
}

// summariseFlares counts flares per class letter inside the window.
//
// Every class in flareClasses gets a sample even when its count is zero. A
// dropped series reads as "the exporter is broken" on a dashboard, where a zero
// reads as "no X-class flares this week", and the second is the true statement.
func (s *Source) summariseFlares(records []flareRecord, start, now time.Time) ([]metric.Sample, time.Time) {
	counts := make(map[string]int, len(flareClasses))
	for _, class := range flareClasses {
		counts[class] = 0
	}

	var newest time.Time
	for _, r := range records {
		at, ok := r.observedAt()
		if !ok {
			s.log.Debug("donki skipped a flare with no readable time",
				"flrID", r.FlrID, "beginTime", r.BeginTime, "peakTime", r.PeakTime)
			continue
		}
		if at.Before(start) || at.After(now) {
			continue
		}
		class, ok := flareClass(r.ClassType)
		if !ok {
			s.log.Debug("donki skipped a flare with an unrecognised class",
				"flrID", r.FlrID, "classType", r.ClassType)
			continue
		}
		counts[class]++
		newest = later(newest, at)
	}

	out := make([]metric.Sample, 0, len(counts))
	for _, class := range flareClasses {
		out = s.emit(out, metric.Sample{
			Desc: metric.FlareEventCount, Labels: []string{class},
			Value: float64(counts[class]), Time: now,
		})
	}
	// A class DONKI catalogued that this package does not list — an A-class
	// entry, say — is still published rather than silently dropped.
	for class, n := range counts {
		if slices.Contains(flareClasses, class) {
			continue
		}
		out = s.emit(out, metric.Sample{
			Desc: metric.FlareEventCount, Labels: []string{class},
			Value: float64(n), Time: now,
		})
	}
	return out, newest
}

// later returns whichever time is more recent.
func later(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}

// emit appends a sample after checking it against its descriptor.
func (s *Source) emit(out []metric.Sample, sample metric.Sample) []metric.Sample {
	if err := sample.Validate(); err != nil {
		s.log.Warn("donki discarded an invalid sample", "error", err)
		return out
	}
	return append(out, sample)
}
