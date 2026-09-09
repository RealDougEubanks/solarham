// Package pota publishes what is currently on the air for Parks on the Air,
// from api.pota.app.
//
// Two endpoints are used, and only two:
//
//	/spot/activator  a JSON array of the activator spots that are live now
//	/activation      a JSON array of scheduled activations
//
// The spot feed is the interesting one. Each record carries an expire field in
// seconds, which means POTA is already maintaining it as a current-state
// snapshot rather than a log — the server drops a spot when it goes stale. That
// is exactly gauge-shaped: the array length is "how many portable stations are
// on the air right now", with no windowing or de-duplication for us to invent.
//
// # Endpoints deliberately not used
//
// /stats, /program/stats, /spot/comments, /spot/activator/latest and the API
// root all answer 403 {"message":"Missing Authentication Token"}. There is no
// public POTA aggregate-statistics endpoint; anything claiming otherwise is
// reading a private one. They are not attempted, because a source that fetches
// an endpoint it knows returns 403 just generates load and log noise.
//
// /program/parks/US (4.6 MB) and /locations (0.7 MB) are static reference data.
// They are not fetched on any interval. Nothing published here needs a park's
// name or coordinates, which is fortunate, because polling 4.6 MB of park
// records every minute to count spots would be indefensible.
//
// # Cardinality
//
// Spots carry reference, activator, spotter, grid4, grid6, latitude and
// longitude. None of them may become a label, and this is not a stylistic
// preference:
//
// reference is a park identifier and there are over sixty thousand of them.
// activator and spotter are callsigns, an unbounded set. grid4 is about 32,000
// values and grid6 is 32 million. A label on any of those turns a dozen series
// into a series per park per band per mode, most of which appear once for
// twenty minutes and then never again — the classic Prometheus cardinality
// explosion, where the churn is worse than the count because every distinct
// label set is retained for the full retention period.
//
// Band and mode are used, and are bounded: about ten bands with any POTA
// traffic and a handful of modes (CW, FM, FT4, FT8, SSB observed live). Band is
// derived here from the frequency rather than taken from the feed, which does
// not carry it.
//
// # Licence and stability
//
// POTA publishes no terms for this API and no documentation for it. It is
// nonetheless in wide use by third-party loggers and spotting tools, which is
// the only reason it is reasonable to rely on. It may change or disappear
// without notice; a shape change shows up here as a parse error rather than as
// silently wrong numbers.
package pota

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/RealDougEubanks/solarham/internal/config"
	"github.com/RealDougEubanks/solarham/internal/httpx"
	"github.com/RealDougEubanks/solarham/internal/metric"
	"github.com/RealDougEubanks/solarham/internal/redact"
	"github.com/RealDougEubanks/solarham/internal/source"
)

// Name identifies this source in logs, metrics and the health endpoints. It is
// stable because it becomes a metric label.
const Name = "pota"

// ProgramLabel is the value of the "program" label. The descriptors are shared
// with other portable-operation programmes, so the label names the programme
// rather than this source.
const ProgramLabel = "pota"

const (
	// DefaultBaseURL is the public API.
	DefaultBaseURL = "https://api.pota.app"

	// PathSpots carries the currently live activator spots.
	PathSpots = "/spot/activator"

	// PathActivations carries the scheduled-activation calendar.
	PathActivations = "/activation"

	// DefaultInterval is one minute. See Schedule for why.
	DefaultInterval = time.Minute

	// minInterval is a self-imposed floor. The feed is CloudFront-cached, so a
	// faster poll returns the same cached bytes; all it does is spend somebody
	// else's bandwidth. A courtesy limit an operator can casually override is
	// not a limit, so this is enforced rather than documented and hoped for.
	minInterval = 30 * time.Second

	// defaultRetries is the number of retries after the first attempt.
	defaultRetries = 2

	// maxBodyBytes bounds each response. Live sizes were 33 KB for 77 spots and
	// 70 KB for 176 scheduled activations. 4 MiB is two orders of magnitude of
	// headroom, which covers a contest weekend many times over.
	maxBodyBytes = 4 << 20

	// spotTimeLayout is how the feed writes spotTime: no zone suffix. POTA's
	// spot times are UTC, and are read as UTC rather than as local time, which
	// is the difference between a data age of thirty seconds and one of five
	// hours on a machine in New York.
	spotTimeLayout = "2006-01-02T15:04:05"

	// dateLayout is how the activation calendar writes startDate and endDate.
	dateLayout = "2006-01-02"
)

// Source polls the POTA spot and activation feeds.
type Source struct {
	base     string
	interval time.Duration
	retries  int
	client   *httpx.Client
	log      *slog.Logger

	// now is time.Now except in tests. It decides which scheduled activations
	// are underway and computes metric.SourceDataAge; the spot counts
	// themselves are stamped with the feed's own newest spotTime.
	now func() time.Time
}

// Compile-time proof the source satisfies the interface the scheduler polls.
// It intentionally does not implement source.Scheduled: this upstream really
// does change continuously, so a fixed interval is the correct model and
// declaring a publication clock would be a fiction.
var _ source.Source = (*Source)(nil)

// New builds the source from validated configuration.
//
// The client is passed in rather than constructed here because politeness is
// enforced across sources sharing a host.
//
// cfg.Timeout is not read; request timeouts belong to the shared client.
func New(cfg config.POTA, client *httpx.Client, log *slog.Logger) (*Source, error) {
	if client == nil {
		return nil, errors.New("pota: a shared httpx client is required")
	}
	if log == nil {
		log = slog.Default()
	}

	base := strings.TrimRight(strings.TrimSpace(cfg.BaseURL), "/")
	if base == "" {
		base = DefaultBaseURL
	}
	parsed, err := url.Parse(base)
	if err != nil {
		return nil, fmt.Errorf("pota: base URL %q is not a URL: %w", redact.URL(base), err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, fmt.Errorf("pota: base URL must be http or https, got %q", parsed.Scheme)
	}
	if parsed.Host == "" {
		return nil, errors.New("pota: base URL has no host")
	}

	interval := cfg.Interval
	if interval <= 0 {
		interval = DefaultInterval
	}
	if interval < minInterval {
		log.Warn("pota interval raised to the courtesy floor",
			"configured", interval, "using", minInterval)
		interval = minInterval
	}

	retries := cfg.Retries
	if retries < 0 {
		retries = 0
	}
	if cfg.Retries == 0 {
		retries = defaultRetries
	}

	return &Source{
		base:     base,
		interval: interval,
		retries:  retries,
		client:   client,
		log:      log.With("source", Name),
		now:      time.Now,
	}, nil
}

// Name identifies the source.
func (s *Source) Name() string { return Name }

// Interval is one minute by default.
//
// Faster gains nothing: the API is served through CloudFront and answers
// x-cache: Hit from cloudfront, so a ten-second poll returns the same cached
// array. Slower loses real observations: a short activation is a station in a
// park for twenty minutes, and spots expire upstream in around half an hour, so
// a five-minute poll would miss the shape of the activity it is meant to
// measure.
func (s *Source) Interval() time.Duration { return s.interval }

// Poll fetches both feeds and converts them into samples.
//
// The two are fetched concurrently and are not equally important. The spot feed
// is the headline; if it fails, the poll fails. The activation calendar is
// supporting detail, and losing it should not cost us the count of stations
// actually on the air, so its failure is logged and the poll succeeds.
func (s *Source) Poll(ctx context.Context) (metric.Batch, error) {
	fetched := s.now().UTC()
	empty := metric.Batch{Source: Name, Fetched: fetched}

	if err := ctx.Err(); err != nil {
		return empty, fmt.Errorf("pota: poll cancelled: %w", err)
	}

	var (
		wg          sync.WaitGroup
		spots       []spot
		spotErr     error
		activations []activation
		actErr      error
	)

	wg.Add(2)
	go func() {
		defer wg.Done()
		spots, spotErr = fetchJSON[spot](ctx, s, PathSpots)
	}()
	go func() {
		defer wg.Done()
		activations, actErr = fetchJSON[activation](ctx, s, PathActivations)
	}()
	wg.Wait()

	if spotErr != nil {
		if errors.Is(spotErr, httpx.ErrRateLimited) {
			return empty, fmt.Errorf("%w: %w", source.ErrRateLimited, spotErr)
		}
		return empty, fmt.Errorf("pota: fetching %s: %w", PathSpots, spotErr)
	}

	batch := metric.Batch{Source: Name, Fetched: fetched}
	batch.Samples = append(batch.Samples, s.spotSamples(spots, fetched)...)

	if actErr != nil {
		// Logged, not returned. A silently missing metric is how a dashboard
		// quietly stops showing a series for a week, so this must be visible
		// without failing a poll that produced the number people came for.
		s.log.Warn("pota scheduled-activation calendar failed; publishing spots only",
			"error", actErr)
	} else {
		batch.Samples = append(batch.Samples, metric.Sample{
			Desc:   metric.ScheduledActivations,
			Labels: []string{ProgramLabel},
			Value:  float64(countUnderway(activations, fetched, s.log)),
			// The calendar carries no publication timestamp, so the closest
			// honest stamp is when we read it.
			Time: fetched,
		})
	}

	return batch, nil
}

// spotSamples converts the spot array into the total, the band-and-mode
// breakdown, and a freshness figure.
func (s *Source) spotSamples(spots []spot, fetched time.Time) []metric.Sample {
	type key struct{ band, mode string }
	var (
		counts  = make(map[key]int, 32)
		newest  time.Time
		unknown int
	)

	for _, sp := range spots {
		if t, ok := sp.time(); ok && t.After(newest) {
			newest = t
		}

		band, ok := BandFor(sp.Frequency)
		mode := strings.ToUpper(strings.TrimSpace(sp.Mode))
		if !ok || mode == "" {
			// No band-labelled sample. A spot on 14074.5 with a blank mode is
			// still a station on the air and still counted in the total; what it
			// is not is evidence about a band. Inventing an "unknown" bucket
			// would put a label value on a dashboard that means "we could not
			// read the feed", which belongs in a log.
			unknown++
			continue
		}
		counts[key{band, mode}]++
	}

	if unknown > 0 {
		s.log.Debug("pota spots without a usable band or mode were counted in the total only",
			"spots", unknown, "of", len(spots))
	}

	// The snapshot is as of the newest spot in it. With no spots at all — which
	// does happen in the small hours — there is no upstream timestamp, so the
	// fetch time is the closest honest approximation.
	observed := newest
	if observed.IsZero() {
		observed = fetched
	}

	// Emitted even when zero. Nobody being on the air is an observation, and a
	// gauge that disappears rather than reading zero breaks every alert written
	// against it.
	out := []metric.Sample{{
		Desc:   metric.ActivationSpotsTotal,
		Labels: []string{ProgramLabel},
		Value:  float64(len(spots)),
		Time:   observed,
	}}

	// Sorted so a batch is deterministic, which makes tests readable and diffs
	// against a recorded batch meaningful.
	keys := make([]key, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}
	sortKeys(keys, func(a, b key) bool {
		if a.band != b.band {
			return a.band < b.band
		}
		return a.mode < b.mode
	})
	for _, k := range keys {
		out = append(out, metric.Sample{
			Desc:   metric.ActivationSpots,
			Labels: []string{ProgramLabel, k.band, k.mode},
			Value:  float64(counts[k]),
			Time:   observed,
		})
	}

	if !newest.IsZero() {
		age := fetched.Sub(newest).Seconds()
		if age < 0 {
			// A spot stamped in the future means clock skew somewhere. A
			// negative age reads as a broken exporter rather than a surprising
			// upstream.
			age = 0
		}
		out = append(out, metric.Sample{
			Desc:   metric.SourceDataAge,
			Labels: []string{Name},
			Value:  age,
			// The age is a property of this moment rather than of the
			// observation, so it is stamped with the fetch time.
			Time: fetched,
		})
	}

	return out
}

// sortKeys is a tiny insertion sort, used rather than pulling in sort for a
// slice that is at most a few dozen entries.
func sortKeys[T any](s []T, less func(a, b T) bool) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && less(s[j], s[j-1]); j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

// spot is one live activator spot. Only the fields used are declared; the feed
// also carries the park name, coordinates, spotter and comments, none of which
// are published, and declaring them would invite somebody to label by one.
type spot struct {
	// Frequency is kHz as a string — "7060.2", "14074". It is a string upstream
	// and is kept as one, because the interesting failure is a value that is not
	// a number at all and json.Unmarshal into a float64 would reject the whole
	// array over one bad record.
	Frequency string `json:"frequency"`
	Mode      string `json:"mode"`
	SpotTime  string `json:"spotTime"`
}

// time reads spotTime as UTC.
func (s spot) time() (time.Time, bool) {
	raw := strings.TrimSpace(s.SpotTime)
	if raw == "" {
		return time.Time{}, false
	}
	t, err := time.Parse(spotTimeLayout, raw)
	if err != nil {
		return time.Time{}, false
	}
	return t.UTC(), true
}

// activation is one scheduled activation.
//
// The feed also carries startTime and endTime. They are deliberately ignored:
// live records routinely pair a multi-month date range with times like
// 20:25–18:59, which cannot be a single span and is presumably a per-day
// operating window or simply whatever the scheduler typed. Guessing at that
// would turn a clear "scheduled today" count into a confidently wrong one, so
// the span is evaluated at whole-UTC-day resolution and the times are dropped.
type activation struct {
	StartDate string `json:"startDate"`
	EndDate   string `json:"endDate"`
}

// countUnderway counts activations whose date span contains now.
//
// The span is inclusive at both ends: an activation scheduled for today only
// has startDate == endDate == today, and an exclusive end would count it as
// zero, which is plainly wrong.
func countUnderway(activations []activation, now time.Time, log *slog.Logger) int {
	today := now.UTC().Truncate(24 * time.Hour)

	var count, malformed int
	for _, a := range activations {
		start, err := time.Parse(dateLayout, strings.TrimSpace(a.StartDate))
		if err != nil {
			malformed++
			continue
		}
		end, err := time.Parse(dateLayout, strings.TrimSpace(a.EndDate))
		if err != nil {
			malformed++
			continue
		}
		if !today.Before(start.UTC()) && !today.After(end.UTC()) {
			count++
		}
	}
	if malformed > 0 {
		log.Debug("pota scheduled activations with unreadable dates were skipped",
			"skipped", malformed, "of", len(activations))
	}
	return count
}

// fetchJSON fetches one endpoint and decodes a JSON array from it.
//
// A parse panic would otherwise take down the poll. This is somebody else's
// undocumented JSON whose field types have no contract, so a shape nobody
// anticipated is a question of when.
func fetchJSON[T any](ctx context.Context, s *Source, path string) (out []T, err error) {
	defer func() {
		if r := recover(); r != nil {
			out = nil
			err = fmt.Errorf("panic decoding %s: %v", path, r)
		}
	}()

	resp, err := s.client.Get(ctx, httpx.Request{
		URL: s.base + path,
		// No conditional request. The feed is a snapshot that changes on almost
		// every poll, and CloudFront's ETag on a body that differs every minute
		// would produce a 304 so rarely that tracking a validator is only a
		// source of confusion.
		Conditional: false,
		Retries:     s.retries,
		MaxBody:     maxBodyBytes,
	})
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(resp.Body, &out); err != nil {
		return nil, fmt.Errorf("decoding %s: %w", path, redact.Error(err))
	}
	return out, nil
}

// band is one amateur allocation, in kHz.
type band struct {
	low, high float64
	label     string
}

// bands is the frequency-to-band table, in ascending order.
//
// Two decisions are baked in.
//
// The bounds are the widest ITU-region union rather than one region's
// allocation: 80 metres is 3500–4000 in region 2 but 3500–3800 in region 1, and
// POTA is worldwide. Using the union means a legal European 80-metre spot is
// never labelled unknown, at the cost of labelling a hypothetical out-of-band
// spot as in-band, which is not a mistake worth optimising against.
//
// The table stops at 13 centimetres. Everything above it is measured in single
// spots per year, and a band label that appears twice and then never again is
// pure churn in a time-series database.
var bands = []band{
	{135.7, 137.8, "2200m"},
	{472, 479, "630m"},
	{1800, 2000, "160m"},
	{3500, 4000, "80m"},
	{5250, 5450, "60m"},
	{7000, 7300, "40m"},
	{10100, 10150, "30m"},
	{14000, 14350, "20m"},
	{18068, 18168, "17m"},
	{21000, 21450, "15m"},
	{24890, 24990, "12m"},
	{28000, 29700, "10m"},
	{50000, 54000, "6m"},
	{70000, 70500, "4m"},
	{144000, 148000, "2m"},
	{219000, 225000, "1.25m"},
	{420000, 450000, "70cm"},
	{902000, 928000, "33cm"},
	{1240000, 1300000, "23cm"},
	{2300000, 2450000, "13cm"},
}

// BandFor maps a frequency in kHz, as the feed writes it, to a metre-band
// label.
//
// It reports false for anything outside every allocation, including garbage,
// blanks and zero. That is a real case rather than a defensive one: spotting
// software lets people type the frequency, and a spot at "0" or "7.060" (MHz,
// in a kHz field) arrives from time to time. Such a spot produces no
// band-labelled sample at all — see spotSamples for why there is no "unknown"
// bucket.
func BandFor(frequency string) (string, bool) {
	khz, err := strconv.ParseFloat(strings.TrimSpace(frequency), 64)
	if err != nil {
		return "", false
	}
	for _, b := range bands {
		// Inclusive at both edges: 7000.0 and 7300.0 are both 40 metres, and
		// band edges are exactly where contest stations sit.
		if khz >= b.low && khz <= b.high {
			return b.label, true
		}
	}
	return "", false
}
