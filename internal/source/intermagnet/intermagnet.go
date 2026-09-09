// Package intermagnet polls the INTERMAGNET geomagnetic observatory network
// for ground magnetometer data: the X, Y, Z and S field components at each
// observatory, and an hourly peak-to-peak horizontal range derived here from
// the component series.
//
// # Licence
//
// INTERMAGNET data are provided to users under a Creative Commons
// Attributions-NonCommercial 4.0 International Licence (CC BY-NC 4.0), unless
// otherwise noted. Some institutes contributing data to INTERMAGNET may do so
// under different licence arrangements.
//
// If data are required for commercial use, sale or bulk distribution you must
// obtain written permission directly from the institute operating the
// observatory.
//
// The required acknowledgement is:
//
//	The results presented in this paper rely on data collected at magnetic
//	observatories. We thank the national institutes that support them and
//	INTERMAGNET for promoting high standards of magnetic observatory practice
//	(www.intermagnet.org).
//
// The acknowledgement is owed to each operating institute, not to INTERMAGNET
// alone. Every response names its own operator in "@info".institute, so the
// set of institutes actually being fetched is logged once, at the first poll,
// rather than left for an operator to reconstruct from a station list.
//
// Enabling this source is an acceptance of those terms, which is why it is
// disabled by default: a non-commercial restriction should be something an
// operator opts into knowingly rather than inherits.
//
// # Where the data actually lives
//
// The canonical intermagnet.org/data/imag-data path is gone — it returns 404.
// The live service is the British Geological Survey's Geomagnetism Information
// Node:
//
//	GET https://imag-data.bgs.ac.uk/GIN_V1/GINServices
//	    ?Request=GetData&format=json&observatoryIagaCode=BOU
//	    &samplesPerDay=minute&publicationState=Best+available
//	    &dataStartDate=2026-09-09&dataDuration=1
//
// The parameter names matter more than they look. The widely copy-pasted form
// testObsList=ESK&Start_Date=...&Duration=P1D returns HTTP 400 "Missing
// observatory code": it is a fragment of an older interface that no longer
// exists. samplesPerDay takes "minute" or "second" and rejects a numeric 1 with
// "Invalid samples/day: 1". publicationState accepts "Best available",
// "adj-or-rep", "adjusted", "definitive", "quasi-def", "reported" and "test".
//
// There is no shorter window parameter. dataDuration is counted in whole days,
// so fetching the current field means fetching a whole day of one-minute
// samples — about 87 KB per observatory — and reading the newest usable value
// off the end of it. That single fact drives two decisions here: the default
// interval is five minutes rather than one, and the default observatory list is
// five stations rather than the network's 154. Polling all 154 would be roughly
// 13 MB per poll, most of it either stale or absent.
//
// # The embargo problem
//
// Nineteen of the 154 observatories embargo their data: ESK, HAD and LER by ten
// days, JCO by six months, and fifteen Canadian and remote sites by a day.
//
// Worse, the capabilities table lies. GetCapabilities advertises
// DataEmbargoHours: 0 for ESK and HAD, and a real query for either returns a
// perfectly well-formed document containing 1,440 timestamps, every value
// null, and embargo_applied: true buried in "@info". Nothing about the HTTP
// response says anything is wrong. The advertised embargo therefore cannot be
// trusted, and GetCapabilities is not consulted at all.
//
// A further class of station answers 200 with a full day of nulls and no
// embargo flag: it simply has not reported today. Measured at 14:00 UT on
// 2026-09-09, NAQ, THL, BFE, UPS, LYC, ABK, KIR, WNG, NGK, THY and KAK were all
// in that state, while HRN, BEL, HLP, IZN and CLF were within ten minutes of
// real time.
//
// So every configured observatory is probed once — on the first poll, since a
// constructor that blocks process start on a remote host is worse than one that
// does not — and one that comes back embargoed or empty is logged by name with
// its reason and dropped from the polling set, rather than fetching 87 KB every
// five minutes forever to publish nothing. Dropped observatories are re-probed
// every six hours, because "no data today" is a statement about today: a
// station silent this morning may be reporting this afternoon, and a permanent
// exclusion decided at 00:05 UT would be a poor way to treat a whole network.
//
// One bad station never stops the others. Startup does not fail, a fetch
// failure is transient and keeps the station in the set, and a poll reports an
// error only when every observatory failed.
//
// # Sentinels
//
// A missing value is JSON null in this format, and 99999.00 (or 88888.00, "not
// observed") in the IAGA-2002 text format the same service can emit. All three
// are skipped, never published. A null is a missing observation, not a zero,
// and publishing it as one would put a 0 nT field at Boulder on a dashboard.
package intermagnet

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/RealDougEubanks/solarham/internal/config"
	"github.com/RealDougEubanks/solarham/internal/httpx"
	"github.com/RealDougEubanks/solarham/internal/metric"
	"github.com/RealDougEubanks/solarham/internal/redact"
	"github.com/RealDougEubanks/solarham/internal/source"
)

// Name identifies this source in logs, metrics and health output.
const Name = "intermagnet"

const (
	// defaultBaseURL is the BGS Geomagnetism Information Node.
	defaultBaseURL = "https://imag-data.bgs.ac.uk"

	// pathServices is the single entry point; the Request parameter selects the
	// operation.
	pathServices = "/GIN_V1/GINServices"

	// defaultInterval is five minutes rather than one because each poll costs a
	// whole day of minute data per observatory and the trailing edge of that
	// series lags several minutes anyway. See the package comment.
	defaultInterval = 5 * time.Minute

	// defaultTimeout bounds one request. An 87 KB document from a UK server is
	// not fast for every deployment, so this is more generous than most sources.
	defaultTimeout = 60 * time.Second

	// maxBodyBytes bounds a response. A full day of minute data for four
	// components is about 87 KB; a day of one-second data would be sixty times
	// that, and this cap is sized to allow it without allowing a runaway.
	maxBodyBytes = 8 << 20

	// maxConcurrentFetches bounds how many observatories are in flight at once.
	// The shared client's per-host spacing already paces requests; this stops a
	// long observatory list from queueing dozens of goroutines behind it.
	maxConcurrentFetches = 3

	// hourlyRange is the span the peak-to-peak horizontal range is computed
	// over.
	hourlyRange = time.Hour

	// reprobeInterval is how long an excluded observatory stays excluded before
	// it is tried again.
	reprobeInterval = 6 * time.Hour

	// samplesPerDay selects one-minute data. "second" is the only alternative
	// and is sixty times the bytes to answer the same question.
	samplesPerDay = "minute"

	// publicationState asks for whatever is best available: definitive data
	// where it exists, reported data at the trailing edge. Asking for
	// "definitive" would return nothing for the current day.
	publicationState = "Best available"

	// dayLayout is the date form dataStartDate takes.
	dayLayout = "2006-01-02"
)

// defaultObservatories is a deliberately short, verified-fresh set: BOU
// (Boulder, USGS), FRD (Fredericksburg, USGS), CLF (Chambon-la-Forêt, IPGP),
// HRN (Hornsund, IGF PAS) and BEL (Belsk, IGF PAS). All five were within ten
// minutes of real time when measured, and between them they cover North
// America, western Europe, Poland and Svalbard — one auroral-zone station and
// four mid-latitude ones, which is what a propagation dashboard can use.
//
// The network has 154 observatories. Iterating all of them would fetch roughly
// 13 MB per poll to publish a few hundred series, most of which would be
// embargoed, absent, or hours out of date. An operator who wants more can list
// more.
var defaultObservatories = []string{"BOU", "FRD", "CLF", "HRN", "BEL"}

// publishedComponents are the field components published, in a fixed order so
// the sample list is deterministic.
//
// Unlike most services here, S is included alongside X, Y and Z. The GIN
// reports it as an independently measured scalar total intensity from a
// separate instrument, not as sqrt(X²+Y²+Z²) computed from the vector channels,
// and the difference between the two is how observatory staff detect a drifting
// vector magnetometer. It is worth one series.
var publishedComponents = []string{"X", "Y", "Z", "S"}

// Exclusion reasons, which become the StationsFiltered "reason" label.
const (
	reasonEmbargoed   = "embargoed"
	reasonNoData      = "no_data"
	reasonFetchFailed = "fetch_failed"
)

// filterReasons is every reason, so each series is published every poll even at
// zero. A gauge that only appears once something is wrong cannot be alerted on.
var filterReasons = []string{reasonEmbargoed, reasonNoData, reasonFetchFailed}

// exclusion records why an observatory was dropped and when.
type exclusion struct {
	reason string
	at     time.Time
}

// Source polls the BGS Geomagnetism Information Node.
type Source struct {
	baseURL       string
	client        *httpx.Client
	observatories []string
	interval      time.Duration
	timeout       time.Duration
	retries       int
	log           *slog.Logger

	// now is injectable so the age and freshness calculations, which are most
	// of the reason this source exists, can be tested against a fixed fixture.
	now func() time.Time

	// mu guards the exclusion set and the once-only institute log. Poll fetches
	// observatories concurrently, and the exclusion set is read to decide what
	// to fetch and written from the results.
	mu               sync.Mutex
	excluded         map[string]exclusion
	institutesLogged bool
}

var _ source.Source = (*Source)(nil)
var _ source.Scheduled = (*Source)(nil)

// New builds a source from validated configuration.
//
// The HTTP client is the shared one: per-host request spacing has to be shared
// across sources or it is not spacing at all. Its timeout is process-wide, so
// cfg.Timeout is applied here as a per-request context deadline instead.
//
// No network request is made here. The observatory probe described in the
// package comment needs a context and must not be able to fail process start,
// so it happens on the first poll.
func New(cfg config.INTERMAGNET, client *httpx.Client, log *slog.Logger) (*Source, error) {
	if client == nil {
		return nil, errors.New("intermagnet: nil HTTP client")
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
		return nil, fmt.Errorf("intermagnet: base URL %q is not a URL: %w", redact.URL(base), err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("intermagnet: base URL must be http or https, got %q", u.Scheme)
	}
	if u.Host == "" {
		return nil, errors.New("intermagnet: base URL has no host")
	}

	codes := normaliseCodes(cfg.Observatories)
	if len(codes) == 0 {
		codes = append([]string(nil), defaultObservatories...)
	}

	interval := cfg.Interval
	if interval <= 0 {
		interval = defaultInterval
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	retries := cfg.Retries
	if retries < 0 {
		retries = 0
	}

	return &Source{
		baseURL:       base,
		client:        client,
		observatories: codes,
		interval:      interval,
		timeout:       timeout,
		retries:       retries,
		log:           log.With("source", Name),
		now:           time.Now,
		excluded:      make(map[string]exclusion),
	}, nil
}

// normaliseCodes upper-cases, trims and de-duplicates IAGA codes while keeping
// the configured order.
func normaliseCodes(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, code := range in {
		code = strings.ToUpper(strings.TrimSpace(code))
		if code == "" {
			continue
		}
		if _, dup := seen[code]; dup {
			continue
		}
		seen[code] = struct{}{}
		out = append(out, code)
	}
	return out
}

// Name identifies the source.
func (s *Source) Name() string { return Name }

// Interval is the nominal spacing between polls.
func (s *Source) Interval() time.Duration { return s.interval }

// Schedule polls on a fixed interval. The service has no publication clock to
// align to: each observatory appends to the current day's file as its data
// arrives, at its own latency.
func (s *Source) Schedule() source.Schedule { return source.Every(s.interval) }

// fetchResult is one observatory's outcome, carried back from its goroutine.
type fetchResult struct {
	code string
	doc  *series
	err  error
}

// Poll fetches every observatory still in the polling set and converts what
// comes back into samples.
//
// Observatories are fetched concurrently, bounded to maxConcurrentFetches, and
// one failing never loses the others: a magnetometer down for maintenance is an
// ordinary Tuesday. The poll fails only when every observatory failed, which
// means the service or the network is the problem rather than a station.
func (s *Source) Poll(ctx context.Context) (metric.Batch, error) {
	now := s.now().UTC()
	batch := metric.Batch{Source: Name, Fetched: now}

	if err := ctx.Err(); err != nil {
		return batch, fmt.Errorf("intermagnet: poll cancelled: %w", err)
	}

	targets := s.targets(now)
	if len(targets) == 0 {
		// Everything configured is currently excluded. That is a real state
		// worth reporting rather than an error: the StationsFiltered series
		// below says so, and the exclusions expire on their own.
		batch.Samples = s.filterSamples(nil, now)
		return batch, nil
	}

	results := s.fetchAll(ctx, targets, now)

	var (
		errs       []error
		samples    []metric.Sample
		newest     time.Time
		succeeded  int
		failures   int
		institutes = make(map[string]struct{})
	)

	for _, res := range results {
		if res.err != nil {
			// A fetch failure is transient, so the observatory stays in the
			// polling set and is simply counted. Only an embargo or an empty
			// day earns an exclusion.
			failures++
			errs = append(errs, res.err)
			if ctx.Err() == nil {
				s.log.Warn("intermagnet observatory fetch failed",
					"observatory", res.code, "error", res.err)
			}
			continue
		}

		if reason, ok := res.doc.unusable(); ok {
			s.exclude(res.code, reason, res.doc, now)
			continue
		}

		succeeded++
		s.reinstate(res.code)
		if inst := strings.TrimSpace(res.doc.Info.Institute); inst != "" {
			institutes[inst] = struct{}{}
		}

		stationSamples, stationNewest := s.samplesFor(res.code, res.doc)
		samples = append(samples, stationSamples...)

		if !stationNewest.IsZero() {
			if stationNewest.After(newest) {
				newest = stationNewest
			}
			// Age is stamped at poll time, not at the observation: "how old is
			// this" is a fact about now, and a sample carrying the observation's
			// own timestamp would age backwards as the scrape moved on.
			samples = s.emit(samples, metric.Sample{
				Desc:   metric.StationDataAge,
				Labels: []string{Name, res.code},
				Value:  now.Sub(stationNewest).Seconds(),
				Time:   now,
			})
		}
	}

	if !newest.IsZero() {
		samples = s.emit(samples, metric.Sample{
			Desc:   metric.SourceDataAge,
			Labels: []string{Name},
			Value:  now.Sub(newest).Seconds(),
			Time:   now,
		})
	}
	samples = append(samples, s.filterSamples(map[string]int{reasonFetchFailed: failures}, now)...)
	batch.Samples = samples

	s.logInstitutesOnce(institutes)

	joined := errors.Join(errs...)
	if joined == nil {
		return batch, nil
	}

	// A rate limit is reported upwards even when something was salvaged, so the
	// scheduler stops knocking on a door that has just been closed.
	if errors.Is(joined, httpx.ErrRateLimited) {
		return metric.Batch{Source: Name, Fetched: now},
			fmt.Errorf("%w: %w", source.ErrRateLimited, joined)
	}
	if succeeded > 0 {
		s.log.Warn("intermagnet partially succeeded; publishing what was fetched",
			"observatories", succeeded, "of", len(targets), "samples", len(batch.Samples))
		return batch, nil
	}
	return metric.Batch{Source: Name, Fetched: now},
		fmt.Errorf("intermagnet: every observatory failed: %w", joined)
}

// fetchAll fetches the given observatories concurrently, bounded, preserving
// the configured order in the returned slice so logs and samples are
// deterministic.
func (s *Source) fetchAll(ctx context.Context, codes []string, now time.Time) []fetchResult {
	results := make([]fetchResult, len(codes))
	sem := make(chan struct{}, maxConcurrentFetches)

	var wg sync.WaitGroup
	for i, code := range codes {
		wg.Add(1)
		go func(i int, code string) {
			defer wg.Done()

			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				results[i] = fetchResult{code: code, err: fmt.Errorf(
					"intermagnet: %s abandoned: %w", code, ctx.Err())}
				return
			}
			defer func() { <-sem }()

			doc, err := s.fetchObservatory(ctx, code, now)
			results[i] = fetchResult{code: code, doc: doc, err: err}
		}(i, code)
	}
	wg.Wait()
	return results
}

// fetchObservatory fetches one observatory's current day, and the previous day
// as well when the hourly window would reach back across midnight UT.
//
// The API's shortest window is a whole day, so in the first hour of a UT day
// today's file holds only a few minutes of samples and the peak-to-peak range
// would be computed over nothing. Rather than pretend, the previous day is
// fetched and prepended — one extra request per observatory per day, confined
// to that hour.
func (s *Source) fetchObservatory(ctx context.Context, code string, now time.Time) (*series, error) {
	today, err := s.fetchDay(ctx, code, now)
	if err != nil {
		return nil, err
	}

	// Only reach back when the window genuinely crosses the day boundary.
	startOfDay := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	if !now.Add(-hourlyRange).Before(startOfDay) {
		return today, nil
	}

	yesterday, err := s.fetchDay(ctx, code, now.AddDate(0, 0, -1))
	if err != nil {
		s.log.Debug("intermagnet previous day unavailable; the hourly range may be short",
			"observatory", code, "error", err)
		return today, nil
	}
	return yesterday.append(today), nil
}

// fetchDay requests one observatory's one-minute data for one UT day.
func (s *Source) fetchDay(ctx context.Context, code string, day time.Time) (*series, error) {
	q := url.Values{}
	q.Set("Request", "GetData")
	q.Set("format", "json")
	q.Set("observatoryIagaCode", code)
	q.Set("samplesPerDay", samplesPerDay)
	q.Set("publicationState", publicationState)
	q.Set("dataStartDate", day.UTC().Format(dayLayout))
	q.Set("dataDuration", "1")
	endpoint := s.baseURL + pathServices + "?" + q.Encode()

	reqCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	resp, err := s.client.Get(reqCtx, httpx.Request{
		URL:     endpoint,
		Retries: s.retries,
		MaxBody: maxBodyBytes,
	})
	if err != nil {
		return nil, fmt.Errorf("intermagnet: %s: %w", code, err)
	}
	if len(resp.Body) == 0 {
		return nil, fmt.Errorf("intermagnet: %s returned an empty body from %s",
			code, redact.URL(endpoint))
	}

	var doc series
	if err := json.Unmarshal(resp.Body, &doc); err != nil {
		// Three observatories were observed answering 200 with a non-JSON error
		// page, so this is a real path rather than a paranoid one.
		return nil, fmt.Errorf("intermagnet: %s returned unparseable JSON from %s: %w",
			code, redact.URL(endpoint), err)
	}
	if len(doc.Times) == 0 {
		return nil, fmt.Errorf("intermagnet: %s returned no timestamps from %s",
			code, redact.URL(endpoint))
	}
	return &doc, nil
}

// samplesFor converts one observatory's series into samples and reports the
// timestamp of its newest usable observation.
func (s *Source) samplesFor(code string, doc *series) ([]metric.Sample, time.Time) {
	var (
		out    []metric.Sample
		newest time.Time
	)

	for _, component := range publishedComponents {
		value, at, ok := doc.newestUsable(component)
		if !ok {
			continue
		}
		out = s.emit(out, metric.Sample{
			Desc:   metric.GeomagneticFieldComponent,
			Labels: []string{code, component},
			Value:  value,
			Time:   at,
		})
		if at.After(newest) {
			newest = at
		}
	}

	if span, at, ok := doc.horizontalRange(hourlyRange); ok {
		out = s.emit(out, metric.Sample{
			Desc:   metric.GeomagneticFieldRange,
			Labels: []string{code},
			Value:  span,
			Time:   at,
		})
		if at.After(newest) {
			newest = at
		}
	}

	return out, newest
}

// targets returns the observatories to fetch this poll: everything configured
// except those excluded within the last reprobeInterval.
func (s *Source) targets(now time.Time) []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]string, 0, len(s.observatories))
	for _, code := range s.observatories {
		if ex, ok := s.excluded[code]; ok && now.Sub(ex.at) < reprobeInterval {
			continue
		}
		out = append(out, code)
	}
	return out
}

// exclude drops an observatory from the polling set and says why.
//
// The first exclusion of a station, or a change of reason, is a warning naming
// the observatory: an operator who configured ESK deserves to be told that ESK
// answers with a day of nulls rather than to watch a silent dashboard. A
// repeated exclusion at re-probe time is only a debug line, so a permanently
// embargoed station does not warn every six hours forever.
func (s *Source) exclude(code, reason string, doc *series, now time.Time) {
	s.mu.Lock()
	prev, existed := s.excluded[code]
	s.excluded[code] = exclusion{reason: reason, at: now}
	s.mu.Unlock()

	detail := "the response carried timestamps but no usable values"
	if reason == reasonEmbargoed {
		detail = "the response set embargo_applied, so this observatory withholds current data"
	}

	attrs := []any{
		"observatory", code,
		"reason", reason,
		"detail", detail,
		"retry_in", reprobeInterval,
	}
	if doc != nil {
		if name := strings.TrimSpace(doc.Info.StationName); name != "" {
			attrs = append(attrs, "station", name)
		}
		if inst := strings.TrimSpace(doc.Info.Institute); inst != "" {
			attrs = append(attrs, "institute", inst)
		}
		attrs = append(attrs, "samples", len(doc.Times))
	}

	if existed && prev.reason == reason {
		s.log.Debug("intermagnet observatory is still not publishing usable data", attrs...)
		return
	}
	s.log.Warn("intermagnet observatory excluded from polling", attrs...)
}

// reinstate clears an exclusion after a successful re-probe.
func (s *Source) reinstate(code string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ex, ok := s.excluded[code]; ok {
		delete(s.excluded, code)
		s.log.Info("intermagnet observatory is publishing again; returning it to the polling set",
			"observatory", code, "was", ex.reason)
	}
}

// filterSamples publishes one count per exclusion reason, so an operator seeing
// no data for a station can tell whether the filter or the upstream is
// responsible. extra carries counts that are per-poll rather than persistent,
// which is to say the fetch failures.
func (s *Source) filterSamples(extra map[string]int, now time.Time) []metric.Sample {
	counts := make(map[string]int, len(filterReasons))
	for _, reason := range filterReasons {
		counts[reason] = 0
	}

	s.mu.Lock()
	for _, ex := range s.excluded {
		counts[ex.reason]++
	}
	s.mu.Unlock()

	for reason, n := range extra {
		counts[reason] += n
	}

	out := make([]metric.Sample, 0, len(filterReasons))
	for _, reason := range filterReasons {
		out = s.emit(out, metric.Sample{
			Desc:   metric.StationsFiltered,
			Labels: []string{Name, reason},
			Value:  float64(counts[reason]),
			Time:   now,
		})
	}
	return out
}

// logInstitutesOnce records the operating institutes whose data this deployment
// is actually fetching.
//
// The licence's acknowledgement is owed to each of them individually, and an
// operator cannot honour that without knowing the list. It is logged once
// rather than every poll.
func (s *Source) logInstitutesOnce(institutes map[string]struct{}) {
	if len(institutes) == 0 {
		return
	}

	s.mu.Lock()
	if s.institutesLogged {
		s.mu.Unlock()
		return
	}
	s.institutesLogged = true
	s.mu.Unlock()

	names := make([]string, 0, len(institutes))
	for name := range institutes {
		names = append(names, name)
	}
	sort.Strings(names)

	s.log.Info("intermagnet data is provided under CC BY-NC 4.0 by these institutes, "+
		"each of which must be acknowledged; commercial use, sale or bulk distribution "+
		"requires their written permission",
		"institutes", strings.Join(names, "; "))
}

// emit appends a sample after checking it against its descriptor, so a label
// mismatch fails here rather than at collection time four layers away.
func (s *Source) emit(out []metric.Sample, sample metric.Sample) []metric.Sample {
	if err := sample.Validate(); err != nil {
		s.log.Warn("intermagnet discarded an invalid sample", "error", err)
		return out
	}
	return append(out, sample)
}

// ---------------------------------------------------------------------------
// Response shape
// ---------------------------------------------------------------------------

// info is the "@info" block, which carries the operating institute and the
// embargo flag.
type info struct {
	Institute      string  `json:"institute"`
	Latitude       float64 `json:"latitude"`
	Longitude      float64 `json:"longitude"`
	Altitude       float64 `json:"altitude"`
	StationName    string  `json:"station_name"`
	IAGACode       string  `json:"iaga_code"`
	DataType       string  `json:"data_type"`
	SamplePeriod   float64 `json:"sample_period"`
	EmbargoApplied bool    `json:"embargo_applied"`
}

// series is one GetData response.
//
// The value slices are []*float64 rather than []float64 because null is the
// sentinel for a missing observation and appears in every response: at the
// trailing edge of a live station, and for all 1,440 entries of an embargoed
// one. Decoding into float64 would turn every gap into a plausible-looking
// zero, and an embargoed observatory into a day of 0 nT readings.
type series struct {
	Times []time.Time `json:"datetime"`
	Info  info        `json:"@info"`

	S []*float64 `json:"S"`
	X []*float64 `json:"X"`
	Y []*float64 `json:"Y"`
	Z []*float64 `json:"Z"`
}

// component returns the values for one component name.
func (s *series) component(name string) ([]*float64, bool) {
	switch name {
	case "S":
		return s.S, s.S != nil
	case "X":
		return s.X, s.X != nil
	case "Y":
		return s.Y, s.Y != nil
	case "Z":
		return s.Z, s.Z != nil
	}
	return nil, false
}

// append concatenates a later series onto this one, for the midnight case.
// Only the time and value slices are joined; the metadata of the newer document
// wins, since it is the one describing the current state of the observatory.
func (s *series) append(next *series) *series {
	if next == nil {
		return s
	}
	out := &series{
		Times: append(append([]time.Time(nil), s.Times...), next.Times...),
		Info:  next.Info,
	}
	out.S = appendValues(s.S, next.S)
	out.X = appendValues(s.X, next.X)
	out.Y = appendValues(s.Y, next.Y)
	out.Z = appendValues(s.Z, next.Z)

	// The embargo flag has to survive the join: an embargoed station is
	// embargoed on both days, and taking only the newer document's metadata
	// would be right for names and wrong here.
	out.Info.EmbargoApplied = s.Info.EmbargoApplied || next.Info.EmbargoApplied
	return out
}

func appendValues(a, b []*float64) []*float64 {
	if a == nil && b == nil {
		return nil
	}
	out := make([]*float64, 0, len(a)+len(b))
	out = append(out, a...)
	return append(out, b...)
}

// unusable reports whether this response should exclude its observatory from
// polling, and why.
//
// Two conditions matter, and they are distinguished because they mean different
// things to an operator. embargo_applied is the observatory telling us, in the
// only place it says so, that it withholds current data — that is permanent
// until the operator changes their policy. A document of nothing but nulls with
// no flag is a station that has not reported today, which may change this
// afternoon. Both produce a full day of timestamps and an HTTP 200, so neither
// is detectable without looking inside.
func (s *series) unusable() (string, bool) {
	if s.Info.EmbargoApplied {
		return reasonEmbargoed, true
	}
	for _, name := range publishedComponents {
		values, ok := s.component(name)
		if !ok {
			continue
		}
		for _, v := range values {
			if usable(v) {
				return "", false
			}
		}
	}
	return reasonNoData, true
}

// newestUsable returns the last non-null, non-sentinel value in a component and
// the timestamp it belongs to.
//
// The search runs backwards from the end because trailing nulls are the normal
// case: the observatory has not delivered the last few minutes when the request
// arrives, and the day's file is padded out to midnight regardless.
func (s *series) newestUsable(name string) (float64, time.Time, bool) {
	values, ok := s.component(name)
	if !ok {
		return 0, time.Time{}, false
	}
	n := min(len(values), len(s.Times))
	for i := n - 1; i >= 0; i-- {
		if !usable(values[i]) {
			continue
		}
		return *values[i], s.Times[i], true
	}
	return 0, time.Time{}, false
}

// horizontalRange is the peak-to-peak variation of the horizontal field over
// the given span ending at the newest usable sample.
//
// The horizontal component is sqrt(X²+Y²), which is the quantity the K index is
// scaled from, so its hourly excursion is a local disturbance measure available
// hours before the three-hourly index is published. Nothing upstream publishes
// it, which is why it is computed here.
//
// Both X and Y must be present at an instant for it to count: a horizontal
// magnitude built from X at one minute and Y at another is not a measurement of
// anything. The returned time is the newest instant that contributed, so the
// sample is stamped with the observation it describes rather than the request
// time.
func (s *series) horizontalRange(span time.Duration) (float64, time.Time, bool) {
	xs, okX := s.component("X")
	ys, okY := s.component("Y")
	if !okX || !okY {
		return 0, time.Time{}, false
	}

	n := min(min(len(xs), len(ys)), len(s.Times))

	// Find the newest instant with both components, which anchors the window.
	last := -1
	for i := n - 1; i >= 0; i-- {
		if usable(xs[i]) && usable(ys[i]) {
			last = i
			break
		}
	}
	if last < 0 {
		return 0, time.Time{}, false
	}
	cutoff := s.Times[last].Add(-span)

	var (
		lo, hi float64
		count  int
	)
	for i := 0; i <= last; i++ {
		if !usable(xs[i]) || !usable(ys[i]) {
			continue
		}
		if s.Times[i].Before(cutoff) {
			continue
		}
		h := math.Hypot(*xs[i], *ys[i])
		if count == 0 || h < lo {
			lo = h
		}
		if count == 0 || h > hi {
			hi = h
		}
		count++
	}

	// A single point has no range. Publishing 0 nT for it would read as a
	// perfectly quiet hour, which is the opposite of "we have one sample".
	if count < 2 {
		return 0, time.Time{}, false
	}
	return hi - lo, s.Times[last], true
}

// IAGA-2002 missing-value sentinels. The JSON format uses null, but the same
// service emits IAGA-2002 text on request and some mirrors of this data pass
// the text sentinels through into JSON, so both are refused here. No real field
// component comes anywhere near either value: the total intensity peaks around
// 68,000 nT.
const (
	sentinelMissing     = 99999.0
	sentinelNotObserved = 88888.0
)

// usable reports whether a decoded value is a real observation.
func usable(v *float64) bool {
	if v == nil {
		return false
	}
	f := *v
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return false
	}
	if math.Abs(f-sentinelMissing) < 0.5 || math.Abs(f-sentinelNotObserved) < 0.5 {
		return false
	}
	return true
}
