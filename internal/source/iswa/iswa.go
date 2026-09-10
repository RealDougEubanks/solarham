// Package iswa polls NASA CCMC's iSWA HAPI server for real-time L1 solar wind,
// the Kyoto Dst quicklook index and GFZ's half-hourly geomagnetic indices.
//
// It matters because SWPC retired several of the real-time solar wind products
// this exporter used to read, and iSWA republishes them — plus ACE, Wind and
// IMAP as independent cross-checks against whichever spacecraft SWPC has
// currently switched to. No credentials, no API key.
//
// # HAPI is an open standard with one sharp edge
//
// HAPI puts the real outcome of a request in a JSON status object, and permits
// a server to return that object under HTTP 200. So a request this package got
// wrong can arrive as a perfectly ordinary 200 whose body says
// {"status":{"code":1411,...}}, and an empty time range can arrive as a 200
// whose body says 1201, "OK - no data for time range". Branching on the HTTP
// code alone therefore gets two different things backwards at once: an error
// parsed as data, and an ordinary empty window reported as a failure. Every
// response here is decided by its HAPI status, never by its HTTP status.
//
// # Why no parameters argument is ever sent
//
// HAPI's /data endpoint accepts a parameters list, and the specification
// requires it to be given in the dataset's declared order. Sending it out of
// order is supposed to yield HAPI error 1411. Probing iswa.gsfc.nasa.gov live
// found something worse than an error: a request for "B_z,B_x" returned two
// columns in the *declared* order, B_x then B_z, with no complaint and no
// status code. A client that trusted its own ordering would have published Bx
// as Bz — a confidently wrong interplanetary magnetic field, which is exactly
// the sort of silent mislabelling that a dashboard cannot reveal.
//
// This package therefore never sends a parameters argument. It requests every
// parameter the dataset has, with include=header, and addresses columns by
// name from the header the server itself returned. That makes 1411 unreachable
// by construction and makes a reordered or renamed parameter list a missing
// sample rather than a wrong one. The cost is a few unused columns per row,
// which on these datasets is a few hundred bytes.
//
// Requesting include=header also folds what would be two requests per dataset,
// /info and /data, into one, and guarantees the header describes the rows it
// came with.
//
// # Trailing windows
//
// Only a short trailing window is requested — ten minutes for a one-minute
// dataset, two minutes for imap_mag at its four-second cadence — and the newest
// row is taken. The alternative, requesting a day and reading the last line,
// transfers thousands of rows to use one.
//
// Each dataset is additionally re-fetched no more often than it publishes. Dst
// is hourly; polling it every minute would fetch the same number sixty times
// per new value.
//
// # One spacecraft, because the descriptors have no room for two
//
// metric.WindSpeed, metric.WindDensity and metric.WindMagneticField carry no
// spacecraft label, and a source may not invent labels its descriptor does not
// declare. So when more than one L1 monitor is configured, all of them are
// fetched but only the primary one is published — chosen from the isPrimary and
// source parameters that the SWPC and ACE products carry — and a warning says
// so. Publishing two spacecraft under one unlabelled series would have them
// overwrite each other in the store, which is worse than dropping one on
// purpose.
package iswa

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
	"github.com/RealDougEubanks/solarham/internal/httpx"
	"github.com/RealDougEubanks/solarham/internal/metric"
	"github.com/RealDougEubanks/solarham/internal/redact"
	"github.com/RealDougEubanks/solarham/internal/source"
)

// Name identifies this source in logs, metrics and health output. It becomes a
// metric label, so it is stable.
const Name = "iswa"

const (
	// defaultBaseURL is the public HAPI endpoint.
	defaultBaseURL = "https://iswa.gsfc.nasa.gov/hapi"

	// pathData is the HAPI data endpoint.
	pathData = "/data"

	// defaultFastInterval matches the one-minute cadence of the real-time solar
	// wind products, which is the fastest thing this source publishes.
	defaultFastInterval = time.Minute

	// minInterval is a floor. Every dataset here publishes at a minute or
	// slower except imap_mag, and re-asking faster than the publisher writes
	// is load spent to learn nothing.
	minInterval = 30 * time.Second

	// maxBodyBytes bounds each response. The largest observed trailing-window
	// response is imap_mag's two minutes of four-second data at about 6 KB; a
	// megabyte is three orders of magnitude of headroom and still far below
	// anything that could exhaust a container.
	maxBodyBytes = 1 << 20

	// maxConcurrentFetches bounds how many datasets are in flight at once.
	// The shared client already spaces requests per host; this keeps the number
	// of goroutines waiting on that spacing bounded too.
	maxConcurrentFetches = 3
)

// Source polls the iSWA HAPI server.
type Source struct {
	baseURL  string
	client   *httpx.Client
	interval time.Duration
	retries  int
	datasets []dataset
	log      *slog.Logger

	// multiCraft records that more than one L1 monitor was configured, so the
	// "only the primary is published" warning can be issued once rather than
	// every minute.
	multiCraft   bool
	warnOnce     sync.Once
	warnCraftMsg string

	// now is injectable so the freshness metric and the per-dataset refetch
	// spacing can be tested against fixtures captured at a known time.
	now func() time.Time

	// mu guards lastFetch. Poll fetches datasets concurrently.
	mu        sync.Mutex
	lastFetch map[string]time.Time
}

var _ source.Source = (*Source)(nil)
var _ source.Scheduled = (*Source)(nil)

// New builds a source from validated configuration.
//
// Everything with a sensible default is defaulted rather than rejected: none of
// these settings is a credential and a missing one should not stop an exporter
// starting. A BaseURL that is not a URL is rejected, because that is a typo an
// operator needs told about.
func New(cfg config.ISWA, client *httpx.Client, log *slog.Logger) (*Source, error) {
	if client == nil {
		return nil, errors.New("iswa: a shared httpx client is required")
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
		return nil, fmt.Errorf("iswa: base URL %q is not a URL: %w", redact.URL(base), err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("iswa: base URL must be http or https, got %q", u.Scheme)
	}
	if u.Host == "" {
		return nil, errors.New("iswa: base URL has no host")
	}

	interval := cfg.FastInterval
	if interval <= 0 {
		interval = defaultFastInterval
	}
	if interval < minInterval {
		log.Warn("iswa interval raised to the floor",
			"configured", interval, "using", minInterval)
		interval = minInterval
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

	// Default to the SWPC real-time product alone. ACE, Wind and IMAP measure
	// the same solar wind from the same neighbourhood; they are cross-checks,
	// not extra data, and only one of them can be published under the
	// unlabelled wind descriptors anyway.
	craft := make([]string, 0, len(cfg.Spacecraft))
	for _, c := range cfg.Spacecraft {
		c = strings.TrimSpace(c)
		if c == "" {
			continue
		}
		if !knownSpacecraftName(c) {
			log.Warn("iswa ignoring an unknown spacecraft selector",
				"spacecraft", c, "known", knownSpacecraft)
			continue
		}
		craft = append(craft, c)
	}
	if len(craft) == 0 {
		craft = []string{"swpc"}
	}

	selected := selectDatasets(craft)
	if len(selected) == 0 {
		return nil, errors.New("iswa: no datasets selected")
	}

	// Count distinct monitors after normalisation, so listing "dscovr" and
	// "swpc" is not mistaken for two spacecraft.
	distinct := make(map[string]struct{}, len(craft))
	for _, c := range craft {
		distinct[normaliseSpacecraft(c)] = struct{}{}
	}

	s := &Source{
		baseURL:    base,
		client:     client,
		interval:   interval,
		retries:    retries,
		datasets:   selected,
		log:        log.With("source", Name),
		multiCraft: len(distinct) > 1,
		now:        time.Now,
		lastFetch:  make(map[string]time.Time, len(selected)),
	}
	s.warnCraftMsg = fmt.Sprintf("%d L1 monitors configured", len(distinct))
	return s, nil
}

// Name identifies the source.
func (s *Source) Name() string { return Name }

// Interval is the nominal spacing between polls.
func (s *Source) Interval() time.Duration { return s.interval }

// Schedule polls on a fixed interval, floored.
//
// The real-time solar wind genuinely does change every minute, so there is no
// publication clock to align to — but the floor is expressed in the schedule
// rather than in validation, because a courtesy limit an operator can override
// by editing a setting is not a limit.
func (s *Source) Schedule() source.Schedule {
	return source.AtLeast(minInterval, source.Every(s.interval))
}

// datasetResult is one dataset's contribution to a poll.
type datasetResult struct {
	set  dataset
	resp *hapiResponse
	row  []string
	at   time.Time
	// ok reports that a newest row was found. A dataset that answered "no data
	// for this window" is a success with ok false.
	ok  bool
	err error
}

// Poll fetches every selected dataset and converts the newest row of each into
// samples.
//
// Datasets are fetched concurrently and independently. One dataset failing does
// not fail the poll: the Dst index being briefly unavailable should not cost an
// operator their solar wind. A poll reports failure only when everything it
// attempted failed.
func (s *Source) Poll(ctx context.Context) (metric.Batch, error) {
	batch := metric.Batch{Source: Name}

	if err := ctx.Err(); err != nil {
		return batch, fmt.Errorf("iswa: poll cancelled: %w", err)
	}

	if s.multiCraft {
		s.warnOnce.Do(func() {
			s.log.Warn("iswa publishes only the primary L1 monitor; the wind descriptors carry no spacecraft label, "+
				"so the others are fetched as a cross-check and discarded",
				"configured", s.warnCraftMsg)
		})
	}

	due := s.dueDatasets()
	if len(due) == 0 {
		// Everything is inside its own publication cadence. Nothing new exists
		// upstream, so this is an ordinary empty poll rather than a failure.
		batch.Fetched = s.now().UTC()
		return batch, nil
	}

	results := s.fetchAll(ctx, due)

	batch.Fetched = s.now().UTC()
	batch.Samples = s.samplesFrom(results)

	var errs []error
	attempted, failed := 0, 0
	for _, r := range results {
		attempted++
		if r.err != nil {
			failed++
			errs = append(errs, fmt.Errorf("%s: %w", r.set.id, r.err))
		}
	}
	joined := errors.Join(errs...)

	if joined != nil && errors.Is(joined, source.ErrRateLimited) {
		// A refusal is reported upwards even when something was salvaged, so
		// the scheduler backs off rather than continuing to knock on a door
		// that has just been closed.
		return metric.Batch{Source: Name, Fetched: batch.Fetched}, joined
	}
	if joined == nil {
		return batch, nil
	}
	if failed < attempted {
		s.log.Warn("iswa partially succeeded; publishing what was fetched",
			"samples", len(batch.Samples), "failed", failed, "of", attempted, "error", joined)
		return batch, nil
	}
	return batch, fmt.Errorf("iswa: every dataset failed: %w", joined)
}

// dueDatasets returns the datasets whose own publication cadence has elapsed
// since they were last fetched.
func (s *Source) dueDatasets() []dataset {
	now := s.now()

	s.mu.Lock()
	defer s.mu.Unlock()

	var due []dataset
	for _, d := range s.datasets {
		last, seen := s.lastFetch[d.id]
		if seen && now.Sub(last) < d.cadence {
			continue
		}
		due = append(due, d)
	}
	return due
}

// noteFetched records that a dataset was asked for, whatever it answered.
//
// The timestamp is recorded on an answered request rather than a successful
// parse: a dataset that keeps returning "no data" should still be asked at its
// own cadence and no faster.
func (s *Source) noteFetched(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastFetch[id] = s.now()
}

// fetchAll fetches the given datasets with bounded concurrency.
func (s *Source) fetchAll(ctx context.Context, due []dataset) []datasetResult {
	results := make([]datasetResult, len(due))
	sem := make(chan struct{}, maxConcurrentFetches)

	var wg sync.WaitGroup
	for i, d := range due {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			results[i] = s.fetchDataset(ctx, d)
		}()
	}
	wg.Wait()
	return results
}

// fetchDataset requests one dataset's trailing window and finds its newest row.
func (s *Source) fetchDataset(ctx context.Context, d dataset) datasetResult {
	out := datasetResult{set: d}

	endpoint := s.dataURL(d, s.now())
	resp, err := s.client.Get(ctx, httpx.Request{
		URL: endpoint,
		// No conditional GET: the time window moves every poll, so the URL is
		// never the same twice and a validator could never match.
		Conditional: false,
		Headers:     map[string]string{"Accept": "text/csv"},
		Retries:     s.retries,
		MaxBody:     maxBodyBytes,
	})
	s.noteFetched(d.id)

	if err != nil {
		switch {
		case errors.Is(err, httpx.ErrRateLimited):
			out.err = fmt.Errorf("%w: %w", source.ErrRateLimited, err)
		case errors.Is(err, httpx.ErrPermanent):
			// A HAPI server signals an empty trailing window with HTTP 404 and
			// status 1406 as well as with HTTP 200 and status 1201 —
			// iswa.gsfc.nasa.gov was observed doing the former. The shared
			// client classifies a 404 before the body is available, so the
			// status cannot be read back; on the data endpoint a 404 is
			// therefore taken as "nothing in this window", which is what it
			// means in practice. It is not retried either way.
			s.log.Debug("iswa dataset returned no data for the requested window",
				"dataset", d.id, "error", err)
		default:
			out.err = err
		}
		return out
	}

	parsed, err := parseHAPI(resp.Body)
	if err != nil {
		out.err = fmt.Errorf("dataset %s: %w", d.id, err)
		return out
	}

	// The HAPI status, not the HTTP status, decides the outcome.
	if !parsed.Status.ok() {
		out.err = fmt.Errorf("dataset %s: %w", d.id, parsed.statusError())
		return out
	}
	if parsed.Status.noData() || len(parsed.Rows) == 0 {
		s.log.Debug("iswa dataset had no rows in the requested window",
			"dataset", d.id, "status", parsed.Status.Code)
		return out
	}

	if dupes := parsed.duplicateColumns(); len(dupes) > 0 {
		s.log.Debug("iswa dataset declared duplicate column names; using the first of each",
			"dataset", d.id, "columns", dupes)
	}

	row, at, found := parsed.newest(d.timeColumn)
	if !found {
		out.err = fmt.Errorf("dataset %s: no parseable %s column in %d rows",
			d.id, d.timeColumn, len(parsed.Rows))
		return out
	}

	out.resp, out.row, out.at, out.ok = parsed, row, at, true
	return out
}

// dataURL builds the /data request for one dataset's trailing window.
//
// No parameters argument is sent; see the package comment for why that is a
// correctness decision rather than a convenience.
func (s *Source) dataURL(d dataset, now time.Time) string {
	end := now.UTC()
	start := end.Add(-d.window)

	q := url.Values{}
	q.Set("id", d.id)
	q.Set("time.min", start.Format(time.RFC3339))
	q.Set("time.max", end.Format(time.RFC3339))
	q.Set("format", "csv")
	q.Set("include", "header")
	return s.baseURL + pathData + "?" + q.Encode()
}

// samplesFrom converts the fetched rows into samples.
func (s *Source) samplesFrom(results []datasetResult) []metric.Sample {
	var out []metric.Sample
	var newest time.Time

	// The wind descriptors have no spacecraft label, so exactly one magnetic
	// field dataset and one plasma dataset may be published.
	if mag, ok := s.primaryFor(results, kindMag); ok {
		if v, ok := mag.resp.float(mag.row, mag.set.cols[colBt]); ok {
			out = s.emit(out, metric.Sample{
				Desc: metric.WindMagneticField, Labels: []string{"bt"}, Value: v, Time: mag.at,
			})
		}
		if v, ok := mag.resp.float(mag.row, mag.set.cols[colBz]); ok {
			out = s.emit(out, metric.Sample{
				Desc: metric.WindMagneticField, Labels: []string{"bz"}, Value: v, Time: mag.at,
			})
		}
		newest = later(newest, mag.at)
		s.logPublished("magnetic field", mag)
	}

	if plasma, ok := s.primaryFor(results, kindPlasma); ok {
		if v, ok := plasma.resp.float(plasma.row, plasma.set.cols[colSpeed]); ok {
			out = s.emit(out, metric.Sample{
				Desc: metric.WindSpeed, Value: v, Time: plasma.at,
			})
		}
		if v, ok := plasma.resp.float(plasma.row, plasma.set.cols[colDensity]); ok {
			out = s.emit(out, metric.Sample{
				Desc: metric.WindDensity, Value: v, Time: plasma.at,
			})
		}
		newest = later(newest, plasma.at)
		s.logPublished("plasma", plasma)
	}

	for _, r := range results {
		if !r.ok {
			continue
		}
		switch r.set.kind {
		case kindDst:
			if v, ok := r.resp.float(r.row, r.set.cols[colDst]); ok {
				out = s.emit(out, metric.Sample{
					Desc: metric.DstNanotesla, Value: v, Time: r.at,
				})
				newest = later(newest, r.at)
			}
		case kindGeomag:
			if v, ok := r.resp.float(r.row, r.set.cols[colHp30]); ok {
				out = s.emit(out, metric.Sample{
					Desc: metric.KIndexHalfHourly, Labels: []string{"30m"}, Value: v, Time: r.at,
				})
				newest = later(newest, r.at)
			}
			if v, ok := r.resp.float(r.row, r.set.cols[colAp30]); ok {
				out = s.emit(out, metric.Sample{
					Desc: metric.APIndex, Labels: []string{"30m"}, Value: v, Time: r.at,
				})
				newest = later(newest, r.at)
			}
		}
	}

	// Freshness is measured from the upstream's own timestamp, never from
	// Last-Modified: an HTTP 200 over content that stopped moving is the
	// dominant failure mode across every space weather publisher here.
	if !newest.IsZero() {
		age := s.now().UTC().Sub(newest).Seconds()
		if age < 0 {
			age = 0
		}
		out = s.emit(out, metric.Sample{
			Desc: metric.SourceDataAge, Labels: []string{Name}, Value: age, Time: newest,
		})
	}

	return out
}

// primaryFor picks the one dataset of a kind whose values are published.
//
// Preference is the isPrimary flag, which is SWPC's own statement of which
// spacecraft is feeding the real-time product right now. Where nothing claims
// to be primary — imap_mag carries no such flag — the first dataset in
// configuration order wins, so the choice is at least the operator's.
func (s *Source) primaryFor(results []datasetResult, kind datasetKind) (datasetResult, bool) {
	var fallback datasetResult
	var haveFallback bool

	for _, r := range results {
		if !r.ok || r.set.kind != kind {
			continue
		}
		if r.set.hasPrimaryFlag {
			if v, ok := r.resp.float(r.row, paramIsPrimary); ok && v == 1 {
				return r, true
			}
		}
		if !haveFallback {
			fallback, haveFallback = r, true
		}
	}
	return fallback, haveFallback
}

// logPublished records which monitor's values went out under the unlabelled
// wind descriptors. Without this there is no way, from the metrics alone, to
// tell whether a step change in Bz was the solar wind or SWPC switching
// spacecraft — the descriptors have nowhere to carry it.
func (s *Source) logPublished(what string, r datasetResult) {
	craft := r.set.craft
	if v, ok := r.resp.value(r.row, paramSource); ok {
		craft = v
	}
	s.log.Debug("iswa published the primary L1 monitor",
		"quantity", what, "dataset", r.set.id, "spacecraft", craft, "observed", r.at)
}

// later returns whichever time is more recent.
func later(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}

// emit appends a sample after checking it against its descriptor.
//
// A sample whose label count or values do not match its descriptor would be
// rejected by Prometheus at collection time — surfacing as a broken scrape of
// every metric rather than one missing series — so it is caught here.
func (s *Source) emit(out []metric.Sample, sample metric.Sample) []metric.Sample {
	if err := sample.Validate(); err != nil {
		s.log.Warn("iswa discarded an invalid sample", "error", err)
		return out
	}
	return append(out, sample)
}
