// Package silso polls the World Data Center SILSO at the Royal Observatory of
// Belgium for the international sunspot number: today's provisional estimate
// with its dispersion and station coverage, and the 13-month smoothed monthly
// series that defines where we are in the solar cycle.
//
// # Licence
//
// SILSO's data is licensed CC BY-NC 4.0 — ATTRIBUTION, NONCOMMERCIAL. The
// required citation, verbatim:
//
//	Source: WDC-SILSO, Royal Observatory of Belgium, Brussels,
//	DOI: https://doi.org/10.24414/qnza-ac80
//
// That citation must accompany anything derived from these numbers, and the
// NonCommercial term means they may not be used in a commercial product. This
// source therefore defaults to disabled: an operator enabling it is accepting
// an obligation, and nobody should inherit that from a default they did not
// read.
//
// # Endpoints, and the one that must not be polled
//
// Three files matter, and their sizes are the whole reason this source is
// shaped the way it is:
//
//	/SILSO/DATA/EISN/EISN_current.csv     414 bytes, current month, all day
//	/SILSO/DATA/SN_ms_tot_V2.0.csv        126 KB, regenerated monthly
//	/SILSO/DATA/SN_d_tot_V2.0.csv         2.9 MB — NEVER FETCHED
//
// The daily total series is 2.9 MB of every daily sunspot number since 1818.
// Nothing in this exporter needs it: the current month is in the 414-byte EISN
// file and the cycle position is in the smoothed series. Polling a 2.9 MB file
// to read its last line would be a rude way to learn one number, so it is not
// polled at all, and there is a test asserting that no request ever goes to it.
//
// /SILSO/FORECASTS/KFprediMonthly.txt is a 404 as of this writing;
// /SILSO/FORECASTS/prediSC.txt is the surviving cycle prediction. It is not
// fetched either, for a different reason: no descriptor in this exporter
// carries a predicted sunspot number, and adding one is a change to the metric
// vocabulary rather than to this source.
//
// # Two files, two schedules
//
// The two files change on completely different clocks and polling them
// together would mean choosing between a stale estimate and a wasted 126 KB.
//
// EISN_current.csv is rewritten through the day as stations report, and is the
// only thing here with any intraday movement, so it is fetched every six hours
// — four times a day, which catches the morning and evening revisions without
// re-fetching an unchanged 414 bytes on a five-minute loop.
//
// SN_ms_tot_V2.0.csv is regenerated once a month. It is fetched at most once
// per day, gated inside Poll rather than by a second schedule, because a source
// has one Schedule and the alternative would be registering this package twice
// under two names. A conditional GET makes the daily fetch a 304 on the
// twenty-nine days a month when nothing has changed.
//
// # The -1 sentinel
//
// Both files spell a missing value as -1, or -1.0 in the smoothed series, and
// both carry rows of them: the smoothed series ends with several months of
// -1.0 because a 13-month smoothing cannot be computed until six months after
// the fact, and the whole file back to 1749 is -1.0 for the columns that did
// not exist then. Publishing that raw would put a sunspot number of minus one
// on a dashboard, which is not merely wrong but obviously wrong in a way that
// destroys trust in everything next to it. Every field is checked against the
// sentinel and a sentinel value produces no sample.
package silso

import (
	"bufio"
	"bytes"
	"context"
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
const Name = "silso"

// Citation is the attribution SILSO requires. It is exported so that startup
// can log it alongside the other restricted sources' terms.
const Citation = "Source: WDC-SILSO, Royal Observatory of Belgium, Brussels, " +
	"DOI: https://doi.org/10.24414/qnza-ac80"

const (
	// defaultBaseURL is the public site.
	defaultBaseURL = "https://www.sidc.be"

	// pathEISN is the 414-byte current-month estimated sunspot number.
	pathEISN = "/SILSO/DATA/EISN/EISN_current.csv"

	// pathSmoothed is the 13-month smoothed monthly series, 126 KB.
	pathSmoothed = "/SILSO/DATA/SN_ms_tot_V2.0.csv"

	// eisnInterval is how often the current-month file is fetched. See the
	// package comment.
	eisnInterval = 6 * time.Hour

	// smoothedInterval is the floor between fetches of the 126 KB monthly
	// series. It is a gate inside Poll rather than a schedule of its own; see
	// the package comment.
	smoothedInterval = 24 * time.Hour

	// defaultRetries is the number of retries after the first attempt. These
	// are static files on a university web server; two is fine and the fetches
	// are hours apart.
	defaultRetries = 2

	// maxBodyEISN bounds the 414-byte file generously. A month of daily rows
	// cannot exceed a couple of kilobytes.
	maxBodyEISN = 64 << 10

	// maxBodySmoothed bounds the 126 KB monthly series. 4 MB is thirty times
	// its current size and still an order of magnitude below the daily file we
	// refuse to fetch, so a misconfiguration pointing this at the wrong file
	// fails loudly rather than quietly downloading 2.9 MB.
	maxBodySmoothed = 4 << 20

	// missing is the sentinel both files use for an unavailable value. It is
	// compared with a tolerance rather than for equality because the smoothed
	// series writes it as "-1.0" and the EISN file as "-1", and a future
	// column could write "-1.00".
	missing = -1.0

	// missingTolerance is how close to the sentinel counts as the sentinel.
	// The comparison is a tolerance rather than an equality because the two
	// files spell it differently ("-1" and "-1.0") and float parsing of a
	// future "-1.00" must land in the same place. A real sunspot number and a
	// real station count are both non-negative, so nothing legitimate is
	// anywhere near this value — zero, a genuinely spotless day, is well clear
	// of it and is published.
	missingTolerance = 0.001
)

// Station-count states, published as the "state" label. Constants because a
// typo in a label value forks a series.
const (
	stateCalculated = "calculated"
	stateTotal      = "total"
)

// Source polls SILSO.
type Source struct {
	base    string
	retries int
	client  *httpx.Client
	log     *slog.Logger

	// now is time.Now except in tests.
	now func() time.Time

	// mu guards the smoothed-series gate. Poll is called from one goroutine by
	// the scheduler, but the gate is state that outlives a poll and a source
	// that quietly corrupts under a concurrent call is a bad bet.
	mu             sync.Mutex
	smoothedFetch  time.Time
	citationLogged bool
}

// Compile-time proof the source is both pollable and schedulable.
var (
	_ source.Source    = (*Source)(nil)
	_ source.Scheduled = (*Source)(nil)
)

// New builds the source from configuration.
//
// There is no interval setting for this source and that is deliberate: the two
// files' publication clocks are known — one is rewritten through the day, the
// other once a month — and an operator has no way to know better than the
// publisher does.
func New(cfg config.SILSO, client *httpx.Client, log *slog.Logger) (*Source, error) {
	if client == nil {
		return nil, errors.New("silso: a shared httpx client is required")
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
		return nil, fmt.Errorf("silso: base URL %q is not a URL: %w", redact.URL(base), err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("silso: base URL must be http or https, got %q", u.Scheme)
	}
	if u.Host == "" {
		return nil, errors.New("silso: base URL has no host")
	}

	retries := cfg.Retries
	if retries < 0 {
		retries = 0
	}
	if cfg.Retries == 0 {
		retries = defaultRetries
	}

	return &Source{
		base:    base,
		retries: retries,
		client:  client,
		log:     log.With("source", Name),
		now:     time.Now,
	}, nil
}

// Name identifies the source.
func (s *Source) Name() string { return Name }

// Interval reports whatever the schedule reports.
func (s *Source) Interval() time.Duration { return s.sched().Interval() }

// Schedule polls every six hours, which is the cadence of the only file here
// with intraday movement. The monthly series is gated to once a day inside
// Poll; see the package comment for why that is not a second Schedule.
func (s *Source) Schedule() source.Schedule { return s.sched() }

func (s *Source) sched() source.Schedule { return source.Every(eisnInterval) }

// SmoothedInterval is the floor between fetches of the 126 KB monthly series.
func (s *Source) SmoothedInterval() time.Duration { return smoothedInterval }

// Poll fetches the current-month estimate, and the smoothed series if a day has
// passed since it was last fetched.
//
// The two are independent quantities. The smoothed series failing must not lose
// today's estimate, and vice versa; a poll fails only when the EISN fetch fails
// and nothing else was produced.
func (s *Source) Poll(ctx context.Context) (metric.Batch, error) {
	fetched := s.now().UTC()
	empty := metric.Batch{Source: Name, Fetched: fetched}

	if err := ctx.Err(); err != nil {
		return empty, fmt.Errorf("silso: cancelled: %w", err)
	}

	s.logCitationOnce()

	batch := metric.Batch{Source: Name, Fetched: fetched}
	var (
		errs        []error
		rateLimited bool
		newest      time.Time
	)

	eisnSamples, eisnWhen, err := s.pollEISN(ctx)
	switch {
	case errors.Is(err, httpx.ErrNotModified):
		// The estimate has not been rewritten since the last poll. Ordinary.
	case err != nil:
		errs = append(errs, fmt.Errorf("EISN: %w", err))
		rateLimited = rateLimited || errors.Is(err, httpx.ErrRateLimited)
	default:
		batch.Samples = append(batch.Samples, eisnSamples...)
		if eisnWhen.After(newest) {
			newest = eisnWhen
		}
	}

	if s.smoothedDue(fetched) {
		smoothedSamples, err := s.pollSmoothed(ctx)
		switch {
		case errors.Is(err, httpx.ErrNotModified):
			// Twenty-nine days a month this is what happens, which is the
			// point of the conditional GET. The gate is still marked so the
			// 304 itself is not repeated for another day.
			s.noteSmoothedFetched(fetched)
		case err != nil:
			errs = append(errs, fmt.Errorf("smoothed series: %w", err))
			rateLimited = rateLimited || errors.Is(err, httpx.ErrRateLimited)
		default:
			batch.Samples = append(batch.Samples, smoothedSamples...)
			s.noteSmoothedFetched(fetched)
		}
	}

	if err := ctx.Err(); err != nil {
		return empty, fmt.Errorf("silso: cancelled: %w", err)
	}

	if len(batch.Samples) == 0 {
		if len(errs) == 0 {
			// Both files answered 304, or the month has only sentinel rows so
			// far. Neither is a failure.
			return empty, source.ErrNotModified
		}
		joined := errors.Join(errs...)
		if rateLimited {
			return empty, fmt.Errorf("%w: %w", source.ErrRateLimited, joined)
		}
		return empty, fmt.Errorf("silso: nothing was published: %w", joined)
	}
	for _, err := range errs {
		s.log.Warn("silso file failed, continuing with the rest", "error", err)
	}

	// Freshness from the estimate's own date. Note the granularity: EISN is a
	// daily value, so this normally reads somewhere between a few hours and a
	// day and a half rather than minutes. A value past three days means SILSO
	// has stopped updating the current month while still serving the file.
	if !newest.IsZero() {
		age := fetched.Sub(newest).Seconds()
		if age < 0 {
			age = 0
		}
		batch.Samples = append(batch.Samples, metric.Sample{
			Desc: metric.SourceDataAge, Labels: []string{Name},
			Value: age, Time: fetched,
		})
	}

	return batch, nil
}

// logCitationOnce puts the required attribution in the operator's logs. CC
// BY-NC obliges them to reproduce it, and a citation buried in a source comment
// is not much use to somebody who enabled this from an environment variable.
func (s *Source) logCitationOnce() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.citationLogged {
		return
	}
	s.citationLogged = true
	s.log.Info("silso data is CC BY-NC 4.0 and requires this citation", "citation", Citation)
}

// smoothedDue reports whether a day has passed since the monthly series was
// last fetched.
func (s *Source) smoothedDue(now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.smoothedFetch.IsZero() {
		return true
	}
	return now.Sub(s.smoothedFetch) >= smoothedInterval
}

func (s *Source) noteSmoothedFetched(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.smoothedFetch = now
}

// pollEISN fetches and parses the current-month estimate.
func (s *Source) pollEISN(ctx context.Context) ([]metric.Sample, time.Time, error) {
	resp, err := s.client.Get(ctx, httpx.Request{
		URL: s.base + pathEISN,
		// SILSO serves Last-Modified on these static files, and this one is
		// rewritten only when a new report arrives, so most polls should be
		// 304s.
		Conditional: true,
		Retries:     s.retries,
		MaxBody:     maxBodyEISN,
	})
	if err != nil {
		return nil, time.Time{}, err
	}
	return s.parseEISN(resp.Body)
}

// pollSmoothed fetches and parses the 13-month smoothed series.
func (s *Source) pollSmoothed(ctx context.Context) ([]metric.Sample, error) {
	resp, err := s.client.Get(ctx, httpx.Request{
		URL:         s.base + pathSmoothed,
		Conditional: true,
		Retries:     s.retries,
		MaxBody:     maxBodySmoothed,
	})
	if err != nil {
		return nil, err
	}
	return s.parseSmoothed(resp.Body)
}

// parseEISN reads EISN_current.csv.
//
// One row per elapsed day of the current month, comma-separated with a trailing
// comma:
//
//	2026, 09, 09, 2026.689, 106,  11.1,  17,  19,
//
// Columns: year, month, day, decimal year, estimated sunspot number, its
// standard deviation across reporting stations, the number of stations the
// estimate was calculated from, and the total number that reported. Only the
// newest row with a usable sunspot number is published: this is a gauge, and a
// month of daily history stamped at one scrape time is not history.
func (s *Source) parseEISN(body []byte) ([]metric.Sample, time.Time, error) {
	type record struct {
		when       time.Time
		sn         float64
		haveSN     bool
		sd         float64
		haveSD     bool
		calculated float64
		haveCalc   bool
		total      float64
		haveTotal  bool
	}

	var (
		best  record
		rows  int
		lines int
	)

	scanner := bufio.NewScanner(bytes.NewReader(body))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		lines++
		fields := splitFields(line)
		if len(fields) < 5 {
			continue
		}

		year, errY := strconv.Atoi(fields[0])
		month, errM := strconv.Atoi(fields[1])
		day, errD := strconv.Atoi(fields[2])
		if errY != nil || errM != nil || errD != nil ||
			month < 1 || month > 12 || day < 1 || day > 31 {
			continue
		}
		rows++

		// Midday rather than midnight. The value is a whole-day figure, and
		// stamping it at 00:00 would date today's estimate before any of the
		// observations behind it were made.
		when := time.Date(year, time.Month(month), day, 12, 0, 0, 0, time.UTC)
		if !when.After(best.when) && !best.when.IsZero() {
			continue
		}

		r := record{when: when}
		r.sn, r.haveSN = usable(fields[4])
		if len(fields) > 5 {
			r.sd, r.haveSD = usable(fields[5])
		}
		if len(fields) > 6 {
			r.calculated, r.haveCalc = usable(fields[6])
		}
		if len(fields) > 7 {
			r.total, r.haveTotal = usable(fields[7])
		}

		// A row whose sunspot number is the sentinel is a day SILSO has not
		// computed yet. Skipping it keeps the newest *usable* row rather than
		// the newest row.
		if !r.haveSN {
			continue
		}
		best = r
	}
	if err := scanner.Err(); err != nil {
		return nil, time.Time{}, fmt.Errorf("silso: reading the EISN file: %w", err)
	}

	if lines == 0 {
		return nil, time.Time{}, fmt.Errorf("silso: the EISN file is empty")
	}
	if rows == 0 {
		return nil, time.Time{}, fmt.Errorf("silso: the EISN file has no parseable rows: %s",
			excerpt(string(body)))
	}
	if best.when.IsZero() {
		// Every row is a sentinel. That happens on the first day or two of a
		// month before any station has reported, and it is not a failure.
		s.log.Debug("silso EISN file has no computed sunspot number yet this month")
		return nil, time.Time{}, nil
	}

	var out []metric.Sample
	out = s.emit(out, metric.Sample{
		Desc: metric.SunspotNumberEISN, Value: best.sn, Time: best.when,
	})
	if best.haveSD {
		out = s.emit(out, metric.Sample{
			Desc: metric.SunspotNumberEISNDeviation, Value: best.sd, Time: best.when,
		})
	}
	// Two labelled series rather than one metric with two names, so that the
	// calculated-to-total ratio — the honest measure of how much today's
	// estimate rests on — is a single division in any query language.
	if best.haveCalc {
		out = s.emit(out, metric.Sample{
			Desc: metric.SunspotStationCount, Labels: []string{stateCalculated},
			Value: best.calculated, Time: best.when,
		})
	}
	if best.haveTotal {
		out = s.emit(out, metric.Sample{
			Desc: metric.SunspotStationCount, Labels: []string{stateTotal},
			Value: best.total, Time: best.when,
		})
	}
	return out, best.when, nil
}

// parseSmoothed reads SN_ms_tot_V2.0.csv.
//
// Semicolon-separated, one row per month back to 1749:
//
//	2026;02;2026.122;  99.8; 15.7;  870;0
//
// Columns: year, month, decimal year, 13-month smoothed sunspot number, its
// standard deviation, the number of observations, and a provisional flag.
//
// The file's last several rows are always -1.0: a 13-month smoothing cannot be
// computed until six months after the fact. The newest row with a real value is
// therefore six or seven months old, which is inherent to the quantity and not
// a staleness problem — it is why this value is not fed into
// metric.SourceDataAge.
func (s *Source) parseSmoothed(body []byte) ([]metric.Sample, error) {
	var (
		bestWhen  time.Time
		bestValue float64
		found     bool
		rows      int
	)

	scanner := bufio.NewScanner(bytes.NewReader(body))
	scanner.Buffer(make([]byte, 0, 64<<10), 1<<20)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := splitFields(line)
		if len(fields) < 4 {
			continue
		}
		year, errY := strconv.Atoi(fields[0])
		month, errM := strconv.Atoi(fields[1])
		if errY != nil || errM != nil || month < 1 || month > 12 {
			continue
		}
		rows++

		value, ok := usable(fields[3])
		if !ok {
			continue
		}
		// Mid-month. The value is centred on the month, and dating it to the
		// first would misplace it by a fortnight on a series whose whole point
		// is the shape of an eleven-year curve.
		when := time.Date(year, time.Month(month), 15, 12, 0, 0, 0, time.UTC)
		if found && !when.After(bestWhen) {
			continue
		}
		bestWhen, bestValue, found = when, value, true
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("silso: reading the smoothed series: %w", err)
	}

	if rows == 0 {
		return nil, fmt.Errorf("silso: the smoothed series has no parseable rows: %s",
			excerpt(string(body)))
	}
	if !found {
		// Every value is a sentinel. Not possible with the real file, which
		// goes back to 1749, so this is a wrong URL or a truncated download.
		return nil, errors.New("silso: every value in the smoothed series is the -1 sentinel; " +
			"the file is probably truncated or the wrong one")
	}

	var out []metric.Sample
	out = s.emit(out, metric.Sample{
		Desc: metric.SunspotNumberSmoothed, Value: bestValue, Time: bestWhen,
	})
	return out, nil
}

// splitFields splits a row on either delimiter and trims each field.
//
// The two files disagree: EISN is comma-separated with a trailing comma,
// the smoothed series is semicolon-separated. Handling both here rather than
// parameterising the parsers means a file that changes delimiter — which SILSO
// has done before, between V1 and V2 of the series — keeps parsing.
func splitFields(line string) []string {
	sep := ","
	if strings.Contains(line, ";") {
		sep = ";"
	}
	parts := strings.Split(line, sep)
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	// Only trailing empties are dropped — EISN ends every row with a comma. An
	// empty field in the middle is kept, because dropping it would shift every
	// column after it and silently publish the station count as the sunspot
	// number.
	for len(parts) > 0 && parts[len(parts)-1] == "" {
		parts = parts[:len(parts)-1]
	}
	return parts
}

// usable parses a field and reports whether it is a real measurement rather
// than the -1 sentinel.
//
// This is the single choke point for the sentinel. Every numeric field in both
// files goes through it, because -1 appears in every column of both and a
// published sunspot number of minus one is not merely wrong but obviously
// wrong in a way that destroys trust in every series next to it.
func usable(field string) (float64, bool) {
	v, err := strconv.ParseFloat(strings.TrimSpace(field), 64)
	if err != nil {
		return 0, false
	}
	if v <= missing+missingTolerance {
		return 0, false
	}
	return v, true
}

// emit appends a sample after checking it against its descriptor.
func (s *Source) emit(out []metric.Sample, sample metric.Sample) []metric.Sample {
	if err := sample.Validate(); err != nil {
		s.log.Warn("silso discarded an invalid sample", "error", err)
		return out
	}
	return append(out, sample)
}

// bodyExcerptRunes bounds how much of a response body reaches an error
// message, since that message is destined for a log.
const bodyExcerptRunes = 160

func excerpt(body string) string {
	body = strings.TrimSpace(body)
	if body == "" {
		return `""`
	}
	runes := []rune(body)
	if len(runes) > bodyExcerptRunes {
		body = string(runes[:bodyExcerptRunes]) + "..."
	}
	return strconv.Quote(body)
}
