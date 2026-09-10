// Package nmdb polls the Neutron Monitor Database for ground-level cosmic ray
// count rates.
//
// Neutron monitor count rate is the one thing in this exporter that measures
// galactic cosmic rays rather than the Sun. It is anticorrelated with solar
// activity over the cycle, and a sudden coordinated drop across several
// monitors is a Forbush decrease following a CME — visible on the ground hours
// before anything shows up in a geomagnetic index.
//
// # Licence
//
// The response body states the terms itself, verbatim:
//
//	Data retrieved via NMDB are the property of the individual data providers.
//	These data are free for non commercial use to within the restriction
//	imposed by the providers.
//
// NON-COMMERCIAL, and the restrictions are the individual providers', not
// NMDB's — which means the obligation depends on which stations you asked for.
// This source therefore defaults to disabled: enabling it accepts an
// attribution duty to a specific list of named institutes, and nobody should
// inherit that from a default they did not read.
//
// NMDB generates the exact required acknowledgement per query, naming the PI
// institute of every station in the result. This package extracts that string
// and logs it once, on the first successful poll, so that the sentence an
// operator is obliged to reproduce is in their logs rather than in a comment
// here that only lists the default stations.
//
// # Endpoint
//
// NEST has no JSON or CSV API. There is one PHP page:
//
//	GET https://www.nmdb.eu/nest/draw_graph.php?formchk=1
//	    &stations[]=OULU&stations[]=KIEL2&stations[]=SOPO
//	    &tabchoice=revori&dtype=corr_for_efficiency&tresolution=1
//	    &force=1&yunits=0&date_choice=last&last_days=1&output=ascii
//
// output=ascii does not mean the response is ascii. It means the ascii table is
// embedded in a full HTML page, inside a <pre><code> block near the end.
//
// # The parsing hazard
//
// Do not strip tags across the whole page. The acknowledgement section of that
// page contains a complete nested HTML document — <html>, <head>, <body> and a
// closing </body></html> — which appears BEFORE the <pre><code> block that
// holds the data. A regex that removes tags globally, or one that slices to the
// first </html>, produces either the navigation furniture interleaved with the
// numbers or nothing at all. The extraction here slices strictly from the
// <pre><code> opener to the next </code> and looks at nothing else on the page.
// There is a test with a fixture containing the nested document specifically to
// keep it that way.
//
// # One request, not N
//
// NEST is a PHP page issuing a MySQL query per request against a shared
// academic host. Every configured station goes into one multi-station request:
// three stations in one query rather than three queries. The response's column
// order is NMDB's own, not the order the stations were asked for — the header
// line inside the <pre> block is parsed to map columns to stations rather than
// assumed.
//
// # Latency and missing data
//
// Data at one-minute resolution runs roughly four minutes behind real time, and
// individual stations drop out for minutes or months. A missing value is the
// literal string "null", which produces no sample rather than a zero: a
// neutron monitor reading zero counts per minute would be a broken detector,
// and publishing that as an observation is worse than publishing nothing. A
// station returning nothing but nulls across the whole window is counted in
// metric.StationsFiltered, because an operator looking at an empty graph needs
// to know whether the filter or the upstream is responsible.
package nmdb

import (
	"bufio"
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
const Name = "nmdb"

const (
	// defaultBaseURL is the public NEST host.
	defaultBaseURL = "https://www.nmdb.eu"

	// path is the only endpoint NEST offers for tabular data.
	path = "/nest/draw_graph.php"

	// defaultInterval is ten minutes.
	//
	// Native resolution is one minute and the data is about four minutes
	// behind, so a faster poll is technically possible. It is not useful:
	// cosmic ray intensity does not change meaningfully from one minute to the
	// next except during a ground-level enhancement, which is a multi-hour
	// event that ten-minute sampling resolves perfectly well. Against a PHP
	// page hitting MySQL on a shared academic host, ten minutes buys the
	// publisher a great deal and costs us nothing.
	defaultInterval = 10 * time.Minute

	// minInterval is the courtesy floor, enforced in the schedule rather than
	// in validation because a limit an operator can lower by editing a setting
	// is not a limit. Five minutes is still one request per MySQL query per
	// five minutes against somebody's university server.
	minInterval = 5 * time.Minute

	// defaultRetries is the number of retries after the first attempt. One:
	// each attempt is a fresh database query, so a retry storm here is a
	// database load problem rather than a bandwidth one.
	defaultRetries = 1

	// maxBodyBytes bounds the response. A one-day, three-station, one-minute
	// query measures about 79 KB of HTML; 8 MB covers a much longer window
	// while still stopping a broken origin from exhausting memory.
	maxBodyBytes = 8 << 20

	// preOpen and preClose delimit the data block. See the package comment for
	// why nothing outside them is looked at.
	preOpen  = "<pre><code>"
	preClose = "</code>"

	// nullValue is how NEST spells a missing measurement.
	nullValue = "null"

	// timeLayout is the timestamp format inside the block. There is no zone in
	// the text; the header states UTC.
	timeLayout = "2006-01-02 15:04:05"
)

// Filter reasons published on metric.StationsFiltered. They are constants
// because they are label values and a typo would fork a series.
const (
	reasonAllNull = "all-null"
	reasonAbsent  = "absent-from-response"
)

// defaultStations is a deliberate spread of geomagnetic cutoff rigidities
// rather than three convenient monitors: Oulu at high latitude sees the softest
// spectrum and responds first to a solar particle event, Kiel is mid-latitude,
// and South Pole has effectively zero geomagnetic cutoff and is the most
// sensitive of the three to low-energy particles. Three monitors that disagree
// tell you about the spectrum; three at the same cutoff tell you about the
// weather.
var defaultStations = []string{"OULU", "KIEL2", "SOPO"}

// Source polls NMDB NEST.
type Source struct {
	base     string
	stations []string
	retries  int
	sched    source.Schedule
	client   *httpx.Client
	log      *slog.Logger

	// now is time.Now except in tests.
	now func() time.Time

	// ackOnce guards the one-time acknowledgement log line. The required
	// attribution names individual monitor PIs and is generated per query, so
	// it is logged when it first arrives rather than hardcoded.
	ackOnce sync.Once
}

// Compile-time proof the source is both pollable and schedulable.
var (
	_ source.Source    = (*Source)(nil)
	_ source.Scheduled = (*Source)(nil)
)

// New builds the source from configuration.
func New(cfg config.NMDB, client *httpx.Client, log *slog.Logger) (*Source, error) {
	if client == nil {
		return nil, errors.New("nmdb: a shared httpx client is required")
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
		return nil, fmt.Errorf("nmdb: base URL %q is not a URL: %w", redact.URL(base), err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("nmdb: base URL must be http or https, got %q", u.Scheme)
	}
	if u.Host == "" {
		return nil, errors.New("nmdb: base URL has no host")
	}

	stations := make([]string, 0, len(cfg.Stations))
	seen := make(map[string]struct{}, len(cfg.Stations))
	for _, code := range cfg.Stations {
		code = strings.ToUpper(strings.TrimSpace(code))
		if code == "" {
			continue
		}
		if _, dup := seen[code]; dup {
			continue
		}
		seen[code] = struct{}{}
		stations = append(stations, code)
	}
	if len(stations) == 0 {
		stations = append(stations, defaultStations...)
	}

	interval := cfg.Interval
	if interval <= 0 {
		interval = defaultInterval
	}
	if interval < minInterval {
		log.Warn("nmdb interval raised to the courtesy floor",
			"configured", interval, "using", minInterval,
			"reason", "NEST is a PHP page issuing a MySQL query per request, and cosmic ray intensity does not change meaningfully minute to minute")
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
		stations: stations,
		retries:  retries,
		sched:    source.AtLeast(minInterval, source.Every(interval)),
		client:   client,
		log:      log.With("source", Name),
		now:      time.Now,
	}, nil
}

// Name identifies the source.
func (s *Source) Name() string { return Name }

// Interval reports whatever the schedule reports, so the two can never
// disagree.
func (s *Source) Interval() time.Duration { return s.sched.Interval() }

// Schedule polls on a fixed interval, floored at five minutes. See
// defaultInterval for why ten minutes rather than the one minute the data is
// available at.
func (s *Source) Schedule() source.Schedule { return s.sched }

// Stations are the configured station codes.
func (s *Source) Stations() []string { return s.stations }

// requestURL builds the one multi-station query.
//
// The parameter set is NEST's form, submitted as a GET. formchk=1 and force=1
// are what the form itself sends; without them the page renders the form rather
// than the data. tabchoice=revori asks for revised data falling back to
// original, which is what a real-time consumer wants: revised data is only
// available for older periods.
func (s *Source) requestURL() string {
	params := url.Values{}
	params.Set("formchk", "1")
	for _, code := range s.stations {
		params.Add("stations[]", code)
	}
	params.Set("tabchoice", "revori")
	params.Set("dtype", "corr_for_efficiency")
	// One-minute native resolution. Averaging upstream would hide a
	// ground-level enhancement's onset, which is the one thing here worth
	// seeing quickly.
	params.Set("tresolution", "1")
	params.Set("force", "1")
	params.Set("yunits", "0")
	params.Set("date_choice", "last")
	// One day is the smallest window the form accepts. Only the newest sample
	// per station is published; the rest of the window is what makes the
	// all-null detection meaningful.
	params.Set("last_days", "1")
	params.Set("output", "ascii")
	return s.base + path + "?" + params.Encode()
}

// Poll fetches the table and converts the newest reading per station into
// samples.
func (s *Source) Poll(ctx context.Context) (metric.Batch, error) {
	fetched := s.now().UTC()
	empty := metric.Batch{Source: Name, Fetched: fetched}

	if err := ctx.Err(); err != nil {
		return empty, fmt.Errorf("nmdb: cancelled: %w", err)
	}

	resp, err := s.client.Get(ctx, httpx.Request{
		URL: s.requestURL(),
		// No conditional GET: the page is generated per request and sends no
		// validators.
		Retries: s.retries,
		MaxBody: maxBodyBytes,
	})
	switch {
	case errors.Is(err, httpx.ErrNotModified):
		return empty, source.ErrNotModified
	case errors.Is(err, httpx.ErrRateLimited):
		return empty, fmt.Errorf("%w: %w", source.ErrRateLimited, err)
	case err != nil:
		return empty, fmt.Errorf("nmdb: query failed: %w", err)
	}

	table, err := extractBlock(string(resp.Body))
	if err != nil {
		return empty, err
	}

	// The acknowledgement is generated per query and names the PI institute of
	// every station in the result. Logged once, at info, because it is the
	// sentence an operator is obliged to reproduce.
	if ack := extractAcknowledgement(table); ack != "" {
		s.ackOnce.Do(func() {
			s.log.Info("nmdb requires this acknowledgement for the configured stations",
				"acknowledgement", ack)
		})
	}

	readings, order, err := parseTable(table)
	if err != nil {
		return empty, err
	}

	batch := metric.Batch{Source: Name, Fetched: fetched}
	var newest time.Time

	// Counts rather than a per-station series: StationsFiltered's label set is
	// (source, reason), and a station code would blow it out to an unbounded
	// third dimension for a number that is only ever 0 or a handful.
	var allNull, absent int

	present := make(map[string]struct{}, len(order))
	for _, code := range order {
		present[code] = struct{}{}
	}

	for _, code := range s.stations {
		if _, ok := present[code]; !ok {
			absent++
			s.log.Warn("nmdb returned no column for a configured station; check the station code",
				"station", code)
			continue
		}
		r, ok := readings[code]
		if !ok {
			// The column exists but every value in it was null. That is a
			// station that is down or embargoed, not a broken query.
			allNull++
			s.log.Debug("nmdb station returned nothing but nulls across the window", "station", code)
			continue
		}
		batch.Samples = s.emit(batch.Samples, metric.Sample{
			Desc: metric.NeutronMonitorRate, Labels: []string{code},
			Value: r.value, Time: r.when,
		})
		if r.when.After(newest) {
			newest = r.when
		}
	}

	// Emitted unconditionally, including as zero. A series that only appears
	// when something is wrong cannot be alerted on, and an operator staring at
	// an empty graph needs to be able to tell "we filtered them" from "the
	// upstream is down".
	for reason, count := range map[string]int{reasonAllNull: allNull, reasonAbsent: absent} {
		batch.Samples = s.emit(batch.Samples, metric.Sample{
			Desc: metric.StationsFiltered, Labels: []string{Name, reason},
			Value: float64(count), Time: fetched,
		})
	}

	// Freshness from the newest reading's own timestamp. At one-minute
	// resolution this normally sits around four minutes, which is NMDB's
	// ingest latency; a value climbing past an hour means the network has gone
	// quiet while NEST keeps serving a perfectly well-formed page.
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

// extractBlock slices the ascii table out of the HTML page.
//
// Strictly from the <pre><code> opener to the next </code>, and nothing else on
// the page is examined. See the package comment for why anything more
// permissive breaks.
func extractBlock(page string) (string, error) {
	start := strings.Index(page, preOpen)
	if start < 0 {
		return "", fmt.Errorf("nmdb: the response has no %s block; "+
			"NEST renders the form rather than the data when formchk or force is missing (body %s)",
			preOpen, excerpt(page))
	}
	start += len(preOpen)
	end := strings.Index(page[start:], preClose)
	if end < 0 {
		return "", fmt.Errorf("nmdb: the %s block is not closed; the response was probably truncated", preOpen)
	}
	return page[start : start+end], nil
}

// reading is one station's newest usable measurement.
type reading struct {
	value float64
	when  time.Time
}

// parseTable reads the ascii block.
//
// The block is a comment header, then a single header line naming the columns
// in NMDB's own order, then semicolon-separated rows of "timestamp;v;v;v".
// Only the newest non-null value per station is kept: this is a gauge, and a
// day of one-minute history would be 1,440 points per station published at a
// single scrape timestamp.
func parseTable(block string) (map[string]reading, []string, error) {
	var (
		order    []string
		readings = map[string]reading{}
		rows     int
	)

	scanner := bufio.NewScanner(strings.NewReader(block))
	scanner.Buffer(make([]byte, 0, 64<<10), 1<<20)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		// The header line is the only non-comment line with no semicolons in
		// it. Parsing it rather than assuming the requested order matters: a
		// request for OULU, KIEL2, SOPO comes back as KIEL2, OULU, SOPO.
		if !strings.Contains(line, ";") {
			if order == nil {
				order = parseHeader(line)
			}
			continue
		}

		if order == nil {
			// Data before a header means the page shape has changed. Guessing
			// which column is which station would silently attribute one
			// monitor's counts to another.
			return nil, nil, errors.New("nmdb: the data block has rows before any column header; " +
				"column-to-station mapping cannot be established")
		}

		fields := strings.Split(line, ";")
		if len(fields) < 2 {
			continue
		}
		when, err := time.Parse(timeLayout, strings.TrimSpace(fields[0]))
		if err != nil {
			// Not fatal: skip the row. A single malformed timestamp in a day
			// of data should not lose the other 1,439.
			continue
		}
		rows++

		values := fields[1:]
		for i, raw := range values {
			if i >= len(order) {
				break
			}
			raw = strings.TrimSpace(raw)
			if raw == "" || strings.EqualFold(raw, nullValue) {
				continue
			}
			v, err := strconv.ParseFloat(raw, 64)
			if err != nil {
				continue
			}
			code := order[i]
			if prev, ok := readings[code]; ok && !when.After(prev.when) {
				continue
			}
			readings[code] = reading{value: v, when: when.UTC()}
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, nil, fmt.Errorf("nmdb: reading the data block: %w", err)
	}

	if order == nil {
		return nil, nil, errors.New("nmdb: the data block has no column header; " +
			"the query may have matched no stations")
	}
	if rows == 0 {
		return nil, nil, errors.New("nmdb: the data block has a header but no rows")
	}
	return readings, order, nil
}

// parseHeader reads the column-name line.
//
// NEST has been observed emitting the header both with and without a leading
// label for the timestamp column ("start_date_time OULU KIEL2 SOPO" and
// "KIEL2 OULU SOPO"). A leading field naming the date or time column is dropped
// so that the remaining names line up with the values after the first
// semicolon; without this, every station's counts would be attributed to the
// station one column to its left.
func parseHeader(line string) []string {
	fields := strings.Fields(line)
	if len(fields) > 0 {
		first := strings.ToLower(fields[0])
		if strings.Contains(first, "date") || strings.Contains(first, "time") {
			fields = fields[1:]
		}
	}
	out := make([]string, 0, len(fields))
	for _, f := range fields {
		out = append(out, strings.ToUpper(f))
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// extractAcknowledgement pulls the generated attribution sentence out of the
// comment header.
//
// The sentence sits inside a fixed-width box drawn with "#|" and "|" borders.
// NMDB hard-wraps it at the box width without regard for word boundaries, so
// rejoining the lines with a single space occasionally leaves a word split —
// "Angew andte" for "Angewandte". The text is reproduced as generated rather
// than reflowed, because guessing where a space belongs risks changing an
// institute's name in an attribution we are obliged to reproduce accurately.
func extractAcknowledgement(block string) string {
	var parts []string
	for _, line := range strings.Split(block, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "#|") {
			continue
		}
		inner := strings.TrimSuffix(strings.TrimPrefix(line, "#|"), "|")
		// The box has rules made of underscores at top and bottom.
		if strings.Trim(inner, "_ ") == "" {
			continue
		}
		parts = append(parts, strings.TrimSpace(inner))
	}
	return strings.Join(parts, " ")
}

// emit appends a sample after checking it against its descriptor.
func (s *Source) emit(out []metric.Sample, sample metric.Sample) []metric.Sample {
	if err := sample.Validate(); err != nil {
		s.log.Warn("nmdb discarded an invalid sample", "error", err)
		return out
	}
	return append(out, sample)
}

// bodyExcerptRunes bounds how much of a response reaches an error message,
// since that message is destined for a log.
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
