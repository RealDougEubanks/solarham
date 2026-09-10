// Package pskreporter polls pskreporter.info for per-band digital-mode
// activity: the network's own weighted activity score, the spot count, and the
// number of transmitting and receiving stations on each frequency.
//
// # Terms
//
// PSK Reporter is one person's server. The operator's stated terms, verbatim:
//
//	Users are encouraged to retrieve reception data no more often than once
//	every five minutes.
//
//	I reserve right to block or rate limit anybody who imposes a significant
//	load on my system.
//
//	If you want to be notified, then please add an additional query parameter
//	of 'appcontact=myemailaddress'
//
// Those are courtesy terms rather than a licence, and they are enforced here in
// code rather than described in a comment:
//
//   - New REFUSES to build a source with an empty Contact. The operator asks
//     for an address specifically so that he can get in touch with a
//     misbehaving client before blocking it. Running an automated poller against
//     somebody's personal server while deliberately withholding the one thing
//     he asked for is not acceptable, so this is a documented refusal rather
//     than a warning. Set PSKReporter.Contact to a real address you read.
//   - appcontact is sent on every request, not just the first.
//   - The schedule is floored at five minutes and the floor is not lowerable
//     from configuration. A limit an operator can override by editing a setting
//     is not a limit.
//
// This source therefore defaults to disabled, like the other restricted ones:
// an operator enabling it is taking on an obligation to a volunteer, and that
// is not something anybody should inherit from a default they did not read.
//
// # Endpoint
//
// The reception-data firehose is an MQTT feed and a heavyweight XML query API.
// Neither is needed for band activity, because there is a small purpose-built
// text endpoint that answers the same question in about a kilobyte:
//
//	GET https://pskreporter.info/cgi-bin/psk-freq.pl?grid=FM05&appcontact=<address>
//
// It answers text/plain, self-documenting, newest-first, with the column
// meanings in a trailing comment:
//
//	14070000 4950 750 2 27
//	18100000 3831 724 1 16
//	# frequency score #spots #tx #rx
//	# grid FM%, 5 mins
//
// Omitting grid asks for the global figure, which this package labels
// grid="global".
//
// # One request per grid
//
// Each configured grid is a separate request, and each request is spaced by the
// shared httpx limiter — five minutes apart, per the policy table. That means
// configuring several grids does not multiply the request rate, but it does
// mean a poll takes several minutes of wall clock to complete while the
// limiter holds each request. More than three grids is warned about at
// construction for that reason: at a five-minute budget the fourth grid's data
// is already older than the poll interval by the time it arrives.
//
// # Freshness
//
// The response carries no timestamp at all — not in the body, and the CGI
// sends no useful Last-Modified. metric.SourceDataAge is therefore computed
// from receipt time, which makes it a measure of our own polling rather than of
// the publisher's freshness. That is worth stating plainly: for this one source
// the age series cannot detect a stalled upstream, because a stalled upstream
// looks identical to a busy one. The trailing comment does tell us the window
// the figures cover, and that window's midpoint is used as the observation
// time so the numbers are not stamped later than the data they describe.
package pskreporter

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
	"time"

	"github.com/RealDougEubanks/solarham/internal/config"
	"github.com/RealDougEubanks/solarham/internal/httpx"
	"github.com/RealDougEubanks/solarham/internal/metric"
	"github.com/RealDougEubanks/solarham/internal/redact"
	"github.com/RealDougEubanks/solarham/internal/source"
)

// Name identifies this source in logs, metrics and the health endpoints. It is
// stable because it becomes a metric label.
const Name = "pskreporter"

// network is the value of the "network" label on every sample here.
const network = "pskreporter"

// globalGrid is the label used when no grid filter was sent.
const globalGrid = "global"

const (
	// defaultBaseURL is the public site.
	defaultBaseURL = "https://pskreporter.info"

	// path is the band-activity CGI.
	path = "/cgi-bin/psk-freq.pl"

	// defaultInterval matches the operator's stated preference exactly.
	defaultInterval = 5 * time.Minute

	// minInterval is his stated limit, and it is a hard floor. This constant
	// is the reason the schedule is wrapped in source.AtLeast rather than
	// validated at load time: validation can be bypassed by a config file the
	// operator edits later, a schedule clamp cannot.
	minInterval = 5 * time.Minute

	// warnAboveGrids is how many grids can be configured before construction
	// warns. Each grid is one request against a five-minute budget.
	warnAboveGrids = 3

	// defaultWindow is the period the figures cover, per the endpoint's own
	// trailing comment ("5 mins"). It is parsed from the response when
	// present; this is the fallback.
	defaultWindow = 5 * time.Minute

	// defaultRetries is the number of retries after the first attempt. One,
	// deliberately: this is a personal server whose operator reserves the
	// right to block clients that load it, and a retry storm is exactly what
	// that clause is aimed at.
	defaultRetries = 1

	// maxBodyBytes bounds the response. The live body is about a kilobyte.
	maxBodyBytes = 256 << 10
)

// Source polls pskreporter.info.
type Source struct {
	base    string
	grids   []string
	contact string
	retries int
	sched   source.Schedule
	client  *httpx.Client
	log     *slog.Logger

	// now is time.Now except in tests.
	now func() time.Time
}

// Compile-time proof the source is both pollable and schedulable.
var (
	_ source.Source    = (*Source)(nil)
	_ source.Scheduled = (*Source)(nil)
)

// ErrContactRequired is returned by New when Contact is empty.
//
// It is exported so that startup can distinguish "the operator has not
// finished configuring this" from a transport failure, and so the refusal is
// testable by identity rather than by matching a message.
var ErrContactRequired = errors.New(
	"pskreporter: Contact is required when this source is enabled — " +
		"the operator of pskreporter.info asks in writing that automated clients " +
		"send an appcontact address so he can get in touch before blocking them; " +
		"set the pskreporter contact setting to an email address you read")

// New builds the source from configuration.
//
// It fails rather than warns when Contact is empty. See ErrContactRequired.
func New(cfg config.PSKReporter, client *httpx.Client, log *slog.Logger) (*Source, error) {
	if client == nil {
		return nil, errors.New("pskreporter: a shared httpx client is required")
	}
	if log == nil {
		log = slog.Default()
	}

	contact := strings.TrimSpace(cfg.Contact)
	if contact == "" {
		return nil, ErrContactRequired
	}
	// Not a full address validator — that is a rabbit hole and the upstream
	// does not care — but something with no "@" in it is a typo or a
	// placeholder, and a placeholder is the same as sending nothing.
	if !strings.Contains(contact, "@") {
		return nil, fmt.Errorf("%w (got %q, which is not an email address)",
			ErrContactRequired, contact)
	}

	base := strings.TrimRight(strings.TrimSpace(cfg.BaseURL), "/")
	if base == "" {
		base = defaultBaseURL
	}
	u, err := url.Parse(base)
	if err != nil {
		return nil, fmt.Errorf("pskreporter: base URL %q is not a URL: %w", redact.URL(base), err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("pskreporter: base URL must be http or https, got %q", u.Scheme)
	}
	if u.Host == "" {
		return nil, errors.New("pskreporter: base URL has no host")
	}

	// An empty grid list means one unfiltered request for the global figure.
	// It is represented as a single empty string so the poll loop has one
	// shape rather than two.
	grids := make([]string, 0, len(cfg.Grids))
	seen := make(map[string]struct{}, len(cfg.Grids))
	for _, g := range cfg.Grids {
		g = strings.ToUpper(strings.TrimSpace(g))
		if g == "" {
			continue
		}
		if _, dup := seen[g]; dup {
			continue
		}
		seen[g] = struct{}{}
		grids = append(grids, g)
	}
	if len(grids) == 0 {
		grids = []string{""}
	}
	if len(grids) > warnAboveGrids {
		log.Warn("pskreporter has more grids configured than fits its request budget",
			"grids", len(grids), "advisable", warnAboveGrids,
			"reason", "each grid is one request, and the operator asks for no more than one request every five minutes; "+
				"the shared limiter will space them, so a poll will take tens of minutes and the later grids' data will be stale on arrival")
	}

	interval := cfg.Interval
	if interval <= 0 {
		interval = defaultInterval
	}
	if interval < minInterval {
		log.Warn("pskreporter interval raised to the operator's stated limit",
			"configured", interval, "using", minInterval,
			"reason", "pskreporter.info asks for no more than one retrieval every five minutes")
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
		grids:   grids,
		contact: contact,
		retries: retries,
		// The floor is applied outside the configured interval, so an operator
		// raising the interval is honoured and one lowering it is not.
		sched:  source.AtLeast(minInterval, source.Every(interval)),
		client: client,
		log:    log.With("source", Name),
		now:    time.Now,
	}, nil
}

// Name identifies the source.
func (s *Source) Name() string { return Name }

// Interval reports whatever the schedule reports, so the two can never
// disagree.
func (s *Source) Interval() time.Duration { return s.sched.Interval() }

// Schedule polls on a fixed interval, floored at five minutes.
//
// A fixed interval is right: reception reports arrive continuously and the CGI
// recomputes on demand, so there is no publication clock to align to. The floor
// is the operator's stated limit and is documented on minInterval.
func (s *Source) Schedule() source.Schedule { return s.sched }

// Grids are the configured grid filters, with the empty string meaning the
// unfiltered global request.
func (s *Source) Grids() []string { return s.grids }

// requestURL builds the GET for one grid. appcontact is always present.
func (s *Source) requestURL(grid string) string {
	params := url.Values{}
	if grid != "" {
		params.Set("grid", grid)
	}
	// Sent on every request, not only the first: the operator's ask is that he
	// can identify the client whose traffic he is looking at, and a contact
	// address that appears once an hour does not do that.
	params.Set("appcontact", s.contact)
	return s.base + path + "?" + params.Encode()
}

// Poll fetches every configured grid and merges the samples.
//
// One grid failing does not fail the poll. A grid whose data is missing is
// logged and the rest is published; a poll fails only when every grid failed.
func (s *Source) Poll(ctx context.Context) (metric.Batch, error) {
	fetched := s.now().UTC()
	empty := metric.Batch{Source: Name, Fetched: fetched}

	// Checked before the first request as well as inside the loop, so a poll
	// cancelled at shutdown never touches the network.
	if err := ctx.Err(); err != nil {
		return empty, fmt.Errorf("pskreporter: cancelled: %w", err)
	}

	batch := metric.Batch{Source: Name, Fetched: fetched}
	var (
		errs        []error
		rateLimited bool
		newest      time.Time
	)

	// Sequential rather than concurrent, deliberately. The shared limiter
	// would serialise these anyway, and firing them concurrently would mean
	// several goroutines each holding a request slot while a personal server
	// decides what to do about it.
	for _, grid := range s.grids {
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
			break
		}
		samples, when, err := s.pollGrid(ctx, grid, fetched)
		if err != nil {
			errs = append(errs, fmt.Errorf("grid %q: %w", labelFor(grid), err))
			if errors.Is(err, httpx.ErrRateLimited) {
				rateLimited = true
			}
			continue
		}
		batch.Samples = append(batch.Samples, samples...)
		if when.After(newest) {
			newest = when
		}
	}

	// Cancellation is a failure of the whole poll regardless of how many grids
	// had already succeeded: the batch is incomplete and the scheduler is
	// shutting down anyway.
	if err := ctx.Err(); err != nil {
		return empty, fmt.Errorf("pskreporter: cancelled after %d of %d grids: %w",
			len(s.grids)-len(errs), len(s.grids), err)
	}

	if len(errs) == len(s.grids) {
		joined := errors.Join(errs...)
		if rateLimited {
			return empty, fmt.Errorf("%w: %w", source.ErrRateLimited, joined)
		}
		if err := ctx.Err(); err != nil {
			return empty, fmt.Errorf("pskreporter: cancelled: %w", err)
		}
		return empty, fmt.Errorf("pskreporter: every grid failed: %w", joined)
	}
	for _, err := range errs {
		s.log.Warn("pskreporter grid failed, continuing with the rest", "error", err)
	}

	// Freshness. See the package comment: this is derived from receipt time
	// because the response carries no timestamp, so it measures our polling
	// rather than the publisher's. It is published anyway, because a source
	// with no age series at all is indistinguishable from one whose age series
	// is broken.
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

// pollGrid fetches and parses one grid, returning the observation time the
// figures should be stamped with.
func (s *Source) pollGrid(ctx context.Context, grid string, fetched time.Time) ([]metric.Sample, time.Time, error) {
	resp, err := s.client.Get(ctx, httpx.Request{
		URL: s.requestURL(grid),
		// No conditional GET: this is a CGI that recomputes per request and
		// sends no validators.
		Retries: s.retries,
		MaxBody: maxBodyBytes,
	})
	if err != nil {
		return nil, time.Time{}, err
	}
	return s.parse(resp.Body, grid, fetched)
}

// parse reads the plain-text table.
//
// The body is newest-first data rows followed by comment lines starting with
// "#". The comments are the endpoint's self-documentation — the column legend
// and the grid and window it answered for — and must not be parsed as data: a
// naive whitespace split of "# frequency score #spots #tx #rx" produces a row
// whose first field is "#", and a parser that skips unreadable fields rather
// than skipping comment lines would happily invent a band from it.
func (s *Source) parse(body []byte, grid string, fetched time.Time) ([]metric.Sample, time.Time, error) {
	label := labelFor(grid)
	window := defaultWindow

	type row struct {
		hz     float64
		score  float64
		spots  float64
		tx     float64
		rx     float64
		lineNo int
	}
	var (
		rows     []row
		comments int
		bad      int
	)

	scanner := bufio.NewScanner(bytes.NewReader(body))
	scanner.Buffer(make([]byte, 0, 64<<10), 1<<20)
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "#") {
			comments++
			if w, ok := parseWindowComment(line); ok {
				window = w
			}
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 5 {
			bad++
			s.log.Debug("pskreporter line has too few fields; skipping it",
				"grid", label, "line", lineNo, "fields", len(fields))
			continue
		}
		nums := make([]float64, 5)
		ok := true
		for i := range nums {
			v, err := strconv.ParseFloat(fields[i], 64)
			if err != nil {
				ok = false
				break
			}
			nums[i] = v
		}
		if !ok {
			bad++
			s.log.Debug("pskreporter line has a non-numeric field; skipping it",
				"grid", label, "line", lineNo)
			continue
		}
		rows = append(rows, row{
			hz: nums[0], score: nums[1], spots: nums[2], tx: nums[3], rx: nums[4],
			lineNo: lineNo,
		})
	}
	if err := scanner.Err(); err != nil {
		return nil, time.Time{}, fmt.Errorf("pskreporter: reading the %s response: %w", label, err)
	}

	if len(rows) == 0 {
		// No data rows and no comments at all means this was not the CGI's
		// output — an error page, a redirect body, an empty file. That is a
		// failure. No data rows but the legend present means a genuinely quiet
		// grid, which is not.
		if comments == 0 {
			return nil, time.Time{}, fmt.Errorf("pskreporter: the %s response has no data and no legend: %s",
				label, excerpt(string(body)))
		}
		s.log.Debug("pskreporter reported no activity for this grid", "grid", label)
		return nil, fetched, nil
	}
	if bad > 0 {
		s.log.Warn("pskreporter skipped unreadable lines", "grid", label, "skipped", bad)
	}

	// The figures describe the window, so they are stamped at its midpoint
	// rather than at receipt. Stamping at receipt would date a five-minute
	// average as an instantaneous reading taken after the window closed.
	observed := fetched.Add(-window / 2)

	// Several dial frequencies fall in the same band — 14070000 and 14080000
	// are both 20 m — so the rows are folded per band before emitting, rather
	// than emitting duplicate series that the store would collapse
	// arbitrarily.
	type agg struct {
		score, spots, tx, rx float64
	}
	order := make([]string, 0, len(rows))
	totals := make(map[string]*agg, len(rows))
	for _, r := range rows {
		band := bandForHz(r.hz)
		if band == "" {
			s.log.Debug("pskreporter reported a frequency outside every known band; skipping it",
				"grid", label, "hz", r.hz, "line", r.lineNo)
			continue
		}
		a, ok := totals[band]
		if !ok {
			a = &agg{}
			totals[band] = a
			order = append(order, band)
		}
		a.score += r.score
		a.spots += r.spots
		a.tx += r.tx
		a.rx += r.rx
	}

	var out []metric.Sample
	for _, band := range order {
		a := totals[band]
		out = s.emit(out, metric.Sample{
			Desc: metric.BandActivityScore, Labels: []string{network, band, label},
			Value: a.score, Time: observed,
		})
		out = s.emit(out, metric.Sample{
			Desc: metric.SpotCount, Labels: []string{network, band},
			Value: a.spots, Time: observed,
		})
		out = s.emit(out, metric.Sample{
			Desc: metric.SpotStationCount, Labels: []string{network, band, "tx"},
			Value: a.tx, Time: observed,
		})
		out = s.emit(out, metric.Sample{
			Desc: metric.SpotStationCount, Labels: []string{network, band, "rx"},
			Value: a.rx, Time: observed,
		})
	}

	return out, observed, nil
}

// labelFor turns a grid filter into a label value. An empty filter asked for
// the global figure.
func labelFor(grid string) string {
	if grid == "" {
		return globalGrid
	}
	return grid
}

// parseWindowComment reads the window out of the trailing legend, which reads
// like "# grid FM%, 5 mins". The endpoint has always answered five minutes,
// but the value is in the response and reading it is cheaper than assuming it.
func parseWindowComment(line string) (time.Duration, bool) {
	line = strings.TrimSpace(strings.TrimPrefix(line, "#"))
	_, after, ok := strings.Cut(line, ",")
	if !ok {
		return 0, false
	}
	fields := strings.Fields(after)
	if len(fields) < 2 {
		return 0, false
	}
	n, err := strconv.Atoi(fields[0])
	if err != nil || n <= 0 {
		return 0, false
	}
	switch {
	case strings.HasPrefix(fields[1], "min"):
		return time.Duration(n) * time.Minute, true
	case strings.HasPrefix(fields[1], "hour"):
		return time.Duration(n) * time.Hour, true
	case strings.HasPrefix(fields[1], "sec"):
		return time.Duration(n) * time.Second, true
	}
	return 0, false
}

// emit appends a sample after checking it against its descriptor.
func (s *Source) emit(out []metric.Sample, sample metric.Sample) []metric.Sample {
	if err := sample.Validate(); err != nil {
		s.log.Warn("pskreporter discarded an invalid sample", "error", err)
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
