// Package lotw publishes how many amateur operators are actually on the air,
// from the ARRL's Logbook of The World user-activity file.
//
// The file is one line per registered callsign carrying the date and time of
// that operator's most recent log upload:
//
//	1A0C,2026-07-27,15:21:03
//	1A0KM,2014-02-08,15:08:08
//
// Counting the callsigns whose most recent upload falls inside a window gives
// a slow, honest measure of the size of the active amateur population. It
// moves over months rather than minutes, which makes it the long-baseline
// companion to the solar-cycle series: a solar maximum that does not show up as
// more people uploading logs is a solar maximum nobody worked.
//
// # Windows are measured against the file, not the wall clock
//
// This is the whole correctness story of this package, and it is easy to get
// wrong.
//
// The file is rebuilt roughly weekly, not daily. Live observation: 6,298,442
// bytes, 235,278 rows, Last-Modified two days old at fetch time and
// Cache-Control max-age of about 4.7 days. So on any given day the bytes we
// hold were generated somewhere between zero and seven days ago.
//
// If "active in the last 7 days" were computed as (time.Now() - upload time),
// the count would fall every day between rebuilds and jump back up when a new
// file lands — a sawtooth that is entirely an artefact of our own clock. Worse,
// it is a plausible-looking sawtooth: it would read as amateur activity
// oscillating weekly.
//
// Every window here is therefore measured from the file's own timestamp. For
// this one source the HTTP Last-Modified header *is* the payload timestamp:
// the file has no internal header, no generation line, nothing but rows, and
// Apache's mtime is the moment the rebuild finished. Every other source in this
// exporter must not trust Last-Modified for freshness, because a CDN or a cron
// job rewriting an unchanged file makes it a lie about the data. Here the file
// and its mtime are produced by the same act.
//
// # Polling policy
//
// Once per UTC day with a conditional request. At weekly rebuilds that means
// roughly six polls in seven cost a 304 and transfer no bytes at all, and the
// one that does transfer costs 6 MB. Polling more often could not learn
// anything: the file does not change between rebuilds, and lotw.arrl.org is
// given a five-minute floor with burst 1 in the shared politeness table.
//
// # Licence
//
// The ARRL publishes no terms for this file; it is served without
// authentication and is widely consumed by third-party tools. Credit
// "ARRL Logbook of The World".
package lotw

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
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
const Name = "lotw"

// ServiceLabel is the value of the "service" label on the operator counts. It
// is separate from Name so that the metric label describes the logging service
// being measured rather than the exporter's internal source name, even though
// the two happen to coincide.
const ServiceLabel = "lotw"

const (
	// DefaultURL is the activity file. Configurable so an operator can point at
	// a local mirror of a 6 MB file rather than fetching it from Newington.
	DefaultURL = "https://lotw.arrl.org/lotw-user-activity.csv"

	// publishLag is added to the scheduled time. See Schedule.
	publishLag = 30 * time.Minute

	// defaultRetries is the number of retries after the first attempt.
	//
	// Deliberately low. A retry here re-transfers 6 MB, and the next poll is
	// only a day away against a file that changes weekly, so there is nothing
	// urgent to rescue.
	defaultRetries = 1

	// maxBodyBytes bounds the response.
	//
	// The live file was 6,298,442 bytes across 235,278 rows. 32 MiB is five
	// times that, which covers the roster growing for many years, and is still
	// small enough that a broken or hostile origin cannot exhaust memory. It is
	// set explicitly because it is four times the httpx default of 8 MiB, and a
	// file this size is only one good year of growth away from that ceiling.
	maxBodyBytes = 32 << 20

	// maxMalformedLogged bounds how many bad rows are individually logged, so a
	// format change upstream cannot turn one poll into 235,278 log lines.
	maxMalformedLogged = 5

	// timeLayout is the date and time as the file writes them, in UTC. The file
	// carries no zone; ARRL's own upload timestamps are UTC.
	dateLayout = "2006-01-02"
	timeLayout = "15:04:05"
)

// window is one activity horizon and the label it publishes under.
type window struct {
	label string
	span  time.Duration
}

// windows are the horizons published, shortest first.
//
// Three, and only three. Seven days is "who is on the air now" at the
// resolution this file can support, thirty days smooths over holidays and
// contest weekends, and a year is the figure that can be compared against the
// solar cycle. A fourth window would be another series describing the same
// slow-moving population.
var windows = []window{
	{"7d", 7 * 24 * time.Hour},
	{"30d", 30 * 24 * time.Hour},
	{"365d", 365 * 24 * time.Hour},
}

// Source polls the LoTW activity file.
type Source struct {
	url     string
	retries int
	client  *httpx.Client
	log     *slog.Logger

	// now is time.Now except in tests. It is used only to stamp Batch.Fetched
	// and to compute metric.SourceDataAge. It is deliberately *not* used to
	// compute the activity windows; see the package comment.
	now func() time.Time
}

// Compile-time proof the source is both pollable and schedulable.
var (
	_ source.Source    = (*Source)(nil)
	_ source.Scheduled = (*Source)(nil)
)

// New builds the source from validated configuration.
//
// The client is passed in rather than constructed here because politeness is
// enforced across sources: the shared table gives lotw.arrl.org a five-minute
// floor, and a per-source client would quietly opt out of it.
//
// cfg.Timeout is not read. Request timeouts are a property of the shared
// client, which is the only thing that knows how many other sources are queued
// behind this one.
func New(cfg config.LoTW, client *httpx.Client, log *slog.Logger) (*Source, error) {
	if client == nil {
		return nil, errors.New("lotw: a shared httpx client is required")
	}
	if log == nil {
		log = slog.Default()
	}

	raw := strings.TrimSpace(cfg.URL)
	if raw == "" {
		raw = DefaultURL
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("lotw: URL %q is not a URL: %w", redact.URL(raw), err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, fmt.Errorf("lotw: URL must be http or https, got %q", parsed.Scheme)
	}
	if parsed.Host == "" {
		return nil, errors.New("lotw: URL has no host")
	}

	retries := cfg.Retries
	if retries < 0 {
		retries = 0
	}
	if cfg.Retries == 0 {
		retries = defaultRetries
	}

	return &Source{
		url:     raw,
		retries: retries,
		client:  client,
		log:     log.With("source", Name),
		now:     time.Now,
	}, nil
}

// Name identifies the source.
func (s *Source) Name() string { return Name }

// Interval reports whatever the schedule reports, so the two can never
// disagree.
func (s *Source) Interval() time.Duration { return s.Schedule().Interval() }

// Schedule polls once per UTC day, at 06:00 plus a thirty-minute lag: 06:30
// UTC.
//
// The reasoning, in order:
//
// Once a day, because the file is rebuilt roughly weekly. The observed
// Cache-Control max-age was about 4.7 days and the observed Last-Modified was
// two days old. A daily poll notices a new file within 24 hours of it landing,
// which is a rounding error against a weekly rebuild, and a faster poll cannot
// see anything a daily one misses.
//
// At 06:00 UTC, because the observed rebuild finished at 05:51:37 UTC. Polling
// at 06:00 plus the lag puts the request roughly forty minutes after the file
// was written on the day it is written, so a new file is picked up on the same
// day rather than the next one.
//
// 06:30 UTC is also 02:30 US Eastern, which is the quietest part of the day for
// a server in Connecticut serving mostly North American operators. The one poll
// in seven that actually transfers 6 MB lands when nobody is competing for it.
//
// The lag exists because a stated or observed generation time is when the
// rebuild starts writing, not when the file is complete and readable.
func (s *Source) Schedule() source.Schedule {
	return source.DailyAt(publishLag, source.TimeOfDay{Hour: 6, Minute: 0})
}

// Poll fetches the activity file and counts operators per window.
func (s *Source) Poll(ctx context.Context) (metric.Batch, error) {
	fetched := s.now().UTC()
	empty := metric.Batch{Source: Name, Fetched: fetched}

	if err := ctx.Err(); err != nil {
		return empty, fmt.Errorf("lotw: poll cancelled: %w", err)
	}

	resp, err := s.client.Get(ctx, httpx.Request{
		URL: s.url,
		// The file serves ETag and Last-Modified, and changes weekly against a
		// daily poll, so most polls should cost a 304 and no body at all. This
		// is the single most important line in the package for the publisher.
		Conditional: true,
		Retries:     s.retries,
		MaxBody:     maxBodyBytes,
	})
	switch {
	case errors.Is(err, httpx.ErrNotModified):
		return empty, source.ErrNotModified
	case errors.Is(err, httpx.ErrRateLimited):
		return empty, fmt.Errorf("%w: %w", source.ErrRateLimited, err)
	case err != nil:
		return empty, fmt.Errorf("lotw: fetching the activity file: %w", err)
	}

	// The file's own timestamp. See the package comment for why this, and only
	// this, is what the windows are measured against.
	reference := resp.LastModified.UTC()
	if resp.LastModified.IsZero() {
		// Falling back to the wall clock reintroduces exactly the drift this
		// package exists to avoid, so it is loud rather than silent. It has not
		// been observed: Apache has always sent the header.
		reference = fetched
		s.log.Warn("lotw activity file arrived with no Last-Modified; " +
			"activity windows will drift as the file ages between rebuilds")
	}

	counts, total, malformed, err := s.count(resp.Body, reference)
	if err != nil {
		return empty, err
	}
	if malformed > 0 {
		s.log.Warn("lotw activity file contained rows that could not be parsed",
			"malformed", malformed, "parsed", total)
	}

	batch := metric.Batch{Source: Name, Fetched: fetched}
	for i, w := range windows {
		batch.Samples = append(batch.Samples, metric.Sample{
			Desc:   metric.ActiveOperators,
			Labels: []string{ServiceLabel, w.label},
			Value:  float64(counts[i]),
			// Stamped with the file's timestamp, not the fetch time. The count
			// is an observation of the world as of the rebuild, and stamping it
			// now would claim a freshness it does not have.
			Time: reference,
		})
	}
	batch.Samples = append(batch.Samples, metric.Sample{
		Desc:   metric.RegisteredOperators,
		Labels: []string{ServiceLabel},
		Value:  float64(total),
		Time:   reference,
	})

	// Freshness. Unusually for this exporter, the HTTP header is the payload
	// timestamp; see the package comment.
	age := fetched.Sub(reference).Seconds()
	if age < 0 {
		// A file stamped in the future means clock skew somewhere. Publishing a
		// negative age would read as a broken exporter rather than a surprising
		// upstream.
		age = 0
	}
	batch.Samples = append(batch.Samples, metric.Sample{
		Desc:   metric.SourceDataAge,
		Labels: []string{Name},
		Value:  age,
		// The age is a property of this moment rather than of the observation,
		// so unlike every other sample here it is stamped with the fetch time.
		Time: fetched,
	})

	return batch, nil
}

// count walks the file once, tallying rows per window against reference.
//
// It scans the bytes rather than splitting them into strings: 235,278 rows
// would otherwise become 705,834 short-lived string allocations per poll, and
// the whole file as a []string is several times the size of the file itself.
// Only the counters survive the call — no callsign is retained, because nothing
// published here is per-callsign and a callsign is exactly the kind of
// unbounded value that must never reach a label.
//
// Rows are assumed one-per-callsign, which is what the file is: the callsign is
// its primary key upstream, so the row count is the registered-operator count
// without needing a set.
func (s *Source) count(body []byte, reference time.Time) (counts []int, total, malformed int, err error) {
	counts = make([]int, len(windows))

	scanner := bufio.NewScanner(bytes.NewReader(body))
	// The longest plausible row is a callsign plus two short fields, well under
	// 128 bytes. A generous buffer costs nothing and means a single corrupt long
	// line fails that line rather than the whole file.
	scanner.Buffer(make([]byte, 0, 4096), 64*1024)

	logged := 0
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}

		upload, ok := parseRow(line)
		if !ok {
			malformed++
			if logged < maxMalformedLogged {
				logged++
				// The row is remote input on its way into a log line, so it is
				// bounded and quoted rather than interpolated raw.
				s.log.Debug("lotw skipped an unparseable row", "row", excerpt(line))
			}
			continue
		}

		total++
		age := reference.Sub(upload)
		for i, w := range windows {
			// Inclusive: an upload exactly one window old is inside it. The
			// boundary case is arbitrary but has to be decided somewhere, and
			// with second resolution over a 7-day window it can only ever
			// affect a handful of rows.
			if age <= w.span {
				counts[i]++
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, 0, 0, fmt.Errorf("lotw: reading the activity file: %w", redact.Error(err))
	}

	// Individual bad rows are tolerated; a file with nothing parseable in it is
	// a format change, and publishing zero active operators from it would look
	// exactly like the entire amateur population going quiet.
	if total == 0 {
		return nil, 0, 0, fmt.Errorf("lotw: no parseable rows in %d-byte activity file (%d malformed); "+
			"the file format has probably changed", len(body), malformed)
	}
	return counts, total, malformed, nil
}

// parseRow reads one "callsign,date,time" row and returns the upload instant.
//
// It takes bytes and converts only the two short timestamp fields to strings,
// which is the one unavoidable allocation per row; the callsign is validated as
// non-empty and then discarded without ever becoming a string.
func parseRow(line []byte) (time.Time, bool) {
	call, rest, ok := bytes.Cut(line, []byte(","))
	if !ok || len(bytes.TrimSpace(call)) == 0 {
		return time.Time{}, false
	}
	date, clock, ok := bytes.Cut(rest, []byte(","))
	if !ok {
		return time.Time{}, false
	}
	// A fourth field means the format has changed under us, so the row is not
	// something this parser understands even if its first three fields look
	// right.
	if bytes.ContainsRune(clock, ',') {
		return time.Time{}, false
	}

	upload, err := time.Parse(dateLayout+" "+timeLayout,
		string(bytes.TrimSpace(date))+" "+string(bytes.TrimSpace(clock)))
	if err != nil {
		return time.Time{}, false
	}
	return upload.UTC(), true
}

// excerpt bounds and quotes a row for logging.
func excerpt(line []byte) string {
	const max = 80
	if len(line) > max {
		line = line[:max]
	}
	return fmt.Sprintf("%q", line)
}
