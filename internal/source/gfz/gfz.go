// Package gfz polls the geomagnetic index service at the GFZ Helmholtz Centre
// for Geosciences (formerly GFZ German Research Centre for Geosciences), which
// is the definitive producer of Kp.
//
// The reason to have this source alongside NOAA SWPC is Hp30. NOAA publishes an
// estimated Kp every minute, but a *thirty-minute* geomagnetic index does not
// exist anywhere else at any cadence: the three-hourly Kp averages a substorm
// away entirely, and Hp30 is the only index published in near real time that
// resolves one. Measured latency from the end of an interval to its appearance
// in the nowcast file is about eleven minutes.
//
// # Licence and attribution — required
//
// The data this source fetches is licensed CC BY 4.0.
//
//   - Attribution: "GFZ German Research Centre for Geosciences".
//   - Source: Geomagnetic Observatory Niemegk, GFZ Helmholtz Centre for
//     Geosciences.
//
// Kp and ap must be cited as:
//
//	Matzka, J., Stolle, C., Yamazaki, Y., Bronkalla, O. and Morschhauser, A.,
//	2021. The geomagnetic Kp index and derived indices of geomagnetic activity.
//	Space Weather. doi:10.1029/2020SW002641
//
// Hp30, Hp60 and their ap equivalents require a *different* citation, which is
// easy to get wrong because the two products are served from the same directory
// in the same format:
//
//	Yamazaki, Y. et al., 2024. Geomagnetic activity index Hpo.
//	doi:10.22541/essoar.171838396.68563140/v1
//
// # The file this source deliberately does not fetch
//
// kp.gfz.de also publishes /app/files/Kp_ap_Ap_SN_F107_nowcast.txt, which is
// tempting because it carries Kp, ap, Ap, sunspot number and F10.7 in a single
// request. It is not fetched, and must not be, because it carries a *mixed*
// licence: the Kp/ap/Ap columns are CC BY 4.0, but the sunspot numbers in it
// originate with WDC-SILSO at the Royal Observatory of Belgium and are CC
// BY-NC. Fetching that file would silently pull non-commercial data into a
// source an operator enabled believing it to be CC BY 4.0, and there is no way
// to tell from the response which columns carry which terms. The Kp columns are
// available separately under a single clean licence, so this source takes the
// extra request instead. F10.7 comes from the drao source, first-hand.
//
// # Sentinel discipline
//
// Both files are written ahead of the data, so the trailing rows of every
// response are placeholders for intervals that have not been scaled yet. The
// marker is -1.000 for Kp/Hp30 and -1 for ap/ap30. Those are not readings of a
// very quiet magnetosphere; they mean "no value". Publishing -1 as a K-index
// would put a number below the bottom of the 0-9 scale on a dashboard, which is
// worse than publishing nothing, so the newest row whose value is *not* the
// sentinel is the one that is published.
//
// # Domain
//
// The base URL is https://kp.gfz.de. The address every document and paper still
// cites, kp.gfz-potsdam.de, now answers 301 to this host. The new name is used
// directly so that a poll costs one request rather than two.
package gfz

import (
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
	"github.com/RealDougEubanks/solarham/internal/source"
)

// Name identifies this source in logs, metrics and the health endpoints. It
// becomes a metric label, so it is stable.
const Name = "gfz"

const (
	// defaultBaseURL is the service. Tests override it; nothing outside this
	// package can, so production traffic can only go here.
	defaultBaseURL = "https://kp.gfz.de"

	// pathKp carries one line per three-hour Kp interval, about 16 KB covering
	// the last month.
	pathKp = "/app/files/Kp_ap_nowcast.txt"

	// pathHp30 carries one line per thirty-minute Hp30 interval in the same
	// column layout, about 88 KB.
	pathHp30 = "/app/files/Hp30_ap30_nowcast.txt"

	// defaultInterval is ten minutes.
	//
	// Kp is three-hourly and Hp30 half-hourly, so neither product *changes*
	// this often. Ten minutes is chosen against the measured eleven-minute
	// publication latency of Hp30 rather than against its cadence: a half-hourly
	// value that appears eleven minutes late is caught within ten minutes of
	// appearing, which bounds the worst-case staleness of the fastest series
	// here at about twenty-one minutes. Polling every minute would fetch the
	// same bytes thirty times per new value; polling every thirty minutes would
	// beat against the publication boundary and routinely miss an interval for a
	// whole cycle. The files are conditional-GET capable, so most of these polls
	// cost a 304 and no body.
	defaultInterval = 10 * time.Minute

	// minInterval is the courtesy floor. Nothing this source publishes can
	// change faster than every thirty minutes, so a poll more often than every
	// five minutes cannot produce an observation — it only produces load on an
	// academic institute's server. A limit an operator can override by editing a
	// setting is not a limit, so it is clamped in the schedule as well as here.
	minInterval = 5 * time.Minute

	// defaultRetries is retries *after* the first attempt.
	defaultRetries = 2

	// maxBodyBytes bounds each response. The Hp30 file is the larger at about
	// 88 KB and grows by roughly 43 bytes per half hour; 4 MB is orders of
	// magnitude of headroom and still small enough that a broken or hostile
	// origin cannot exhaust memory.
	maxBodyBytes = 4 << 20
)

// Status values for the IndexStatusInfo label, from the D column.
const (
	statusDefinitive  = "definitive"
	statusPreliminary = "preliminary"
)

// Sentinels. The file header states them literally: "missing data indicated by
// -1.000 for Kp and -1 for ap".
const (
	sentinelIndex = -1.0
	sentinelAp    = -1
)

// The two "days" columns in each row are days since 1932-01-01 00:00 UT, to the
// start and the mid of the interval. They are parsed for shape but not used to
// derive a timestamp: the YYYY MM DD hh.h and hh._m columns are unambiguous and
// need no epoch arithmetic.

// Source polls the GFZ nowcast files.
type Source struct {
	baseURL    string
	halfHourly bool
	interval   time.Duration
	retries    int
	maxBody    int64
	client     *httpx.Client
	log        *slog.Logger

	// now is time.Now except in tests. Only the data-age computation needs it;
	// every other timestamp in this package comes from the payload.
	now func() time.Time
}

// Compile-time proof this satisfies what the scheduler polls.
var (
	_ source.Source    = (*Source)(nil)
	_ source.Scheduled = (*Source)(nil)
)

// New builds a source from validated configuration.
//
// Everything defaultable is defaulted rather than rejected: none of these
// settings is a credential and a missing one should not stop an exporter that
// also serves a dozen other sources from starting. A BaseURL that is not a URL
// is rejected, because that is a typo an operator needs told about.
func New(cfg config.GFZ, client *httpx.Client, log *slog.Logger) (*Source, error) {
	if client == nil {
		return nil, errors.New("gfz: a shared httpx client is required")
	}
	if log == nil {
		log = slog.Default()
	}

	base := strings.TrimRight(strings.TrimSpace(cfg.BaseURL), "/")
	if base == "" {
		base = defaultBaseURL
	}
	u, err := url.Parse(base)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return nil, fmt.Errorf("gfz: base URL is not absolute: %q", base)
	}

	// cfg.Timeout is deliberately not read. Per-request timeouts belong to the
	// shared httpx client, which owns them for every source at once; honouring a
	// per-source value here would need a second http.Client and would bypass the
	// shared rate limiter, which is the one thing this package must not do. The
	// setting is left in the config struct because an operator who sets it
	// expects it to mean something, and the exporter's single HTTP timeout is where it now lives.

	interval := cfg.Interval
	if interval <= 0 {
		interval = defaultInterval
	}
	if interval < minInterval {
		log.Warn("gfz poll interval raised to the courtesy floor; "+
			"nothing this source publishes changes faster than every thirty minutes",
			"configured", interval, "floor", minInterval)
		interval = minInterval
	}

	// Zero is "unset", not "never retry", so a zero-valued config gets the
	// documented default. A negative value is the only way to express "one
	// attempt" on an int field, so it is honoured.
	retries := cfg.Retries
	switch {
	case retries < 0:
		retries = 0
	case retries == 0:
		retries = defaultRetries
	}

	return &Source{
		baseURL:    base,
		halfHourly: cfg.HalfHourly,
		interval:   interval,
		retries:    retries,
		maxBody:    maxBodyBytes,
		client:     client,
		log:        log.With("source", Name),
		now:        time.Now,
	}, nil
}

// Name identifies the source.
func (s *Source) Name() string { return Name }

// Interval is the nominal spacing between polls, used for the staleness window
// and the failure backoff.
func (s *Source) Interval() time.Duration { return s.interval }

// Schedule polls on a fixed interval, clamped to the courtesy floor.
//
// A DailyAt schedule would be wrong here despite Kp being three-hourly: the
// files are rebuilt at least every thirty minutes, the eight Kp intervals do not
// land at a fixed publication time, and Hp30 moves twice an hour. A fixed
// interval sized against publication latency is the honest model, and AtLeast
// makes the floor part of the schedule rather than a rule that only validation
// enforces.
func (s *Source) Schedule() source.Schedule {
	return source.AtLeast(minInterval, source.Every(s.interval))
}

// Poll fetches the enabled files once and converts them into samples.
//
// Kp is always fetched. Hp30 is fetched only when HalfHourly is set, because it
// is five times the bytes of the Kp file for a product many operators do not
// need.
//
// The three outcomes:
//
//   - every enabled file answered 304: an empty batch and source.ErrNotModified.
//     Nothing new, and not a failure.
//   - every enabled file failed: the joined error, so the operator sees all the
//     causes rather than whichever finished first.
//   - anything else: whatever parsed, with failures logged. Kp still being
//     available when Hp30 is briefly 500ing is worth more than a failed poll.
func (s *Source) Poll(ctx context.Context) (metric.Batch, error) {
	fetched := s.now().UTC()
	batch := metric.Batch{Source: Name, Fetched: fetched}

	type job struct {
		path  string
		kind  seriesKind
		index string
	}
	jobs := []job{{path: pathKp, kind: kindKp, index: "kp"}}
	if s.halfHourly {
		jobs = append(jobs, job{path: pathHp30, kind: kindHp30, index: "hp30"})
	}

	var (
		errs        []error
		notModified int
		newest      time.Time
	)

	// Fetched sequentially rather than concurrently. Two requests to one
	// academic host, one of which is usually a 304, is not worth the
	// concurrency, and the shared limiter would serialise them anyway.
	for _, j := range jobs {
		samples, observed, err := s.fetchAndParse(ctx, j.path, j.kind, j.index)
		switch {
		case errors.Is(err, httpx.ErrNotModified):
			notModified++
		case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
			// A cancelled context is not a per-file failure to log around; the
			// whole poll is over.
			return metric.Batch{Source: Name, Fetched: fetched}, fmt.Errorf("gfz: %s: %w", j.path, err)
		case errors.Is(err, httpx.ErrRateLimited):
			// Surfaced in the source vocabulary so the scheduler backs off
			// rather than treating it as an ordinary failure.
			return metric.Batch{Source: Name, Fetched: fetched},
				fmt.Errorf("%w: %w", source.ErrRateLimited, err)
		case err != nil:
			errs = append(errs, fmt.Errorf("%s: %w", j.path, err))
		default:
			batch.Samples = append(batch.Samples, samples...)
			if observed.After(newest) {
				newest = observed
			}
		}
	}

	if len(errs) == len(jobs) {
		return metric.Batch{Source: Name, Fetched: fetched},
			fmt.Errorf("gfz: every file failed: %w", errors.Join(errs...))
	}
	for _, err := range errs {
		// Logged, not returned. But a silently missing file is how a dashboard
		// quietly stops showing a metric for a week, so it is logged at warn.
		s.log.Warn("gfz file failed, continuing with the rest", "error", err)
	}

	if notModified == len(jobs) {
		return metric.Batch{Source: Name, Fetched: fetched}, source.ErrNotModified
	}

	// Age is computed from the payload's own interval time, never from
	// Last-Modified: these files are rewritten on a timer whether or not the
	// newest interval has been scaled, so a fresh Last-Modified says nothing
	// about whether there is fresh data behind it.
	if !newest.IsZero() {
		age := fetched.Sub(newest).Seconds()
		if age < 0 {
			age = 0
		}
		s.add(&batch, metric.SourceDataAge, age, fetched, Name)
	}

	return batch, nil
}

// fetchAndParse retrieves one file and converts its newest usable row into
// samples. The returned time is that row's interval mid-time.
func (s *Source) fetchAndParse(ctx context.Context, path string, kind seriesKind, indexLabel string) (out []metric.Sample, observed time.Time, err error) {
	// These are fixed-width tables written by somebody else's code. A shape
	// nobody anticipated is a question of when, and a panic here would take the
	// exporter down rather than one poll.
	defer func() {
		if r := recover(); r != nil {
			out, observed, err = nil, time.Time{}, fmt.Errorf("gfz: panic parsing %s: %v", path, r)
		}
	}()

	resp, err := s.client.Get(ctx, httpx.Request{
		URL: s.baseURL + path,
		// These files send Last-Modified (and an ETag), and at a ten-minute
		// poll against a half-hourly product most requests should earn a 304.
		Conditional: true,
		Retries:     s.retries,
		MaxBody:     s.maxBody,
	})
	if err != nil {
		return nil, time.Time{}, err
	}

	row, err := newestScaledRow(string(resp.Body))
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("gfz: %s: %w", path, err)
	}

	samples := s.samplesFor(kind, indexLabel, row)
	return samples, row.mid, nil
}

// seriesKind selects which descriptors a parsed row publishes as.
type seriesKind uint8

const (
	kindKp seriesKind = iota
	kindHp30
)

// samplesFor turns one row into the samples for its product.
func (s *Source) samplesFor(kind seriesKind, indexLabel string, row indexRow) []metric.Sample {
	var b metric.Batch

	switch kind {
	case kindKp:
		// station="planetary" because Kp is a planetary index derived from
		// thirteen observatories, not a reading from one. The KIndex descriptor
		// is shared with per-observatory sources, and a label saying which is
		// which is the difference between a comparable series and a misleading
		// one.
		s.add(&b, metric.KIndex, row.index, row.mid, "planetary")
		s.add(&b, metric.APIndex, float64(row.ap), row.mid, "3h")
	case kindHp30:
		s.add(&b, metric.KIndexHalfHourly, row.index, row.mid, "30m")
		s.add(&b, metric.APIndex, float64(row.ap), row.mid, "30m")
	}

	// Whether the newest usable value is definitive or still preliminary is
	// operationally significant: a preliminary Kp can move by a whole step when
	// it is finally scaled, and a dashboard that does not distinguish them makes
	// that look like a geomagnetic event.
	s.add(&b, metric.IndexStatusInfo, 1, row.mid, indexLabel, row.status())

	return b.Samples
}

// add appends a validated sample, dropping and logging an invalid one.
//
// A sample whose label values do not match its descriptor is rejected by
// Prometheus at collection time and silently corrupts InfluxDB tag sets.
// Catching it here turns a confusing runtime failure into an obvious log line.
func (s *Source) add(b *metric.Batch, desc *metric.Descriptor, value float64, at time.Time, labels ...string) {
	sample := metric.Sample{Desc: desc, Labels: labels, Value: value, Time: at}
	if err := sample.Validate(); err != nil {
		s.log.Warn("gfz produced an invalid sample; dropping it", "error", err)
		return
	}
	b.Samples = append(b.Samples, sample)
}

// indexRow is one parsed line of either nowcast file.
type indexRow struct {
	// start and mid are the interval's start and mid times in UT, from the
	// YYYY MM DD hh.h and hh._m columns.
	start time.Time
	mid   time.Time

	// index is Kp or Hp30 on the 0-9 quasi-logarithmic scale.
	index float64

	// ap is the linear equivalent amplitude.
	ap int

	// definitive is the D column: 1 definitive, 0 preliminary.
	definitive bool
}

func (r indexRow) status() string {
	if r.definitive {
		return statusDefinitive
	}
	return statusPreliminary
}

// errNoScaledRow means every row in the file was a sentinel placeholder.
//
// This is a genuine upstream condition rather than a parse failure — it is what
// a file looks like if scaling has stalled — but it must be an error rather than
// a silent empty result, because a source that publishes nothing indefinitely
// while reporting success is exactly the failure SourceDataAge exists to catch.
var errNoScaledRow = errors.New("no scaled row: every value in the file is the missing-data sentinel")

// newestScaledRow parses the file and returns the last row whose value is not
// the sentinel.
//
// The file is read forwards and the last usable row kept rather than being read
// backwards, because "the newest non-sentinel row" is not necessarily the last
// non-sentinel row in a corrupted file and forwards is the order that makes a
// malformed line's position reportable.
func newestScaledRow(body string) (indexRow, error) {
	var (
		best  indexRow
		found bool
		rows  int
	)

	for lineNo, line := range strings.Split(body, "\n") {
		line = strings.TrimRight(line, "\r")
		trimmed := strings.TrimSpace(line)

		// The header is thirty-odd lines each starting with '#'. Skipping by
		// prefix rather than by a fixed count is deliberate: the Kp header
		// documents itself as thirty lines and the Hp30 header is thirty-two,
		// and a count that is right today is a silent off-by-one when they add
		// a citation line.
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}

		row, err := parseRow(trimmed)
		if err != nil {
			return indexRow{}, fmt.Errorf("line %d: %w", lineNo+1, err)
		}
		rows++

		// The sentinel check. Both columns are checked because a file with one
		// column filled and the other not is a shape we have not seen and would
		// rather refuse than half-publish.
		if row.index <= sentinelIndex || row.ap <= sentinelAp {
			continue
		}

		best, found = row, true
	}

	if rows == 0 {
		return indexRow{}, errors.New("no data rows after the header")
	}
	if !found {
		return indexRow{}, errNoScaledRow
	}
	return best, nil
}

// parseRow reads one data line.
//
// The layout is fixed-width but blank-separated, and the header states it as
// "iii ii ii ff.f ff.ff fffff.fffff fffff.fffff ff.fff iiii i". Splitting on
// whitespace is used rather than byte offsets because a negative sign in the
// Kp column shifts the field by one character, which is precisely the case the
// sentinel rows exercise.
func parseRow(line string) (indexRow, error) {
	f := strings.Fields(line)

	// YYYY MM DD hh.h hh._m days days_m value ap D
	const wantFields = 10
	if len(f) != wantFields {
		return indexRow{}, fmt.Errorf("expected %d fields, got %d in %q", wantFields, len(f), line)
	}

	year, err := strconv.Atoi(f[0])
	if err != nil || year < 1932 || year > 3000 {
		return indexRow{}, fmt.Errorf("bad year %q", f[0])
	}
	month, err := strconv.Atoi(f[1])
	if err != nil || month < 1 || month > 12 {
		return indexRow{}, fmt.Errorf("bad month %q", f[1])
	}
	day, err := strconv.Atoi(f[2])
	if err != nil || day < 1 || day > 31 {
		return indexRow{}, fmt.Errorf("bad day %q", f[2])
	}

	startHours, err := strconv.ParseFloat(f[3], 64)
	if err != nil || startHours < 0 || startHours >= 24 {
		return indexRow{}, fmt.Errorf("bad interval start %q", f[3])
	}
	midHours, err := strconv.ParseFloat(f[4], 64)
	if err != nil || midHours < 0 || midHours >= 24 {
		return indexRow{}, fmt.Errorf("bad interval mid %q", f[4])
	}

	index, err := strconv.ParseFloat(f[7], 64)
	if err != nil {
		return indexRow{}, fmt.Errorf("bad index value %q", f[7])
	}
	ap, err := strconv.Atoi(f[8])
	if err != nil {
		return indexRow{}, fmt.Errorf("bad ap value %q", f[8])
	}

	definitive, err := strconv.Atoi(f[9])
	if err != nil || (definitive != 0 && definitive != 1) {
		return indexRow{}, fmt.Errorf("bad definitive flag %q", f[9])
	}

	midnight := time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
	return indexRow{
		start:      midnight.Add(hoursToDuration(startHours)),
		mid:        midnight.Add(hoursToDuration(midHours)),
		index:      index,
		ap:         ap,
		definitive: definitive == 1,
	}, nil
}

// hoursToDuration converts a fractional hour column to a duration. The columns
// are quarter-hour multiples at worst (00.25, 00.75 in the Hp30 file), so
// rounding to the second removes float noise without losing resolution.
func hoursToDuration(h float64) time.Duration {
	return time.Duration(h * float64(time.Hour)).Round(time.Second)
}
