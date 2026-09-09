// Package drao polls the Dominion Radio Astrophysical Observatory's solar flux
// table at Penticton, British Columbia — the first-hand source of F10.7.
//
// F10.7 is the single most quoted number in amateur radio propagation, and
// everybody republishes it: NOAA SWPC, hamqsl, LISIRD. This is where it comes
// from. Penticton measures it and Natural Resources Canada publishes the table;
// the value on every other site is this measurement, hours later and rounded.
//
// # The URL
//
// Every document that cites this feed, including NOAA's own pages and most
// exporters, gives the address as:
//
//	ftp://ftp.seismo.nrcan.gc.ca/spaceweather/solar_flux/daily_flux_values/fluxtable.txt
//
// That FTP server is dead — the connection fails, it is not a slow or a
// refused login — and the obvious https translation of the same path answers
// 404. The working address is on the spaceweather.gc.ca web host at a different
// path entirely:
//
//	https://www.spaceweather.gc.ca/solar_flux_data/daily_flux_values/fluxtable.txt
//
// This is recorded at length because the dead URL is what a maintainer will
// find if they go looking, and they will conclude the feed has been withdrawn.
//
// # Why a Range request
//
// The file is one table of every measurement since 2004: about 2.1 MB and
// 23,900 lines, of which this source needs the last one. Downloading 2 MB three
// times a day to read 70 bytes of it is 6 MB a day of somebody else's bandwidth
// for no reason.
//
// The server is nginx with accept-ranges: bytes, and a suffix range works: a
// request with "Range: bytes=-8000" answers 206 Partial Content with the last
// 8 KB. The shared httpx client passes a 206 through as a normal success — its
// status classification only rejects 3xx and above — so the tail is fetched with
// one header and no special-casing. The first line of the response is almost
// certainly a fragment of a row cut mid-number, so it is discarded rather than
// parsed.
//
// # Schedule
//
// Penticton measures the flux three times per UT day, at 17:00, 20:00 and
// 23:00, and the table is published with about a day's lag: on 2026-09-09 the
// newest row in the file was 2026-09-08 23:00. This source is therefore the
// clearest case in the exporter for source.DailyAt — three polls a day against
// the 288 a five-minute interval would make, for data that changes three times.
//
// # Licence
//
// Government of Canada, Natural Resources Canada, under the Open Government
// Licence – Canada. Attribution: "Natural Resources Canada". The measurement is
// made by the Dominion Radio Astrophysical Observatory, Penticton, BC, operated
// by the National Research Council of Canada.
package drao

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
const Name = "drao"

const (
	// defaultBaseURL is the web host. Tests override it; nothing outside this
	// package can.
	defaultBaseURL = "https://www.spaceweather.gc.ca"

	// pathFluxTable is the whole-history table. See the package comment for why
	// this is not the FTP address everything cites.
	pathFluxTable = "/solar_flux_data/daily_flux_values/fluxtable.txt"

	// tailBytes is how much of the end of the file to ask for.
	//
	// A row is about 74 bytes, so this is roughly a hundred rows, or a month of
	// measurements. Only the newest is published, but asking for a month rather
	// than for two rows means a change in row width, a run of blank lines or a
	// day of duplicated rows cannot leave the response with no complete row in
	// it. Eight kilobytes is still 0.4% of the file.
	tailBytes = 8000

	// defaultRetries is retries *after* the first attempt.
	defaultRetries = 2

	// maxBodyBytes bounds the response read. It is deliberately only a little
	// above tailBytes: if the server ever ignores the Range header and answers
	// 200 with the whole 2 MB table, this source must fail loudly rather than
	// quietly start downloading 2 MB three times a day forever. That is the
	// failure mode the range exists to prevent, so it is the one worth an alert.
	maxBodyBytes = 64 << 10

	// publicationLag is added to each measurement time to decide when to poll.
	//
	// The measurements are made at 17:00, 20:00 and 23:00 UT but the table is
	// not rewritten the instant the radiometer finishes: the newest row observed
	// in the file has consistently been the previous UT day's 23:00 reading, and
	// the file's own Last-Modified moves through the day. Ninety minutes is
	// enough to be past the observed rewrite for the reading just taken while
	// still landing on the same UT day, so a poll cannot both miss the new row
	// and then wait until tomorrow to notice. The stated time is when the
	// publisher starts writing, not when the file is readable, and polling at
	// exactly the stated time fetches yesterday's copy.
	publicationLag = 90 * time.Minute
)

// measurementTimes are the three UT times Penticton measures at. They are fixed
// in code rather than configured because an operator has no way to know better
// than the observatory does when the observatory takes a reading.
var measurementTimes = []source.TimeOfDay{
	{Hour: 17, Minute: 0},
	{Hour: 20, Minute: 0},
	{Hour: 23, Minute: 0},
}

// Adjustment label values for RadioFluxMultiFrequency.
//
// The three flux columns are the same measurement expressed three ways, and
// which one a consumer wants depends entirely on what they are doing:
//
//   - observed is what the antenna measured, at the Earth-Sun distance of the
//     day. This is the number NOAA and everybody else calls "F10.7" and it is
//     what FluxSFU carries.
//   - adjusted is scaled to 1 AU, which removes the annual ~7% swing from
//     Earth's orbital eccentricity. This is the one to use for anything
//     comparing across months.
//   - ursi is the observed value multiplied by the URSI-agreed 0.9 scale factor,
//     which reconciles Penticton's absolute calibration with the international
//     standard flux scale.
const (
	adjustmentObserved = "observed"
	adjustmentAdjusted = "adjusted"
	adjustmentURSI     = "ursi"
)

// wavelengthCm labels the 10.7 cm flux. The descriptor is shared with the lasp
// source, which publishes 8, 15, 30 and 32 cm from the CLS series, so the
// wavelength has to be a label rather than being implied by the metric name.
const wavelengthCm = "10.7"

// Source polls the Penticton flux table.
type Source struct {
	baseURL string
	retries int
	maxBody int64
	client  *httpx.Client
	log     *slog.Logger

	// now is time.Now except in tests. Only the data-age computation needs it.
	now func() time.Time
}

// Compile-time proof this satisfies what the scheduler polls.
var (
	_ source.Source    = (*Source)(nil)
	_ source.Scheduled = (*Source)(nil)
)

// New builds a source from validated configuration.
//
// There is no interval to configure: the schedule is a property of the
// observatory, not of this deployment. A BaseURL that is not a URL is rejected
// because that is a typo an operator needs told about; everything else is
// defaulted, since none of it is a credential and a missing setting should not
// stop an exporter starting.
func New(cfg config.DRAO, client *httpx.Client, log *slog.Logger) (*Source, error) {
	if client == nil {
		return nil, errors.New("drao: a shared httpx client is required")
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
		return nil, fmt.Errorf("drao: base URL is not absolute: %q", base)
	}

	// cfg.Timeout is deliberately not read. Per-request timeouts belong to the
	// shared httpx client, which owns them for every source at once; honouring a
	// per-source value here would need a second http.Client and would bypass the
	// shared rate limiter, which is the one thing this package must not do. The
	// setting is left in the config struct because an operator who sets it
	// expects it to mean something, and the exporter's single HTTP timeout is where it now lives.

	// Zero is "unset", not "never retry". A negative value is the only way to
	// express "one attempt" on an int field, so it is honoured.
	retries := cfg.Retries
	switch {
	case retries < 0:
		retries = 0
	case retries == 0:
		retries = defaultRetries
	}

	return &Source{
		baseURL: base,
		retries: retries,
		maxBody: maxBodyBytes,
		client:  client,
		log:     log.With("source", Name),
		now:     time.Now,
	}, nil
}

// Name identifies the source.
func (s *Source) Name() string { return Name }

// Interval is the nominal spacing between polls, used for the staleness window
// and the failure backoff. It is the shortest gap between measurements — 17:00
// to 20:00 — rather than a third of a day, so staleness is judged against the
// tightest cadence the schedule actually achieves.
func (s *Source) Interval() time.Duration { return s.Schedule().Interval() }

// Schedule polls shortly after each of the three daily measurements.
func (s *Source) Schedule() source.Schedule {
	return source.DailyAt(publicationLag, measurementTimes...)
}

// Poll fetches the tail of the flux table and publishes its newest row.
func (s *Source) Poll(ctx context.Context) (metric.Batch, error) {
	fetched := s.now().UTC()
	batch := metric.Batch{Source: Name, Fetched: fetched}

	body, err := s.fetchTail(ctx)
	switch {
	case errors.Is(err, httpx.ErrNotModified):
		// The table is rewritten in place with no new measurement quite often,
		// so a 304 at three polls a day is ordinary rather than suspicious.
		return batch, source.ErrNotModified
	case errors.Is(err, httpx.ErrRateLimited):
		return batch, fmt.Errorf("%w: %v", source.ErrRateLimited, err)
	case err != nil:
		return batch, err
	}

	row, err := newestRow(string(body))
	if err != nil {
		return batch, fmt.Errorf("drao: %w", err)
	}

	// FluxSFU is the headline F10.7 an operator recognises, and it carries the
	// observed value because that is what "F10.7 is 110 today" means everywhere
	// else. The adjusted and URSI figures are only available through the
	// labelled multi-frequency descriptor, so nobody can accidentally graph the
	// 1 AU value believing it to be the observed one.
	s.add(&batch, metric.FluxSFU, row.observed, row.at)
	s.add(&batch, metric.RadioFluxMultiFrequency, row.observed, row.at, wavelengthCm, adjustmentObserved)
	s.add(&batch, metric.RadioFluxMultiFrequency, row.adjusted, row.at, wavelengthCm, adjustmentAdjusted)
	s.add(&batch, metric.RadioFluxMultiFrequency, row.ursi, row.at, wavelengthCm, adjustmentURSI)

	// Age comes from the row's own fluxdate and fluxtime, never from
	// Last-Modified. The table's modification time moves through the day
	// whether or not a new measurement has been appended, so a fresh
	// Last-Modified over a two-day-old newest row is exactly the stale-but-200
	// failure this metric exists to expose.
	age := fetched.Sub(row.at).Seconds()
	if age < 0 {
		age = 0
	}
	s.add(&batch, metric.SourceDataAge, age, row.at, Name)

	return batch, nil
}

// fetchTail requests the last tailBytes of the table.
func (s *Source) fetchTail(ctx context.Context) ([]byte, error) {
	resp, err := s.client.Get(ctx, httpx.Request{
		URL: s.baseURL + pathFluxTable,
		Headers: map[string]string{
			// A suffix range. The server answers 206 with the last tailBytes,
			// and httpx returns a 206 body unchanged.
			"Range": fmt.Sprintf("bytes=-%d", tailBytes),
		},
		// The table sends both ETag and Last-Modified. A conditional GET is
		// combined with the range deliberately: three polls a day against a
		// file that gains one row per poll means roughly two of the three earn
		// a body and the third a 304.
		Conditional: true,
		Retries:     s.retries,
		MaxBody:     s.maxBody,
	})
	if err != nil {
		return nil, err
	}

	// A server that ignores Range answers 200 with the whole file. maxBodyBytes
	// already fails that case loudly, but if the file ever shrinks below the
	// limit the range would be silently ineffective, so it is logged.
	if resp.StatusCode != 206 {
		s.log.Warn("drao ignored our Range header and sent a full response; "+
			"this fetches the whole 2 MB table on every poll",
			"status", resp.StatusCode, "bytes", len(resp.Body))
	}

	return resp.Body, nil
}

// add appends a validated sample, dropping and logging an invalid one.
func (s *Source) add(b *metric.Batch, desc *metric.Descriptor, value float64, at time.Time, labels ...string) {
	sample := metric.Sample{Desc: desc, Labels: labels, Value: value, Time: at}
	if err := sample.Validate(); err != nil {
		s.log.Warn("drao produced an invalid sample; dropping it", "error", err)
		return
	}
	b.Samples = append(b.Samples, sample)
}

// fluxRow is one parsed measurement.
type fluxRow struct {
	// at is the measurement time in UT, from the fluxdate and fluxtime columns.
	at time.Time

	// observed, adjusted and ursi are the three flux columns in SFU.
	observed float64
	adjusted float64
	ursi     float64
}

// sentinel values. The table has no documented missing-data marker, and none
// has been observed in twenty-two years of rows, but a zero or negative flux is
// physically impossible — the quiet-Sun floor is about 64 SFU — so it is treated
// as one rather than published.
const (
	minPlausibleFlux = 1.0
	maxPlausibleFlux = 10000.0
)

// newestRow parses a tail response and returns its last complete row.
//
// The first line of a suffix-range response is almost always a fragment of a
// row cut mid-field, and the header line is only present when the whole file
// was returned. Both are handled the same way: a line that does not parse as a
// complete row is skipped, not fatal, as long as at least one line does. A tail
// with no parseable row at all is an error, because that means the format
// changed and publishing nothing while reporting success is how a source goes
// quiet unnoticed.
func newestRow(body string) (fluxRow, error) {
	var (
		best      fluxRow
		found     bool
		lastSkip  string
		nonHeader int
	)

	for _, line := range strings.Split(body, "\n") {
		line = strings.TrimSpace(strings.TrimRight(line, "\r"))
		if line == "" {
			continue
		}
		// The column header, present only if the range was ignored.
		if strings.HasPrefix(line, "fluxdate") || strings.HasPrefix(line, "#") {
			continue
		}
		nonHeader++

		row, err := parseRow(line)
		if err != nil {
			lastSkip = err.Error()
			continue
		}
		found = true
		best = row
	}

	if nonHeader == 0 {
		return fluxRow{}, errors.New("response contained no data lines")
	}
	if !found {
		return fluxRow{}, fmt.Errorf("no parseable row in %d data line(s); last failure: %s", nonHeader, lastSkip)
	}
	return best, nil
}

// parseRow reads one measurement line.
//
//	fluxdate fluxtime fluxjulian fluxcarrington fluxobsflux fluxadjflux fluxursi
//	20260908 170000   2461292.197 2315.36        0109.1      0110.8      0099.7
//
// The flux columns are zero-padded to four digits before the decimal point,
// which ParseFloat handles, and fluxtime is HHMMSS zero-padded to six digits.
// The julian and carrington columns are parsed for shape but not used: the
// julian date is the same instant as fluxdate/fluxtime and going through it
// would introduce a rounding step for nothing.
func parseRow(line string) (fluxRow, error) {
	f := strings.Fields(line)

	const wantFields = 7
	if len(f) != wantFields {
		return fluxRow{}, fmt.Errorf("expected %d fields, got %d", wantFields, len(f))
	}

	at, err := parseDateTime(f[0], f[1])
	if err != nil {
		return fluxRow{}, err
	}

	if _, err := strconv.ParseFloat(f[2], 64); err != nil {
		return fluxRow{}, fmt.Errorf("bad julian date %q", f[2])
	}
	if _, err := strconv.ParseFloat(f[3], 64); err != nil {
		return fluxRow{}, fmt.Errorf("bad carrington rotation %q", f[3])
	}

	observed, err := parseFlux(f[4], "observed")
	if err != nil {
		return fluxRow{}, err
	}
	adjusted, err := parseFlux(f[5], "adjusted")
	if err != nil {
		return fluxRow{}, err
	}
	ursi, err := parseFlux(f[6], "ursi")
	if err != nil {
		return fluxRow{}, err
	}

	return fluxRow{at: at, observed: observed, adjusted: adjusted, ursi: ursi}, nil
}

// parseDateTime reads the YYYYMMDD and HHMMSS columns as a UT instant.
func parseDateTime(date, clock string) (time.Time, error) {
	if len(date) != 8 {
		return time.Time{}, fmt.Errorf("bad fluxdate %q", date)
	}
	if len(clock) != 6 {
		return time.Time{}, fmt.Errorf("bad fluxtime %q", clock)
	}
	t, err := time.ParseInLocation("20060102150405", date+clock, time.UTC)
	if err != nil {
		return time.Time{}, fmt.Errorf("bad fluxdate/fluxtime %q %q", date, clock)
	}
	return t, nil
}

// parseFlux reads one zero-padded flux column and rejects a physically
// impossible value rather than publishing it.
func parseFlux(field, which string) (float64, error) {
	v, err := strconv.ParseFloat(field, 64)
	if err != nil {
		return 0, fmt.Errorf("bad %s flux %q", which, field)
	}
	if v < minPlausibleFlux || v > maxPlausibleFlux {
		return 0, fmt.Errorf("%s flux %g is outside the plausible range", which, v)
	}
	return v, nil
}
