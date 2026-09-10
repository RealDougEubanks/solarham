package lasp

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/RealDougEubanks/solarham/internal/httpx"
	"github.com/RealDougEubanks/solarham/internal/metric"
)

const (
	// pathEVE is the SDO/EVE Level 0CS quicklook file. Note "evewebdata" and
	// the plural "DIODES"; the singular and the shorter directory names that
	// are commonly cited both 404.
	pathEVE = "/eve/data_access/evewebdata/quicklook/L0CS/LATEST_EVE_L0CS_DIODES_1m.txt"

	// eveInterval matches the file's cadence. It is one-minute averaged data
	// republished every minute with about a minute of latency, so this is one of
	// the few products in the exporter where a one-minute poll is honest rather
	// than wasteful.
	eveInterval = time.Minute

	// eveTailBytes is how much of the end of the file to request.
	//
	// The file starts each UT day empty and grows by about 180 bytes a minute to
	// roughly 260 KB by 23:59. Fetching all of it every minute would be about
	// 190 MB a day off a university web server to read the last 180 bytes.
	// Sixteen kilobytes is roughly the last ninety minutes, which is far more
	// than the newest row but leaves room for a run of sentinel rows during a
	// downlink gap without the response containing no usable row at all.
	eveTailBytes = 16 << 10

	// eveHeaderBytes is how much of the start of the file to request when the
	// UT date is needed. The header is 35 lines and about 2.4 KB, and the date
	// line immediately follows ";END_OF_HEADER" on line 36.
	eveHeaderBytes = 4 << 10

	// eveMaxBody bounds an EVE response. It is deliberately close to the two
	// range sizes: if the server ever ignores Range and answers 200 with the
	// whole file, this source must fail loudly rather than quietly start
	// downloading a quarter of a megabyte every minute.
	eveMaxBody = 64 << 10

	// eveHeaderMarker terminates the ';'-prefixed comment header.
	eveHeaderMarker = ";END_OF_HEADER"
)

// eveSentinel is the missing-data marker, which the file's own header states as
// "-1.00e+00".
//
// It is compared as a numeric threshold rather than as a string because the
// columns are not written to a consistent width: the same missing value appears
// as "-1.000e+00" in the irradiance columns and "-1.0000e+00" in the MEGS-P
// column. Every quantity this source publishes from the file is a
// strictly positive irradiance, so "at or below zero" and "sentinel" are the
// same test, and one that cannot be defeated by a formatting change.
const eveSentinel = 0.0

// eveBand maps a column to its band label on metric.EUVIrradiance.
//
// Only the five bands with a working instrument behind them are listed. 121.6
// nm (MEGS-P) is deliberately absent: it reports the sentinel on essentially
// every row because the channel has degraded, and a band label that never
// produces a sample is a series an operator will spend an afternoon looking for.
// 36.6 nm is listed even though it is also currently sentinel-valued on every
// row, because that channel's dropout is intermittent rather than permanent.
type eveBand struct {
	column int
	label  string
}

// Column indices into a data row, zero-based, in the order the file's own
// "; Format:" block documents them.
const (
	colHHMM = iota
	colXRSBProxy
	colXRSAProxy
	colSEMProxy
	col0107ESPQuad
	col171ESP
	col257ESP
	col304ESP
	col366ESP
	colDarkESP
	col1216MEGSP
	colDarkMEGSP
	colQ0ESP
	colQ1ESP
	colQ2ESP
	colQ3ESP
	colCMLat
	colCMLon
	colXCoolProxy
	colOldXRSBProxy

	// eveColumns is the number of fields a complete data row has.
	eveColumns
)

// eveBands are the EUV irradiance columns published, in wavelength order.
var eveBands = []eveBand{
	{column: col0107ESPQuad, label: "0.1-7nm"},
	{column: col171ESP, label: "17.1nm"},
	{column: col257ESP, label: "25.7nm"},
	{column: col304ESP, label: "30.4nm"},
	{column: col366ESP, label: "36.6nm"},
}

// xrayBandLabel names the XRS-B proxy on metric.XRayFlux.
//
// The "-proxy" suffix is not decoration. This column is a *model* of what GOES
// XRS-B would report, computed by EVE's two-component method from ESP
// measurements; it is not a GOES measurement. The swpc source publishes the real
// GOES 0.1-0.8 nm flux under the band label "0.1-0.8nm", and two series that
// look identical but are a measurement and a model of it must not share a label.
const xrayBandLabel = "0.1-0.8nm-proxy"

// eveDateCache holds the UT date from the file's header.
//
// A suffix-range request for the tail of the file cannot see the header, and the
// data rows carry only HHMM — no date. Rather than assume the file's date is
// today's UT date, the header is fetched and the date read from it, then cached:
// the date changes once a day and the tail is fetched once a minute, so this
// turns 2,880 requests a day into 1,441.
//
// The cached date is re-read from the header when it is absent, when the wall
// clock's UT date no longer matches it, or when the newest HHMM in the tail goes
// backwards — the last being what a day rollover looks like from the tail alone,
// and a check that does not depend on this process's clock being right.
type eveDateCache struct {
	mu sync.Mutex

	// day is midnight UT of the date the header reported.
	day time.Time

	// lastMinute is the newest HHMM seen, as minutes past midnight, to detect a
	// rollover.
	lastMinute int
	haveMinute bool
}

// pollEVE fetches the tail of the quicklook file and publishes its newest usable
// row. The returned time is that row's own timestamp.
func (s *Source) pollEVE(ctx context.Context) (out []metric.Sample, observed time.Time, err error) {
	// A fixed-width table written by somebody else's code. A panic here would
	// take the exporter down rather than one poll.
	defer func() {
		if r := recover(); r != nil {
			out, observed, err = nil, time.Time{}, fmt.Errorf("panic parsing the EVE quicklook file: %v", r)
		}
	}()

	body, err := s.fetchEVETail(ctx)
	if err != nil {
		return nil, time.Time{}, err
	}

	row, err := newestEVERow(string(body))
	if err != nil {
		return nil, time.Time{}, err
	}

	day, err := s.eveDay(ctx, row.minute)
	if err != nil {
		return nil, time.Time{}, err
	}
	at := day.Add(time.Duration(row.minute) * time.Minute)

	var b metric.Batch
	for _, band := range eveBands {
		if v, ok := row.value(band.column); ok {
			s.add(&b, metric.EUVIrradiance, v, at, band.label)
		}
	}

	// The centroid columns. These are the reason to fetch this file at all, and
	// they are the one pair where the sentinel test cannot be "at or below
	// zero": a latitude of -9.7 degrees and a longitude of -30.6 are perfectly
	// ordinary readings. They are tested against the sentinel value itself,
	// within a tolerance, and against the physical range.
	if v, ok := row.angle(colCMLat); ok {
		s.add(&b, metric.EUVSourceLatitude, v, at)
	}
	if v, ok := row.angle(colCMLon); ok {
		s.add(&b, metric.EUVSourceLongitude, v, at)
	}

	if v, ok := row.value(colXRSBProxy); ok {
		s.add(&b, metric.XRayFlux, v, at, xrayBandLabel)
	}

	if len(b.Samples) == 0 {
		// Every column in the newest usable row was a sentinel. That is a real
		// upstream condition during a downlink gap, and it is a success with no
		// samples rather than an error — but the row's timestamp is still worth
		// returning so SourceDataAge reflects it.
		s.log.Debug("eve newest row carried no usable column", "at", at)
	}

	return b.Samples, at, nil
}

// fetchEVETail requests the last eveTailBytes of the quicklook file.
func (s *Source) fetchEVETail(ctx context.Context) ([]byte, error) {
	resp, err := s.client.Get(ctx, httpx.Request{
		URL: s.baseURL + pathEVE,
		Headers: map[string]string{
			"Range": fmt.Sprintf("bytes=-%d", eveTailBytes),
		},
		// The file sends ETag and Last-Modified and is rewritten every minute,
		// so a 304 is rare — but it does happen when a poll lands inside the
		// same minute as the last one, and paying a 304 for that is free.
		Conditional: true,
		Retries:     s.retries,
		MaxBody:     eveMaxBody,
	})
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 206 {
		s.log.Warn("eve ignored our Range header and sent a full response; "+
			"this fetches the whole quicklook file every minute",
			"status", resp.StatusCode, "bytes", len(resp.Body))
	}
	return resp.Body, nil
}

// eveDay returns midnight UT of the file's date, fetching the header when the
// cache cannot be trusted.
func (s *Source) eveDay(ctx context.Context, minute int) (time.Time, error) {
	c := &s.eveDate
	c.mu.Lock()
	cached := c.day
	rollover := c.haveMinute && minute < c.lastMinute
	c.lastMinute, c.haveMinute = minute, true
	c.mu.Unlock()

	today := s.now().UTC().Truncate(24 * time.Hour)
	if !cached.IsZero() && !rollover && cached.Equal(today) {
		return cached, nil
	}

	day, err := s.fetchEVEDate(ctx)
	if err != nil {
		if !cached.IsZero() {
			// The cached date is stale by our clock but re-reading the header
			// failed. Using yesterday's date would mis-stamp every sample by
			// twenty-four hours, which is worse than a failed poll.
			return time.Time{}, fmt.Errorf("re-reading the EVE header date: %w", err)
		}
		return time.Time{}, fmt.Errorf("reading the EVE header date: %w", err)
	}

	c.mu.Lock()
	c.day = day
	c.mu.Unlock()
	return day, nil
}

// fetchEVEDate reads the UT date from the front of the file.
func (s *Source) fetchEVEDate(ctx context.Context) (time.Time, error) {
	resp, err := s.client.Get(ctx, httpx.Request{
		URL: s.baseURL + pathEVE,
		Headers: map[string]string{
			"Range": fmt.Sprintf("bytes=0-%d", eveHeaderBytes-1),
		},
		// Deliberately not conditional. This request shares its URL with the
		// tail request, and httpx caches validators per URL, so replaying the
		// tail's ETag here would earn a 304 and no date. The two ranges are two
		// different resources behind one URL, which is exactly the case
		// conditional GET does not model.
		Retries: s.retries,
		MaxBody: eveMaxBody,
	})
	if err != nil {
		return time.Time{}, err
	}
	return parseEVEDate(string(resp.Body))
}

// parseEVEDate reads the "YYYY DOY MO DD" line that follows the header marker.
//
// The line is redundant on purpose — day-of-year and month/day are the same
// date twice — so the two are cross-checked. A file whose day-of-year and
// calendar date disagree has been assembled wrongly, and stamping a day's worth
// of one-minute samples with the wrong date is not a failure anybody would spot
// on a graph.
func parseEVEDate(body string) (time.Time, error) {
	_, after, ok := strings.Cut(body, eveHeaderMarker)
	if !ok {
		return time.Time{}, fmt.Errorf("no %s in the response", eveHeaderMarker)
	}

	for line := range strings.SplitSeq(after, "\n") {
		line = strings.TrimSpace(strings.TrimRight(line, "\r"))
		if line == "" || strings.HasPrefix(line, ";") {
			continue
		}

		f := strings.Fields(line)
		if len(f) != 4 {
			return time.Time{}, fmt.Errorf("date line %q does not have four fields", line)
		}
		year, errY := strconv.Atoi(f[0])
		doy, errD := strconv.Atoi(f[1])
		month, errM := strconv.Atoi(f[2])
		day, errDay := strconv.Atoi(f[3])
		if errY != nil || errD != nil || errM != nil || errDay != nil {
			return time.Time{}, fmt.Errorf("date line %q is not four integers", line)
		}
		if year < 2010 || year > 3000 || month < 1 || month > 12 || day < 1 || day > 31 || doy < 1 || doy > 366 {
			return time.Time{}, fmt.Errorf("date line %q is out of range", line)
		}

		at := time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
		if at.YearDay() != doy {
			return time.Time{}, fmt.Errorf("date line %q disagrees with itself: day-of-year %d is not %s",
				line, doy, at.Format("2006-01-02"))
		}
		return at, nil
	}

	return time.Time{}, errors.New("no date line after the header marker")
}

// eveRow is one parsed data line.
type eveRow struct {
	// minute is HHMM as minutes past midnight UT.
	minute int

	// fields is the row's columns, unparsed. They are converted on demand
	// because most of them are never published and parsing a dark-diode count
	// rate to throw it away is work for nothing.
	fields []string
}

// value reads a column that must be strictly positive, reporting false for the
// sentinel and for anything unparseable.
//
// An unparseable number is treated as absent rather than as an error: the
// columns published from this file are independent measurements from two
// instruments, and one garbled field should not discard a row that carries a
// good EUV centroid.
func (r eveRow) value(col int) (float64, bool) {
	if col >= len(r.fields) {
		return 0, false
	}
	v, err := strconv.ParseFloat(r.fields[col], 64)
	if err != nil || v <= eveSentinel {
		return 0, false
	}
	return v, true
}

// angle reads one of the centroid columns, which are signed and may legitimately
// be negative.
//
// The sentinel here has to be matched as a value rather than by sign. -1.0 is
// within the physical range of both a heliographic latitude and a longitude, so
// this test has a genuine false-positive: a real centroid at exactly -1.0
// degrees is discarded. That is accepted, because the columns are written to one
// decimal place and a single dropped minute of a one-minute series costs
// nothing, whereas publishing the sentinel as a position puts a wrong number on
// the one metric nobody else publishes.
func (r eveRow) angle(col int) (float64, bool) {
	if col >= len(r.fields) {
		return 0, false
	}
	v, err := strconv.ParseFloat(r.fields[col], 64)
	if err != nil {
		return 0, false
	}
	// Heliographic latitude runs to +/-90 and the centroid longitude to +/-90,
	// since the far side of the disc is not visible.
	if v < -90 || v > 90 {
		return 0, false
	}
	if isEVESentinel(v) {
		return 0, false
	}
	return v, true
}

// isEVESentinel reports whether v is the -1.00e+00 marker. The tolerance covers
// the differing precisions the file writes it at.
func isEVESentinel(v float64) bool {
	const tolerance = 1e-9
	return v > -1-tolerance && v < -1+tolerance
}

// newestEVERow returns the last complete data row in a response.
//
// "Complete" excludes the truncated first line of a suffix-range response and
// any remaining header comment. A row is kept if it has the full column count
// and a readable HHMM, whether or not its individual columns are sentinels; the
// per-column sentinel filter runs later, so a downlink gap yields a correctly
// timestamped row with no samples rather than an older row presented as current.
func newestEVERow(body string) (eveRow, error) {
	var (
		best      eveRow
		found     bool
		dataLines int
		lastSkip  string
	)

	// If the whole file came back, skip everything up to the header marker.
	if _, after, ok := strings.Cut(body, eveHeaderMarker); ok {
		body = after
	}

	for line := range strings.SplitSeq(body, "\n") {
		line = strings.TrimSpace(strings.TrimRight(line, "\r"))
		if line == "" || strings.HasPrefix(line, ";") {
			continue
		}

		f := strings.Fields(line)

		// The four-field "YYYY DOY MO DD" date line, present when the range was
		// ignored. Not a data row and not an error.
		if len(f) == 4 {
			continue
		}

		dataLines++
		if len(f) != eveColumns {
			lastSkip = fmt.Sprintf("expected %d fields, got %d", eveColumns, len(f))
			continue
		}
		minute, err := parseHHMM(f[colHHMM])
		if err != nil {
			lastSkip = err.Error()
			continue
		}

		best, found = eveRow{minute: minute, fields: f}, true
	}

	if dataLines == 0 {
		return eveRow{}, errors.New("EVE response contained no data lines")
	}
	if !found {
		return eveRow{}, fmt.Errorf("no complete row in %d data line(s); last failure: %s", dataLines, lastSkip)
	}
	return best, nil
}

// parseHHMM reads the four-digit UT time column as minutes past midnight.
func parseHHMM(field string) (int, error) {
	if len(field) != 4 {
		return 0, fmt.Errorf("HHMM %q is not four digits", field)
	}
	hour, err := strconv.Atoi(field[:2])
	if err != nil || hour < 0 || hour > 23 {
		return 0, fmt.Errorf("HHMM %q has a bad hour", field)
	}
	minute, err := strconv.Atoi(field[2:])
	if err != nil || minute < 0 || minute > 59 {
		return 0, fmt.Errorf("HHMM %q has a bad minute", field)
	}
	return hour*60 + minute, nil
}
