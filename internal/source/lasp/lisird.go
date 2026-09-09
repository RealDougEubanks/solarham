package lasp

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/RealDougEubanks/solarham/internal/httpx"
	"github.com/RealDougEubanks/solarham/internal/metric"
)

const (
	// lisirdPathFormat is the LATIS DAP endpoint. The dataset name goes in the
	// path and the time filter in the query.
	//
	// The ">" in the filter must be percent-encoded as %3E. LATIS reads the raw
	// query string rather than a decoded parameter map, so a literal ">" — which
	// most HTTP clients will happily send unescaped — is not recognised as an
	// operator and the server answers with the dataset's entire history.
	lisirdPathFormat = "/lisird/latis/dap/%s.csv?time%%3E=%s"

	// lisirdInterval is how often the LISIRD datasets are fetched.
	//
	// Three hours, rather than a source.DailyAt schedule, and the choice needs
	// justifying because DailyAt is the better tool almost everywhere else in
	// this exporter. It is not used here because there is no single publication
	// clock to name: these are six independent datasets assembled by five
	// different institutions on cadences LISIRD does not document. tsis_tsi_6hr
	// appears in six-hour buckets, bremen_composite_mgii and
	// composite_lyman_alpha arrive daily but with a lag of days that varies with
	// whoever runs the reprocessing, and the CLS radio flux series come out of
	// Toulouse on their own schedule. Naming three fixed times would be
	// inventing a schedule rather than declaring one, and getting it wrong means
	// a dataset sits a full day stale.
	//
	// Three hours is eight polls a day of six responses each — 48 requests
	// against datasets that update at most a handful of times a day — for
	// payloads of 130 bytes to 500 bytes. That is well inside anybody's
	// definition of polite for a university server, and far inside the
	// "no faster than hourly" floor. All six are conditional, so an unchanged
	// dataset costs a 304.
	lisirdInterval = 3 * time.Hour

	// lisirdWindowDays is how far back each query asks.
	//
	// Only the newest row is published, but the endpoint needs a lower bound and
	// the reprocessing lag varies wildly per dataset. A four-day window was
	// tried first and tsis_tsi_6hr answered with a header and no rows at all:
	// its lag was measured at about five days, and the Mg II and Lyman-alpha
	// composites run two to four days behind. A window shorter than the slowest
	// dataset's lag turns that dataset into a permanent empty response, which is
	// indistinguishable from the dataset having been withdrawn.
	//
	// Thirty days is comfortably past every observed lag while keeping the
	// largest response — tsis_tsi_6hr at four rows a day — around 30 KB, and the
	// daily ones under 2 KB.
	lisirdWindowDays = 30

	// lisirdMaxBody bounds one response. Live responses over a ten-day window
	// are 130 bytes to 500 bytes; 1 MB is three orders of magnitude of headroom
	// and still bounded.
	lisirdMaxBody = 1 << 20
)

// lisirdMinInterval is the courtesy floor. These are daily datasets; polling
// faster than hourly cannot produce an observation.
const lisirdMinInterval = time.Hour

// The LISIRD gotchas worth recording, because both cost an afternoon to
// diagnose from the response alone:
//
//   - noaa_radio_flux.csv answers HTTP 200 with a body of exactly one byte. The
//     dataset is registered and the endpoint is live, but it has no recent data,
//     so a client that treats 200 as success and an empty parse as "no samples"
//     reports a healthy source that publishes nothing forever. It is not
//     fetched.
//
//   - the time column's units differ per dataset and are stated in the CSV
//     header rather than being discoverable any other way. Four encodings appear
//     across the datasets fetched here: Julian Date, "yyyy MM dd", days since an
//     epoch, and milliseconds since an epoch. The header is read and the units
//     honoured; nothing is assumed from the dataset name.

// lisirdDataset describes one dataset and how to publish it.
type lisirdDataset struct {
	// name is the LATIS dataset identifier, used in the URL.
	name string

	// emit converts the newest row into samples.
	emit func(s *Source, b *metric.Batch, row lisirdRow, at time.Time)
}

// f107Datasets are the two LISIRD datasets that carry 10.7 cm flux.
//
// They are not fetched by default, and that is a deliberate wiring decision
// rather than an oversight. Both publish the same quantity, under the same
// wavelength_cm="10.7" label, as the drao source — penticton_radio_flux is
// literally a republication of the file drao fetches first-hand, and
// cls_radio_flux_f107 is CLS's assembly of the same Penticton measurements. Two
// sources writing the same series from two upstreams that round differently is
// worse than one source writing it once: the series flickers between two values
// with no way for a consumer to tell which poll produced which.
//
// So the first-hand source wins by default and these stay off. A deployment with
// drao disabled — because Natural Resources Canada is unreachable from it, say —
// calls EnableF107Datasets to get F10.7 from here instead.
var f107Datasets = []lisirdDataset{
	{
		name: "penticton_radio_flux",
		emit: func(s *Source, b *metric.Batch, row lisirdRow, at time.Time) {
			s.emitFlux(b, row, at, "10.7", "observed_flux", "adjusted_flux")
		},
	},
	{
		name: "cls_radio_flux_f107",
		emit: func(s *Source, b *metric.Batch, row lisirdRow, at time.Time) {
			s.emitFlux(b, row, at, "10.7", "absolute_f107", "adjusted_f107")
		},
	},
}

// defaultDatasets are fetched whenever LISIRD is enabled. None of them
// duplicates a series any other source in this exporter publishes.
var defaultDatasets = []lisirdDataset{
	// The CLS multi-frequency radio flux series. The ratio of the 30 cm to the
	// 10.7 cm flux is a spectral-hardness diagnostic, which is the reason to
	// have more than one wavelength at all.
	//
	// Each series has four columns per adjustment: the value, a "_c" corrected
	// value, a "_p" uncertainty and an "_f" quality flag. The plain value is
	// published. The corrected column differs from it only when the flag is
	// non-zero, which marks a measurement contaminated by a radio burst — on
	// 2026-09-02, absolute_f107 read 139.0 with flag 4 while absolute_f107_c
	// read 99.3. The uncorrected value is the honest reading of what the
	// radiometer saw and is what every other publisher of these series carries,
	// so it is the one published; the corrected column is not, because a series
	// that silently switches between two derivations is unusable for anything
	// requiring consistency.
	{
		name: "cls_radio_flux_f8",
		emit: func(s *Source, b *metric.Batch, row lisirdRow, at time.Time) {
			s.emitFlux(b, row, at, "8", "absolute_f8", "adjusted_f8")
		},
	},
	{
		name: "cls_radio_flux_f15",
		emit: func(s *Source, b *metric.Batch, row lisirdRow, at time.Time) {
			s.emitFlux(b, row, at, "15", "absolute_f15", "adjusted_f15")
		},
	},
	{
		name: "cls_radio_flux_f30",
		emit: func(s *Source, b *metric.Batch, row lisirdRow, at time.Time) {
			s.emitFlux(b, row, at, "30", "absolute_f30", "adjusted_f30")
		},
	},
	{
		name: "cls_radio_flux_f32",
		emit: func(s *Source, b *metric.Batch, row lisirdRow, at time.Time) {
			s.emitFlux(b, row, at, "32", "absolute_f32", "adjusted_f32")
		},
	},

	// TSIS-1 total solar irradiance, at 1 AU. The true-Earth column in the same
	// response is the same measurement without the distance correction, and is
	// not published: TSI varies by about 0.1% over a solar cycle, so a series
	// carrying the 7% annual eccentricity swing on top of that hides the signal
	// entirely.
	{
		name: "tsis_tsi_6hr",
		emit: func(s *Source, b *metric.Batch, row lisirdRow, at time.Time) {
			if v, ok := row.positive("tsi_1au"); ok {
				s.add(b, metric.TotalSolarIrradiance, v, at)
			}
		},
	},

	// Bremen composite Mg II core-to-wing index. Tracks EUV better than sunspot
	// number does, and unlike EUV it has an unbroken record back to 1978.
	{
		name: "bremen_composite_mgii",
		emit: func(s *Source, b *metric.Batch, row lisirdRow, at time.Time) {
			if v, ok := row.positive("mgii"); ok {
				s.add(b, metric.MgIIIndex, v, at)
			}
		},
	},

	// Composite Lyman-alpha irradiance, a driver of D-layer ionisation and
	// therefore of daytime HF absorption.
	{
		name: "composite_lyman_alpha",
		emit: func(s *Source, b *metric.Batch, row lisirdRow, at time.Time) {
			if v, ok := row.positive("irradiance"); ok {
				s.add(b, metric.LymanAlphaIrradiance, v, at)
			}
		},
	},
}

// EnableF107Datasets adds penticton_radio_flux and cls_radio_flux_f107 to the
// datasets this source fetches.
//
// Call this only when the drao source is disabled. See f107Datasets for why
// enabling both writes one series from two upstreams. It must be called before
// the source is handed to the scheduler; it is not safe to call concurrently
// with Poll.
func (s *Source) EnableF107Datasets() {
	s.f107 = true
	s.log.Info("lasp will publish 10.7 cm flux from LISIRD; " +
		"the drao source must be disabled, or the two will write the same series")
}

// lisirdDue reports whether the LISIRD datasets should be fetched on this tick.
//
// When EVE is also enabled the scheduler runs every minute, which is right for
// EVE and 180 times too often for a daily dataset, so the gating is done here
// rather than by pretending one schedule fits both.
func (s *Source) lisirdDue(now time.Time) bool {
	s.lisirdMu.Lock()
	defer s.lisirdMu.Unlock()

	if s.lisirdLast.IsZero() {
		return true
	}
	return !now.Before(s.lisirdLast.Add(lisirdInterval))
}

// noteLISIRDPolled records a successful LISIRD fetch, so the next one waits a
// full interval. A failed fetch deliberately does not update it: a dataset that
// errored should be retried on the next tick rather than in three hours.
func (s *Source) noteLISIRDPolled(now time.Time) {
	s.lisirdMu.Lock()
	defer s.lisirdMu.Unlock()
	s.lisirdLast = now
}

// pollLISIRD fetches every enabled dataset and returns their samples plus the
// newest payload timestamp across them.
//
// One dataset failing does not fail the group. These are six unrelated products
// from five institutions and they fail independently; losing the Mg II index
// because Toulouse's radio flux is briefly 500ing would be absurd.
func (s *Source) pollLISIRD(ctx context.Context, now time.Time) ([]metric.Sample, time.Time, error) {
	datasets := defaultDatasets
	if s.f107 {
		datasets = append(append([]lisirdDataset(nil), datasets...), f107Datasets...)
	}

	since := now.AddDate(0, 0, -lisirdWindowDays).Format("2006-01-02")

	var (
		batch  metric.Batch
		errs   []error
		newest time.Time
	)

	for _, ds := range datasets {
		samples, observed, err := s.fetchDataset(ctx, ds, since)
		switch {
		case errors.Is(err, httpx.ErrNotModified):
			// Ordinary at a three-hour poll against a daily dataset. It carries
			// no timestamp, so it contributes nothing to the age calculation —
			// which is correct: a 304 says the data has not changed, not that it
			// is fresh.
			continue
		case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded),
			errors.Is(err, httpx.ErrRateLimited):
			// Both are conditions about the whole group rather than this
			// dataset, so they stop the loop rather than joining the errors.
			return nil, time.Time{}, err
		case err != nil:
			errs = append(errs, fmt.Errorf("%s: %w", ds.name, err))
			continue
		}

		batch.Samples = append(batch.Samples, samples...)
		if observed.After(newest) {
			newest = observed
		}
	}

	if len(errs) == len(datasets) {
		return nil, time.Time{}, fmt.Errorf("every dataset failed: %w", errors.Join(errs...))
	}
	for _, err := range errs {
		s.log.Warn("lisird dataset failed, continuing with the rest", "error", err)
	}

	return batch.Samples, newest, nil
}

// fetchDataset retrieves and parses one dataset.
func (s *Source) fetchDataset(ctx context.Context, ds lisirdDataset, since string) (out []metric.Sample, observed time.Time, err error) {
	defer func() {
		if r := recover(); r != nil {
			out, observed, err = nil, time.Time{}, fmt.Errorf("panic parsing %s: %v", ds.name, r)
		}
	}()

	resp, err := s.client.Get(ctx, httpx.Request{
		URL:         s.baseURL + fmt.Sprintf(lisirdPathFormat, ds.name, since),
		Conditional: true,
		Retries:     s.retries,
		MaxBody:     lisirdMaxBody,
	})
	if err != nil {
		return nil, time.Time{}, err
	}

	// The noaa_radio_flux one-byte-body case, generalised. A response too short
	// to hold a header line is an error rather than an empty result, because
	// "the endpoint is alive but the dataset is dead" is precisely the failure
	// an operator needs told about and precisely the one that looks like success.
	if len(strings.TrimSpace(string(resp.Body))) < 2 {
		return nil, time.Time{}, fmt.Errorf("empty response: the dataset exists but returned no data")
	}

	table, err := parseLISIRD(resp.Body)
	if err != nil {
		return nil, time.Time{}, err
	}
	row, at, err := table.newestRow()
	if err != nil {
		return nil, time.Time{}, err
	}

	var b metric.Batch
	ds.emit(s, &b, row, at)
	return b.Samples, at, nil
}

// emitFlux publishes the observed and adjusted flux for one wavelength.
//
// A dataset that has one of the two columns and not the other still publishes
// the one it has. The columns come from independent processing steps and a
// missing adjusted value is no reason to withhold the observed measurement.
func (s *Source) emitFlux(b *metric.Batch, row lisirdRow, at time.Time, wavelength, observedCol, adjustedCol string) {
	if v, ok := row.positive(observedCol); ok {
		s.add(b, metric.RadioFluxMultiFrequency, v, at, wavelength, "observed")
	}
	if v, ok := row.positive(adjustedCol); ok {
		s.add(b, metric.RadioFluxMultiFrequency, v, at, wavelength, "adjusted")
	}
}

// ---------------------------------------------------------------------------
// CSV parsing
// ---------------------------------------------------------------------------

// lisirdTable is a parsed LATIS CSV response.
type lisirdTable struct {
	// columns maps a bare column name to its index. The header writes each
	// column as "name (units)"; the units are stripped here and kept in units
	// below.
	columns map[string]int

	// timeUnits is the parenthesised units of the first column, verbatim from
	// the header. This is the only place the time encoding is stated.
	timeUnits string

	// rows are the data rows in file order.
	rows [][]string
}

// lisirdRow is one data row with its table's column index, so a value can be
// looked up by name.
type lisirdRow struct {
	table  *lisirdTable
	fields []string
}

// parseLISIRD reads a LATIS CSV response.
func parseLISIRD(body []byte) (*lisirdTable, error) {
	r := csv.NewReader(strings.NewReader(string(body)))

	// The number of columns is genuinely per-dataset and the header states it,
	// so the reader is not asked to enforce a count.
	r.FieldsPerRecord = -1
	r.TrimLeadingSpace = true

	header, err := r.Read()
	if err != nil {
		return nil, fmt.Errorf("reading the CSV header: %w", err)
	}
	if len(header) == 0 {
		return nil, errors.New("CSV header is empty")
	}

	table := &lisirdTable{columns: make(map[string]int, len(header))}
	for i, raw := range header {
		name, units := splitColumnUnits(raw)
		if name == "" {
			return nil, fmt.Errorf("CSV column %d has no name in %q", i, raw)
		}
		table.columns[name] = i
		if i == 0 {
			table.timeUnits = units
		}
	}
	if _, ok := table.columns["time"]; !ok {
		return nil, fmt.Errorf("CSV header has no time column: %v", header)
	}

	for {
		rec, err := r.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("reading a CSV row: %w", err)
		}
		if len(rec) == 0 || (len(rec) == 1 && strings.TrimSpace(rec[0]) == "") {
			continue
		}
		table.rows = append(table.rows, rec)
	}

	if len(table.rows) == 0 {
		return nil, errors.New("CSV has a header but no data rows")
	}
	return table, nil
}

// splitColumnUnits splits `observed_flux (solar flux unit (SFU))` into the name
// and the units.
//
// The units themselves contain parentheses, so the split is on the *first* open
// paren and the trailing close paren, not on a balanced match.
func splitColumnUnits(raw string) (name, units string) {
	raw = strings.TrimSpace(raw)
	open := strings.Index(raw, "(")
	if open < 0 {
		return raw, ""
	}
	name = strings.TrimSpace(raw[:open])
	units = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(raw[open+1:]), ")"))
	return name, units
}

// newestRow returns the last row in the response and its timestamp.
//
// LATIS returns rows in ascending time order, but the order is not depended on:
// the row with the greatest timestamp wins. A row whose time will not convert is
// skipped rather than fatal, since a dataset with one bad row still has a usable
// newest value.
func (t *lisirdTable) newestRow() (lisirdRow, time.Time, error) {
	timeCol := t.columns["time"]

	var (
		best     lisirdRow
		bestAt   time.Time
		found    bool
		lastSkip string
	)

	for _, rec := range t.rows {
		if timeCol >= len(rec) {
			lastSkip = "row is shorter than its time column"
			continue
		}
		at, err := parseLISIRDTime(rec[timeCol], t.timeUnits)
		if err != nil {
			lastSkip = err.Error()
			continue
		}
		if found && !at.After(bestAt) {
			continue
		}
		best, bestAt, found = lisirdRow{table: t, fields: rec}, at, true
	}

	if !found {
		return lisirdRow{}, time.Time{}, fmt.Errorf("no row with a readable time in %d row(s); last failure: %s",
			len(t.rows), lastSkip)
	}
	return best, bestAt, nil
}

// positive reads a named column that must be strictly positive.
//
// Every quantity published from LISIRD — a radio flux, a total irradiance, a
// core-to-wing ratio, a Lyman-alpha irradiance — is physically positive, so
// "not positive" and "not a measurement" are the same condition. That covers
// the tsis_tsi_6hr all-zero placeholder row without needing a declared
// sentinel, which LISIRD does not provide. Reporting a total solar irradiance
// of zero would be a claim that the Sun went out.
func (r lisirdRow) positive(column string) (float64, bool) {
	idx, ok := r.table.columns[column]
	if !ok || idx >= len(r.fields) {
		return 0, false
	}
	field := strings.TrimSpace(r.fields[idx])
	if field == "" {
		return 0, false
	}
	v, err := strconv.ParseFloat(field, 64)
	if err != nil || math.IsNaN(v) || math.IsInf(v, 0) || v <= 0 {
		return 0, false
	}
	return v, true
}

// ---------------------------------------------------------------------------
// Time conversion
// ---------------------------------------------------------------------------

// julianEpoch is JD 0.0: noon UT on 1 January 4713 BC in the Julian calendar.
// Rather than compute from that, the conversion is anchored on the Unix epoch,
// whose Julian Date is exactly 2440587.5.
const unixEpochJD = 2440587.5

// secondsPerDay is used by every "since an epoch" conversion.
const secondsPerDay = 86400

// parseLISIRDTime converts a time column value using the units the CSV header
// declared.
//
// There is one converter for every dataset rather than a per-dataset table
// because the units string is authoritative and self-describing, and a table
// keyed on dataset name would silently produce a date decades wrong the day
// LISIRD changes a dataset's encoding — which it has the right to do, since the
// header says what the encoding is. The four forms seen live are:
//
//	time (Julian Date)                       2461285.197
//	time (yyyy MM dd)                        2026 9 1   /   2026 09 01
//	time (days since 1947-01-01T12:00)       29098.0
//	time (milliseconds since 1970-01-01)     1757376000000
//
// Anything else is an error rather than a guess. A timestamp guessed wrong is
// worse than a missing one: it puts today's measurement at a point in the past
// or the future, and every sink here keys on the sample's own time.
func parseLISIRDTime(field, units string) (time.Time, error) {
	field = strings.TrimSpace(field)
	if field == "" {
		return time.Time{}, errors.New("time field is empty")
	}
	lower := strings.ToLower(strings.TrimSpace(units))

	switch {
	case lower == "":
		return time.Time{}, errors.New("the CSV header states no units for the time column")

	case lower == "julian date", lower == "jd":
		jd, err := strconv.ParseFloat(field, 64)
		if err != nil {
			return time.Time{}, fmt.Errorf("time %q is not a Julian Date", field)
		}
		return fromEpochDays(time.Unix(0, 0).UTC(), jd-unixEpochJD)

	case strings.HasPrefix(lower, "yyyy"):
		return parseFormattedDate(field, units)

	case strings.Contains(lower, " since "):
		return parseSinceEpoch(field, lower)
	}

	return time.Time{}, fmt.Errorf("unrecognised time units %q", units)
}

// parseFormattedDate handles a units string that is itself a date pattern, such
// as "yyyy MM dd".
//
// The fields are matched positionally rather than by translating the pattern
// into a Go layout, because the widths are not stable: the same "yyyy MM dd"
// pattern is written "2026 9 1" by the CLS datasets and "2026 09 01" by the
// Bremen one, and a fixed layout matches one and rejects the other.
func parseFormattedDate(field, units string) (time.Time, error) {
	pattern := strings.Fields(strings.ToLower(units))
	values := strings.Fields(field)
	if len(pattern) != len(values) {
		return time.Time{}, fmt.Errorf("time %q does not match the pattern %q", field, units)
	}

	var (
		year  = -1
		month = 1
		day   = 1
	)
	for i, p := range pattern {
		n, err := strconv.Atoi(values[i])
		if err != nil {
			return time.Time{}, fmt.Errorf("time %q has a non-numeric field %q", field, values[i])
		}
		switch p {
		case "yyyy", "yy":
			year = n
		case "mm":
			month = n
		case "dd":
			day = n
		default:
			return time.Time{}, fmt.Errorf("unrecognised date pattern field %q in %q", p, units)
		}
	}

	if year < 1600 || year > 3000 || month < 1 || month > 12 || day < 1 || day > 31 {
		return time.Time{}, fmt.Errorf("time %q is out of range", field)
	}
	return time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC), nil
}

// parseSinceEpoch handles "<unit> since <timestamp>", the form LATIS uses for
// every offset encoding.
func parseSinceEpoch(field, lowerUnits string) (time.Time, error) {
	unitName, epochText, ok := strings.Cut(lowerUnits, " since ")
	if !ok {
		return time.Time{}, fmt.Errorf("units %q are not of the form \"<unit> since <epoch>\"", lowerUnits)
	}

	epoch, err := parseEpochStamp(strings.TrimSpace(epochText))
	if err != nil {
		return time.Time{}, err
	}

	offset, err := strconv.ParseFloat(field, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("time %q is not a number", field)
	}

	switch strings.TrimSpace(unitName) {
	case "days", "day", "d":
		return fromEpochDays(epoch, offset)
	case "hours", "hour", "hr", "h":
		return fromEpochSeconds(epoch, offset*3600)
	case "minutes", "minute", "min":
		return fromEpochSeconds(epoch, offset*60)
	case "seconds", "second", "sec", "s":
		return fromEpochSeconds(epoch, offset)
	case "milliseconds", "millisecond", "ms", "msec":
		return fromEpochSeconds(epoch, offset/1e3)
	case "microseconds", "microsecond", "us", "usec":
		return fromEpochSeconds(epoch, offset/1e6)
	}
	return time.Time{}, fmt.Errorf("unrecognised time unit %q", unitName)
}

// parseEpochStamp reads the epoch out of a "since" units string. The layouts
// are those LATIS has been seen to write; the bare-date form is the common one
// and the "T12:00" form appears on the Lyman-alpha composite, whose epoch is
// deliberately at noon.
func parseEpochStamp(text string) (time.Time, error) {
	layouts := []string{
		"2006-01-02t15:04:05",
		"2006-01-02t15:04",
		"2006-01-02 15:04:05",
		"2006-01-02 15:04",
		"2006-01-02",
		"20060102",
	}
	for _, layout := range layouts {
		if t, err := time.ParseInLocation(layout, text, time.UTC); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("unrecognised epoch %q in the time units", text)
}

// fromEpochDays adds a fractional number of days to an epoch.
func fromEpochDays(epoch time.Time, days float64) (time.Time, error) {
	return fromEpochSeconds(epoch, days*secondsPerDay)
}

// fromEpochSeconds adds a fractional number of seconds to an epoch, rounding to
// the second.
//
// Rounding is safe and deliberate. These are daily and six-hourly datasets whose
// nominal times are midnight, noon or a six-hour boundary; the sub-second
// residue is float noise from the day arithmetic, and carrying it through would
// make otherwise identical timestamps compare unequal.
//
// The range check catches a units string that was misread — a Julian Date
// interpreted as days since 1970 lands in the year 8700 — rather than letting a
// nonsensical timestamp reach a sink.
func fromEpochSeconds(epoch time.Time, seconds float64) (time.Time, error) {
	if math.IsNaN(seconds) || math.IsInf(seconds, 0) {
		return time.Time{}, errors.New("time offset is not a finite number")
	}
	at := epoch.Add(time.Duration(seconds * float64(time.Second))).UTC().Round(time.Second)

	// Nothing LISIRD publishes predates the 1610 telescope or postdates now by
	// more than a little; the datasets are measurements, not forecasts.
	if at.Year() < 1600 || at.Year() > 2200 {
		return time.Time{}, fmt.Errorf("converted time %s is implausible; the units were probably misread",
			at.Format(time.RFC3339))
	}
	return at, nil
}
