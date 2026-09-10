// Package wsprlive polls wspr.live for aggregate WSPR reception activity: spot
// counts, distinct transmitters and receivers, mean reported SNR and mean
// great-circle distance, per band.
//
// # Licence
//
// The service's terms, verbatim:
//
//	You are allowed to use the services provided on wspr.live for your own
//	reasearch and projects, as long as the results are accessable free of
//	charge for everyone. You are not allowed to use this service for any
//	commercial or profit oriented use cases.
//
// (The spelling is the publisher's.) NON-COMMERCIAL ONLY, and conditional on
// the results being freely accessible. This source therefore defaults to
// disabled: an operator enabling it is accepting an obligation about how the
// numbers it publishes may be used and who may see them, and that is not
// something anybody should inherit from a default they did not read.
//
// Attribution is owed to wspr.live for the query service and to wsprnet.org,
// whose network of volunteer receivers produced every spot behind these
// numbers.
//
// # Interface
//
// wspr.live exposes a ClickHouse HTTP endpoint that takes SQL directly:
//
//	GET https://db1.wspr.live/?query=<url-encoded SQL>
//
// Their documentation is specific about the shape of a well-behaved query, and
// this package follows it exactly: always bound the range by time, always
// GROUP BY band, no JOINs. Only the query parameter is honoured — other
// parameters and most headers are stripped — and POST is not accepted, so the
// whole request is one GET with one parameter.
//
// One query returns the entire gauge set for every band, which is the reason
// this source is cheap enough to run at all: a per-band or per-station API
// would be seventeen requests where this is one, and the measured response
// time is a few milliseconds.
//
// Schema introspection is blocked upstream: SHOW TABLES returns an empty
// result and DESCRIBE TABLE returns 403. The column list is therefore
// hard-coded, and nothing in this package tries to discover it. A schema change
// will surface as a parse error naming the column, which is a better failure
// than an introspection request that quietly 403s on every poll.
//
// # The band column is not metres
//
// wspr.live's band column is the leading MHz digit or digits of the dial
// frequency, not a wavelength: 14 means 20 metres, 1 means 160 metres, -1 means
// LF. Publishing it raw would put band="14" next to band="1296" on a dashboard
// and mean 20 m and 23 cm respectively. bandLabels maps every documented code
// to its metre label; a code that is not in the map still produces a series,
// labelled with the raw number, because dropping activity on a band the
// network has just started reporting is worse than an unfamiliar label. Codes
// 13 and 40 have both been observed live and are in neither the documentation
// nor the map.
package wsprlive

import (
	"context"
	"encoding/json"
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
const Name = "wspr-live"

// network is the value of the "network" label on every sample here. It names
// the reporting network rather than the query service, so that a future
// second WSPR aggregator would land on the same series.
const network = "wspr"

const (
	// defaultBaseURL is the public ClickHouse endpoint.
	defaultBaseURL = "https://db1.wspr.live"

	// defaultInterval is five minutes. WSPR's transmission cycle is two
	// minutes, so five minutes covers two or three cycles per poll.
	defaultInterval = 5 * time.Minute

	// minInterval is the courtesy floor. wspr.live documents a limit of 20
	// requests per minute shared across every user of the service, so one
	// query every two minutes is far inside it — but two minutes is also the
	// WSPR transmission cycle, and no query can learn anything new inside one
	// cycle. The floor is enforced in the schedule rather than in validation:
	// a limit an operator can lower by editing a setting is not a limit.
	minInterval = 2 * time.Minute

	// defaultWindow is how far back each query aggregates. Fifteen minutes is
	// seven WSPR cycles: long enough that a quiet band still has spots in it,
	// short enough that "recent" means recent.
	defaultWindow = 15 * time.Minute

	// minWindow and maxWindow bound the configured window. A window under one
	// cycle can return zero rows for every band; a window of days makes the
	// upstream scan far more than it needs to for a number nobody would call
	// current.
	minWindow = 2 * time.Minute
	maxWindow = 6 * time.Hour

	// defaultRetries is the number of retries after the first attempt. The
	// query is a few milliseconds of work upstream, so one more try on a
	// transient failure is cheap; more than that starts to look like load.
	defaultRetries = 1

	// maxBodyBytes bounds the response. The live result for all bands is about
	// 1.5 KB; 256 KB is enormous headroom and still far too small to hurt us.
	maxBodyBytes = 256 << 10
)

// clickhouseTimeLayout is how ClickHouse renders a DateTime in JSON. There is
// no zone in the text; wspr.live stores UTC.
const clickhouseTimeLayout = "2006-01-02 15:04:05"

// bandLabels maps wspr.live's band code to a metre or centimetre label.
//
// Every entry is from wspr.live's own band list. The map is deliberately
// closed: see the package comment for what happens to a code that is not here.
var bandLabels = map[int]string{
	-1:   "LF",
	0:    "MF",
	1:    "160m",
	3:    "80m",
	5:    "60m",
	7:    "40m",
	10:   "30m",
	14:   "20m",
	18:   "17m",
	21:   "15m",
	24:   "12m",
	28:   "10m",
	50:   "6m",
	70:   "4m",
	144:  "2m",
	430:  "70cm",
	1296: "23cm",
}

// Source polls wspr.live.
type Source struct {
	base    string
	window  time.Duration
	retries int
	sched   source.Schedule
	client  *httpx.Client
	log     *slog.Logger

	// now is time.Now except in tests. Every published value's timestamp comes
	// from the upstream's own max(time); now is used only for Batch.Fetched and
	// for the data-age calculation.
	now func() time.Time
}

// Compile-time proof the source is both pollable and schedulable.
var (
	_ source.Source    = (*Source)(nil)
	_ source.Scheduled = (*Source)(nil)
)

// New builds the source from configuration.
//
// The client is passed in rather than constructed here because politeness has
// to be enforced across sources rather than within one: httpx already carries
// the per-host policy for db1.wspr.live, shared with anything else that ever
// fetches from it.
func New(cfg config.WSPRLive, client *httpx.Client, log *slog.Logger) (*Source, error) {
	if client == nil {
		return nil, errors.New("wsprlive: a shared httpx client is required")
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
		return nil, fmt.Errorf("wsprlive: base URL %q is not a URL: %w", redact.URL(base), err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("wsprlive: base URL must be http or https, got %q", u.Scheme)
	}
	if u.Host == "" {
		return nil, errors.New("wsprlive: base URL has no host")
	}

	interval := cfg.Interval
	if interval <= 0 {
		interval = defaultInterval
	}
	if interval < minInterval {
		log.Warn("wspr.live interval raised to the courtesy floor",
			"configured", interval, "using", minInterval,
			"reason", "the WSPR transmission cycle is two minutes; a faster poll cannot learn anything new")
	}

	window := cfg.Window
	if window <= 0 {
		window = defaultWindow
	}
	if window < minWindow {
		log.Warn("wspr.live window raised to one WSPR cycle",
			"configured", window, "using", minWindow)
		window = minWindow
	}
	if window > maxWindow {
		log.Warn("wspr.live window lowered; a longer range makes the upstream scan far more than it needs to",
			"configured", window, "using", maxWindow)
		window = maxWindow
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
		window:  window,
		retries: retries,
		// AtLeast clamps whatever the operator configured, so the floor holds
		// even though the configured interval is kept for the nominal cadence.
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

// Schedule polls on a fixed interval, floored at two minutes.
//
// A fixed interval is right here: WSPR spots arrive continuously, so there is
// no publication clock to align to. The floor is the whole substance of the
// schedule and is documented on minInterval.
func (s *Source) Schedule() source.Schedule { return s.sched }

// Window is the range each query aggregates over.
func (s *Source) Window() time.Duration { return s.window }

// query builds the aggregate SQL for the configured window.
//
// The shape follows wspr.live's documented guidance: bounded by time, grouped
// by band, no JOINs, ordered so the result is deterministic. max(time) is
// selected alongside the aggregates so that metric.SourceDataAge can be
// computed from the data's own newest timestamp rather than from receipt time;
// it costs nothing, since the rows are already being scanned.
func (s *Source) query() string {
	minutes := int64(s.window / time.Minute)
	if minutes < 1 {
		minutes = 1
	}
	return "SELECT band, count() AS spots, uniq(tx_sign) AS tx, uniq(rx_sign) AS rx, " +
		"round(avg(snr),2) AS avg_snr, round(avg(distance),1) AS avg_km, max(time) AS newest " +
		"FROM wspr.rx WHERE time > now() - INTERVAL " + strconv.FormatInt(minutes, 10) + " MINUTE " +
		"GROUP BY band ORDER BY band FORMAT JSONCompact"
}

// requestURL is the full GET, with the query as the only parameter. Anything
// else would be stripped by the upstream anyway.
func (s *Source) requestURL() string {
	return s.base + "/?query=" + url.QueryEscape(s.query())
}

// jsonCompact is ClickHouse's FORMAT JSONCompact envelope: named columns in
// meta, positional rows in data.
type jsonCompact struct {
	Meta []struct {
		Name string `json:"name"`
		Type string `json:"type"`
	} `json:"meta"`
	Data [][]json.RawMessage `json:"data"`
	Rows int                 `json:"rows"`
}

// columns are the names this package expects, in the order the query selects
// them. They are matched by name against meta rather than trusted by position,
// so a future column added upstream cannot silently shift the values.
var columns = []string{"band", "spots", "tx", "rx", "avg_snr", "avg_km", "newest"}

// Poll runs the query and converts the result into samples.
func (s *Source) Poll(ctx context.Context) (metric.Batch, error) {
	fetched := s.now().UTC()
	empty := metric.Batch{Source: Name, Fetched: fetched}

	resp, err := s.client.Get(ctx, httpx.Request{
		URL: s.requestURL(),
		// No conditional GET: the response is a fresh aggregate every time
		// and ClickHouse sends no validators, so If-None-Match would cost a
		// header and buy nothing.
		Retries: s.retries,
		MaxBody: maxBodyBytes,
	})
	switch {
	case errors.Is(err, httpx.ErrNotModified):
		return empty, source.ErrNotModified
	case errors.Is(err, httpx.ErrRateLimited):
		return empty, fmt.Errorf("%w: %w", source.ErrRateLimited, err)
	case err != nil:
		return empty, fmt.Errorf("wsprlive: query failed: %w", err)
	}

	samples, newest, err := s.parse(resp.Body)
	if err != nil {
		return empty, err
	}

	batch := metric.Batch{Source: Name, Fetched: fetched, Samples: samples}

	// Freshness from the payload's own newest spot time. HTTP 200 with stale
	// content is the dominant failure mode across every upstream in this
	// exporter, and a ClickHouse endpoint whose ingest has stalled answers a
	// perfectly well-formed empty-ish result.
	if !newest.IsZero() {
		age := fetched.Sub(newest).Seconds()
		if age < 0 {
			// Clock skew between us and the upstream. A negative age looks
			// like a broken exporter rather than a surprising publisher.
			age = 0
		}
		batch.Samples = append(batch.Samples, metric.Sample{
			Desc:   metric.SourceDataAge,
			Labels: []string{Name},
			Value:  age,
			// The age is a property of this moment, not of the observation.
			Time: fetched,
		})
	}

	return batch, nil
}

// parse reads a FORMAT JSONCompact result into samples, returning the newest
// spot time any row carried.
func (s *Source) parse(body []byte) ([]metric.Sample, time.Time, error) {
	var doc jsonCompact
	if err := json.Unmarshal(body, &doc); err != nil {
		// ClickHouse reports SQL errors as a plain-text body with a 200 in
		// some configurations, so an unmarshal failure is as likely to be a
		// rejected query as a corrupt response. Excerpting it is the only way
		// an operator finds out which.
		return nil, time.Time{}, fmt.Errorf("wsprlive: response is not JSONCompact: %w (body %s)",
			err, excerpt(string(body)))
	}

	index := make(map[string]int, len(doc.Meta))
	for i, m := range doc.Meta {
		index[m.Name] = i
	}
	for _, name := range columns {
		if _, ok := index[name]; !ok {
			return nil, time.Time{}, fmt.Errorf("wsprlive: response is missing column %q; "+
				"the wspr.rx schema may have changed (columns are hard-coded because introspection is blocked upstream)", name)
		}
	}

	var (
		out    []metric.Sample
		newest time.Time
	)

	for rowNum, row := range doc.Data {
		fields := make(map[string]json.RawMessage, len(columns))
		ok := true
		for _, name := range columns {
			i := index[name]
			if i >= len(row) {
				s.log.Warn("wspr.live row has fewer values than columns; skipping it",
					"row", rowNum, "values", len(row), "columns", len(doc.Meta))
				ok = false
				break
			}
			fields[name] = row[i]
		}
		if !ok {
			continue
		}

		code, err := decodeInt(fields["band"])
		if err != nil {
			s.log.Warn("wspr.live row has an unreadable band code; skipping it",
				"row", rowNum, "error", err)
			continue
		}
		band := bandLabel(code)
		if _, known := bandLabels[code]; !known {
			// Debug rather than warn: an unknown code is interesting to
			// somebody debugging a dashboard, but it is not an operational
			// problem and it happens whenever wspr.live adds a band.
			s.log.Debug("wspr.live reported a band code with no metre label; publishing the raw code",
				"code", code, "label", band)
		}

		// The row's own newest spot time is the honest timestamp for every
		// value derived from it.
		when, err := decodeTime(fields["newest"])
		if err != nil {
			s.log.Warn("wspr.live row has an unreadable timestamp; skipping it",
				"row", rowNum, "band", band, "error", err)
			continue
		}
		if when.After(newest) {
			newest = when
		}

		spots, err := decodeFloat(fields["spots"])
		if err != nil {
			s.log.Warn("wspr.live row has an unreadable spot count; skipping it",
				"row", rowNum, "band", band, "error", err)
			continue
		}

		out = s.emit(out, metric.Sample{
			Desc: metric.SpotCount, Labels: []string{network, band},
			Value: spots, Time: when,
		})

		// Spots per minute, derived rather than queried. The window is known
		// exactly and the count is over it, so the rate is arithmetic; asking
		// the upstream for it would be a second query for the same rows.
		if minutes := s.window.Minutes(); minutes > 0 {
			out = s.emit(out, metric.Sample{
				Desc: metric.SpotRate, Labels: []string{network, band},
				Value: spots / minutes, Time: when,
			})
		}

		// SNR and distance are means, and a mean over zero rows is not a
		// number. ClickHouse returns 0 rather than null for avg() over an
		// empty group, but a group cannot be empty here — it exists because it
		// had rows — so these are published as they arrive. A null is treated
		// as "not measured" and skipped rather than published as zero.
		if v, err := decodeFloat(fields["avg_snr"]); err == nil {
			out = s.emit(out, metric.Sample{
				Desc: metric.SpotSNRMean, Labels: []string{network, band},
				Value: v, Time: when,
			})
		} else if !isNull(fields["avg_snr"]) {
			s.log.Warn("wspr.live row has an unreadable mean SNR", "band", band, "error", err)
		}

		if v, err := decodeFloat(fields["avg_km"]); err == nil {
			out = s.emit(out, metric.Sample{
				Desc: metric.SpotDistanceMean, Labels: []string{network, band},
				Value: v, Time: when,
			})
		} else if !isNull(fields["avg_km"]) {
			s.log.Warn("wspr.live row has an unreadable mean distance", "band", band, "error", err)
		}

		for _, role := range [...]struct {
			label  string
			column string
		}{{"tx", "tx"}, {"rx", "rx"}} {
			v, err := decodeFloat(fields[role.column])
			if err != nil {
				s.log.Warn("wspr.live row has an unreadable station count",
					"band", band, "role", role.label, "error", err)
				continue
			}
			out = s.emit(out, metric.Sample{
				Desc: metric.SpotStationCount, Labels: []string{network, band, role.label},
				Value: v, Time: when,
			})
		}
	}

	return out, newest, nil
}

// bandLabel maps a wspr.live band code to a label, falling back to the raw
// code. See the package comment for why the fallback publishes rather than
// drops.
func bandLabel(code int) string {
	if label, ok := bandLabels[code]; ok {
		return label
	}
	return "band-" + strconv.Itoa(code)
}

// emit appends a sample after checking it against its descriptor. A sample
// whose label count does not match is rejected by Prometheus at collection
// time and silently corrupts InfluxDB tag sets, so it is caught here.
func (s *Source) emit(out []metric.Sample, sample metric.Sample) []metric.Sample {
	if err := sample.Validate(); err != nil {
		s.log.Warn("wsprlive discarded an invalid sample", "error", err)
		return out
	}
	return append(out, sample)
}

// isNull reports whether a JSON value is literally null.
func isNull(raw json.RawMessage) bool {
	return string(raw) == "null"
}

// decodeInt reads a ClickHouse integer, which JSONCompact may render as a
// number or, for the 64-bit types under some settings, as a quoted string.
func decodeInt(raw json.RawMessage) (int, error) {
	f, err := decodeFloat(raw)
	if err != nil {
		return 0, err
	}
	return int(f), nil
}

// decodeFloat reads a number that may be quoted.
func decodeFloat(raw json.RawMessage) (float64, error) {
	if len(raw) == 0 || isNull(raw) {
		return 0, errors.New("value is null")
	}
	var n json.Number
	if err := json.Unmarshal(raw, &n); err == nil {
		return n.Float64()
	}
	var str string
	if err := json.Unmarshal(raw, &str); err != nil {
		return 0, fmt.Errorf("value %s is neither a number nor a string", excerpt(string(raw)))
	}
	return strconv.ParseFloat(strings.TrimSpace(str), 64)
}

// decodeTime reads a ClickHouse DateTime, which arrives as a quoted string
// with no zone. wspr.live stores UTC.
func decodeTime(raw json.RawMessage) (time.Time, error) {
	if len(raw) == 0 || isNull(raw) {
		return time.Time{}, errors.New("timestamp is null")
	}
	var str string
	if err := json.Unmarshal(raw, &str); err != nil {
		return time.Time{}, fmt.Errorf("timestamp %s is not a string", excerpt(string(raw)))
	}
	str = strings.TrimSpace(str)
	t, err := time.Parse(clickhouseTimeLayout, str)
	if err != nil {
		return time.Time{}, fmt.Errorf("timestamp %q is not %q", str, clickhouseTimeLayout)
	}
	return t.UTC(), nil
}

// bodyExcerptRunes bounds how much of a response body reaches an error
// message, since that message is destined for a log.
const bodyExcerptRunes = 160

// excerpt trims remote input down to something loggable, quoted rather than
// interpolated raw.
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
