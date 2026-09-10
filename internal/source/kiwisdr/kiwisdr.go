// Package kiwisdr polls KiwiSDR receivers for their measured HF noise floor.
//
// A KiwiSDR continuously computes a power distribution across the HF spectrum
// and exposes it at /snr as a rolling series of hourly integrations. The median
// received power in each sub-band, p50, is the actual noise floor at that
// antenna, in dBm. Nobody publishes that number: regulators model man-made
// noise, ITU-R P.372 tabulates it by environment category, and neither tells an
// operator what their own site is doing this afternoon. This is a measurement.
//
// # Etiquette
//
// Point this at your own receiver.
//
// The /snr endpoint is unauthenticated on every public KiwiSDR, and the public
// receiver list is a few hundred entries long, so nothing technical stops this
// source being aimed at somebody else's machine. That does not make it
// acceptable. These are private receivers — a Beaglebone and an antenna in
// somebody's garden, often on a domestic connection — that their owners chose
// to share for people to listen through. Adding an unannounced automated
// client to one, permanently, on a timer, is not what they agreed to, and a
// KiwiSDR has few enough resources that it is not free either. Ask first, or
// use your own.
//
// httpx's policy table lists no KiwiSDR host, so any receiver falls under the
// conservative fallback policy — thirty seconds of spacing, burst of one —
// which is the right default for a machine nobody has thought about. The
// intended targets are localhost and LAN addresses, where the receiver is
// yours and the request never leaves the building.
//
// # Reading /snr
//
// The response is served as text/plain but contains JSON: an array of
// integration records, one per hour, each with a timestamp and six sub-band
// measurements.
//
//	{"ts":"Mon Sep  7 20:12:42 2026","seq":0,"utc":0,"imin":60,"ant":0,
//	 "snr":[{"lo":0,"hi":30000,"min":-110,"max":-39,
//	         "p50":-96,"p95":-59,"snr":37}, ...]}
//
// lo and hi are kHz. imin is the integration length in minutes, sixty in every
// observation so far, and the array is a rolling window — sixty records, one
// per hour, wrapping — so the newest record is taken by timestamp rather than
// by position. seq increments and wraps, so it is not a reliable ordering key
// across a restart.
//
// # Timestamps
//
// ts uses the C asctime format, which is Go's time.ANSIC:
// "Mon Jan _2 15:04:05 2006". Note the day is space-padded, so a single-digit
// day produces two spaces after the month — "Mon Sep  7" against
// "Thu Sep 10". A layout with a literal single space fails on half the month.
//
// It is parsed as UTC. The record's utc field reports the receiver's time
// basis, and a receiver reporting utc:0 has stamped local time with no offset
// or zone name anywhere in the response — so there is nothing to convert with,
// and assuming UTC is the only option available. That is recorded at debug
// rather than hidden, because it means SourceDataAge for such a receiver
// carries the receiver's UTC offset as an error term. On a correctly
// configured receiver, which is the intended target, it is zero.
//
// # Resilience
//
// Each receiver is an independent, optional target, which is unlike every
// other source here. One being down — rebooted, on a flaky domestic uplink,
// unplugged — must not lose the others. Receivers are fetched concurrently
// with a small bound, a failed receiver contributes no samples and logs at
// debug, and the poll fails only when every configured receiver failed.
package kiwisdr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/url"
	"sort"
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

// Name identifies this source in logs, metrics and health output.
const Name = "kiwisdr"

const (
	// pathSNR is the noise-floor endpoint.
	pathSNR = "/snr"

	// defaultInterval is thirty minutes against hourly data. Each record is a
	// sixty-minute integration, so polling faster than the integration length
	// returns the same numbers again; half the integration length keeps the
	// staleness bounded without asking for anything new twice.
	defaultInterval = 30 * time.Minute

	// defaultTimeout is short because a KiwiSDR is a small machine. Waiting
	// thirty seconds on a receiver that is busy serving audio to eight
	// listeners is worse for the receiver than giving up.
	defaultTimeout = 10 * time.Second

	// maxConcurrent bounds how many receivers are in flight at once. A handful
	// of small machines does not need more, and an unbounded fan-out across a
	// long receiver list would be exactly the impolite behaviour the package
	// comment argues against.
	maxConcurrent = 4

	// maxBodyBytes bounds a response. A full sixty-record series measured
	// about 31 KB.
	maxBodyBytes = 1 << 20

	// tsLayout is the C asctime format the receiver stamps records with. It is
	// time.ANSIC, restated here so the space-padded day is visible at the
	// point of use.
	tsLayout = time.ANSIC
)

// Source polls one or more KiwiSDR receivers.
type Source struct {
	receivers []receiver
	client    *httpx.Client
	interval  time.Duration
	timeout   time.Duration
	retries   int
	log       *slog.Logger

	now func() time.Time
}

// receiver is one configured target.
type receiver struct {
	name    string
	baseURL string
}

var _ source.Source = (*Source)(nil)

// New builds a source from validated configuration.
//
// The HTTP client is the shared one. That matters even here, where the targets
// are usually private: the fallback host policy is what keeps a misconfigured
// interval from turning into a request every second against somebody's
// Beaglebone.
func New(cfg config.KiwiSDR, client *httpx.Client, log *slog.Logger) (*Source, error) {
	if client == nil {
		return nil, errors.New("kiwisdr: nil HTTP client")
	}
	if log == nil {
		log = slog.Default()
	}
	if len(cfg.Receivers) == 0 {
		return nil, errors.New("kiwisdr: no receivers configured; " +
			"set at least one as name=http://host:8073, ideally your own")
	}

	names := make([]string, 0, len(cfg.Receivers))
	for name := range cfg.Receivers {
		names = append(names, name)
	}
	// Sorted so the batch order, and the order receivers are dispatched in, is
	// stable across restarts rather than following Go's randomised map order.
	sort.Strings(names)

	receivers := make([]receiver, 0, len(names))
	for _, name := range names {
		trimmed := strings.TrimSpace(name)
		if trimmed == "" {
			return nil, errors.New("kiwisdr: a receiver has an empty name")
		}

		base := strings.TrimRight(strings.TrimSpace(cfg.Receivers[name]), "/")
		if base == "" {
			return nil, fmt.Errorf("kiwisdr: receiver %q has no base URL", trimmed)
		}
		u, err := url.Parse(base)
		if err != nil {
			return nil, fmt.Errorf("kiwisdr: receiver %q base URL is not a URL: %w",
				trimmed, err)
		}
		if u.Scheme != "http" && u.Scheme != "https" {
			return nil, fmt.Errorf("kiwisdr: receiver %q base URL must be http or https, got %q",
				trimmed, u.Scheme)
		}
		if u.Host == "" {
			return nil, fmt.Errorf("kiwisdr: receiver %q base URL has no host", trimmed)
		}
		receivers = append(receivers, receiver{name: trimmed, baseURL: base})
	}

	interval := cfg.Interval
	if interval <= 0 {
		interval = defaultInterval
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	retries := cfg.Retries
	if retries < 0 {
		retries = 0
	}

	return &Source{
		receivers: receivers,
		client:    client,
		interval:  interval,
		timeout:   timeout,
		retries:   retries,
		log:       log.With("source", Name),
		now:       time.Now,
	}, nil
}

// Name identifies the source.
func (s *Source) Name() string { return Name }

// Interval is the nominal spacing between polls.
func (s *Source) Interval() time.Duration { return s.interval }

// Schedule polls on a fixed interval. A receiver's integrations complete on
// its own clock, which is not exposed, so there is no publication time to
// align to.
func (s *Source) Schedule() source.Schedule { return source.Every(s.interval) }

// Poll fetches every receiver concurrently and returns whatever answered.
//
// A receiver that failed contributes no samples and does not fail the batch.
// The poll returns an error only when every receiver failed, which distinguishes
// "one small machine rebooted" from "the network or the configuration is
// wrong".
func (s *Source) Poll(ctx context.Context) (metric.Batch, error) {
	now := s.now().UTC()
	batch := metric.Batch{Source: Name, Fetched: now}

	if err := ctx.Err(); err != nil {
		return batch, fmt.Errorf("kiwisdr: poll cancelled: %w", err)
	}

	type result struct {
		samples []metric.Sample
		newest  time.Time
		err     error
	}
	results := make([]result, len(s.receivers))

	var (
		wg   sync.WaitGroup
		gate = make(chan struct{}, min(maxConcurrent, len(s.receivers)))
	)
	for i, rx := range s.receivers {
		wg.Add(1)
		go func(i int, rx receiver) {
			defer wg.Done()

			select {
			case gate <- struct{}{}:
			case <-ctx.Done():
				results[i].err = ctx.Err()
				return
			}
			defer func() { <-gate }()

			samples, newest, err := s.pollReceiver(ctx, rx, now)
			results[i] = result{samples: samples, newest: newest, err: err}
		}(i, rx)
	}
	wg.Wait()

	var (
		samples   []metric.Sample
		newest    time.Time
		errs      []error
		succeeded int
	)
	for i, r := range results {
		if r.err != nil {
			// Debug, not warn: a shared receiver being unreachable is the
			// expected steady state for an optional target, and a warning per
			// poll per receiver would bury everything else in the log.
			s.log.Debug("kiwisdr receiver did not answer",
				"receiver", s.receivers[i].name, "error", r.err)
			errs = append(errs, fmt.Errorf("%s: %w", s.receivers[i].name, r.err))
			continue
		}
		succeeded++
		samples = append(samples, r.samples...)
		if r.newest.After(newest) {
			newest = r.newest
		}
	}

	if !newest.IsZero() {
		samples = s.emit(samples, metric.Sample{
			Desc:   metric.SourceDataAge,
			Labels: []string{Name},
			Value:  now.Sub(newest).Seconds(),
			Time:   now,
		})
	}
	batch.Samples = samples

	if succeeded > 0 {
		return batch, nil
	}
	joined := errors.Join(errs...)
	if joined == nil {
		joined = errors.New("no receivers were polled")
	}
	if errors.Is(joined, httpx.ErrRateLimited) {
		return metric.Batch{Source: Name, Fetched: now},
			fmt.Errorf("%w: %w", source.ErrRateLimited, joined)
	}
	return metric.Batch{Source: Name, Fetched: now},
		fmt.Errorf("kiwisdr: every configured receiver failed: %w", joined)
}

// pollReceiver fetches and converts one receiver's /snr document.
func (s *Source) pollReceiver(ctx context.Context, rx receiver, now time.Time) ([]metric.Sample, time.Time, error) {
	endpoint := rx.baseURL + pathSNR

	// The timeout is per receiver rather than per poll: one slow machine must
	// not eat the budget of the others.
	reqCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	resp, err := s.client.Get(reqCtx, httpx.Request{
		URL:     endpoint,
		Retries: s.retries,
		MaxBody: maxBodyBytes,
	})
	if err != nil {
		return nil, time.Time{}, err
	}
	if len(resp.Body) == 0 {
		return nil, time.Time{}, fmt.Errorf("%s returned an empty body", redact.URL(endpoint))
	}

	// Served as text/plain, parsed as JSON. The content type is the receiver's
	// choice and is not worth failing over.
	var records []snrRecord
	if err := json.Unmarshal(resp.Body, &records); err != nil {
		return nil, time.Time{}, fmt.Errorf("%s returned unparseable JSON: %w",
			redact.URL(endpoint), err)
	}
	if len(records) == 0 {
		return nil, time.Time{}, fmt.Errorf("%s returned no integration records",
			redact.URL(endpoint))
	}

	newest, at, ok := newestRecord(records)
	if !ok {
		return nil, time.Time{}, fmt.Errorf("%s returned no record with a readable timestamp",
			redact.URL(endpoint))
	}
	if newest.UTC == 0 {
		s.log.Debug("kiwisdr receiver stamps records in local time; ages carry its UTC offset",
			"receiver", rx.name, "ts", newest.TS)
	}

	var out []metric.Sample
	for _, band := range newest.SNR {
		label, ok := bandLabel(band.Lo, band.Hi)
		if !ok {
			s.log.Debug("kiwisdr skipped a sub-band with no usable frequency range",
				"receiver", rx.name, "lo", band.Lo, "hi", band.Hi)
			continue
		}

		// p50 and p95 are dBm and are legitimately negative; snr is a
		// difference in dB. None of them has a null sentinel in any observed
		// response, but a receiver mid-startup can report a band with no
		// samples, so each is checked for being a real number.
		if usable(band.P50) {
			out = s.emit(out, metric.Sample{
				Desc:   metric.NoiseFloor,
				Labels: []string{rx.name, label},
				Value:  band.P50,
				Time:   at,
			})
		}
		if usable(band.P95) {
			out = s.emit(out, metric.Sample{
				Desc:   metric.NoiseFloorPeak,
				Labels: []string{rx.name, label},
				Value:  band.P95,
				Time:   at,
			})
		}
		if usable(band.SNR) {
			out = s.emit(out, metric.Sample{
				Desc:   metric.BandSignalToNoise,
				Labels: []string{rx.name, label},
				Value:  band.SNR,
				Time:   at,
			})
		}
	}
	if len(out) == 0 {
		return nil, time.Time{}, fmt.Errorf("%s returned no usable sub-bands",
			redact.URL(endpoint))
	}
	return out, at, nil
}

// newestRecord returns the record with the latest timestamp.
//
// The array is a rolling hourly window, so the newest record is somewhere in
// the middle once seq has wrapped. seq itself wraps and resets on a receiver
// restart, so it cannot be used for ordering; the timestamp can. Ties resolve
// to the later position, since a receiver that stamped two records in the same
// second wrote the later one second.
func newestRecord(records []snrRecord) (snrRecord, time.Time, bool) {
	var (
		best   snrRecord
		bestAt time.Time
		found  bool
	)
	for _, r := range records {
		at, err := parseTS(r.TS)
		if err != nil {
			continue
		}
		if found && at.Before(bestAt) {
			continue
		}
		best, bestAt, found = r, at, true
	}
	return best, bestAt, found
}

// parseTS reads a record timestamp as UTC. See the package comment for why UTC
// is the only available choice and what it costs on a receiver set to local
// time.
func parseTS(ts string) (time.Time, error) {
	trimmed := strings.TrimSpace(ts)
	if trimmed == "" {
		return time.Time{}, errors.New("empty timestamp")
	}
	// ParseInLocation with time.UTC rather than Parse, so the result is
	// explicitly UTC rather than whatever the exporter's host is set to.
	return time.ParseInLocation(tsLayout, trimmed, time.UTC)
}

// bandLabel derives the band label from a sub-band's kHz bounds.
//
// The scheme is "<lo>_<hi>" in kHz, printed as plain integers: 0_1800,
// 1800_10000, 10000_20000, 20000_30000, 1800_30000, 0_30000. It is stable
// across restarts and across receivers because it is a pure function of the
// two numbers the receiver reports, with no lookup table, no ordering
// assumption and no index into the array. That last part matters: KiwiSDR
// firmware has changed the order and the count of these entries before, so a
// label derived from position — band_0, band_1 — would silently re-point every
// series at a different slice of spectrum after a firmware update.
//
// Human band names are deliberately not used. The sub-bands are not amateur
// bands: 1800-10000 kHz spans 160 m through 30 m, and calling it "lower HF"
// would invent a boundary the receiver did not measure.
func bandLabel(lo, hi float64) (string, bool) {
	if !usable(lo) || !usable(hi) {
		return "", false
	}
	if hi <= lo {
		return "", false
	}
	return formatKHz(lo) + "_" + formatKHz(hi), true
}

// formatKHz prints a kHz bound without a decimal point where it has no
// fractional part, which is every observed case. A fractional bound would
// otherwise become "1800.5" rather than being silently truncated into a label
// that collides with its neighbour.
func formatKHz(v float64) string {
	if v == math.Trunc(v) && math.Abs(v) < 1e15 {
		return strconv.FormatInt(int64(v), 10)
	}
	return strconv.FormatFloat(v, 'f', -1, 64)
}

// usable reports whether a decoded number is real.
func usable(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }

// emit appends a sample after checking it against its descriptor.
func (s *Source) emit(out []metric.Sample, sample metric.Sample) []metric.Sample {
	if err := sample.Validate(); err != nil {
		s.log.Warn("kiwisdr discarded an invalid sample", "error", err)
		return out
	}
	return append(out, sample)
}

// ---------------------------------------------------------------------------
// Response shapes
// ---------------------------------------------------------------------------

// snrRecord is one integration period.
type snrRecord struct {
	TS  string `json:"ts"`
	Seq int    `json:"seq"`

	// UTC reports the receiver's time basis for TS. Zero means it is not UTC.
	UTC int `json:"utc"`

	// IMin is the integration length in minutes.
	IMin int `json:"imin"`

	// Ant is the antenna the measurement was taken on, for receivers with a
	// switch. It is not published as a label: it is an index into a
	// receiver-local antenna list this source has no way to name.
	Ant int `json:"ant"`

	SNR []snrBand `json:"snr"`
}

// snrBand is one sub-band's power distribution. All powers are dBm; SNR is a
// dB difference between P95 and P50.
type snrBand struct {
	Lo  float64 `json:"lo"`
	Hi  float64 `json:"hi"`
	Min float64 `json:"min"`
	Max float64 `json:"max"`
	P50 float64 `json:"p50"`
	P95 float64 `json:"p95"`
	SNR float64 `json:"snr"`
}
