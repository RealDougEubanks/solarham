// Package usgsgeomag polls the USGS Geomagnetism Program's web service for
// ground magnetometer data: the raw field components at each observatory, the
// server-computed rate of change of the field, and an hourly peak-to-peak
// horizontal range derived here from the component series.
//
// # What this service does and does not offer
//
// The service is documented at /ws/docs with an OpenAPI description at
// /ws/openapi.json. Three endpoints matter:
//
//	/ws/observatories/    station metadata, forty GeoJSON features
//	/ws/data/             raw or adjusted field components, in nanotesla
//	/ws/algorithms/dbdt/  rate of change of the field, in nT/min
//
// There is NO USGS Kp or Dst endpoint. This is worth stating plainly because
// "the USGS Kp/Dst API" is widely cited and does not exist: as verified
// against the live service, both /ws/algorithms/dst/ and /ws/algorithms/
// return 404, and the OpenAPI document lists no such path. Kp comes from GFZ
// Potsdam and Dst from the Kyoto WDC; nobody should waste an afternoon looking
// for them here. dbdt is the only algorithm endpoint that responds.
//
// # Observatory selection
//
// cfg.Observatories takes IAGA codes. The service lists forty, but that list
// is not forty useful observatories. Ten of them are test channels duplicating
// a real station at the same coordinates — BDT and TST for Boulder, BRT for
// Barrow, CMT for College, DHT for Deadhorse, FDT for Fredericksburg, GUT for
// Guam, HOT for Honolulu, SJT for San Juan — plus a bare "USGS" entry with no
// geometry at all. Fifteen more are Natural Resources Canada and INTERMAGNET
// partner stations relayed through this service, several of which report zero
// altitude and are better fetched from their own operators.
//
// The default is therefore a curated five: BOU, FRD, NEW, TUC, SIT. That is
// one observatory per broad region of the continental US plus Alaska, which is
// what a propagation dashboard can actually use. Polling all forty would be
// eighty requests per poll for roughly 320 series, most of them duplicates or
// stations nobody asked about, and cardinality spent on a test channel is
// cardinality not spent on something real. An operator who wants more can list
// more.
//
// # Freshness
//
// The trailing edge of every series is null: the newest minute or four have
// not been processed yet. A null is a missing observation, not a zero, and
// zeroing it would publish a 0 nT field at Boulder. Nulls are skipped and the
// newest non-null value is taken, with the timestamp that value actually
// carries, so SourceDataAge and StationDataAge report the real latency rather
// than the request time.
//
// # Licence
//
// Work of the US Government, in the public domain. Attribute the USGS
// Geomagnetism Program.
package usgsgeomag

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/url"
	"sort"
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
const Name = "usgs-geomag"

const (
	// defaultBaseURL is the public service.
	defaultBaseURL = "https://geomag.usgs.gov"

	// pathDbDt is the rate-of-change algorithm. The service differentiates the
	// series itself, which is better than doing it here: it has the full
	// one-second stream and knows which samples are flagged.
	pathDbDt = "/ws/algorithms/dbdt/"

	// pathData is the raw field.
	pathData = "/ws/data/"

	// pathObservatories is station metadata, fetched once at first poll.
	pathObservatories = "/ws/observatories/"

	// defaultInterval matches what a faster poll would gain, which is nothing:
	// responses carry cache-control max-age=60 and the trailing edge of the
	// series lags several minutes regardless.
	defaultInterval = 5 * time.Minute

	// defaultTimeout bounds one request.
	defaultTimeout = 30 * time.Second

	// rateWindow is the trailing window requested from dbdt. Only the newest
	// usable value is published, so this needs to be just long enough to clear
	// the unprocessed trailing nulls — four to five minutes of them have been
	// observed. Requesting a day would return 1,440 points to use one.
	rateWindow = 15 * time.Minute

	// rangeWindow is the trailing window requested from /ws/data/. The hourly
	// peak-to-peak horizontal range needs a full hour of samples, plus a
	// margin for the trailing nulls, so this window is necessarily longer than
	// rateWindow. It is still a window rather than a day.
	rangeWindow = 65 * time.Minute

	// hourlyRange is the span the peak-to-peak range is computed over.
	hourlyRange = time.Hour

	// samplingPeriod is one-minute data. The service also offers one-second,
	// which is sixty times the bytes to answer the same question.
	samplingPeriod = "60"

	// dataType selects variation data — the reported field, as recorded. The
	// alternative, "adjusted", is a later corrected product and is not
	// available in real time.
	dataType = "variation"

	// maxBodyBytes bounds a response. A 65-minute four-component series is
	// about 7 KB and the observatory list about 11 KB, so this is generous.
	maxBodyBytes = 1 << 20
)

// defaultObservatories is the curated set described in the package comment.
var defaultObservatories = []string{"BOU", "FRD", "NEW", "TUC", "SIT"}

// testChannels are the codes on /ws/observatories/ that duplicate a real
// station for instrument testing, or carry no station at all. Configuring one
// is almost certainly a mistake, so it earns a warning rather than silence.
var testChannels = map[string]struct{}{
	"BDT":  {},
	"BRT":  {},
	"CMT":  {},
	"DHT":  {},
	"FDT":  {},
	"GUT":  {},
	"HOT":  {},
	"SJT":  {},
	"TST":  {},
	"USGS": {},
}

// publishedComponents are the field components published, in a fixed order so
// the sample list is deterministic. F is deliberately omitted: it is the
// scalar total intensity, derivable from the other three, and publishing it
// adds a series that carries no independent information.
var publishedComponents = []string{"X", "Y", "Z"}

// Source polls the USGS geomagnetism web service.
type Source struct {
	baseURL       string
	client        *httpx.Client
	observatories []string
	interval      time.Duration
	timeout       time.Duration
	retries       int
	log           *slog.Logger

	// now is injectable so the age calculations, which are the reason this
	// source exists, can be tested against a fixed fixture.
	now func() time.Time

	// metaMu guards the observatory name table. The table is fetched at most
	// once per process on success; a failure leaves it unfetched so a later
	// poll retries, because losing station names permanently over one bad
	// afternoon is worse than one extra request.
	metaMu     sync.Mutex
	metaLoaded bool
	names      map[string]string
}

var _ source.Source = (*Source)(nil)

// New builds a source from validated configuration.
//
// The HTTP client is the shared one: per-host request spacing has to be shared
// across sources or it is not spacing at all. Its timeout is process-wide, so
// cfg.Timeout is applied here as a per-poll context deadline instead.
func New(cfg config.USGSGeomag, client *httpx.Client, log *slog.Logger) (*Source, error) {
	if client == nil {
		return nil, errors.New("usgsgeomag: nil HTTP client")
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
		return nil, fmt.Errorf("usgsgeomag: base URL %q is not a URL: %w", redact.URL(base), err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("usgsgeomag: base URL must be http or https, got %q", u.Scheme)
	}
	if u.Host == "" {
		return nil, errors.New("usgsgeomag: base URL has no host")
	}

	codes := normaliseCodes(cfg.Observatories)
	if len(codes) == 0 {
		codes = append([]string(nil), defaultObservatories...)
	}
	for _, code := range codes {
		if _, bad := testChannels[code]; bad {
			log.Warn("usgs-geomag observatory is a test channel, not a real station",
				"observatory", code)
		}
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
		baseURL:       base,
		client:        client,
		observatories: codes,
		interval:      interval,
		timeout:       timeout,
		retries:       retries,
		log:           log.With("source", Name),
		now:           time.Now,
		names:         make(map[string]string),
	}, nil
}

// normaliseCodes upper-cases, trims and de-duplicates IAGA codes while keeping
// the configured order.
func normaliseCodes(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, code := range in {
		code = strings.ToUpper(strings.TrimSpace(code))
		if code == "" {
			continue
		}
		if _, dup := seen[code]; dup {
			continue
		}
		seen[code] = struct{}{}
		out = append(out, code)
	}
	return out
}

// Name identifies the source.
func (s *Source) Name() string { return Name }

// Interval is the nominal spacing between polls.
func (s *Source) Interval() time.Duration { return s.interval }

// Schedule polls on a fixed interval. The service has no publication clock to
// align to; it streams continuously and republishes with a minute of caching.
func (s *Source) Schedule() source.Schedule { return source.Every(s.interval) }

// Poll fetches every configured observatory and converts what comes back into
// samples.
//
// Observatories are fetched one after another rather than concurrently. The
// shared client's spacing for this host would serialise them anyway, and a
// handful of sequential requests inside a five-minute interval is not a
// latency problem worth solving with goroutines.
//
// One observatory failing does not lose the others: a magnetometer being down
// for maintenance is an ordinary Tuesday. The poll fails only when every
// observatory failed, which means the service or the network is the problem.
func (s *Source) Poll(ctx context.Context) (metric.Batch, error) {
	now := s.now().UTC()
	batch := metric.Batch{Source: Name, Fetched: now}

	if err := ctx.Err(); err != nil {
		return batch, fmt.Errorf("usgsgeomag: poll cancelled: %w", err)
	}

	// Station metadata is fetched at most once, not on the poll interval. It
	// is 11 KB describing forty stations whose names and coordinates have not
	// changed in decades; re-fetching it every five minutes forever would be
	// pure waste.
	s.ensureObservatories(ctx)

	var (
		errs        []error
		newest      time.Time
		succeeded   int
		allSamples  []metric.Sample
		observatory = s.observatories
	)

	for _, code := range observatory {
		samples, stationNewest, err := s.pollObservatory(ctx, code, now)
		if err != nil {
			// A cancelled context is not the observatory's fault and every
			// remaining one will fail the same way, so stop rather than
			// hammering out the rest of the list.
			if ctx.Err() != nil {
				errs = append(errs, err)
				break
			}
			s.log.Warn("usgs-geomag observatory failed", "observatory", code, "error", err)
			errs = append(errs, err)
			continue
		}
		succeeded++
		allSamples = append(allSamples, samples...)

		if !stationNewest.IsZero() {
			if stationNewest.After(newest) {
				newest = stationNewest
			}
			allSamples = s.emit(allSamples, metric.Sample{
				Desc:   metric.StationDataAge,
				Labels: []string{Name, code},
				Value:  now.Sub(stationNewest).Seconds(),
				Time:   now,
			})
		}
	}

	if !newest.IsZero() {
		allSamples = s.emit(allSamples, metric.Sample{
			Desc:   metric.SourceDataAge,
			Labels: []string{Name},
			Value:  now.Sub(newest).Seconds(),
			Time:   now,
		})
	}
	batch.Samples = allSamples

	joined := errors.Join(errs...)
	if joined == nil {
		return batch, nil
	}

	// A rate limit is reported upwards even when something was salvaged, so
	// the scheduler stops knocking on a door that has just been closed.
	if errors.Is(joined, httpx.ErrRateLimited) {
		return metric.Batch{Source: Name, Fetched: now},
			fmt.Errorf("%w: %v", source.ErrRateLimited, joined)
	}
	if succeeded > 0 {
		s.log.Warn("usgs-geomag partially succeeded; publishing what was fetched",
			"observatories", succeeded, "of", len(observatory), "samples", len(batch.Samples))
		return batch, nil
	}
	return metric.Batch{Source: Name, Fetched: now},
		fmt.Errorf("usgsgeomag: every observatory failed: %w", joined)
}

// pollObservatory fetches both endpoints for one station and returns its
// samples plus the timestamp of its newest usable observation.
func (s *Source) pollObservatory(ctx context.Context, code string, now time.Time) ([]metric.Sample, time.Time, error) {
	var (
		out    []metric.Sample
		newest time.Time
	)

	rate, rateErr := s.fetchSeries(ctx, pathDbDt, code, rateWindow, now)
	field, fieldErr := s.fetchSeries(ctx, pathData, code, rangeWindow, now)
	if rateErr != nil && fieldErr != nil {
		return nil, time.Time{}, fmt.Errorf("%s: %w", code, errors.Join(rateErr, fieldErr))
	}

	if rate != nil {
		for _, component := range publishedComponents {
			// dbdt names its channels X_DT, Y_DT and so on; the descriptor
			// label is the bare component.
			value, at, ok := rate.newestUsable(component + "_DT")
			if !ok {
				continue
			}
			out = s.emit(out, metric.Sample{
				Desc:   metric.GeomagneticFieldRate,
				Labels: []string{code, component},
				Value:  value,
				Time:   at,
			})
			if at.After(newest) {
				newest = at
			}
		}
	}

	if field != nil {
		for _, component := range publishedComponents {
			value, at, ok := field.newestUsable(component)
			if !ok {
				continue
			}
			out = s.emit(out, metric.Sample{
				Desc:   metric.GeomagneticFieldComponent,
				Labels: []string{code, component},
				Value:  value,
				Time:   at,
			})
			if at.After(newest) {
				newest = at
			}
		}

		if span, at, ok := field.horizontalRange(hourlyRange); ok {
			out = s.emit(out, metric.Sample{
				Desc:   metric.GeomagneticFieldRange,
				Labels: []string{code},
				Value:  span,
				Time:   at,
			})
			if at.After(newest) {
				newest = at
			}
		}
	}

	// One endpoint failing while the other answered is worth a debug line but
	// not a lost observatory.
	if err := errors.Join(rateErr, fieldErr); err != nil {
		s.log.Debug("usgs-geomag endpoint failed for an observatory",
			"observatory", code, "error", err)
	}
	return out, newest, nil
}

// fetchSeries requests one endpoint for one observatory over a trailing window.
func (s *Source) fetchSeries(ctx context.Context, path, code string, window time.Duration, now time.Time) (*timeseries, error) {
	q := url.Values{}
	q.Set("id", code)
	q.Set("format", "json")
	q.Set("starttime", now.Add(-window).UTC().Format(queryTimeLayout))
	q.Set("endtime", now.UTC().Format(queryTimeLayout))
	if path == pathData {
		q.Set("type", dataType)
		q.Set("sampling_period", samplingPeriod)
	}
	endpoint := s.baseURL + path + "?" + q.Encode()

	body, err := s.fetch(ctx, endpoint)
	if err != nil {
		return nil, err
	}

	var ts timeseries
	if err := json.Unmarshal(body, &ts); err != nil {
		return nil, fmt.Errorf("usgsgeomag: %s returned unparseable JSON: %w",
			redact.URL(endpoint), err)
	}
	if ts.Metadata.Status != 0 && ts.Metadata.Status != 200 {
		return nil, fmt.Errorf("usgsgeomag: %s reported status %d in its own metadata",
			redact.URL(endpoint), ts.Metadata.Status)
	}
	if len(ts.Times) == 0 {
		return nil, fmt.Errorf("usgsgeomag: %s returned no timestamps", redact.URL(endpoint))
	}
	return &ts, nil
}

// queryTimeLayout is the ISO 8601 form the service accepts for starttime and
// endtime. It also accepts fractional seconds, which buys nothing here.
const queryTimeLayout = "2006-01-02T15:04:05Z"

// fetch performs one GET through the shared client, bounded by the configured
// timeout, and translates the client's sentinels into this package's terms.
func (s *Source) fetch(ctx context.Context, endpoint string) ([]byte, error) {
	reqCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	resp, err := s.client.Get(reqCtx, httpx.Request{
		URL:     endpoint,
		Retries: s.retries,
		MaxBody: maxBodyBytes,
	})
	if err != nil {
		return nil, err
	}
	if len(resp.Body) == 0 {
		return nil, fmt.Errorf("usgsgeomag: %s returned an empty body", redact.URL(endpoint))
	}
	return resp.Body, nil
}

// ensureObservatories loads the station name table if it has not loaded yet.
//
// The names are not published as labels — GeomagneticFieldComponent and its
// siblings carry only the IAGA code, deliberately, since a station name is not
// a dimension anybody groups by. The table is fetched so that a typo in
// configuration produces "no such observatory: BOO" at startup instead of five
// minutes of silent empty responses, and so logs can say "Boulder".
func (s *Source) ensureObservatories(ctx context.Context) {
	s.metaMu.Lock()
	defer s.metaMu.Unlock()

	if s.metaLoaded {
		return
	}

	body, err := s.fetch(ctx, s.baseURL+pathObservatories)
	if err != nil {
		s.log.Debug("usgs-geomag observatory metadata unavailable; will retry next poll",
			"error", err)
		return
	}

	var doc observatoryCollection
	if err := json.Unmarshal(body, &doc); err != nil {
		s.log.Warn("usgs-geomag observatory metadata was unparseable", "error", err)
		// Marked loaded regardless: a document this service will keep serving
		// in the same broken shape is not worth re-fetching every poll.
		s.metaLoaded = true
		return
	}

	names := make(map[string]string, len(doc.Features))
	for _, f := range doc.Features {
		code := strings.ToUpper(strings.TrimSpace(f.ID))
		if code == "" {
			continue
		}
		names[code] = strings.TrimSpace(f.Properties.Name)
	}
	s.names = names
	s.metaLoaded = true

	var unknown []string
	for _, code := range s.observatories {
		if _, ok := names[code]; !ok {
			unknown = append(unknown, code)
		}
	}
	sort.Strings(unknown)
	if len(unknown) > 0 {
		s.log.Warn("usgs-geomag configured observatories are not in the service's list",
			"observatories", strings.Join(unknown, ","), "listed", len(names))
	}
	s.log.Debug("usgs-geomag observatory metadata loaded", "stations", len(names))
}

// observatoryName returns the human name for a code, for logs.
func (s *Source) observatoryName(code string) string {
	s.metaMu.Lock()
	defer s.metaMu.Unlock()
	return s.names[code]
}

// emit appends a sample after checking it against its descriptor, so a label
// mismatch fails here rather than at collection time four layers away.
func (s *Source) emit(out []metric.Sample, sample metric.Sample) []metric.Sample {
	if err := sample.Validate(); err != nil {
		s.log.Warn("usgs-geomag discarded an invalid sample", "error", err)
		return out
	}
	return append(out, sample)
}

// ---------------------------------------------------------------------------
// Response shapes
// ---------------------------------------------------------------------------

// timeseries is the shared shape of /ws/data/ and /ws/algorithms/dbdt/.
//
// Values are *float64 rather than float64 because null is the sentinel for a
// missing observation and appears routinely at the trailing edge of every
// series. Decoding into float64 would turn every gap into a plausible-looking
// zero.
type timeseries struct {
	Type     string `json:"type"`
	Metadata struct {
		Intermagnet struct {
			IMO struct {
				IAGACode    string     `json:"iaga_code"`
				Name        string     `json:"name"`
				Coordinates []*float64 `json:"coordinates"`
			} `json:"imo"`
			ReportedOrientation string  `json:"reported_orientation"`
			DataType            string  `json:"data_type"`
			SamplingPeriod      float64 `json:"sampling_period"`
		} `json:"intermagnet"`
		Status    int       `json:"status"`
		Generated time.Time `json:"generated"`
	} `json:"metadata"`

	// Times is parallel to every channel's Values slice.
	Times []time.Time `json:"times"`

	Values []struct {
		ID       string `json:"id"`
		Metadata struct {
			Element string `json:"element"`
			Station string `json:"station"`
			Channel string `json:"channel"`
		} `json:"metadata"`
		Values []*float64 `json:"values"`
	} `json:"values"`
}

// channel returns the values for one channel id.
func (t *timeseries) channel(id string) ([]*float64, bool) {
	for i := range t.Values {
		if t.Values[i].ID == id {
			return t.Values[i].Values, true
		}
	}
	return nil, false
}

// newestUsable returns the last non-null value in a channel and the timestamp
// it belongs to.
//
// The search runs backwards from the end because the trailing nulls are the
// normal case: the service has not finished processing the last few minutes
// when the request arrives.
func (t *timeseries) newestUsable(id string) (float64, time.Time, bool) {
	values, ok := t.channel(id)
	if !ok {
		return 0, time.Time{}, false
	}
	n := min(len(values), len(t.Times))
	for i := n - 1; i >= 0; i-- {
		v := values[i]
		if v == nil || math.IsNaN(*v) || math.IsInf(*v, 0) {
			continue
		}
		return *v, t.Times[i], true
	}
	return 0, time.Time{}, false
}

// horizontalRange is the peak-to-peak variation of the horizontal field over
// the given span ending at the newest usable sample.
//
// The horizontal component is sqrt(X^2 + Y^2), which is the quantity the K
// index is scaled from, so its hourly excursion is a local disturbance measure
// available an hour or more before the three-hourly index is published. It is
// computed here rather than fetched because the service publishes no such
// product.
//
// Both X and Y must be present at an instant for it to count: a horizontal
// magnitude built from X at one minute and Y at another is not a measurement
// of anything. The returned time is the newest instant that contributed, so
// the sample is stamped with the observation it describes rather than the
// request time.
func (t *timeseries) horizontalRange(span time.Duration) (float64, time.Time, bool) {
	xs, okX := t.channel("X")
	ys, okY := t.channel("Y")
	if !okX || !okY {
		return 0, time.Time{}, false
	}

	n := min(min(len(xs), len(ys)), len(t.Times))

	// Find the newest instant with both components, which anchors the window.
	last := -1
	for i := n - 1; i >= 0; i-- {
		if usable(xs[i]) && usable(ys[i]) {
			last = i
			break
		}
	}
	if last < 0 {
		return 0, time.Time{}, false
	}
	cutoff := t.Times[last].Add(-span)

	var (
		lo, hi float64
		count  int
	)
	for i := 0; i <= last; i++ {
		if !usable(xs[i]) || !usable(ys[i]) {
			continue
		}
		if t.Times[i].Before(cutoff) {
			continue
		}
		h := math.Hypot(*xs[i], *ys[i])
		if count == 0 || h < lo {
			lo = h
		}
		if count == 0 || h > hi {
			hi = h
		}
		count++
	}

	// A single point has no range. Publishing 0 nT for it would read as a
	// perfectly quiet hour, which is the opposite of "we have one sample".
	if count < 2 {
		return 0, time.Time{}, false
	}
	return hi - lo, t.Times[last], true
}

// usable reports whether a decoded value is a real number.
func usable(v *float64) bool {
	return v != nil && !math.IsNaN(*v) && !math.IsInf(*v, 0)
}

// observatoryCollection is the /ws/observatories/ GeoJSON document.
type observatoryCollection struct {
	Type     string `json:"type"`
	Features []struct {
		ID         string `json:"id"`
		Properties struct {
			Name       string `json:"name"`
			Agency     string `json:"agency"`
			AgencyName string `json:"agency_name"`
			Network    string `json:"network"`
		} `json:"properties"`
		Geometry struct {
			Coordinates []float64 `json:"coordinates"`
		} `json:"geometry"`
	} `json:"features"`
}
