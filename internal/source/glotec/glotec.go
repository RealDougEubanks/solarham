// Package glotec polls NOAA SWPC's GloTEC assimilative total electron content
// model.
//
// GloTEC ingests real-time GNSS observations into a background ionospheric
// model and publishes a global grid of vertical TEC, F2 peak height and F2 peak
// density every ten minutes. For HF work it is the closest thing to a
// measurement of the ionosphere's actual state that exists at global coverage:
// TEC sets the usable frequency, and the anomaly field says whether today's
// ionosphere is behaving.
//
// Three properties of the product shape everything in this package.
//
// # The index must not be polled
//
// The directory listing at /products/glotec/geojson_2d_urt.json is a flat array
// of every file published in the last month: about 4,500 entries and 522 KB.
// Fetching half a megabyte every ten minutes to learn one filename is absurd,
// and the filenames are entirely predictable — they land on :05, :15, :25, :35,
// :45 and :55. So the filename is constructed from the clock and the index is
// consulted only when construction fails.
//
// # Each grid file is 2.5 MB
//
// Hence MaxBody is raised to 16 MB (see maxBodyBytes) and every request is
// conditional, so an unchanged file costs a 304 rather than two and a half
// megabytes.
//
// # Cardinality is the whole design problem
//
// The native grid is 72 latitudes by 72 longitudes — 5,184 points — carrying
// four values each. Published whole that is more than twenty thousand series
// from one source, which is more than everything else in this exporter
// combined. The default path is therefore a handful of point samples at
// configured locations; latitude-band statistics and the full grid are both
// opt-in. See Poll.
package glotec

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/RealDougEubanks/solarham/internal/config"
	"github.com/RealDougEubanks/solarham/internal/httpx"
	"github.com/RealDougEubanks/solarham/internal/metric"
	"github.com/RealDougEubanks/solarham/internal/source"
)

// Name identifies this source in logs, metrics and the health endpoints. It is
// stable because it becomes a metric label.
const Name = "glotec"

// Upstream locations. The base is hardcoded for the same reason as in the
// swpcforecast package: the existing swpc source already exposes a base URL
// override for this host, and a second knob for the same host is only a second
// thing to get out of step. The field on Source exists so tests can aim at an
// httptest server.
const (
	defaultBaseURL = "https://services.swpc.noaa.gov"

	// gridDir holds the per-timestamp GeoJSON files.
	gridDir = "/products/glotec/geojson_2d_urt/"

	// indexPath is the directory listing. It is the fallback, never the
	// routine path: see the package comment.
	indexPath = "/products/glotec/geojson_2d_urt.json"

	// filenameLayout formats a slot time into a grid filename. The published
	// names are of the form glotec_icao_20260909T151500Z.geojson.
	filenameLayout = "20060102T150405"
)

// Cadence and publication lag.
const (
	// cadence is how often a new grid is published. This is the model's own
	// "cadence": 10 field, and it is genuinely ten minutes, so a fixed
	// interval is the right schedule here rather than a publication clock.
	cadence = 10 * time.Minute

	// publishLag is how far behind the wall clock the newest readable file
	// sits. Measured live: at 15:43 UTC the newest published file was the
	// 15:15 slot, a lag of 28 minutes, which is the model run plus the
	// CloudFront cache fill.
	//
	// This is why the filename cannot simply be built from time.Now(): the
	// current slot reliably 404s.
	publishLag = 25 * time.Minute

	// probeSlots is how many slots back from the lagged clock to try before
	// giving up and reading the index. Four slots covers a lag of 25 to 65
	// minutes, which absorbs a late model run without ever walking far enough
	// back to publish stale data as current.
	probeSlots = 4
)

// maxBodyBytes bounds a grid response.
//
// The real file is 2.5 MB, which is well over httpx.DefaultMaxBody, so this
// override is required rather than merely generous. Sixteen megabytes is six
// times the observed size: enough headroom for SWPC to add variables to the
// grid without breaking the exporter, and still small enough that a hostile
// origin cannot exhaust memory with one response.
const maxBodyBytes = 16 << 20

// maxIndexBytes bounds the fallback index read. The listing was measured at
// 522 KB and grows by one entry per ten minutes within a rolling month.
const maxIndexBytes = 4 << 20

// retries is the number of retries after the first attempt. A 2.5 MB body over
// a flaky link is worth one more try; more than that and the next ten-minute
// slot is closer than the retry budget.
const retries = 1

// minUsableQualityFlag is the lowest quality_flag a point sample is published
// from.
//
// The flag is not a confidence grade with an arbitrary scale — SWPC's
// documentation defines it as the mean number of F-region observations ingested
// into that vertical profile, rounded up and capped at five. Higher is better,
// and zero means no observations at all: the cell's TEC is the background model
// with nothing assimilated into it.
//
// So the policy is: quality_flag >= 1 is published, quality_flag 0 produces no
// sample. Publishing a zero-observation cell would present climatology as a
// nowcast, and the whole reason to prefer GloTEC over a model is that it is
// data-driven. In the grid measured on 2026-09-09, 3,822 of 5,184 cells were
// flag 0 — mostly ocean, where there are no receivers — and 1,362 carried at
// least one observation.
//
// A point over open water will therefore often produce no samples. That is the
// honest answer, and it is logged at debug level so an operator can see why.
const minUsableQualityFlag = 1

// defaultGridStep subsamples the full grid when it is enabled without a step.
// Ten on both axes turns 5,184 points into 64, which is a coarse global picture
// rather than a cardinality incident.
const defaultGridStep = 10

// latitudeBands are the six 30-degree bands the band statistics summarise.
//
// Thirty degrees is not arbitrary: it separates the equatorial anomaly, the
// mid-latitudes and the auroral zone, which behave differently enough that a
// single global statistic would average the interesting part away.
var latitudeBands = []struct {
	label    string
	from, to float64
}{
	{"-90_-60", -90, -60},
	{"-60_-30", -60, -30},
	{"-30_0", -30, 0},
	{"0_30", 0, 30},
	{"30_60", 30, 60},
	{"60_90", 60, 90},
}

// point is a configured sample location.
type point struct {
	name string
	lat  float64
	lon  float64
}

// Source polls the GloTEC grid.
type Source struct {
	base     string
	points   []point
	bands    bool
	grid     bool
	gridStep int

	client *httpx.Client
	log    *slog.Logger

	// now is time.Now except in tests. It is used to construct the candidate
	// filename and to compute metric.SourceDataAge; every published value's
	// timestamp comes from the grid file's own time_tag.
	now func() time.Time

	// mu guards lastTimeTag. Poll runs on the scheduler's per-source
	// goroutine, but the health endpoints may read the source concurrently.
	mu sync.Mutex

	// lastTimeTag is the time_tag of the newest grid successfully parsed. It
	// stops a probe from re-fetching 2.5 MB that we already hold when the
	// upstream has not published a new slot yet.
	lastTimeTag time.Time
}

// Compile-time proof the source is both pollable and schedulable.
var (
	_ source.Source    = (*Source)(nil)
	_ source.Scheduled = (*Source)(nil)
)

// New builds the source from validated configuration.
//
// A malformed point is a configuration error and is refused rather than
// skipped. An operator who typed "51.5;-0.1" wants to be told; silently
// dropping the location would leave them staring at a dashboard with a missing
// series and no explanation anywhere.
func New(cfg config.GloTEC, client *httpx.Client, log *slog.Logger) (*Source, error) {
	if client == nil {
		return nil, errors.New("glotec: a shared httpx client is required")
	}
	if log == nil {
		log = slog.Default()
	}

	points, err := parsePoints(cfg.Points)
	if err != nil {
		return nil, err
	}

	step := cfg.GridStep
	if step < 1 {
		step = defaultGridStep
	}

	s := &Source{
		base:     defaultBaseURL,
		points:   points,
		bands:    cfg.LatitudeBands,
		grid:     cfg.Grid,
		gridStep: step,
		client:   client,
		log:      log.With("source", Name),
		now:      time.Now,
	}

	if s.grid {
		// The count is named rather than described, because "high
		// cardinality" means nothing to somebody who has not counted it and
		// this is the setting most likely to fill a Prometheus server.
		s.log.Warn("glotec full grid publishing is enabled; this is a large number of series",
			"grid_step", step, "projected_series", projectedGridSeries(step),
			"native_grid_points", gridLatitudes*gridLongitudes)
	}
	if len(points) == 0 && !cfg.LatitudeBands && !cfg.Grid {
		s.log.Warn("glotec is enabled but no points, latitude bands or grid are configured; " +
			"the source will fetch the grid and publish only its data age")
	}

	return s, nil
}

// Native grid dimensions, measured live on 2026-09-09: latitudes -88.75 to
// 88.75 in 2.5-degree steps, longitudes -177.5 to 177.5 in 5-degree steps.
// They are used only to project a series count for the warning above.
const (
	gridLatitudes  = 72
	gridLongitudes = 72
)

// projectedGridSeries estimates how many series a step will produce on the
// native grid.
func projectedGridSeries(step int) int {
	if step < 1 {
		step = 1
	}
	lats := (gridLatitudes + step - 1) / step
	lons := (gridLongitudes + step - 1) / step
	return lats * lons
}

// parsePoints reads the configured name-to-"lat,lon" map.
//
// The result is sorted by name so a batch is deterministic; iterating the map
// directly would reorder the samples on every poll, which makes a recorded
// batch useless as a test fixture and a diff between two polls unreadable.
func parsePoints(raw map[string]string) ([]point, error) {
	if len(raw) == 0 {
		return nil, nil
	}

	names := make([]string, 0, len(raw))
	for name := range raw {
		names = append(names, name)
	}
	sort.Strings(names)

	points := make([]point, 0, len(names))
	for _, name := range names {
		trimmed := strings.TrimSpace(name)
		if trimmed == "" {
			return nil, errors.New("glotec: a sample point has an empty name")
		}

		spec := strings.TrimSpace(raw[name])
		latStr, lonStr, ok := strings.Cut(spec, ",")
		if !ok {
			return nil, fmt.Errorf("glotec: sample point %q is %q, want \"lat,lon\" in decimal degrees", trimmed, spec)
		}

		lat, err := strconv.ParseFloat(strings.TrimSpace(latStr), 64)
		if err != nil {
			return nil, fmt.Errorf("glotec: sample point %q has an unreadable latitude %q", trimmed, strings.TrimSpace(latStr))
		}
		lon, err := strconv.ParseFloat(strings.TrimSpace(lonStr), 64)
		if err != nil {
			return nil, fmt.Errorf("glotec: sample point %q has an unreadable longitude %q", trimmed, strings.TrimSpace(lonStr))
		}
		if lat < -90 || lat > 90 {
			return nil, fmt.Errorf("glotec: sample point %q has latitude %g, which is outside -90 to 90", trimmed, lat)
		}
		if lon < -180 || lon > 180 {
			return nil, fmt.Errorf("glotec: sample point %q has longitude %g, which is outside -180 to 180", trimmed, lon)
		}
		if math.IsNaN(lat) || math.IsNaN(lon) {
			return nil, fmt.Errorf("glotec: sample point %q is not a location", trimmed)
		}

		points = append(points, point{name: trimmed, lat: lat, lon: lon})
	}
	return points, nil
}

// Name identifies the source.
func (s *Source) Name() string { return Name }

// Interval is the nominal spacing between polls. It reports what the schedule
// reports, so the two can never disagree.
func (s *Source) Interval() time.Duration { return s.Schedule().Interval() }

// Schedule polls on a fixed ten-minute interval.
//
// Unlike the forecast products, this one genuinely regenerates every ten
// minutes — the file says so in its own "cadence" field — so there is no
// publication clock to align to beyond the cadence itself. Conditional requests
// carry the cost: an unchanged file answers 304 rather than 2.5 MB.
func (s *Source) Schedule() source.Schedule { return source.Every(cadence) }

// Poll fetches the newest grid and converts it into samples.
//
// What is published, in order of increasing cardinality:
//
//  1. Point samples at each configured location — TEC, anomaly, F2 peak height
//     and F2 peak density from the nearest grid cell. This is the default and
//     is four series per point.
//  2. Latitude-band statistics, if enabled: max, mean and 95th percentile TEC
//     over each of six 30-degree bands. Eighteen series.
//  3. The full grid, if enabled, subsampled by the configured step.
//
// Plus the source's data age, always.
func (s *Source) Poll(ctx context.Context) (metric.Batch, error) {
	fetched := s.now().UTC()
	empty := metric.Batch{Source: Name, Fetched: fetched}

	body, err := s.fetchNewestGrid(ctx, fetched)
	switch {
	case errors.Is(err, source.ErrNotModified), errors.Is(err, httpx.ErrNotModified):
		return empty, source.ErrNotModified
	case errors.Is(err, httpx.ErrRateLimited):
		return empty, fmt.Errorf("%w: %w", source.ErrRateLimited, err)
	case err != nil:
		return empty, err
	}

	grid, err := parseGrid(body)
	if err != nil {
		return empty, err
	}

	s.mu.Lock()
	s.lastTimeTag = grid.timeTag
	s.mu.Unlock()

	batch := metric.Batch{Source: Name, Fetched: fetched}
	batch.Samples = append(batch.Samples, s.pointSamples(grid)...)
	if s.bands {
		batch.Samples = append(batch.Samples, s.bandSamples(grid)...)
	}
	if s.grid {
		batch.Samples = append(batch.Samples, s.gridSamples(grid)...)
	}

	age := fetched.Sub(grid.timeTag).Seconds()
	if age < 0 {
		// A grid stamped in the future means SWPC's clock and ours disagree,
		// not that the data is fresher than fresh. A negative age looks like a
		// broken exporter rather than a surprising upstream.
		age = 0
	}
	batch.Samples = append(batch.Samples, metric.Sample{
		Desc: metric.SourceDataAge, Labels: []string{Name}, Value: age,
		// The age is a property of this moment, not of the observation, so
		// unlike every other sample here it is stamped with the fetch time.
		Time: fetched,
	})

	// Validate at the boundary rather than in each emitter: a sample with the
	// wrong number of label values is rejected by Prometheus at collection
	// time and silently corrupts InfluxDB tag sets.
	kept := batch.Samples[:0]
	for _, sample := range batch.Samples {
		if err := sample.Validate(); err != nil {
			s.log.Warn("glotec dropped an invalid sample", "error", err)
			continue
		}
		kept = append(kept, sample)
	}
	batch.Samples = kept

	return batch, nil
}

// fetchNewestGrid returns the body of the newest readable grid file.
//
// It walks back from the lagged clock trying constructed filenames, and reads
// the index only if every candidate 404s. A candidate no newer than the last
// grid we parsed short-circuits to ErrNotModified without a request at all,
// because every older candidate is older still.
func (s *Source) fetchNewestGrid(ctx context.Context, now time.Time) ([]byte, error) {
	s.mu.Lock()
	last := s.lastTimeTag
	s.mu.Unlock()

	slot := floorSlot(now.Add(-publishLag))

	var missing []string
	for range probeSlots {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("glotec: cancelled: %w", err)
		}
		if !last.IsZero() && !slot.After(last) {
			// Nothing has been published since the last grid we parsed. This is
			// the common case at a ten-minute poll against a twenty-five
			// minute lag, and it costs no request.
			return nil, source.ErrNotModified
		}

		body, err := s.fetchGridAt(ctx, slot)
		switch {
		case err == nil:
			return body, nil
		case errors.Is(err, httpx.ErrPermanent):
			// A 404 means this slot has not been published. Walk back; do not
			// retry, because it will 404 identically next time.
			missing = append(missing, slot.Format(filenameLayout))
			slot = slot.Add(-cadence)
		default:
			return nil, err
		}
	}

	s.log.Debug("glotec constructed filenames all 404ed; falling back to the directory index",
		"slots", strings.Join(missing, ", "))
	return s.fetchViaIndex(ctx, last)
}

// fetchGridAt fetches the grid file for one slot.
func (s *Source) fetchGridAt(ctx context.Context, slot time.Time) ([]byte, error) {
	return s.fetchURL(ctx, s.base+gridDir+gridFilename(slot), maxBodyBytes)
}

// fetchURL performs one conditional GET.
func (s *Source) fetchURL(ctx context.Context, url string, maxBody int64) ([]byte, error) {
	resp, err := s.client.Get(ctx, httpx.Request{
		URL: url,
		// SWPC serves ETag and Last-Modified through CloudFront. On a 2.5 MB
		// file that is the difference between a poll costing nothing and a poll
		// costing two and a half megabytes.
		Conditional: true,
		Retries:     retries,
		MaxBody:     maxBody,
	})
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

// indexEntry is one row of the directory listing.
type indexEntry struct {
	URL     string `json:"url"`
	TimeTag string `json:"time_tag"`
}

// fetchViaIndex reads the directory listing and fetches whatever it says is
// newest.
//
// This is the fallback path, taken only when every constructed filename 404s —
// a model outage long enough to exceed the probe window, or a change to the
// naming convention. It costs half a megabyte, which is exactly why it is not
// the routine path.
func (s *Source) fetchViaIndex(ctx context.Context, last time.Time) ([]byte, error) {
	body, err := s.fetchURL(ctx, s.base+indexPath, maxIndexBytes)
	if errors.Is(err, httpx.ErrNotModified) {
		return nil, source.ErrNotModified
	}
	if err != nil {
		return nil, fmt.Errorf("glotec: reading the grid index: %w", err)
	}

	var entries []indexEntry
	if err := json.Unmarshal(body, &entries); err != nil {
		return nil, fmt.Errorf("glotec: parsing the grid index: %w", err)
	}
	if len(entries) == 0 {
		return nil, errors.New("glotec: the grid index is empty")
	}

	// The listing has been oldest-first every time it has been observed, but
	// "the file happens to be sorted" is not a contract and reading the wrong
	// end would publish a month-old ionosphere as current.
	var (
		newest indexEntry
		when   time.Time
	)
	for _, e := range entries {
		t, err := time.Parse(time.RFC3339, strings.TrimSpace(e.TimeTag))
		if err != nil || strings.TrimSpace(e.URL) == "" {
			continue
		}
		if when.IsZero() || t.After(when) {
			newest, when = e, t.UTC()
		}
	}
	if when.IsZero() {
		return nil, errors.New("glotec: no index entry carried a readable time_tag and url")
	}
	if !last.IsZero() && !when.After(last) {
		return nil, source.ErrNotModified
	}

	// The index gives a host-relative path. Joining it onto the configured
	// base rather than trusting it as a URL keeps a compromised or mistaken
	// index from redirecting the fetch off-host.
	return s.fetchURL(ctx, s.base+"/"+strings.TrimLeft(newest.URL, "/"), maxBodyBytes)
}

// gridFilename builds the published filename for a slot.
func gridFilename(slot time.Time) string {
	return "glotec_icao_" + slot.UTC().Format(filenameLayout) + "Z.geojson"
}

// floorSlot rounds t down to the most recent publication slot.
//
// Files land on :05, :15, :25, :35, :45 and :55 — ten-minute cadence offset by
// five minutes — so this is not a plain Truncate.
func floorSlot(t time.Time) time.Time {
	t = t.UTC().Truncate(time.Minute)
	offset := (t.Minute() - 5) % 10
	if offset < 0 {
		offset += 10
	}
	return t.Add(-time.Duration(offset) * time.Minute)
}
