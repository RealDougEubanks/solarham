package glotec

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/RealDougEubanks/solarham/internal/metric"
)

// collection is the GeoJSON document the model publishes.
//
// Only the fields this source uses are decoded. The metadata block carries
// units and per-variable min/max, which are already fixed in the metric
// descriptors, and re-deriving them from the payload would let an upstream
// change silently rewrite what a series means.
type collection struct {
	Type     string    `json:"type"`
	TimeTag  string    `json:"time_tag"`
	Cadence  int       `json:"cadence"`
	Features []feature `json:"features"`
}

// feature is one grid cell.
type feature struct {
	Geometry struct {
		Type string `json:"type"`
		// Coordinates are [longitude, latitude], which is GeoJSON's order and
		// the reverse of how everybody says it out loud. Getting this backwards
		// produces a plausible-looking global map that is wrong everywhere.
		Coordinates []float64 `json:"coordinates"`
	} `json:"geometry"`

	Properties struct {
		// Pointers, not bare floats: a JSON null decoded into a float64
		// becomes 0, and a TEC of zero published as a fact is a different
		// claim from no value being available.
		TEC     *float64 `json:"tec"`
		Anomaly *float64 `json:"anomaly"`
		HmF2    *float64 `json:"hmF2"`
		NmF2    *float64 `json:"NmF2"`

		// QualityFlag is the mean number of F-region observations assimilated
		// into this cell's profile, rounded up and capped at five. Higher is
		// better; see minUsableQualityFlag.
		QualityFlag *int `json:"quality_flag"`
	} `json:"properties"`
}

// cell is one decoded grid point.
type cell struct {
	lat, lon float64
	tec      *float64
	anomaly  *float64
	hmF2     *float64
	nmF2     *float64
	quality  int
}

// usable reports whether this cell carries assimilated observations.
func (c cell) usable() bool { return c.quality >= minUsableQualityFlag }

// grid is a parsed grid file.
type grid struct {
	timeTag time.Time
	cells   []cell
}

// parseGrid decodes a grid file.
//
// A file with no readable time_tag is rejected outright. Everything this source
// publishes is stamped from it, and substituting the wall clock would make a
// stalled model indistinguishable from a working one — which is the failure
// mode metric.SourceDataAge exists to catch.
func parseGrid(body []byte) (*grid, error) {
	var doc collection
	if err := json.Unmarshal(body, &doc); err != nil {
		return nil, fmt.Errorf("glotec: parsing the grid: %w", err)
	}

	when, err := time.Parse(time.RFC3339, strings.TrimSpace(doc.TimeTag))
	if err != nil {
		return nil, fmt.Errorf("glotec: grid time_tag %q is unreadable: %w", doc.TimeTag, err)
	}
	if len(doc.Features) == 0 {
		return nil, errors.New("glotec: the grid carries no features")
	}

	g := &grid{timeTag: when.UTC(), cells: make([]cell, 0, len(doc.Features))}
	for _, f := range doc.Features {
		if len(f.Geometry.Coordinates) < 2 {
			continue
		}
		lon, lat := f.Geometry.Coordinates[0], f.Geometry.Coordinates[1]
		if math.IsNaN(lat) || math.IsNaN(lon) {
			continue
		}

		// A missing quality_flag is treated as zero observations rather than as
		// unknown. The conservative reading is the one that does not present
		// climatology as a nowcast.
		quality := 0
		if f.Properties.QualityFlag != nil {
			quality = *f.Properties.QualityFlag
		}

		g.cells = append(g.cells, cell{
			lat: lat, lon: lon,
			tec:     f.Properties.TEC,
			anomaly: f.Properties.Anomaly,
			hmF2:    f.Properties.HmF2,
			nmF2:    f.Properties.NmF2,
			quality: quality,
		})
	}
	if len(g.cells) == 0 {
		return nil, errors.New("glotec: no grid feature carried a usable geometry")
	}
	return g, nil
}

// pointSamples publishes the four ionospheric values at each configured
// location, read from the nearest grid cell.
//
// A point whose nearest cell has quality_flag 0 produces no samples at all,
// which is common over open ocean where there are no GNSS receivers. See
// minUsableQualityFlag for why.
func (s *Source) pointSamples(g *grid) []metric.Sample {
	if len(s.points) == 0 {
		return nil
	}

	samples := make([]metric.Sample, 0, len(s.points)*4)
	for _, p := range s.points {
		nearest, ok := g.nearest(p.lat, p.lon)
		if !ok {
			continue
		}
		if !nearest.usable() {
			s.log.Debug("glotec point has no assimilated observations; publishing nothing for it",
				"point", p.name, "latitude", p.lat, "longitude", p.lon,
				"cell_latitude", nearest.lat, "cell_longitude", nearest.lon,
				"quality_flag", nearest.quality)
			continue
		}

		add := func(desc *metric.Descriptor, v *float64) {
			if v == nil {
				return
			}
			samples = append(samples, metric.Sample{
				Desc: desc, Labels: []string{p.name}, Value: *v, Time: g.timeTag,
			})
		}
		add(metric.TotalElectronContent, nearest.tec)
		add(metric.TECAnomaly, nearest.anomaly)
		add(metric.PeakHeightF2, nearest.hmF2)
		add(metric.PeakDensityF2, nearest.nmF2)
	}
	return samples
}

// nearest finds the grid cell closest to a location.
//
// Great-circle distance rather than a squared difference in degrees, for two
// reasons that both produce wrong answers otherwise: a degree of longitude is
// 111 km at the equator and 4 km at 87.5 degrees latitude, so a flat metric
// picks the wrong cell at high latitudes; and a point just west of the
// antimeridian is 3 degrees from a cell just east of it, not 357.
func (g *grid) nearest(lat, lon float64) (cell, bool) {
	var (
		best     cell
		bestDist = math.Inf(1)
		found    bool
	)
	for _, c := range g.cells {
		d := angularDistance(lat, lon, c.lat, c.lon)
		if d < bestDist {
			best, bestDist, found = c, d, true
		}
	}
	return best, found
}

// angularDistance is the haversine central angle between two locations, in
// radians. Only the ordering matters here, so it is never converted to
// kilometres.
func angularDistance(lat1, lon1, lat2, lon2 float64) float64 {
	const deg = math.Pi / 180
	dLat := (lat2 - lat1) * deg
	dLon := (lon2 - lon1) * deg
	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(lat1*deg)*math.Cos(lat2*deg)*math.Sin(dLon/2)*math.Sin(dLon/2)
	return 2 * math.Asin(math.Sqrt(math.Min(1, a)))
}

// bandSamples publishes max, mean and 95th-percentile TEC over each latitude
// band: eighteen series for the whole globe.
//
// Every cell counts, whatever its quality_flag. That is deliberate and it is
// the opposite of the point-sample policy. GloTEC produces a TEC value
// everywhere, from assimilated observations where they exist and from the
// background model where they do not; a band statistic restricted to
// observation-backed cells would quietly become a statistic about GNSS
// receiver coverage, and would jump whenever coverage changed rather than when
// the ionosphere did. A band statistic is a summary of the model field, and the
// model field is defined at every cell.
func (s *Source) bandSamples(g *grid) []metric.Sample {
	samples := make([]metric.Sample, 0, len(latitudeBands)*3)

	for _, band := range latitudeBands {
		values := make([]float64, 0, 1024)
		for _, c := range g.cells {
			if c.tec == nil || math.IsNaN(*c.tec) {
				continue
			}
			// Half-open bands, so a cell on a boundary is counted once. The
			// topmost band closes at the pole because nothing lies above it.
			if c.lat < band.from {
				continue
			}
			if c.lat > band.to || (c.lat == band.to && band.to != 90) {
				continue
			}
			values = append(values, *c.tec)
		}
		if len(values) == 0 {
			// An empty band is a truncated or regional grid, not a TEC of
			// zero. Publishing zero would put a false quiet band on a map.
			continue
		}

		sort.Float64s(values)

		var sum float64
		for _, v := range values {
			sum += v
		}

		add := func(statistic string, value float64) {
			samples = append(samples, metric.Sample{
				Desc:   metric.TECStatistic,
				Labels: []string{statistic, band.label},
				Value:  value,
				Time:   g.timeTag,
			})
		}
		add("max", values[len(values)-1])
		add("mean", sum/float64(len(values)))
		add("p95", percentile(values, 0.95))
	}

	return samples
}

// percentile returns the nearest-rank percentile of a sorted slice.
//
// Nearest-rank rather than interpolated: the value returned is one that
// actually occurs in the grid, which matters when the question is "how high
// does TEC get in this band" and an interpolated answer would be a number no
// cell reported.
func percentile(sorted []float64, q float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	rank := int(math.Ceil(q*float64(len(sorted)))) - 1
	if rank < 0 {
		rank = 0
	}
	if rank >= len(sorted) {
		rank = len(sorted) - 1
	}
	return sorted[rank]
}

// gridSamples publishes the full grid, subsampled.
//
// Only TEC is published, not all four variables: the grid is the cardinality
// problem in this package and quadrupling it for the sake of a global map of
// F2 peak density is not a trade anybody would take. The location goes in the
// "point" label, which metric.TotalElectronContent already carries, as
// "lat,lon" in decimal degrees.
//
// Subsampling is by index on each axis, the same approach the swpc package
// takes to the D-RAP grid: the cells are sorted into a stable latitude-major
// order first, so a given step always selects the same cells and the series set
// does not shuffle between polls.
func (s *Source) gridSamples(g *grid) []metric.Sample {
	step := s.gridStep
	if step < 1 {
		step = 1
	}

	// Distinct axis values, so the step means "every Nth latitude" rather than
	// "every Nth feature in whatever order the file happened to list them".
	lats := axisValues(g.cells, func(c cell) float64 { return c.lat })
	lons := axisValues(g.cells, func(c cell) float64 { return c.lon })

	keepLat := make(map[float64]bool, len(lats))
	for i, v := range lats {
		if i%step == 0 {
			keepLat[v] = true
		}
	}
	keepLon := make(map[float64]bool, len(lons))
	for i, v := range lons {
		if i%step == 0 {
			keepLon[v] = true
		}
	}

	// Latitude-major, so the emitted order is a readable sweep of the globe.
	ordered := make([]cell, len(g.cells))
	copy(ordered, g.cells)
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].lat != ordered[j].lat {
			return ordered[i].lat < ordered[j].lat
		}
		return ordered[i].lon < ordered[j].lon
	})

	samples := make([]metric.Sample, 0, len(keepLat)*len(keepLon))
	for _, c := range ordered {
		if c.tec == nil || !keepLat[c.lat] || !keepLon[c.lon] {
			continue
		}
		samples = append(samples, metric.Sample{
			Desc:   metric.TotalElectronContent,
			Labels: []string{formatCoordinate(c.lat) + "," + formatCoordinate(c.lon)},
			Value:  *c.tec,
			Time:   g.timeTag,
		})
	}
	return samples
}

// axisValues returns the sorted distinct values of one axis.
func axisValues(cells []cell, pick func(cell) float64) []float64 {
	seen := make(map[float64]struct{}, 128)
	out := make([]float64, 0, 128)
	for _, c := range cells {
		v := pick(c)
		if _, dup := seen[v]; dup {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	sort.Float64s(out)
	return out
}

// formatCoordinate renders a coordinate as a label value with no trailing
// zeros, so a cell at -88.75 is labelled "-88.75" rather than "-88.750000".
func formatCoordinate(v float64) string {
	return strconv.FormatFloat(v, 'f', -1, 64)
}
