package glotec

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/RealDougEubanks/solarham/internal/config"
	"github.com/RealDougEubanks/solarham/internal/httpx"
	"github.com/RealDougEubanks/solarham/internal/metric"
	"github.com/RealDougEubanks/solarham/internal/source"
)

// gridTimeTag is the time_tag both recorded fixtures carry.
var gridTimeTag = time.Date(2026, 9, 9, 15, 15, 0, 0, time.UTC)

// pollTime is the injected wall clock. It is chosen so that the first
// constructed filename — floorSlot(pollTime - publishLag) — is exactly the
// 15:15 slot the fixtures hold, and so that a correct SourceDataAge is 1500
// seconds.
var pollTime = time.Date(2026, 9, 9, 15, 40, 0, 0, time.UTC)

// wantedFilename is the grid file a correct first probe asks for.
var wantedFilename = "glotec_icao_20260909T151500Z.geojson"

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// testClient returns an httpx client with the loopback host's politeness
// spacing removed. The spacing is the right default against a real publisher
// and pure latency against an httptest server two goroutines away.
func testClient(t *testing.T) *httpx.Client {
	t.Helper()
	return httpx.New(httpx.Options{
		Timeout:  10 * time.Second,
		Policies: map[string]httpx.HostPolicy{"127.0.0.1": {MinInterval: time.Microsecond, Burst: 64}},
		Fallback: httpx.HostPolicy{MinInterval: time.Microsecond, Burst: 64},
	}, quietLogger())
}

func fixture(t *testing.T, name string) []byte {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("reading fixture %s: %v", name, err)
	}
	return body
}

// gridServer serves one grid body for the expected filename and 404s
// everything else, counting requests per path.
func gridServer(t *testing.T, body []byte) (*httptest.Server, func() map[string]int) {
	t.Helper()

	var (
		mu     sync.Mutex
		counts = map[string]int{}
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		counts[r.URL.Path]++
		mu.Unlock()

		if r.URL.Path == gridDir+wantedFilename {
			_, _ = w.Write(body)
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(srv.Close)

	return srv, func() map[string]int {
		mu.Lock()
		defer mu.Unlock()
		out := make(map[string]int, len(counts))
		for k, v := range counts {
			out[k] = v
		}
		return out
	}
}

func newTestSource(t *testing.T, cfg config.GloTEC, srv *httptest.Server, log *slog.Logger) *Source {
	t.Helper()
	if log == nil {
		log = quietLogger()
	}
	src, err := New(cfg, testClient(t), log)
	if err != nil {
		t.Fatalf("New returned an unexpected error: %v", err)
	}
	src.base = srv.URL
	src.now = func() time.Time { return pollTime }
	return src
}

func key(s metric.Sample) string {
	return s.Desc.FullName() + "{" + strings.Join(s.Labels, ",") + "}"
}

func index(t *testing.T, b metric.Batch) map[string]metric.Sample {
	t.Helper()
	out := make(map[string]metric.Sample, len(b.Samples))
	for _, s := range b.Samples {
		k := key(s)
		if _, dup := out[k]; dup {
			t.Errorf("batch contains series %s twice", k)
		}
		out[k] = s
	}
	return out
}

// nearlyEqual reports whether two floats agree to within a tolerance that survives
// JSON round-tripping without hiding a wrong calculation.
func nearlyEqual(got, want float64) bool {
	d := got - want
	if d < 0 {
		d = -d
	}
	return d <= 1e-9
}

func TestNewRejectsAMissingHTTPClient(t *testing.T) {
	if _, err := New(config.GloTEC{}, nil, quietLogger()); err == nil {
		t.Error("New accepted a nil httpx client; the shared client is what enforces politeness across sources and must not be optional")
	}
}

func TestNewRejectsAMalformedSamplePointRatherThanSkippingIt(t *testing.T) {
	// Each of these is a plausible typo. Skipping the point would leave the
	// operator staring at a dashboard with a missing series and no
	// explanation anywhere.
	cases := map[string]string{
		"a semicolon instead of a comma":              "51.5;-0.1",
		"no separator at all":                         "51.5",
		"an unreadable latitude":                      "north,-0.1",
		"an unreadable longitude":                     "51.5,west",
		"a latitude beyond the pole":                  "95,-0.1",
		"a longitude beyond the antimeridian":         "51.5,-200",
		"an empty specification":                      "",
		"only a comma":                                ",",
		"a latitude that is a degrees-minutes string": "51:30,-0.1",
	}
	for name, spec := range cases {
		if _, err := New(config.GloTEC{Points: map[string]string{"somewhere": spec}}, testClient(t), quietLogger()); err == nil {
			t.Errorf("New accepted %s (%q), want a configuration error", name, spec)
		}
	}

	if _, err := New(config.GloTEC{Points: map[string]string{"  ": "51.5,-0.1"}}, testClient(t), quietLogger()); err == nil {
		t.Error("New accepted a point with a blank name, want a configuration error")
	}
}

func TestNewAcceptsWellFormedSamplePointsAndSortsThemForDeterminism(t *testing.T) {
	src, err := New(config.GloTEC{Points: map[string]string{
		"tokyo":   "35.7, 139.7",
		"boulder": "40.0,-105.3",
		"sydney":  "-33.9,151.2",
	}}, testClient(t), quietLogger())
	if err != nil {
		t.Fatalf("New rejected well-formed points: %v", err)
	}

	want := []string{"boulder", "sydney", "tokyo"}
	if len(src.points) != len(want) {
		t.Fatalf("parsed %d points, want %d", len(src.points), len(want))
	}
	for i, name := range want {
		if src.points[i].name != name {
			t.Errorf("point %d is %q, want %q; points are sorted so a batch does not reorder between polls", i, src.points[i].name, name)
		}
	}
	if got := src.points[0]; !nearlyEqual(got.lat, 40.0) || !nearlyEqual(got.lon, -105.3) {
		t.Errorf("boulder parsed as %v,%v, want 40,-105.3", got.lat, got.lon)
	}
}

func TestPollParsesTheRecordedGridIntoTheExpectedSeriesCounts(t *testing.T) {
	srv, _ := gridServer(t, fixture(t, "grid.geojson"))
	src := newTestSource(t, config.GloTEC{
		Points:        map[string]string{"london": "51.5,-0.1"},
		LatitudeBands: true,
		Grid:          true,
		GridStep:      10,
	}, srv, nil)

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll returned an unexpected error: %v", err)
	}

	// One point (4 series) + six latitude bands times three statistics (18)
	// + the 24x12 fixture grid subsampled by 10 into 3 latitudes by 2
	// longitudes (6) + the data age (1).
	const want = 29
	if got := batch.Len(); got != want {
		t.Errorf("Poll produced %d samples, want %d", got, want)
		for _, s := range batch.Samples {
			t.Logf("  %s = %v", key(s), s.Value)
		}
	}

	for _, s := range batch.Samples {
		if err := s.Validate(); err != nil {
			t.Errorf("Poll emitted an invalid sample: %v", err)
		}
		if s.Desc != metric.SourceDataAge && !s.Time.Equal(gridTimeTag) {
			t.Errorf("series %s is stamped %s, want the grid's own %s",
				key(s), s.Time.Format(time.RFC3339), gridTimeTag.Format(time.RFC3339))
		}
	}
}

func TestPointSamplingReadsTheNearestGridCell(t *testing.T) {
	srv, _ := gridServer(t, fixture(t, "grid.geojson"))
	src := newTestSource(t, config.GloTEC{Points: map[string]string{"london": "51.5,-0.1"}}, srv, nil)

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll returned an unexpected error: %v", err)
	}
	byKey := index(t, batch)

	// The fixture's nearest cell to 51.5N 0.1W is 53.75N 2.5E, whose recorded
	// values these are. A cell one row away would give visibly different
	// numbers.
	want := map[string]float64{
		"solar_total_electron_content_tecu{london}":          17.120824407721763,
		"solar_total_electron_content_anomaly_ratio{london}": -1.0285073206099824,
		"solar_f2_peak_height_kilometers{london}":            272.08539082090516,
		"solar_f2_peak_density_per_cubic_meter{london}":      560339481394.0635,
	}
	for k, wantValue := range want {
		got, ok := byKey[k]
		if !ok {
			t.Errorf("series %s is missing from the batch", k)
			continue
		}
		if !nearlyEqual(got.Value, wantValue) {
			t.Errorf("series %s is %v, want %v (the cell at 53.75N 2.5E)", k, got.Value, wantValue)
		}
	}
}

func TestNearestCellIsChosenAcrossTheAntimeridianRatherThanByDegreeArithmetic(t *testing.T) {
	// 179W is three degrees from a cell at 178E, and 199 degrees from one at
	// 20E. Plain subtraction of longitudes picks the wrong one.
	srv, _ := gridServer(t, fixture(t, "small.geojson"))
	src := newTestSource(t, config.GloTEC{Points: map[string]string{"dateline": "75,-179"}}, srv, nil)

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll returned an unexpected error: %v", err)
	}

	got, ok := index(t, batch)["solar_total_electron_content_tecu{dateline}"]
	if !ok {
		t.Fatal("no TEC was published for the dateline point")
	}
	if !nearlyEqual(got.Value, 70) {
		t.Errorf("dateline TEC is %v, want 70 (the cell at 75N 178E); 60 would mean the cell at 75N 20E was chosen", got.Value)
	}
}

func TestNearestCellIsChosenByGreatCircleDistanceNotFlatDegrees(t *testing.T) {
	// 16N 24E is one degree of latitude and one degree of longitude from the
	// cell at 15N 25E, and one degree of latitude and four of longitude from
	// the one at 15N 20E.
	srv, _ := gridServer(t, fixture(t, "small.geojson"))
	src := newTestSource(t, config.GloTEC{Points: map[string]string{"near": "16,24"}}, srv, nil)

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll returned an unexpected error: %v", err)
	}

	got, ok := index(t, batch)["solar_total_electron_content_tecu{near}"]
	if !ok {
		t.Fatal("no TEC was published for the near point")
	}
	if !nearlyEqual(got.Value, 44) {
		t.Errorf("near TEC is %v, want 44 (the cell at 15N 25E)", got.Value)
	}
}

func TestACellWithNoAssimilatedObservationsProducesNoPointSample(t *testing.T) {
	// The small fixture's cell at 45S 20E carries quality_flag 0: the model
	// produced a TEC there but no GNSS observations went into it. Publishing
	// it would present climatology as a nowcast.
	srv, _ := gridServer(t, fixture(t, "small.geojson"))
	src := newTestSource(t, config.GloTEC{Points: map[string]string{
		"unobserved": "-45,20",
		"observed":   "-15,20",
	}}, srv, nil)

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll returned an unexpected error: %v", err)
	}
	byKey := index(t, batch)

	for _, desc := range []*metric.Descriptor{
		metric.TotalElectronContent, metric.TECAnomaly, metric.PeakHeightF2, metric.PeakDensityF2,
	} {
		if _, ok := byKey[desc.FullName()+"{unobserved}"]; ok {
			t.Errorf("%s was published for a cell with quality_flag 0", desc.FullName())
		}
	}

	// The neighbouring point, whose cell carries flag 5, must still be there:
	// the filter must drop one point, not disable point sampling.
	if got, ok := byKey["solar_total_electron_content_tecu{observed}"]; !ok {
		t.Error("the observation-backed point was dropped along with the unobserved one")
	} else if !nearlyEqual(got.Value, 30) {
		t.Errorf("observed TEC is %v, want 30", got.Value)
	}
}

func TestLatitudeBandStatisticsAreAbsentByDefault(t *testing.T) {
	srv, _ := gridServer(t, fixture(t, "grid.geojson"))
	src := newTestSource(t, config.GloTEC{Points: map[string]string{"london": "51.5,-0.1"}}, srv, nil)

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll returned an unexpected error: %v", err)
	}
	for _, s := range batch.Samples {
		if s.Desc == metric.TECStatistic {
			t.Errorf("band statistic %s was published with cfg.LatitudeBands unset", key(s))
		}
	}
}

func TestLatitudeBandStatisticsAreCorrectOnAHandCheckedGrid(t *testing.T) {
	// The small fixture holds one or two cells per band with round TEC
	// values, so every expectation below is arithmetic anybody can redo:
	//
	//   -90_-60: [10]      max 10, mean 10, p95 10
	//   -60_-30: [20]      max 20, mean 20, p95 20
	//   -30_0:   [30]      max 30, mean 30, p95 30
	//   0_30:    [40, 44]  max 44, mean 42, p95 44
	//   30_60:   [50]      max 50, mean 50, p95 50
	//   60_90:   [60, 70]  max 70, mean 65, p95 70
	//
	// The 0_30 band also proves the flag-0 policy does not leak into the band
	// statistics: its cells carry flags 2 and 1, while the -60_-30 band's
	// single cell carries flag 0 and is still counted, because a band
	// statistic summarises the model field rather than receiver coverage.
	srv, _ := gridServer(t, fixture(t, "small.geojson"))
	src := newTestSource(t, config.GloTEC{LatitudeBands: true}, srv, nil)

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll returned an unexpected error: %v", err)
	}
	byKey := index(t, batch)

	want := map[string]float64{
		"solar_total_electron_content_tecu_stat{max,-90_-60}":  10,
		"solar_total_electron_content_tecu_stat{mean,-90_-60}": 10,
		"solar_total_electron_content_tecu_stat{p95,-90_-60}":  10,
		"solar_total_electron_content_tecu_stat{max,-60_-30}":  20,
		"solar_total_electron_content_tecu_stat{mean,-60_-30}": 20,
		"solar_total_electron_content_tecu_stat{p95,-60_-30}":  20,
		"solar_total_electron_content_tecu_stat{max,-30_0}":    30,
		"solar_total_electron_content_tecu_stat{mean,-30_0}":   30,
		"solar_total_electron_content_tecu_stat{p95,-30_0}":    30,
		"solar_total_electron_content_tecu_stat{max,0_30}":     44,
		"solar_total_electron_content_tecu_stat{mean,0_30}":    42,
		"solar_total_electron_content_tecu_stat{p95,0_30}":     44,
		"solar_total_electron_content_tecu_stat{max,30_60}":    50,
		"solar_total_electron_content_tecu_stat{mean,30_60}":   50,
		"solar_total_electron_content_tecu_stat{p95,30_60}":    50,
		"solar_total_electron_content_tecu_stat{max,60_90}":    70,
		"solar_total_electron_content_tecu_stat{mean,60_90}":   65,
		"solar_total_electron_content_tecu_stat{p95,60_90}":    70,
	}

	var bandSeries int
	for _, s := range batch.Samples {
		if s.Desc == metric.TECStatistic {
			bandSeries++
		}
	}
	if bandSeries != len(want) {
		t.Errorf("published %d band series, want %d (six bands times three statistics)", bandSeries, len(want))
	}

	for k, wantValue := range want {
		got, ok := byKey[k]
		if !ok {
			t.Errorf("series %s is missing from the batch", k)
			continue
		}
		if !nearlyEqual(got.Value, wantValue) {
			t.Errorf("series %s is %v, want %v", k, got.Value, wantValue)
		}
	}
}

func TestEachCellFallsInExactlyOneLatitudeBand(t *testing.T) {
	// Half-open bands, so a cell exactly on a boundary is counted once. The
	// real grid's latitudes all end in .25 or .75, but a change of grid
	// resolution upstream would put cells on the boundaries, and a cell
	// double-counted would inflate two bands at once.
	g := &grid{timeTag: gridTimeTag}
	for _, lat := range []float64{-90, -60, -30, 0, 30, 60, 90} {
		v := 1.0
		g.cells = append(g.cells, cell{lat: lat, lon: 0, tec: &v, quality: 5})
	}

	src := &Source{bands: true, log: quietLogger()}
	samples := src.bandSamples(g)

	counted := map[string]float64{}
	for _, s := range samples {
		if s.Labels[0] == "mean" {
			counted[s.Labels[1]] = s.Value
		}
	}
	if len(counted) != len(latitudeBands) {
		t.Errorf("%d bands carried a mean, want %d", len(counted), len(latitudeBands))
	}
	for band, mean := range counted {
		if !nearlyEqual(mean, 1) {
			t.Errorf("band %s has mean %v, want 1; a value other than 1 means a cell was counted in two bands", band, mean)
		}
	}
}

func TestTheFullGridIsAbsentByDefaultAndSubsampledWhenEnabled(t *testing.T) {
	body := fixture(t, "grid.geojson")

	srv, _ := gridServer(t, body)
	off, err := newTestSource(t, config.GloTEC{}, srv, nil).Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll with the grid disabled returned an unexpected error: %v", err)
	}
	for _, s := range off.Samples {
		if s.Desc == metric.TotalElectronContent {
			t.Errorf("grid series %s was published with cfg.Grid unset; the full grid is more series than the rest of the exporter combined", key(s))
		}
	}

	// The fixture is 24 latitudes by 12 longitudes.
	for step, wantSeries := range map[int]int{1: 288, 5: 15, 10: 6} {
		srv, _ := gridServer(t, body)
		on, err := newTestSource(t, config.GloTEC{Grid: true, GridStep: step}, srv, nil).Poll(context.Background())
		if err != nil {
			t.Fatalf("Poll with grid step %d returned an unexpected error: %v", step, err)
		}
		var got int
		for _, s := range on.Samples {
			if s.Desc == metric.TotalElectronContent {
				got++
			}
		}
		if got != wantSeries {
			t.Errorf("grid step %d produced %d series, want %d", step, got, wantSeries)
		}
	}
}

func TestGridCellsAreLabelledWithTheirOwnCoordinates(t *testing.T) {
	srv, _ := gridServer(t, fixture(t, "grid.geojson"))
	src := newTestSource(t, config.GloTEC{Grid: true, GridStep: 10}, srv, nil)

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll returned an unexpected error: %v", err)
	}
	byKey := index(t, batch)

	// The corners of the step-10 selection, from the recorded fixture.
	want := map[string]float64{
		"solar_total_electron_content_tecu{-88.75,-177.5}": 5.991683217188725,
		"solar_total_electron_content_tecu{-88.75,122.5}":  5.958353498288515,
		"solar_total_electron_content_tecu{61.25,-177.5}":  5.26924162864808,
		"solar_total_electron_content_tecu{61.25,122.5}":   6.93541980641454,
	}
	for k, wantValue := range want {
		got, ok := byKey[k]
		if !ok {
			t.Errorf("grid series %s is missing from the batch", k)
			continue
		}
		if !nearlyEqual(got.Value, wantValue) {
			t.Errorf("grid series %s is %v, want %v", k, got.Value, wantValue)
		}
	}
}

func TestEnablingTheFullGridLogsAWarningNamingTheSeriesCount(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))

	if _, err := New(config.GloTEC{Grid: true, GridStep: 10}, testClient(t), log); err != nil {
		t.Fatalf("New returned an unexpected error: %v", err)
	}

	out := buf.String()
	if !strings.Contains(out, "projected_series") {
		t.Errorf("enabling the full grid logged no series count; %q", out)
	}
	// 72 latitudes and 72 longitudes stepped by 10 is 8 by 8.
	if !strings.Contains(out, "projected_series=64") {
		t.Errorf("the warning does not name the projected count of 64; %q", out)
	}
}

func TestSourceDataAgeIsComputedFromThePayloadTimestampNotTheWallClock(t *testing.T) {
	srv, _ := gridServer(t, fixture(t, "grid.geojson"))
	src := newTestSource(t, config.GloTEC{}, srv, nil)

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll returned an unexpected error: %v", err)
	}

	age, ok := index(t, batch)["solar_source_data_age_seconds{"+Name+"}"]
	if !ok {
		t.Fatal("the batch carries no source data age; a source that cannot report its own staleness hides the dominant failure mode of these upstreams")
	}
	// pollTime is 15:40 and the grid's own time_tag is 15:15.
	if want := 1500.0; !nearlyEqual(age.Value, want) {
		t.Errorf("source data age is %v seconds, want %v", age.Value, want)
	}
	if !age.Time.Equal(pollTime) {
		t.Errorf("the age sample is stamped %s, want the fetch time %s", age.Time, pollTime)
	}
}

func TestScheduleIsTheModelsOwnTenMinuteCadence(t *testing.T) {
	src, err := New(config.GloTEC{}, testClient(t), quietLogger())
	if err != nil {
		t.Fatalf("New returned an unexpected error: %v", err)
	}
	if got, want := src.Schedule().Interval(), 10*time.Minute; got != want {
		t.Errorf("the schedule interval is %s, want %s", got, want)
	}
	if got, want := src.Interval(), src.Schedule().Interval(); got != want {
		t.Errorf("Interval is %s but the schedule reports %s; the two must never disagree", got, want)
	}
	next := src.Schedule().NextAfter(pollTime)
	if want := pollTime.Add(10 * time.Minute); !next.Equal(want) {
		t.Errorf("NextAfter(%s) is %s, want %s", pollTime, next, want)
	}
}

func TestAnUnchangedGridReportsNotModifiedRatherThanFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotModified)
	}))
	defer srv.Close()

	src := newTestSource(t, config.GloTEC{}, srv, nil)
	batch, err := src.Poll(context.Background())
	if !errors.Is(err, source.ErrNotModified) {
		t.Errorf("Poll returned %v, want source.ErrNotModified; a 2.5 MB file answered 304 is the whole point of the conditional request", err)
	}
	if batch.Len() != 0 {
		t.Errorf("a not-modified poll produced %d samples, want none", batch.Len())
	}
}

func TestAMissingGridFileIsNotRetried(t *testing.T) {
	var (
		mu     sync.Mutex
		counts = map[string]int{}
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		counts[r.URL.Path]++
		mu.Unlock()
		http.NotFound(w, r)
	}))
	defer srv.Close()

	src := newTestSource(t, config.GloTEC{}, srv, nil)
	if _, err := src.Poll(context.Background()); err == nil {
		t.Error("Poll succeeded with every candidate 404ing, want an error")
	}

	mu.Lock()
	defer mu.Unlock()
	for path, n := range counts {
		if n != 1 {
			t.Errorf("%s was requested %d times; a 404 slot will 404 identically next time, so retrying it only adds load", path, n)
		}
	}
	// Four constructed candidates plus the index fallback.
	if want := probeSlots + 1; len(counts) != want {
		t.Errorf("%d distinct paths were requested, want %d", len(counts), want)
		for path := range counts {
			t.Logf("  %s", path)
		}
	}
}

func TestARefusalForBeingTooFrequentReportsRateLimited(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "1")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	src := newTestSource(t, config.GloTEC{}, srv, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	batch, err := src.Poll(ctx)
	if !errors.Is(err, source.ErrRateLimited) {
		t.Errorf("Poll returned %v, want source.ErrRateLimited so the scheduler backs off rather than retrying", err)
	}
	if batch.Len() != 0 {
		t.Errorf("a rate-limited poll produced %d samples, want none", batch.Len())
	}
}

func TestMalformedJSONErrorsRatherThanPanicking(t *testing.T) {
	for name, body := range map[string]string{
		"truncated object":      `{"type":"FeatureCollection","features":[`,
		"an array, not a doc":   `[1,2,3]`,
		"no time_tag":           `{"type":"FeatureCollection","features":[{"geometry":{"coordinates":[0,0]},"properties":{"tec":1}}]}`,
		"an unreadable time":    `{"time_tag":"yesterday","features":[{"geometry":{"coordinates":[0,0]},"properties":{"tec":1}}]}`,
		"no features":           `{"time_tag":"2026-09-09T15:15:00Z","features":[]}`,
		"features with no geom": `{"time_tag":"2026-09-09T15:15:00Z","features":[{"properties":{"tec":1}}]}`,
	} {
		srv, _ := gridServer(t, []byte(body))
		src := newTestSource(t, config.GloTEC{Points: map[string]string{"p": "0,0"}}, srv, nil)

		batch, err := src.Poll(context.Background())
		if err == nil {
			t.Errorf("Poll succeeded on %s, want an error", name)
			continue
		}
		if batch.Len() != 0 {
			t.Errorf("%s produced %d samples on a failed poll, want none", name, batch.Len())
		}
		if strings.Contains(err.Error(), "panic") {
			t.Errorf("%s panicked instead of erroring: %v", name, err)
		}
	}
}

func TestPollHonoursContextCancellation(t *testing.T) {
	// The server blocks until its request context is cancelled, so a Poll
	// that ignored ctx would hang here rather than fail.
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer srv.Close()

	src := newTestSource(t, config.GloTEC{}, srv, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, err := src.Poll(ctx); err == nil {
			t.Error("Poll succeeded with a cancelled context, want an error")
		}
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Poll did not return within five seconds of a cancelled context")
	}
}

func TestTheIndexIsReadOnlyWhenEveryConstructedFilenameIsMissing(t *testing.T) {
	// The index is half a megabyte. Fetching it on every poll to learn a
	// filename that is entirely predictable is what this fallback exists to
	// avoid, so the test asserts it is not touched on the happy path and is
	// touched when construction fails.
	body := fixture(t, "grid.geojson")
	oldFilename := "glotec_icao_20260909T140500Z.geojson"

	var (
		mu     sync.Mutex
		counts = map[string]int{}
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		counts[r.URL.Path]++
		mu.Unlock()

		switch r.URL.Path {
		case indexPath:
			_, _ = io.WriteString(w, `[
			  {"url":"/products/glotec/geojson_2d_urt/glotec_icao_20260909T135500Z.geojson","time_tag":"2026-09-09T13:55:00Z"},
			  {"url":"/products/glotec/geojson_2d_urt/`+oldFilename+`","time_tag":"2026-09-09T14:05:00Z"}
			]`)
		case gridDir + oldFilename:
			_, _ = w.Write(body)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	src := newTestSource(t, config.GloTEC{Points: map[string]string{"london": "51.5,-0.1"}}, srv, nil)
	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll did not fall back to the index: %v", err)
	}
	if batch.Len() == 0 {
		t.Error("the index fallback produced no samples")
	}

	mu.Lock()
	defer mu.Unlock()
	if counts[indexPath] != 1 {
		t.Errorf("the index was fetched %d times, want exactly once", counts[indexPath])
	}
	if counts[gridDir+oldFilename] != 1 {
		t.Errorf("the grid named by the index was fetched %d times, want once", counts[gridDir+oldFilename])
	}
}

func TestTheIndexIsNotFetchedOnTheHappyPath(t *testing.T) {
	srv, requests := gridServer(t, fixture(t, "grid.geojson"))
	src := newTestSource(t, config.GloTEC{}, srv, nil)

	if _, err := src.Poll(context.Background()); err != nil {
		t.Fatalf("Poll returned an unexpected error: %v", err)
	}

	counts := requests()
	if n := counts[indexPath]; n != 0 {
		t.Errorf("the 522 KB directory index was fetched %d times on a poll whose constructed filename worked", n)
	}
	if n := counts[gridDir+wantedFilename]; n != 1 {
		t.Errorf("the constructed filename was fetched %d times, want once", n)
	}
}

func TestASecondPollWithNoNewSlotCostsNoRequest(t *testing.T) {
	srv, requests := gridServer(t, fixture(t, "grid.geojson"))
	src := newTestSource(t, config.GloTEC{}, srv, nil)

	if _, err := src.Poll(context.Background()); err != nil {
		t.Fatalf("the first poll failed: %v", err)
	}
	before := requests()[gridDir+wantedFilename]

	// The clock has not moved, so the newest constructible slot is the one
	// already held. Asking for 2.5 MB again to be told it is unchanged is a
	// request that need never be made.
	if _, err := src.Poll(context.Background()); !errors.Is(err, source.ErrNotModified) {
		t.Errorf("the second poll returned %v, want source.ErrNotModified", err)
	}
	if after := requests()[gridDir+wantedFilename]; after != before {
		t.Errorf("the second poll made %d further request(s) for a slot already held", after-before)
	}
}

func TestGridFilenamesAreBuiltForThePublishedFiveMinuteOffsetSlots(t *testing.T) {
	// Files land on :05, :15, :25, :35, :45 and :55, so rounding down is not
	// a plain truncation to ten minutes.
	cases := map[string]string{
		"2026-09-09T15:15:00Z": "2026-09-09T15:15:00Z",
		"2026-09-09T15:19:59Z": "2026-09-09T15:15:00Z",
		"2026-09-09T15:25:00Z": "2026-09-09T15:25:00Z",
		"2026-09-09T15:05:00Z": "2026-09-09T15:05:00Z",
		"2026-09-09T15:00:00Z": "2026-09-09T14:55:00Z",
		"2026-09-09T15:04:59Z": "2026-09-09T14:55:00Z",
		"2026-09-09T00:02:00Z": "2026-09-08T23:55:00Z",
	}
	for in, want := range cases {
		at, err := time.Parse(time.RFC3339, in)
		if err != nil {
			t.Fatalf("bad test input %q: %v", in, err)
		}
		wantAt, err := time.Parse(time.RFC3339, want)
		if err != nil {
			t.Fatalf("bad test expectation %q: %v", want, err)
		}
		if got := floorSlot(at); !got.Equal(wantAt) {
			t.Errorf("floorSlot(%s) is %s, want %s", in, got.Format(time.RFC3339), want)
		}
	}

	at, _ := time.Parse(time.RFC3339, "2026-09-09T15:15:00Z")
	if got := gridFilename(at); got != wantedFilename {
		t.Errorf("gridFilename is %q, want %q", got, wantedFilename)
	}
}

func TestANullValueProducesNoSampleRatherThanAZero(t *testing.T) {
	// A TEC of zero published as a fact is a different claim from no value
	// being available.
	body := `{"time_tag":"2026-09-09T15:15:00Z","features":[
	  {"geometry":{"coordinates":[0,0]},"properties":{"tec":12.5,"anomaly":null,"hmF2":null,"NmF2":null,"quality_flag":4}}
	]}`
	srv, _ := gridServer(t, []byte(body))
	src := newTestSource(t, config.GloTEC{Points: map[string]string{"origin": "0,0"}}, srv, nil)

	batch, err := src.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll returned an unexpected error: %v", err)
	}
	byKey := index(t, batch)

	if got, ok := byKey["solar_total_electron_content_tecu{origin}"]; !ok {
		t.Error("the TEC value was dropped along with the null fields")
	} else if !nearlyEqual(got.Value, 12.5) {
		t.Errorf("TEC is %v, want 12.5", got.Value)
	}
	for _, desc := range []*metric.Descriptor{metric.TECAnomaly, metric.PeakHeightF2, metric.PeakDensityF2} {
		if _, ok := byKey[desc.FullName()+"{origin}"]; ok {
			t.Errorf("%s was published from a JSON null", desc.FullName())
		}
	}
}

func TestTheRecordedFixturesExistAndSpanEveryLatitudeBand(t *testing.T) {
	g, err := parseGrid(fixture(t, "grid.geojson"))
	if err != nil {
		t.Fatalf("the recorded grid fixture does not parse: %v", err)
	}
	if want := 288; len(g.cells) != want {
		t.Errorf("the recorded grid has %d cells, want %d", len(g.cells), want)
	}
	if !g.timeTag.Equal(gridTimeTag) {
		t.Errorf("the recorded grid's time_tag is %s, want %s", g.timeTag, gridTimeTag)
	}

	seen := map[string]bool{}
	for _, band := range latitudeBands {
		for _, c := range g.cells {
			if c.lat >= band.from && (c.lat < band.to || (band.to == 90 && c.lat <= 90)) {
				seen[band.label] = true
			}
		}
	}
	if len(seen) != len(latitudeBands) {
		t.Errorf("the recorded grid covers %d of %d latitude bands; a truncated fixture would make the band tests vacuous",
			len(seen), len(latitudeBands))
	}
}
