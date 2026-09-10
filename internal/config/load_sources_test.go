package config

import (
	"strings"
	"testing"
	"time"
)

// sourceEnabledByDefault is every source's enable setting and the default it
// must have.
//
// This table is the licence-safety property of the whole package written down.
// A source whose data carries a non-commercial or courtesy licence must be
// something an operator opts into knowingly, so flipping one of these false
// entries to true is a decision somebody has to make deliberately and in a
// diff, not something that can drift in as a side effect of adding a source.
var sourceEnabledByDefault = map[string]bool{
	// Public domain or openly licensed: on by default.
	"HAMQSL_ENABLED":              true,
	"SWPC_ENABLED":                true,
	"SOLAR_PROBABILITIES_ENABLED": true,
	"GLOTEC_ENABLED":              true,
	"ISWA_ENABLED":                true,
	"DONKI_ENABLED":               true,
	"USGS_GEOMAG_ENABLED":         true,
	"GFZ_ENABLED":                 true,
	"DRAO_ENABLED":                true,
	"LASP_ENABLED":                true,
	"FMI_ENABLED":                 true,
	"LOTW_ENABLED":                true,
	"POTA_ENABLED":                true,
	"CELESTRAK_ENABLED":           true,

	// Restricted licences: off by default.
	"KC2G_ENABLED":        false,
	"WSPR_LIVE_ENABLED":   false,
	"PSKREPORTER_ENABLED": false,
	"NMDB_ENABLED":        false,
	"INTERMAGNET_ENABLED": false,
	"SILSO_ENABLED":       false,

	// Off by default for want of a target rather than a licence.
	"KIWISDR_ENABLED": false,
}

// enabledFlag reads a source's resolved Enabled field by its setting name, so
// the table above can be checked against the struct fields it describes.
func enabledFlag(t *testing.T, cfg *Config, setting string) bool {
	t.Helper()
	switch setting {
	case "HAMQSL_ENABLED":
		return cfg.Hamqsl.Enabled
	case "SWPC_ENABLED":
		return cfg.SWPC.Enabled
	case "KC2G_ENABLED":
		return cfg.KC2G.Enabled
	case "SOLAR_PROBABILITIES_ENABLED":
		return cfg.SolarProbabilities.Enabled
	case "GLOTEC_ENABLED":
		return cfg.GloTEC.Enabled
	case "ISWA_ENABLED":
		return cfg.ISWA.Enabled
	case "DONKI_ENABLED":
		return cfg.DONKI.Enabled
	case "USGS_GEOMAG_ENABLED":
		return cfg.USGSGeomag.Enabled
	case "GFZ_ENABLED":
		return cfg.GFZ.Enabled
	case "DRAO_ENABLED":
		return cfg.DRAO.Enabled
	case "LASP_ENABLED":
		return cfg.LASP.Enabled
	case "FMI_ENABLED":
		return cfg.FMI.Enabled
	case "KIWISDR_ENABLED":
		return cfg.KiwiSDR.Enabled
	case "LOTW_ENABLED":
		return cfg.LoTW.Enabled
	case "POTA_ENABLED":
		return cfg.POTA.Enabled
	case "CELESTRAK_ENABLED":
		return cfg.Celestrak.Enabled
	case "WSPR_LIVE_ENABLED":
		return cfg.WSPRLive.Enabled
	case "PSKREPORTER_ENABLED":
		return cfg.PSKReporter.Enabled
	case "NMDB_ENABLED":
		return cfg.NMDB.Enabled
	case "INTERMAGNET_ENABLED":
		return cfg.INTERMAGNET.Enabled
	case "SILSO_ENABLED":
		return cfg.SILSO.Enabled
	default:
		t.Fatalf("no Enabled field is wired up for %s%s", EnvPrefix, setting)
		return false
	}
}

// allSourcesOff returns an environment that disables every source, plus any
// extra settings the caller wants.
func allSourcesOff(extra map[string]string) map[string]string {
	env := map[string]string{}
	for setting := range sourceEnabledByDefault {
		env[setting] = "false"
	}
	for k, v := range extra {
		env[k] = v
	}
	return env
}

// TestExactlyTheUnrestrictedSourcesAreEnabledByDefault is the test that must
// not regress: an operator who configures nothing must not end up collecting
// data under a licence they never read.
func TestExactlyTheUnrestrictedSourcesAreEnabledByDefault(t *testing.T) {
	cfg, err := loadEnv(t, nil, nil)
	if err != nil {
		t.Fatalf("Load = %v, want a valid configuration", err)
	}

	for setting, want := range sourceEnabledByDefault {
		t.Run(setting, func(t *testing.T) {
			if got := enabledFlag(t, cfg, setting); got != want {
				t.Fatalf("%s%s defaulted to %v, want %v", EnvPrefix, setting, got, want)
			}
		})
	}
}

// TestNoRestrictedSourceIsEnabledByDefault checks the same property through the
// list startup logs, so a new restricted source that is wired into
// RestrictedSources but defaulted on is caught here too.
func TestNoRestrictedSourceIsEnabledByDefault(t *testing.T) {
	cfg, err := loadEnv(t, nil, nil)
	if err != nil {
		t.Fatalf("Load = %v", err)
	}
	if got := cfg.RestrictedSources(); len(got) != 0 {
		t.Fatalf("RestrictedSources = %v with no configuration, want none", got)
	}
}

// TestEnablingARestrictedSourceIsReportedToTheOperator covers the other half:
// once opted into, the licence is named in a startup note.
func TestEnablingARestrictedSourceIsReportedToTheOperator(t *testing.T) {
	cfg, err := loadEnv(t, map[string]string{
		"NMDB_ENABLED":  "true",
		"SILSO_ENABLED": "true",
	}, nil)
	if err != nil {
		t.Fatalf("Load = %v", err)
	}

	got := strings.Join(cfg.RestrictedSources(), "\n")
	for _, want := range []string{"nmdb.eu", "non-commercial", "silso", "CC BY-NC"} {
		if !strings.Contains(got, want) {
			t.Fatalf("RestrictedSources = %q, want it to mention %q", got, want)
		}
	}
}

func TestPostReleaseSourceDefaultsAreTheDocumentedOnes(t *testing.T) {
	cfg, err := loadEnv(t, nil, nil)
	if err != nil {
		t.Fatalf("Load = %v", err)
	}

	// Region numbers churn as active regions rotate across the disc, so the
	// per-region series set is opt-in even though its source is on.
	if cfg.SolarProbabilities.Regions {
		t.Fatal("per-region flare probabilities defaulted to enabled")
	}

	if cfg.GloTEC.Points != nil || !cfg.GloTEC.LatitudeBands ||
		cfg.GloTEC.Grid || cfg.GloTEC.GridStep != 10 {
		t.Fatalf("glotec defaults = %+v", cfg.GloTEC)
	}

	if cfg.ISWA.BaseURL != "https://iswa.gsfc.nasa.gov/hapi" ||
		cfg.ISWA.FastInterval != time.Minute ||
		len(cfg.ISWA.Spacecraft) != 1 || cfg.ISWA.Spacecraft[0] != "swpc" ||
		cfg.ISWA.Timeout != 20*time.Second || cfg.ISWA.Retries != 2 {
		t.Fatalf("iswa defaults = %+v", cfg.ISWA)
	}

	if cfg.DONKI.BaseURL != "https://webtools.ccmc.gsfc.nasa.gov/DONKI/WS/get" ||
		cfg.DONKI.Interval != 30*time.Minute || cfg.DONKI.Window != 72*time.Hour ||
		cfg.DONKI.Timeout != 20*time.Second || cfg.DONKI.Retries != 2 {
		t.Fatalf("donki defaults = %+v", cfg.DONKI)
	}

	if cfg.USGSGeomag.BaseURL != "https://geomag.usgs.gov" ||
		strings.Join(cfg.USGSGeomag.Observatories, ",") != "BOU,FRD,NEW,TUC,SIT" ||
		cfg.USGSGeomag.Interval != 5*time.Minute ||
		cfg.USGSGeomag.Timeout != 20*time.Second || cfg.USGSGeomag.Retries != 2 {
		t.Fatalf("usgs geomag defaults = %+v", cfg.USGSGeomag)
	}

	if cfg.GFZ.BaseURL != "https://kp.gfz.de" || !cfg.GFZ.HalfHourly ||
		cfg.GFZ.Interval != 10*time.Minute ||
		cfg.GFZ.Timeout != 20*time.Second || cfg.GFZ.Retries != 2 {
		t.Fatalf("gfz defaults = %+v", cfg.GFZ)
	}

	if cfg.DRAO.BaseURL != "https://www.spaceweather.gc.ca" ||
		cfg.DRAO.Timeout != 30*time.Second || cfg.DRAO.Retries != 2 {
		t.Fatalf("drao defaults = %+v", cfg.DRAO)
	}

	if cfg.LASP.BaseURL != "https://lasp.colorado.edu" || !cfg.LASP.EVE ||
		!cfg.LASP.LISIRD || cfg.LASP.Timeout != 20*time.Second || cfg.LASP.Retries != 2 {
		t.Fatalf("lasp defaults = %+v", cfg.LASP)
	}

	if cfg.FMI.BaseURL != "https://space.fmi.fi" || cfg.FMI.Interval != 5*time.Minute ||
		cfg.FMI.Timeout != 20*time.Second || cfg.FMI.Retries != 2 {
		t.Fatalf("fmi defaults = %+v", cfg.FMI)
	}

	if cfg.KiwiSDR.Receivers != nil || cfg.KiwiSDR.Interval != 30*time.Minute ||
		cfg.KiwiSDR.Timeout != 15*time.Second || cfg.KiwiSDR.Retries != 1 {
		t.Fatalf("kiwisdr defaults = %+v", cfg.KiwiSDR)
	}

	// The activity file is 6 MB, which is why its timeout is minutes rather
	// than the twenty seconds every other source gets.
	if cfg.LoTW.URL != "https://lotw.arrl.org/lotw-user-activity.csv" ||
		cfg.LoTW.Timeout != 2*time.Minute || cfg.LoTW.Retries != 1 {
		t.Fatalf("lotw defaults = %+v", cfg.LoTW)
	}

	if cfg.POTA.BaseURL != "https://api.pota.app" || cfg.POTA.Interval != 60*time.Second ||
		cfg.POTA.Timeout != 20*time.Second || cfg.POTA.Retries != 2 {
		t.Fatalf("pota defaults = %+v", cfg.POTA)
	}

	if cfg.Celestrak.BaseURL != "https://celestrak.org" || cfg.Celestrak.Group != "amateur" ||
		cfg.Celestrak.Timeout != 30*time.Second || cfg.Celestrak.Retries != 1 {
		t.Fatalf("celestrak defaults = %+v", cfg.Celestrak)
	}

	if cfg.WSPRLive.BaseURL != "https://db1.wspr.live" ||
		cfg.WSPRLive.Interval != 5*time.Minute || cfg.WSPRLive.Window != 15*time.Minute ||
		cfg.WSPRLive.Timeout != 20*time.Second || cfg.WSPRLive.Retries != 2 {
		t.Fatalf("wspr.live defaults = %+v", cfg.WSPRLive)
	}

	if cfg.PSKReporter.BaseURL != "https://pskreporter.info" ||
		cfg.PSKReporter.Grids != nil || cfg.PSKReporter.Contact != "" ||
		cfg.PSKReporter.Interval != 5*time.Minute ||
		cfg.PSKReporter.Timeout != 20*time.Second || cfg.PSKReporter.Retries != 1 {
		t.Fatalf("pskreporter defaults = %+v", cfg.PSKReporter)
	}

	if cfg.NMDB.BaseURL != "https://www.nmdb.eu" ||
		strings.Join(cfg.NMDB.Stations, ",") != "OULU,KIEL2,SOPO" ||
		cfg.NMDB.Interval != 10*time.Minute ||
		cfg.NMDB.Timeout != 30*time.Second || cfg.NMDB.Retries != 1 {
		t.Fatalf("nmdb defaults = %+v", cfg.NMDB)
	}

	if cfg.INTERMAGNET.BaseURL != "https://imag-data.bgs.ac.uk" ||
		strings.Join(cfg.INTERMAGNET.Observatories, ",") != "BOU,FRD,CLF,HRN,BEL" ||
		cfg.INTERMAGNET.Interval != 5*time.Minute ||
		cfg.INTERMAGNET.Timeout != 30*time.Second || cfg.INTERMAGNET.Retries != 2 {
		t.Fatalf("intermagnet defaults = %+v", cfg.INTERMAGNET)
	}

	if cfg.SILSO.BaseURL != "https://www.sidc.be" ||
		cfg.SILSO.Timeout != 20*time.Second || cfg.SILSO.Retries != 2 {
		t.Fatalf("silso defaults = %+v", cfg.SILSO)
	}
}

// TestEnvironmentOverridesEveryPostReleaseAccessor exercises one setting of
// each accessor type per source group: bool, string, int, duration, slice and
// map all have to be wired to the right name.
func TestEnvironmentOverridesEveryPostReleaseAccessor(t *testing.T) {
	cfg, err := loadEnv(t, map[string]string{
		"SOLAR_PROBABILITIES_REGIONS": "true",
		"GLOTEC_POINTS":               "Boulder=40.015,-105.271",
		"GLOTEC_LATITUDE_BANDS":       "false",
		"GLOTEC_GRID_STEP":            "5",
		"ISWA_BASE_URL":               "http://iswa.internal/hapi",
		"ISWA_FAST_INTERVAL":          "2m",
		"ISWA_SPACECRAFT":             "ace, wind",
		"DONKI_INTERVAL":              "1h",
		"DONKI_WINDOW":                "168h",
		"USGS_GEOMAG_OBSERVATORIES":   "BOU",
		"GFZ_HALF_HOURLY":             "false",
		"DRAO_TIMEOUT":                "45s",
		"LASP_LISIRD":                 "false",
		"FMI_INTERVAL":                "90s",
		"KIWISDR_ENABLED":             "true",
		"KIWISDR_RECEIVERS":           "shack=http://localhost:8073",
		"LOTW_RETRIES":                "3",
		"POTA_INTERVAL":               "45s",
		"CELESTRAK_GROUP":             "weather",
		"WSPR_LIVE_ENABLED":           "true",
		"WSPR_LIVE_WINDOW":            "30m",
		"PSKREPORTER_ENABLED":         "true",
		"PSKREPORTER_CONTACT":         "operator@example.org",
		"PSKREPORTER_GRIDS":           "FN20,IO91",
		"NMDB_STATIONS":               "OULU",
		"INTERMAGNET_INTERVAL":        "2m",
		"SILSO_RETRIES":               "0",
	}, nil)
	if err != nil {
		t.Fatalf("Load = %v, want a valid configuration", err)
	}

	if !cfg.SolarProbabilities.Regions {
		t.Fatal("SOLAR_PROBABILITIES_REGIONS did not take effect")
	}
	if cfg.GloTEC.Points["Boulder"] != "40.015,-105.271" || cfg.GloTEC.LatitudeBands ||
		cfg.GloTEC.GridStep != 5 {
		t.Fatalf("glotec = %+v", cfg.GloTEC)
	}
	if cfg.ISWA.BaseURL != "http://iswa.internal/hapi" || cfg.ISWA.FastInterval != 2*time.Minute ||
		strings.Join(cfg.ISWA.Spacecraft, ",") != "ace,wind" {
		t.Fatalf("iswa = %+v", cfg.ISWA)
	}
	if cfg.DONKI.Interval != time.Hour || cfg.DONKI.Window != 168*time.Hour {
		t.Fatalf("donki = %+v", cfg.DONKI)
	}
	if strings.Join(cfg.USGSGeomag.Observatories, ",") != "BOU" {
		t.Fatalf("usgs observatories = %v", cfg.USGSGeomag.Observatories)
	}
	if cfg.GFZ.HalfHourly {
		t.Fatal("GFZ_HALF_HOURLY did not take effect")
	}
	if cfg.DRAO.Timeout != 45*time.Second {
		t.Fatalf("drao timeout = %s", cfg.DRAO.Timeout)
	}
	if cfg.LASP.LISIRD || !cfg.LASP.EVE {
		t.Fatalf("lasp = %+v", cfg.LASP)
	}
	if cfg.FMI.Interval != 90*time.Second {
		t.Fatalf("fmi interval = %s", cfg.FMI.Interval)
	}
	if !cfg.KiwiSDR.Enabled || cfg.KiwiSDR.Receivers["shack"] != "http://localhost:8073" {
		t.Fatalf("kiwisdr = %+v", cfg.KiwiSDR)
	}
	if cfg.LoTW.Retries != 3 {
		t.Fatalf("lotw retries = %d", cfg.LoTW.Retries)
	}
	if cfg.POTA.Interval != 45*time.Second {
		t.Fatalf("pota interval = %s", cfg.POTA.Interval)
	}
	if cfg.Celestrak.Group != "weather" {
		t.Fatalf("celestrak group = %q", cfg.Celestrak.Group)
	}
	if !cfg.WSPRLive.Enabled || cfg.WSPRLive.Window != 30*time.Minute {
		t.Fatalf("wspr.live = %+v", cfg.WSPRLive)
	}
	if !cfg.PSKReporter.Enabled || cfg.PSKReporter.Contact != "operator@example.org" ||
		strings.Join(cfg.PSKReporter.Grids, ",") != "FN20,IO91" {
		t.Fatalf("pskreporter = %+v", cfg.PSKReporter)
	}
	if strings.Join(cfg.NMDB.Stations, ",") != "OULU" {
		t.Fatalf("nmdb stations = %v", cfg.NMDB.Stations)
	}
	if cfg.INTERMAGNET.Interval != 2*time.Minute {
		t.Fatalf("intermagnet interval = %s", cfg.INTERMAGNET.Interval)
	}
	if cfg.SILSO.Retries != 0 {
		t.Fatalf("silso retries = %d", cfg.SILSO.Retries)
	}
}

// TestPSKReporterRequiresAContactAddress covers a rule that exists because its
// upstream asked for it in writing, not because it is tidy.
func TestPSKReporterRequiresAContactAddress(t *testing.T) {
	_, err := loadEnv(t, map[string]string{"PSKREPORTER_ENABLED": "true"}, nil)
	assertProblem(t, err, EnvPrefix+"PSKREPORTER_CONTACT", "contact address", "required")

	// Whitespace is not a contact address.
	_, err = loadEnv(t, map[string]string{
		"PSKREPORTER_ENABLED": "true",
		"PSKREPORTER_CONTACT": "   ",
	}, nil)
	assertProblem(t, err, EnvPrefix+"PSKREPORTER_CONTACT", "required")

	if _, err := loadEnv(t, map[string]string{
		"PSKREPORTER_ENABLED": "true",
		"PSKREPORTER_CONTACT": "operator@example.org",
	}, nil); err != nil {
		t.Fatalf("Load with a contact = %v, want a valid configuration", err)
	}

	// Disabled, the setting is nobody's business.
	if _, err := loadEnv(t, nil, nil); err != nil {
		t.Fatalf("Load with pskreporter off = %v", err)
	}
}

// TestKiwiSDREnabledWithoutAReceiverIsAnError covers the source that cannot
// have a default target, because a public KiwiSDR belongs to somebody.
func TestKiwiSDREnabledWithoutAReceiverIsAnError(t *testing.T) {
	_, err := loadEnv(t, map[string]string{"KIWISDR_ENABLED": "true"}, nil)
	assertProblem(t, err, EnvPrefix+"KIWISDR_RECEIVERS", "no receiver")

	if _, err := loadEnv(t, map[string]string{
		"KIWISDR_ENABLED":   "true",
		"KIWISDR_RECEIVERS": "shack=http://localhost:8073",
	}, nil); err != nil {
		t.Fatalf("Load with a receiver = %v, want a valid configuration", err)
	}
}

func TestAKiwiSDRReceiverMustBeAFetchableURL(t *testing.T) {
	_, err := loadEnv(t, map[string]string{
		"KIWISDR_ENABLED":   "true",
		"KIWISDR_RECEIVERS": "shack=localhost:8073",
	}, nil)
	assertProblem(t, err, EnvPrefix+"KIWISDR_RECEIVERS", "shack", "http://")
}

// TestAMalformedGloTECPointNamesTheKeyItCameFrom matters because an operator
// with six sample points needs to know which one to fix.
func TestAMalformedGloTECPointNamesTheKeyItCameFrom(t *testing.T) {
	cases := []struct {
		name      string
		points    string
		fragments []string
	}{
		{
			name:      "no comma at all",
			points:    "Boulder=40.015",
			fragments: []string{"Boulder", "no comma"},
		},
		{
			name:      "a latitude that is not a number",
			points:    "Ottawa=north,-75.7",
			fragments: []string{"Ottawa", "latitude is not a number"},
		},
		{
			name:      "a longitude that is not a number",
			points:    "Ottawa=45.4,west",
			fragments: []string{"Ottawa", "longitude is not a number"},
		},
		{
			name:      "a latitude beyond the pole",
			points:    "Nowhere=91,0",
			fragments: []string{"Nowhere", "latitude is outside -90..90"},
		},
		{
			name:      "a longitude past the antimeridian",
			points:    "Nowhere=0,181",
			fragments: []string{"Nowhere", "longitude is outside -180..180"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := loadEnv(t, map[string]string{"GLOTEC_POINTS": tc.points}, nil)
			assertProblem(t, err, append([]string{EnvPrefix + "GLOTEC_POINTS"}, tc.fragments...)...)
		})
	}
}

func TestWellFormedGloTECPointsAreAccepted(t *testing.T) {
	cfg, err := loadEnv(t, map[string]string{
		"GLOTEC_POINTS": "Boulder=40.015,-105.271; Ottawa = 45.42 , -75.69 ;Pole=-90,180",
	}, nil)
	if err != nil {
		t.Fatalf("Load = %v, want a valid configuration", err)
	}
	if len(cfg.GloTEC.Points) != 3 {
		t.Fatalf("points = %v, want three", cfg.GloTEC.Points)
	}
}

// TestEveryMalformedGloTECPointIsReported is the accumulation property applied
// to a single setting: fixing one typo should not reveal the next.
func TestEveryMalformedGloTECPointIsReported(t *testing.T) {
	_, err := loadEnv(t, map[string]string{
		"GLOTEC_POINTS": "Alpha=91,0;Beta=0,181",
	}, nil)
	assertProblem(t, err, "Alpha", "latitude is outside")
	assertProblem(t, err, "Beta", "longitude is outside")
}

// TestTheGloTECFullGridIsPermittedButNoted covers a setting that is legitimate
// and expensive: an error would be wrong, and silence would be worse.
func TestTheGloTECFullGridIsPermittedButNoted(t *testing.T) {
	cfg, err := loadEnv(t, map[string]string{
		"GLOTEC_GRID":      "true",
		"GLOTEC_GRID_STEP": "1",
	}, nil)
	if err != nil {
		t.Fatalf("Load = %v, want the full grid to be permitted", err)
	}

	notes := strings.Join(cfg.Notes(), "\n")
	if !strings.Contains(notes, EnvPrefix+"GLOTEC_GRID_STEP") ||
		!strings.Contains(notes, "tens of thousands of series") {
		t.Fatalf("notes = %q, want a warning about the series count", notes)
	}
}

func TestASubsampledGloTECGridIsNotNoted(t *testing.T) {
	cfg, err := loadEnv(t, map[string]string{"GLOTEC_GRID": "true"}, nil)
	if err != nil {
		t.Fatalf("Load = %v", err)
	}
	if len(cfg.Notes()) != 0 {
		t.Fatalf("notes = %v, want none for the default grid step", cfg.Notes())
	}

	// The step only matters when the grid is actually being published.
	cfg, err = loadEnv(t, map[string]string{"GLOTEC_GRID_STEP": "1"}, nil)
	if err != nil {
		t.Fatalf("Load = %v", err)
	}
	if len(cfg.Notes()) != 0 {
		t.Fatalf("notes = %v, want none while the grid is off", cfg.Notes())
	}
}

// TestPostReleaseIntervalFloorsAreEnforced covers the cadences the upstreams
// asked for, which have to survive a copy-pasted compose file.
func TestPostReleaseIntervalFloorsAreEnforced(t *testing.T) {
	cases := []struct {
		name    string
		setting string
		value   string
		bounds  string
	}{
		{
			name:    "DONKI is curated by hand and gains nothing from faster polling",
			setting: "DONKI_INTERVAL",
			value:   "1m",
			bounds:  "15m0s..24h0m0s",
		},
		{
			name:    "wspr.live documents a request limit",
			setting: "WSPR_LIVE_INTERVAL",
			value:   "30s",
			bounds:  "2m0s..6h0m0s",
		},
		{
			name:    "the PSK Reporter operator asked in writing for five minutes",
			setting: "PSKREPORTER_INTERVAL",
			value:   "1m",
			bounds:  "5m0s..6h0m0s",
		},
		{
			name:    "NMDB queries MySQL per request",
			setting: "NMDB_INTERVAL",
			value:   "1m",
			bounds:  "5m0s..6h0m0s",
		},
		{
			name:    "GFZ publishes Hp30 at about eleven minutes latency",
			setting: "GFZ_INTERVAL",
			value:   "1m",
			bounds:  "5m0s..6h0m0s",
		},
		{
			name:    "KiwiSDR receivers are somebody's private machine",
			setting: "KIWISDR_INTERVAL",
			value:   "1m",
			bounds:  "5m0s..24h0m0s",
		},
		{
			name:    "POTA spots are not worth more than one poll every thirty seconds",
			setting: "POTA_INTERVAL",
			value:   "5s",
			bounds:  "30s..1h0m0s",
		},
		{
			name:    "the iSWA fast tier matches its one-minute datasets",
			setting: "ISWA_FAST_INTERVAL",
			value:   "10s",
			bounds:  "1m0s..1h0m0s",
		},
		{
			name:    "USGS geomagnetism",
			setting: "USGS_GEOMAG_INTERVAL",
			value:   "10s",
			bounds:  "1m0s..6h0m0s",
		},
		{
			name:    "INTERMAGNET",
			setting: "INTERMAGNET_INTERVAL",
			value:   "10s",
			bounds:  "1m0s..6h0m0s",
		},
		{
			name:    "FMI",
			setting: "FMI_INTERVAL",
			value:   "10s",
			bounds:  "1m0s..6h0m0s",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := loadEnv(t, map[string]string{tc.setting: tc.value}, nil)
			assertProblem(t, err, EnvPrefix+tc.setting,
				"outside the accepted range "+tc.bounds)
		})
	}
}

func TestPostReleaseRangesRejectOutOfBoundsValues(t *testing.T) {
	cases := []struct {
		name      string
		env       map[string]string
		fragments []string
	}{
		{
			name:      "a grid step below one",
			env:       map[string]string{"GLOTEC_GRID_STEP": "0"},
			fragments: []string{EnvPrefix + "GLOTEC_GRID_STEP", "outside the accepted range 1..36"},
		},
		{
			name:      "a grid step past the grid",
			env:       map[string]string{"GLOTEC_GRID_STEP": "37"},
			fragments: []string{EnvPrefix + "GLOTEC_GRID_STEP", "outside the accepted range 1..36"},
		},
		{
			name:      "a DONKI window shorter than the catalogue's own latency",
			env:       map[string]string{"DONKI_WINDOW": "1h"},
			fragments: []string{EnvPrefix + "DONKI_WINDOW", "outside the accepted range"},
		},
		{
			name:      "a DONKI window beyond a month",
			env:       map[string]string{"DONKI_WINDOW": "800h"},
			fragments: []string{EnvPrefix + "DONKI_WINDOW", "outside the accepted range"},
		},
		{
			name:      "a wspr.live window outside five to sixty minutes",
			env:       map[string]string{"WSPR_LIVE_WINDOW": "2m"},
			fragments: []string{EnvPrefix + "WSPR_LIVE_WINDOW", "outside the accepted range"},
		},
		{
			name:      "too many retries",
			env:       map[string]string{"CELESTRAK_RETRIES": "11"},
			fragments: []string{EnvPrefix + "CELESTRAK_RETRIES", "outside the accepted range 0..10"},
		},
		{
			name:      "an unparseable duration",
			env:       map[string]string{"LASP_TIMEOUT": "20"},
			fragments: []string{EnvPrefix + "LASP_TIMEOUT", "is not a duration"},
		},
		{
			name:      "an unparseable bool",
			env:       map[string]string{"GFZ_HALF_HOURLY": "yes please"},
			fragments: []string{EnvPrefix + "GFZ_HALF_HOURLY", "is not a boolean"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := loadEnv(t, tc.env, nil)
			assertProblem(t, err, tc.fragments...)
		})
	}
}

// TestAPostReleaseTimeoutMustFitInsideItsOwnInterval catches the configuration
// that turns one slow upstream into a backlog of overlapping polls.
func TestAPostReleaseTimeoutMustFitInsideItsOwnInterval(t *testing.T) {
	cases := []struct {
		timeoutName  string
		intervalName string
		env          map[string]string
	}{
		{
			timeoutName:  "ISWA_TIMEOUT",
			intervalName: "ISWA_FAST_INTERVAL",
			env:          map[string]string{"ISWA_TIMEOUT": "90s", "ISWA_FAST_INTERVAL": "1m"},
		},
		{
			timeoutName:  "POTA_TIMEOUT",
			intervalName: "POTA_INTERVAL",
			env:          map[string]string{"POTA_TIMEOUT": "45s", "POTA_INTERVAL": "30s"},
		},
		{
			timeoutName:  "INTERMAGNET_TIMEOUT",
			intervalName: "INTERMAGNET_INTERVAL",
			env: map[string]string{
				"INTERMAGNET_ENABLED":  "true",
				"INTERMAGNET_TIMEOUT":  "90s",
				"INTERMAGNET_INTERVAL": "1m",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.timeoutName, func(t *testing.T) {
			_, err := loadEnv(t, tc.env, nil)
			assertProblem(t, err, EnvPrefix+tc.timeoutName, EnvPrefix+tc.intervalName,
				"must be shorter")
		})
	}
}

// TestAPostReleaseTimeoutIsOnlyCheckedWhileTheSourceIsEnabled keeps a disabled
// source from failing startup over a setting nothing reads.
func TestAPostReleaseTimeoutIsOnlyCheckedWhileTheSourceIsEnabled(t *testing.T) {
	if _, err := loadEnv(t, map[string]string{
		"INTERMAGNET_TIMEOUT":  "90s",
		"INTERMAGNET_INTERVAL": "1m",
	}, nil); err != nil {
		t.Fatalf("Load with intermagnet disabled = %v, want a valid configuration", err)
	}
}

func TestAnEnabledSourceNeedsAFetchableURL(t *testing.T) {
	cases := []struct {
		name      string
		env       map[string]string
		fragments []string
	}{
		{
			name:      "an empty base URL",
			env:       map[string]string{"GFZ_BASE_URL": ""},
			fragments: []string{EnvPrefix + "GFZ_BASE_URL", "is not set"},
		},
		{
			name:      "a bare hostname, which parses as a relative URL",
			env:       map[string]string{"POTA_BASE_URL": "api.pota.app"},
			fragments: []string{EnvPrefix + "POTA_BASE_URL", "absolute http:// or https:// URL"},
		},
		{
			name:      "a scheme no HTTP client will fetch",
			env:       map[string]string{"LOTW_URL": "ftp://lotw.arrl.org/activity.csv"},
			fragments: []string{EnvPrefix + "LOTW_URL", "absolute http:// or https:// URL"},
		},
		{
			name:      "a scheme with no host behind it",
			env:       map[string]string{"DRAO_BASE_URL": "https:///solarflux"},
			fragments: []string{EnvPrefix + "DRAO_BASE_URL", "names no host"},
		},
		{
			name:      "a restricted source that is switched on",
			env:       map[string]string{"SILSO_ENABLED": "true", "SILSO_BASE_URL": "sidc.be"},
			fragments: []string{EnvPrefix + "SILSO_BASE_URL", "absolute http:// or https:// URL"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := loadEnv(t, tc.env, nil)
			assertProblem(t, err, tc.fragments...)
		})
	}
}

// TestADisabledSourceURLIsNotChecked is the other half: a half-filled setting
// for a source nobody enabled must not stop the process starting.
func TestADisabledSourceURLIsNotChecked(t *testing.T) {
	if _, err := loadEnv(t, map[string]string{"NMDB_BASE_URL": "nonsense"}, nil); err != nil {
		t.Fatalf("Load = %v, want a disabled source's URL to be ignored", err)
	}
}

// TestPostReleaseProblemsAreAllReportedTogether is the accumulation property
// across the new sources: six mistakes must not need six restarts to find.
func TestPostReleaseProblemsAreAllReportedTogether(t *testing.T) {
	_, err := loadEnv(t, map[string]string{
		"PSKREPORTER_ENABLED": "true",
		"KIWISDR_ENABLED":     "true",
		"GLOTEC_POINTS":       "Boulder=40.015",
		"DONKI_INTERVAL":      "1m",
		"POTA_BASE_URL":       "api.pota.app",
		"CELESTRAK_RETRIES":   "11",
	}, nil)

	got := problems(t, err)
	if len(got) < 6 {
		t.Fatalf("got %d problems, want at least six reported at once:\n%s", len(got), err)
	}

	assertProblem(t, err, EnvPrefix+"PSKREPORTER_CONTACT")
	assertProblem(t, err, EnvPrefix+"KIWISDR_RECEIVERS")
	assertProblem(t, err, EnvPrefix+"GLOTEC_POINTS", "Boulder")
	assertProblem(t, err, EnvPrefix+"DONKI_INTERVAL")
	assertProblem(t, err, EnvPrefix+"POTA_BASE_URL")
	assertProblem(t, err, EnvPrefix+"CELESTRAK_RETRIES")

	for _, p := range got {
		if !strings.Contains(p, EnvPrefix) {
			t.Fatalf("problem %q names no setting", p)
		}
	}
}

// TestStringSliceDefaultCanBeClearedByAnEmptyValue covers the difference
// between "use the curated default" and "no filter at all", which for these
// station lists is the difference between five series and none.
func TestStringSliceDefaultCanBeClearedByAnEmptyValue(t *testing.T) {
	cases := []struct {
		name  string
		set   bool
		value string
		want  []string
	}{
		{name: "unset uses the curated default", want: []string{"BOU", "FRD", "NEW", "TUC", "SIT"}},
		{name: "an empty value clears it", set: true, value: "", want: nil},
		{name: "whitespace only clears it", set: true, value: "   ", want: nil},
		{name: "commas with nothing between them clear it", set: true, value: " , , ", want: nil},
		{name: "surrounding whitespace is trimmed", set: true, value: " BOU , FRD ", want: []string{"BOU", "FRD"}},
		{name: "empty items are dropped", set: true, value: "BOU,,FRD,", want: []string{"BOU", "FRD"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := map[string]string{}
			if tc.set {
				env["USGS_GEOMAG_OBSERVATORIES"] = tc.value
			}
			cfg, err := loadEnv(t, env, nil)
			if err != nil {
				t.Fatalf("Load = %v", err)
			}
			if got := cfg.USGSGeomag.Observatories; strings.Join(got, ",") != strings.Join(tc.want, ",") {
				t.Fatalf("observatories = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestStringMapAcceptsEmptyAndWhitespaceOnlyInput covers the same distinction
// for the map settings, where an empty value means "no points" rather than an
// entry keyed on the empty string.
func TestStringMapAcceptsEmptyAndWhitespaceOnlyInput(t *testing.T) {
	for _, value := range []string{"", "   ", " ; ; "} {
		cfg, err := loadEnv(t, map[string]string{"GLOTEC_POINTS": value}, nil)
		if err != nil {
			t.Fatalf("Load with GLOTEC_POINTS=%q = %v", value, err)
		}
		if cfg.GloTEC.Points != nil {
			t.Fatalf("points = %v with GLOTEC_POINTS=%q, want none", cfg.GloTEC.Points, value)
		}
	}

	_, err := loadEnv(t, map[string]string{"GLOTEC_POINTS": "Boulder"}, nil)
	assertProblem(t, err, EnvPrefix+"GLOTEC_POINTS", "is not in key=value form")
}

// TestNoPostReleaseSourceValueReachesTheLogInFull checks that the receiver map,
// which can name somebody's home network, is logged by key only.
func TestNoPostReleaseSourceValueReachesTheLogInFull(t *testing.T) {
	const canary = "kiwi.private.example.lan"
	cfg, err := loadEnv(t, map[string]string{
		"KIWISDR_ENABLED":   "true",
		"KIWISDR_RECEIVERS": "shack=http://" + canary + ":8073",
	}, nil)
	if err != nil {
		t.Fatalf("Load = %v", err)
	}

	for _, r := range cfg.resolved {
		if strings.Contains(r.Display, canary) {
			t.Fatalf("%s recorded %q, which contains a receiver address", r.Key, r.Display)
		}
	}
}

// TestEverySourceCanBeTurnedOff is what makes the "no sources" check
// reachable, and doubles as a check that every enable setting is wired up.
func TestEverySourceCanBeTurnedOff(t *testing.T) {
	_, err := loadEnv(t, allSourcesOff(nil), nil)
	assertProblem(t, err, "no sources are enabled", EnvPrefix+"SWPC_ENABLED=true")
}
