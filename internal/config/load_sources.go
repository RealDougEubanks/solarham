package config

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Loading for the sources added after the initial release.
//
// Two conventions from config_sources.go are visible here. A source only takes
// an interval where its upstream genuinely changes continuously; where the
// upstream publishes on a known clock, the schedule lives in
// internal/source/schedule.go and there is nothing to configure. And a source
// whose data carries a restricted licence defaults to disabled, with the
// licence named in the doc comment of its loader so the reason survives
// alongside the default.

// loadSources reads every post-release source.
//
// They are loaded in the order they appear in Config so the startup log reads
// in the same order as the documentation.
func loadSources(l *loader, cfg *Config) {
	loadSolarProbabilities(l, cfg)
	loadGloTEC(l, cfg)
	loadISWA(l, cfg)
	loadDONKI(l, cfg)
	loadUSGSGeomag(l, cfg)
	loadGFZ(l, cfg)
	loadDRAO(l, cfg)
	loadLASP(l, cfg)
	loadFMI(l, cfg)
	loadKiwiSDR(l, cfg)
	loadLoTW(l, cfg)
	loadPOTA(l, cfg)
	loadCelestrak(l, cfg)

	loadWSPRLive(l, cfg)
	loadPSKReporter(l, cfg)
	loadNMDB(l, cfg)
	loadINTERMAGNET(l, cfg)
	loadSILSO(l, cfg)
}

// loadSolarProbabilities reads the SWPC flare and particle-event probability
// forecast. It shares the SWPC host settings and so has no URL or cadence of
// its own.
//
// Per-region probabilities are off by default: region numbers churn as regions
// rotate across the disc, so that series set is not stable over weeks.
func loadSolarProbabilities(l *loader, cfg *Config) {
	cfg.SolarProbabilities = SolarProbabilities{
		Enabled: l.Bool("SOLAR_PROBABILITIES_ENABLED", true),
		Regions: l.Bool("SOLAR_PROBABILITIES_REGIONS", false),
	}
}

// loadGloTEC reads the SWPC assimilative total electron content model.
//
// Like the probability forecast it rides on the SWPC host and cadence. The full
// grid is off by default because it is more series than everything else in the
// exporter combined; sampling named points is what an operator almost always
// wants.
//
// Points are separated by semicolons rather than the usual commas, because the
// comma belongs to the coordinate: "Boulder=40.015,-105.271;Ottawa=45.42,-75.69".
func loadGloTEC(l *loader, cfg *Config) {
	cfg.GloTEC = GloTEC{
		Enabled:       l.Bool("GLOTEC_ENABLED", true),
		Points:        l.StringMapDelim("GLOTEC_POINTS", ";"),
		LatitudeBands: l.Bool("GLOTEC_LATITUDE_BANDS", true),
		Grid:          l.Bool("GLOTEC_GRID", false),
		GridStep:      l.Int("GLOTEC_GRID_STEP", 10, 1, 36),
	}
}

// loadISWA reads NASA CCMC's iSWA HAPI server, the live replacement for the
// real-time solar wind products SWPC retired.
func loadISWA(l *loader, cfg *Config) {
	cfg.ISWA = ISWA{
		Enabled:      l.Bool("ISWA_ENABLED", true),
		BaseURL:      l.String("ISWA_BASE_URL", "https://iswa.gsfc.nasa.gov/hapi"),
		FastInterval: l.Duration("ISWA_FAST_INTERVAL", time.Minute, time.Minute, time.Hour),
		Spacecraft:   l.StringSliceDefault("ISWA_SPACECRAFT", []string{"swpc"}),
		Timeout:      l.Duration("ISWA_TIMEOUT", 20*time.Second, time.Second, 2*time.Minute),
		Retries:      l.Int("ISWA_RETRIES", 2, 0, 10),
	}
}

// loadDONKI reads NASA CCMC's space weather event catalogue.
//
// The interval floor of fifteen minutes exists because the catalogue is curated
// by hand: entries appear minutes to hours after the event, so polling faster
// than that fetches an identical document to learn nothing.
func loadDONKI(l *loader, cfg *Config) {
	cfg.DONKI = DONKI{
		Enabled:  l.Bool("DONKI_ENABLED", true),
		BaseURL:  l.String("DONKI_BASE_URL", "https://webtools.ccmc.gsfc.nasa.gov/DONKI/WS/get"),
		Interval: l.Duration("DONKI_INTERVAL", 30*time.Minute, 15*time.Minute, 24*time.Hour),
		Window:   l.Duration("DONKI_WINDOW", 72*time.Hour, 6*time.Hour, 30*24*time.Hour),
		Timeout:  l.Duration("DONKI_TIMEOUT", 20*time.Second, time.Second, 2*time.Minute),
		Retries:  l.Int("DONKI_RETRIES", 2, 0, 10),
	}
}

// loadUSGSGeomag reads the USGS geomagnetism web service.
//
// The observatory default is a curated handful rather than the forty codes the
// service lists, about fourteen of which are test channels.
func loadUSGSGeomag(l *loader, cfg *Config) {
	cfg.USGSGeomag = USGSGeomag{
		Enabled:       l.Bool("USGS_GEOMAG_ENABLED", true),
		BaseURL:       l.String("USGS_GEOMAG_BASE_URL", "https://geomag.usgs.gov"),
		Observatories: l.StringSliceDefault("USGS_GEOMAG_OBSERVATORIES", []string{"BOU", "FRD", "NEW", "TUC", "SIT"}),
		Interval:      l.Duration("USGS_GEOMAG_INTERVAL", 5*time.Minute, time.Minute, 6*time.Hour),
		Timeout:       l.Duration("USGS_GEOMAG_TIMEOUT", 20*time.Second, time.Second, 2*time.Minute),
		Retries:       l.Int("USGS_GEOMAG_RETRIES", 2, 0, 10),
	}
}

// loadGFZ reads the GFZ Potsdam geomagnetic index service.
//
// Hp30 is the reason to have this at all, so the half-hourly series are on by
// default.
func loadGFZ(l *loader, cfg *Config) {
	cfg.GFZ = GFZ{
		Enabled:    l.Bool("GFZ_ENABLED", true),
		BaseURL:    l.String("GFZ_BASE_URL", "https://kp.gfz.de"),
		HalfHourly: l.Bool("GFZ_HALF_HOURLY", true),
		Interval:   l.Duration("GFZ_INTERVAL", 10*time.Minute, 5*time.Minute, 6*time.Hour),
		Timeout:    l.Duration("GFZ_TIMEOUT", 20*time.Second, time.Second, 2*time.Minute),
		Retries:    l.Int("GFZ_RETRIES", 2, 0, 10),
	}
}

// loadDRAO reads the Penticton solar flux observatory feed.
//
// There is deliberately no interval: the flux is measured three times per UT
// day and the publication clock is declared in code, because an operator has no
// way to know better than the observatory does.
func loadDRAO(l *loader, cfg *Config) {
	cfg.DRAO = DRAO{
		Enabled: l.Bool("DRAO_ENABLED", true),
		BaseURL: l.String("DRAO_BASE_URL", "https://www.spaceweather.gc.ca"),
		Timeout: l.Duration("DRAO_TIMEOUT", 30*time.Second, time.Second, 5*time.Minute),
		Retries: l.Int("DRAO_RETRIES", 2, 0, 10),
	}
}

// loadLASP reads the LASP instrument-data services at the University of
// Colorado: SDO/EVE quicklook irradiance and the LISIRD collection.
func loadLASP(l *loader, cfg *Config) {
	cfg.LASP = LASP{
		Enabled: l.Bool("LASP_ENABLED", true),
		BaseURL: l.String("LASP_BASE_URL", "https://lasp.colorado.edu"),
		EVE:     l.Bool("LASP_EVE", true),
		LISIRD:  l.Bool("LASP_LISIRD", true),
		Timeout: l.Duration("LASP_TIMEOUT", 20*time.Second, time.Second, 2*time.Minute),
		Retries: l.Int("LASP_RETRIES", 2, 0, 10),
	}
}

// loadFMI reads the Finnish Meteorological Institute regional auroral index.
//
// Only the index is fetched. Their JSON is CC BY 4.0, unlike their raw
// magnetometer files, which may not be redistributed and are not touched, so
// this source carries no restriction an operator has to opt into.
func loadFMI(l *loader, cfg *Config) {
	cfg.FMI = FMI{
		Enabled:  l.Bool("FMI_ENABLED", true),
		BaseURL:  l.String("FMI_BASE_URL", "https://space.fmi.fi"),
		Interval: l.Duration("FMI_INTERVAL", 5*time.Minute, time.Minute, 6*time.Hour),
		Timeout:  l.Duration("FMI_TIMEOUT", 20*time.Second, time.Second, 2*time.Minute),
		Retries:  l.Int("FMI_RETRIES", 2, 0, 10),
	}
}

// loadKiwiSDR reads the KiwiSDR noise floor poller.
//
// It cannot default to enabled because it has no default target: a KiwiSDR is a
// private machine somebody chose to share, and polling one on a schedule
// without asking is not on. Point it at your own receiver.
func loadKiwiSDR(l *loader, cfg *Config) {
	cfg.KiwiSDR = KiwiSDR{
		Enabled:   l.Bool("KIWISDR_ENABLED", false),
		Receivers: l.StringMap("KIWISDR_RECEIVERS"),
		Interval:  l.Duration("KIWISDR_INTERVAL", 30*time.Minute, 5*time.Minute, 24*time.Hour),
		Timeout:   l.Duration("KIWISDR_TIMEOUT", 15*time.Second, time.Second, 2*time.Minute),
		Retries:   l.Int("KIWISDR_RETRIES", 1, 0, 10),
	}
}

// loadLoTW reads the ARRL Logbook of the World activity file.
//
// The timeout is two minutes rather than the usual twenty seconds because the
// file is 6 MB. There is no interval: it is fetched once a day on a schedule
// declared in code, with a conditional request that answers 304 most days.
func loadLoTW(l *loader, cfg *Config) {
	cfg.LoTW = LoTW{
		Enabled: l.Bool("LOTW_ENABLED", true),
		URL:     l.String("LOTW_URL", "https://lotw.arrl.org/lotw-user-activity.csv"),
		Timeout: l.Duration("LOTW_TIMEOUT", 2*time.Minute, time.Second, 10*time.Minute),
		Retries: l.Int("LOTW_RETRIES", 1, 0, 10),
	}
}

// loadPOTA reads the Parks on the Air spot feed.
func loadPOTA(l *loader, cfg *Config) {
	cfg.POTA = POTA{
		Enabled:  l.Bool("POTA_ENABLED", true),
		BaseURL:  l.String("POTA_BASE_URL", "https://api.pota.app"),
		Interval: l.Duration("POTA_INTERVAL", 60*time.Second, 30*time.Second, time.Hour),
		Timeout:  l.Duration("POTA_TIMEOUT", 20*time.Second, time.Second, 2*time.Minute),
		Retries:  l.Int("POTA_RETRIES", 2, 0, 10),
	}
}

// loadCelestrak reads orbital element-set freshness monitoring.
//
// No interval: Celestrak's terms prohibit high-frequency automated retrieval
// and element sets change daily, so the daily schedule is fixed in code.
func loadCelestrak(l *loader, cfg *Config) {
	cfg.Celestrak = Celestrak{
		Enabled: l.Bool("CELESTRAK_ENABLED", true),
		BaseURL: l.String("CELESTRAK_BASE_URL", "https://celestrak.org"),
		Group:   l.String("CELESTRAK_GROUP", "amateur"),
		Timeout: l.Duration("CELESTRAK_TIMEOUT", 30*time.Second, time.Second, 5*time.Minute),
		Retries: l.Int("CELESTRAK_RETRIES", 1, 0, 10),
	}
}

// ---------------------------------------------------------------------------
// Restricted-licence sources. All default to disabled, and each loader names
// the licence so the reason for the default sits next to the default.
// ---------------------------------------------------------------------------

// loadWSPRLive reads the wspr.live ClickHouse query service.
//
// Disabled by default: the data is non-commercial only, and their terms ask
// that results derived from it be freely accessible. The two-minute interval
// floor keeps a copy-pasted compose file inside their documented limit of
// twenty requests a minute.
func loadWSPRLive(l *loader, cfg *Config) {
	cfg.WSPRLive = WSPRLive{
		Enabled:  l.Bool("WSPR_LIVE_ENABLED", false),
		BaseURL:  l.String("WSPR_LIVE_BASE_URL", "https://db1.wspr.live"),
		Interval: l.Duration("WSPR_LIVE_INTERVAL", 5*time.Minute, 2*time.Minute, 6*time.Hour),
		Window:   l.Duration("WSPR_LIVE_WINDOW", 15*time.Minute, 5*time.Minute, 60*time.Minute),
		Timeout:  l.Duration("WSPR_LIVE_TIMEOUT", 20*time.Second, time.Second, 2*time.Minute),
		Retries:  l.Int("WSPR_LIVE_RETRIES", 2, 0, 10),
	}
}

// loadPSKReporter reads the PSK Reporter band-activity feed.
//
// Disabled by default: the feed runs on courtesy terms rather than an open
// licence. Its operator asks in writing for no more than one request every five
// minutes, which is why the interval floor equals the default, and asks that
// automated clients identify a contact address, which is why Contact is
// required rather than optional once this source is enabled.
func loadPSKReporter(l *loader, cfg *Config) {
	cfg.PSKReporter = PSKReporter{
		Enabled:  l.Bool("PSKREPORTER_ENABLED", false),
		BaseURL:  l.String("PSKREPORTER_BASE_URL", "https://pskreporter.info"),
		Grids:    l.StringSlice("PSKREPORTER_GRIDS"),
		Contact:  l.String("PSKREPORTER_CONTACT", ""),
		Interval: l.Duration("PSKREPORTER_INTERVAL", 5*time.Minute, 5*time.Minute, 6*time.Hour),
		Timeout:  l.Duration("PSKREPORTER_TIMEOUT", 20*time.Second, time.Second, 2*time.Minute),
		Retries:  l.Int("PSKREPORTER_RETRIES", 1, 0, 10),
	}
}

// loadNMDB reads the Neutron Monitor Database.
//
// Disabled by default: the data is non-commercial, and NMDB asks that both the
// database and the individual monitor principal investigators be acknowledged.
// NEST is a PHP page querying MySQL per request, so the station list becomes
// one multi-station query rather than several single-station ones.
func loadNMDB(l *loader, cfg *Config) {
	cfg.NMDB = NMDB{
		Enabled:  l.Bool("NMDB_ENABLED", false),
		BaseURL:  l.String("NMDB_BASE_URL", "https://www.nmdb.eu"),
		Stations: l.StringSliceDefault("NMDB_STATIONS", []string{"OULU", "KIEL2", "SOPO"}),
		Interval: l.Duration("NMDB_INTERVAL", 10*time.Minute, 5*time.Minute, 6*time.Hour),
		Timeout:  l.Duration("NMDB_TIMEOUT", 30*time.Second, time.Second, 2*time.Minute),
		Retries:  l.Int("NMDB_RETRIES", 1, 0, 10),
	}
}

// loadINTERMAGNET reads the geomagnetic observatory network via the British
// Geological Survey node.
//
// Disabled by default: the data is CC BY-NC and the operating institutes must
// be acknowledged. The observatory default is five codes, not the network's
// 154, which would be 13 MB per poll and most of it stale.
func loadINTERMAGNET(l *loader, cfg *Config) {
	cfg.INTERMAGNET = INTERMAGNET{
		Enabled:       l.Bool("INTERMAGNET_ENABLED", false),
		BaseURL:       l.String("INTERMAGNET_BASE_URL", "https://imag-data.bgs.ac.uk"),
		Observatories: l.StringSliceDefault("INTERMAGNET_OBSERVATORIES", []string{"BOU", "FRD", "CLF", "HRN", "BEL"}),
		Interval:      l.Duration("INTERMAGNET_INTERVAL", 5*time.Minute, time.Minute, 6*time.Hour),
		Timeout:       l.Duration("INTERMAGNET_TIMEOUT", 30*time.Second, time.Second, 2*time.Minute),
		Retries:       l.Int("INTERMAGNET_RETRIES", 2, 0, 10),
	}
}

// loadSILSO reads the Royal Observatory of Belgium sunspot number service.
//
// Disabled by default: the data is CC BY-NC and must be cited as WDC-SILSO,
// Royal Observatory of Belgium. No interval — only the 414-byte current-month
// estimate and the monthly prediction are fetched, on a schedule in code, since
// the full daily series is 2.9 MB and must not be polled.
func loadSILSO(l *loader, cfg *Config) {
	cfg.SILSO = SILSO{
		Enabled: l.Bool("SILSO_ENABLED", false),
		BaseURL: l.String("SILSO_BASE_URL", "https://www.sidc.be"),
		Timeout: l.Duration("SILSO_TIMEOUT", 20*time.Second, time.Second, 2*time.Minute),
		Retries: l.Int("SILSO_RETRIES", 2, 0, 10),
	}
}

// validateSources applies the cross-setting rules for the post-release sources.
func validateSources(l *loader, cfg *Config) {
	// Every enabled source needs somewhere to fetch from, and a hostname or a
	// bare path is not somewhere: it fails at the first request, hours after
	// startup, rather than here.
	requireURL(l, cfg.ISWA.Enabled, "iSWA", "ISWA_BASE_URL", cfg.ISWA.BaseURL)
	requireURL(l, cfg.DONKI.Enabled, "DONKI", "DONKI_BASE_URL", cfg.DONKI.BaseURL)
	requireURL(l, cfg.USGSGeomag.Enabled, "USGS geomagnetism", "USGS_GEOMAG_BASE_URL", cfg.USGSGeomag.BaseURL)
	requireURL(l, cfg.GFZ.Enabled, "GFZ", "GFZ_BASE_URL", cfg.GFZ.BaseURL)
	requireURL(l, cfg.DRAO.Enabled, "DRAO", "DRAO_BASE_URL", cfg.DRAO.BaseURL)
	requireURL(l, cfg.LASP.Enabled, "LASP", "LASP_BASE_URL", cfg.LASP.BaseURL)
	requireURL(l, cfg.FMI.Enabled, "FMI", "FMI_BASE_URL", cfg.FMI.BaseURL)
	requireURL(l, cfg.LoTW.Enabled, "LoTW", "LOTW_URL", cfg.LoTW.URL)
	requireURL(l, cfg.POTA.Enabled, "POTA", "POTA_BASE_URL", cfg.POTA.BaseURL)
	requireURL(l, cfg.Celestrak.Enabled, "Celestrak", "CELESTRAK_BASE_URL", cfg.Celestrak.BaseURL)
	requireURL(l, cfg.WSPRLive.Enabled, "wspr.live", "WSPR_LIVE_BASE_URL", cfg.WSPRLive.BaseURL)
	requireURL(l, cfg.PSKReporter.Enabled, "PSK Reporter", "PSKREPORTER_BASE_URL", cfg.PSKReporter.BaseURL)
	requireURL(l, cfg.NMDB.Enabled, "NMDB", "NMDB_BASE_URL", cfg.NMDB.BaseURL)
	requireURL(l, cfg.INTERMAGNET.Enabled, "INTERMAGNET", "INTERMAGNET_BASE_URL", cfg.INTERMAGNET.BaseURL)
	requireURL(l, cfg.SILSO.Enabled, "SILSO", "SILSO_BASE_URL", cfg.SILSO.BaseURL)

	// A timeout at or above the interval turns one slow upstream into a growing
	// backlog of overlapping polls rather than a skipped sample. Only the
	// sources that have an interval are checked; the rest run on a publication
	// schedule declared in code.
	requireTimeoutBelowInterval(l, cfg.ISWA.Enabled,
		"ISWA_TIMEOUT", cfg.ISWA.Timeout, "ISWA_FAST_INTERVAL", cfg.ISWA.FastInterval)
	requireTimeoutBelowInterval(l, cfg.DONKI.Enabled,
		"DONKI_TIMEOUT", cfg.DONKI.Timeout, "DONKI_INTERVAL", cfg.DONKI.Interval)
	requireTimeoutBelowInterval(l, cfg.USGSGeomag.Enabled,
		"USGS_GEOMAG_TIMEOUT", cfg.USGSGeomag.Timeout, "USGS_GEOMAG_INTERVAL", cfg.USGSGeomag.Interval)
	requireTimeoutBelowInterval(l, cfg.GFZ.Enabled,
		"GFZ_TIMEOUT", cfg.GFZ.Timeout, "GFZ_INTERVAL", cfg.GFZ.Interval)
	requireTimeoutBelowInterval(l, cfg.FMI.Enabled,
		"FMI_TIMEOUT", cfg.FMI.Timeout, "FMI_INTERVAL", cfg.FMI.Interval)
	requireTimeoutBelowInterval(l, cfg.KiwiSDR.Enabled,
		"KIWISDR_TIMEOUT", cfg.KiwiSDR.Timeout, "KIWISDR_INTERVAL", cfg.KiwiSDR.Interval)
	requireTimeoutBelowInterval(l, cfg.POTA.Enabled,
		"POTA_TIMEOUT", cfg.POTA.Timeout, "POTA_INTERVAL", cfg.POTA.Interval)
	requireTimeoutBelowInterval(l, cfg.WSPRLive.Enabled,
		"WSPR_LIVE_TIMEOUT", cfg.WSPRLive.Timeout, "WSPR_LIVE_INTERVAL", cfg.WSPRLive.Interval)
	requireTimeoutBelowInterval(l, cfg.PSKReporter.Enabled,
		"PSKREPORTER_TIMEOUT", cfg.PSKReporter.Timeout, "PSKREPORTER_INTERVAL", cfg.PSKReporter.Interval)
	requireTimeoutBelowInterval(l, cfg.NMDB.Enabled,
		"NMDB_TIMEOUT", cfg.NMDB.Timeout, "NMDB_INTERVAL", cfg.NMDB.Interval)
	requireTimeoutBelowInterval(l, cfg.INTERMAGNET.Enabled,
		"INTERMAGNET_TIMEOUT", cfg.INTERMAGNET.Timeout, "INTERMAGNET_INTERVAL", cfg.INTERMAGNET.Interval)

	validateGloTEC(l, cfg)
	validateKiwiSDR(l, cfg)
	validatePSKReporter(l, cfg)
}

// validateGloTEC checks the sample points and warns about the grid setting that
// publishes more series than a small Prometheus can hold.
func validateGloTEC(l *loader, cfg *Config) {
	if !cfg.GloTEC.Enabled {
		return
	}

	for _, name := range sortedKeys(cfg.GloTEC.Points) {
		if err := validateLatLon(cfg.GloTEC.Points[name]); err != nil {
			l.errf("%sGLOTEC_POINTS: point %q is %v; each value must be \"lat,lon\", "+
				"and points are separated by semicolons, as %q",
				EnvPrefix, name, err, "Boulder=40.015,-105.271;Ottawa=45.42,-75.69")
		}
	}

	// A grid step of one is legitimate — somebody with the storage for it may
	// genuinely want the full model — so it is a note rather than an error. It
	// is worth saying out loud, because the cost does not show up until the
	// first scrape.
	if cfg.GloTEC.Grid && cfg.GloTEC.GridStep == 1 {
		cfg.notes = append(cfg.notes, fmt.Sprintf(
			"%sGLOTEC_GRID is on with %sGLOTEC_GRID_STEP=1, which publishes the "+
				"full model grid: tens of thousands of series, more than everything "+
				"else in the exporter combined; raise the step to subsample it",
			EnvPrefix, EnvPrefix))
	}
}

// validateKiwiSDR requires a receiver, since there is no sensible default one.
func validateKiwiSDR(l *loader, cfg *Config) {
	if !cfg.KiwiSDR.Enabled {
		return
	}
	if len(cfg.KiwiSDR.Receivers) == 0 {
		l.errf("KiwiSDR is enabled but %sKIWISDR_RECEIVERS names no receiver; "+
			"there is no default, because a public KiwiSDR is a private machine "+
			"somebody chose to share; set it to your own, as "+
			"\"shack=http://localhost:8073\"", EnvPrefix)
		return
	}
	for _, name := range sortedKeys(cfg.KiwiSDR.Receivers) {
		if err := validateHTTPURL(cfg.KiwiSDR.Receivers[name]); err != nil {
			l.errf("%sKIWISDR_RECEIVERS: receiver %q is %v; each value must be a "+
				"base URL, as %q", EnvPrefix, name, err, "shack=http://localhost:8073")
		}
	}
}

// validatePSKReporter enforces the contact address.
//
// This is not a hygiene rule. The feed's operator asks in writing that
// automated clients identify a contact address so he can get in touch with
// whoever is running one before he has to block it, and an exporter that
// ignored that would be the reason the feed stops being available to everybody
// else. So it is required rather than optional, and the error says why.
func validatePSKReporter(l *loader, cfg *Config) {
	if !cfg.PSKReporter.Enabled {
		return
	}
	if strings.TrimSpace(cfg.PSKReporter.Contact) == "" {
		l.errf("PSK Reporter is enabled but %sPSKREPORTER_CONTACT is not set; "+
			"the feed's operator asks in writing that automated clients supply a "+
			"contact address so he can reach whoever is running one before "+
			"blocking it, so it is required rather than optional",
			EnvPrefix)
	}
}

// validateLatLon checks a "lat,lon" pair from GLOTEC_POINTS.
//
// The error is returned rather than recorded so the caller can name the key it
// came from; "-105.271 is not a latitude" on its own tells an operator with six
// points nothing about which one to fix.
func validateLatLon(value string) error {
	lat, lon, found := strings.Cut(value, ",")
	if !found {
		return fmt.Errorf("%q, which has no comma", value)
	}

	latitude, err := strconv.ParseFloat(strings.TrimSpace(lat), 64)
	if err != nil {
		return fmt.Errorf("%q, whose latitude is not a number", value)
	}
	longitude, err := strconv.ParseFloat(strings.TrimSpace(lon), 64)
	if err != nil {
		return fmt.Errorf("%q, whose longitude is not a number", value)
	}

	if latitude < -90 || latitude > 90 {
		return fmt.Errorf("%q, whose latitude is outside -90..90", value)
	}
	if longitude < -180 || longitude > 180 {
		return fmt.Errorf("%q, whose longitude is outside -180..180", value)
	}
	return nil
}

// requireURL reports a URL that is missing or is not something an HTTP client
// can fetch.
//
// A bare hostname parses happily as a relative URL, so checking for a scheme
// and a host is the difference between failing here and failing on every
// request for the lifetime of the process.
func requireURL(l *loader, enabled bool, name, settingName, value string) {
	if !enabled {
		return
	}

	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		l.require(name, settingName, trimmed)
		return
	}
	if err := validateHTTPURL(trimmed); err != nil {
		l.errf("%s%s is %v", EnvPrefix, settingName, err)
	}
}

// validateHTTPURL reports why a value is not something an HTTP client can
// fetch, or nil if it is.
//
// The error is returned rather than recorded because two callers need to name
// different things: a setting, or the map key a value came from.
func validateHTTPURL(value string) error {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return fmt.Errorf("empty")
	}

	parsed, err := url.Parse(trimmed)
	if err != nil {
		return fmt.Errorf("%q, which is not a URL: %v", trimmed, err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return fmt.Errorf("%q, which is not an absolute http:// or https:// URL", trimmed)
	}
	if parsed.Host == "" {
		return fmt.Errorf("%q, which names no host", trimmed)
	}
	return nil
}
