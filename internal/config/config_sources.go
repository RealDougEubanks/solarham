package config

import "time"

// Settings for the sources added after the initial release.
//
// Two conventions run through all of these.
//
// Cadence is expressed as an interval only where the upstream genuinely
// changes continuously. Where it publishes on a known clock — NOAA's daily
// indices at about 02:25 UT, DRAO's flux measurements at 17:00, 20:00 and
// 23:00 — the source declares that schedule in code and there is no interval
// to configure, because an operator has no way to know better than the
// publisher does.
//
// Tier is not a setting. Sources whose data carries a non-commercial or
// otherwise restricted licence default to disabled and say so in their help
// text, so nobody inherits a licence obligation from a default they did not
// read.

// SolarProbabilities configures NOAA SWPC's flare and particle-event
// probability forecast.
type SolarProbabilities struct {
	Enabled bool
	// Regions publishes per-active-region flare probabilities in addition to
	// the aggregates. Region numbers churn as regions rotate across the disc,
	// so this series set is not stable over weeks.
	Regions bool
}

// GloTEC configures NOAA SWPC's assimilative total electron content model.
type GloTEC struct {
	Enabled bool

	// Points are locations to sample the grid at, as "name=lat,lon". Sampling
	// a handful of places you care about is almost always what you want.
	Points map[string]string

	// LatitudeBands publishes summary statistics per 30-degree latitude band.
	LatitudeBands bool

	// Grid publishes the whole 5,184-point grid, subsampled by GridStep. This
	// is more series than everything else in the exporter combined, which is
	// why it is off by default.
	Grid     bool
	GridStep int
}

// ISWA configures NASA CCMC's iSWA HAPI server.
//
// This is the live replacement for the real-time solar wind products SWPC
// retired, and it adds ACE, Wind and IMAP as independent cross-checks against
// DSCOVR.
type ISWA struct {
	Enabled bool
	BaseURL string

	// FastInterval covers the one-minute solar wind and magnetic field
	// datasets.
	FastInterval time.Duration

	// Spacecraft selects which L1 monitors to publish. More than a couple is
	// rarely useful; they measure the same solar wind.
	Spacecraft []string

	Timeout time.Duration
	Retries int
}

// DONKI configures NASA CCMC's space weather event catalogue.
//
// The CCMC host is used rather than api.nasa.gov: it serves byte-identical
// JSON with no API key and no rate limit, where the api.nasa.gov demo key was
// measured at ten requests an hour.
type DONKI struct {
	Enabled  bool
	BaseURL  string
	Interval time.Duration

	// Window is how far back to count events.
	Window time.Duration

	Timeout time.Duration
	Retries int
}

// USGSGeomag configures the USGS geomagnetism web service.
type USGSGeomag struct {
	Enabled bool
	BaseURL string

	// Observatories are IAGA codes. The service lists forty, about fourteen of
	// which are test channels; a curated handful is the intent.
	Observatories []string

	Interval time.Duration
	Timeout  time.Duration
	Retries  int
}

// GFZ configures the GFZ Potsdam geomagnetic index service.
//
// Hp30 is the reason to have this: thirty-minute geomagnetic resolution at
// roughly eleven minutes latency, which NOAA does not publish at any cadence.
type GFZ struct {
	Enabled bool
	BaseURL string

	// HalfHourly publishes Hp30 and ap30 in addition to the three-hourly Kp.
	HalfHourly bool

	Interval time.Duration
	Timeout  time.Duration
	Retries  int
}

// DRAO configures the Penticton solar flux observatory feed.
//
// The flux table is a 2 MB file containing every measurement since 2004,
// fetched with a Range request for the tail. It is measured three times per UT
// day, so the schedule is fixed in code rather than configured.
type DRAO struct {
	Enabled bool
	BaseURL string
	Timeout time.Duration
	Retries int
}

// LASP configures the LASP instrument-data services at the University of
// Colorado: SDO/EVE quicklook irradiance and the LISIRD dataset collection.
type LASP struct {
	Enabled bool
	BaseURL string

	// EVE publishes the one-minute EUV irradiance and, uniquely, the
	// heliographic centroid of EUV emission.
	EVE bool

	// LISIRD publishes the slower irradiance and multi-frequency radio flux
	// datasets.
	LISIRD bool

	Timeout time.Duration
	Retries int
}

// FMI configures the Finnish Meteorological Institute regional auroral index.
//
// Their JSON is CC BY 4.0, unlike their raw magnetometer files, which may not
// be redistributed. Only the index is fetched.
type FMI struct {
	Enabled  bool
	BaseURL  string
	Interval time.Duration
	Timeout  time.Duration
	Retries  int
}

// KiwiSDR configures polling of KiwiSDR receivers for their measured noise
// floor.
//
// Point this at your own receiver. The endpoint is unauthenticated on any
// public KiwiSDR, but these are private machines somebody chose to share, and
// polling one on a schedule without asking is not on. Localhost is exempt from
// the shared host rate limit for exactly this reason.
type KiwiSDR struct {
	Enabled bool

	// Receivers are base URLs, as "name=http://host:8073".
	Receivers map[string]string

	Interval time.Duration
	Timeout  time.Duration
	Retries  int
}

// LoTW configures the ARRL Logbook of the World activity file.
//
// The file is 6 MB and rebuilt roughly weekly, so it is fetched once a day
// with a conditional request that answers 304 most days.
type LoTW struct {
	Enabled bool
	URL     string
	Timeout time.Duration
	Retries int
}

// POTA configures the Parks on the Air spot feed.
type POTA struct {
	Enabled  bool
	BaseURL  string
	Interval time.Duration
	Timeout  time.Duration
	Retries  int
}

// Celestrak configures orbital element-set freshness monitoring.
//
// Pass predictions are deliberately not published; see the TLEAge descriptor
// for why. Celestrak's terms prohibit high-frequency automated retrieval and
// element sets change daily, so this runs on a daily schedule.
type Celestrak struct {
	Enabled bool
	BaseURL string
	Group   string
	Timeout time.Duration
	Retries int
}

// ---------------------------------------------------------------------------
// Restricted-licence sources. All default to disabled.
// ---------------------------------------------------------------------------

// WSPRLive configures the wspr.live ClickHouse query service.
//
// Non-commercial only, and their terms ask that results be freely accessible.
// Their documented limit is twenty requests a minute; one query every few
// minutes sits far inside it.
type WSPRLive struct {
	Enabled  bool
	BaseURL  string
	Interval time.Duration

	// Window is the time range each query aggregates over.
	Window time.Duration

	Timeout time.Duration
	Retries int
}

// PSKReporter configures the PSK Reporter band-activity feed.
//
// The operator asks in writing for no more than one request every five
// minutes, and asks that automated clients identify a contact address so he
// can get in touch before blocking them. Contact is therefore required rather
// than optional when this source is enabled.
type PSKReporter struct {
	Enabled bool
	BaseURL string

	// Grids are Maidenhead prefixes to request activity for. Empty requests
	// the global figure.
	Grids []string

	// Contact is an email address sent as the appcontact parameter. Required.
	Contact string

	Interval time.Duration
	Timeout  time.Duration
	Retries  int
}

// NMDB configures the Neutron Monitor Database.
//
// Non-commercial. NEST is a PHP page querying MySQL per request, so one
// multi-station query is made rather than several single-station ones.
type NMDB struct {
	Enabled  bool
	BaseURL  string
	Stations []string
	Interval time.Duration
	Timeout  time.Duration
	Retries  int
}

// INTERMAGNET configures the geomagnetic observatory network, via the British
// Geological Survey node.
//
// CC BY-NC. Several observatories embargo their data for hours or weeks and
// return nulls rather than an error, so stations are probed at startup and a
// warning is logged rather than publishing empty series.
type INTERMAGNET struct {
	Enabled bool
	BaseURL string

	// Observatories are IAGA codes. The network has 154; iterating all of them
	// would be 13 MB per poll, most of it stale.
	Observatories []string

	Interval time.Duration
	Timeout  time.Duration
	Retries  int
}

// SILSO configures the Royal Observatory of Belgium sunspot number service.
//
// CC BY-NC. Only the 414-byte current-month estimate and the monthly
// prediction are fetched; the full daily series is 2.9 MB and must not be
// polled.
type SILSO struct {
	Enabled bool
	BaseURL string
	Timeout time.Duration
	Retries int
}

// RestrictedSources lists the settings whose data carries a licence an
// operator should accept knowingly. Startup logs a note naming each enabled
// one and its terms.
func (c *Config) RestrictedSources() []string {
	var out []string
	if c.KC2G.Enabled {
		out = append(out, "kc2g (CC BY-NC-SA, non-commercial; data from GIRO and INGV via WWROF funding)")
	}
	if c.WSPRLive.Enabled {
		out = append(out, "wspr.live (non-commercial; results should be freely accessible)")
	}
	if c.PSKReporter.Enabled {
		out = append(out, "pskreporter.info (courtesy terms; five-minute minimum, contact address required)")
	}
	if c.NMDB.Enabled {
		out = append(out, "nmdb.eu (non-commercial; acknowledge NMDB and the individual monitor PIs)")
	}
	if c.INTERMAGNET.Enabled {
		out = append(out, "intermagnet (CC BY-NC; acknowledge the operating institutes)")
	}
	if c.SILSO.Enabled {
		out = append(out, "silso (CC BY-NC; cite WDC-SILSO, Royal Observatory of Belgium)")
	}
	return out
}
