package httpx

import (
	"log/slog"
	"time"
)

// DefaultPolicies is the per-host politeness table.
//
// Every number here is a decision about somebody else's server, not a
// performance tuning knob. The spacing applies across all sources sharing the
// host, so adding a source that fetches from a host already listed costs
// nothing extra in request rate.
//
// Where a publisher states a limit, that number is used and cited. Where none
// is stated, the spacing is derived from how often the data actually changes —
// polling a document that is rewritten once a day more than a few times a day
// is pure waste.
//
// Fallback is deliberately strict, because an unlisted host is one nobody has
// thought about yet and the safe assumption is that it belongs to a volunteer.
var DefaultPolicies = map[string]HostPolicy{
	// NOAA SWPC. US government infrastructure, no published rate limit, and
	// everything is served through CloudFront with cache-control max-age=60.
	// We fetch a dozen documents from here across three tiers, so the burst
	// covers a tier firing at once while the spacing keeps the sustained rate
	// reasonable.
	"services.swpc.noaa.gov": {MinInterval: 2 * time.Second, Burst: 8},

	// NASA CCMC — iSWA HAPI and DONKI. Government, no stated limit. The
	// DONKI endpoints here are the key-free equivalents of api.nasa.gov,
	// which measured a limit of 10 requests/hour on its demo key.
	"iswa.gsfc.nasa.gov":          {MinInterval: 2 * time.Second, Burst: 6},
	"webtools.ccmc.gsfc.nasa.gov": {MinInterval: 5 * time.Second, Burst: 3},
	"kauai.ccmc.gsfc.nasa.gov":    {MinInterval: 5 * time.Second, Burst: 3},
	"cdaweb.gsfc.nasa.gov":        {MinInterval: 5 * time.Second, Burst: 2},
	"api.helioviewer.org":         {MinInterval: 10 * time.Second, Burst: 2},

	// USGS Geomagnetism. Government, cache-control max-age=60.
	"geomag.usgs.gov": {MinInterval: 3 * time.Second, Burst: 4},

	// LASP, University of Colorado. Academic. SDO/EVE republishes once a
	// minute; LISIRD datasets are daily. Small responses, but it is a
	// university server rather than a CDN.
	"lasp.colorado.edu": {MinInterval: 5 * time.Second, Burst: 4},

	// GFZ Potsdam. Their only stated guidance is to use the bulk download for
	// large ranges, which we do not need. Hp30 is the fastest thing here at a
	// half-hourly cadence.
	"kp.gfz.de": {MinInterval: 5 * time.Second, Burst: 3},

	// Natural Resources Canada — DRAO Penticton. The flux table is a 2 MB
	// file measured three times a day, fetched with a Range request.
	"www.spaceweather.gc.ca": {MinInterval: 10 * time.Second, Burst: 2},

	// SILSO, Royal Observatory of Belgium. The current-month EISN file is
	// 414 bytes; the full daily series is 2.9 MB and must not be polled.
	"www.sidc.be": {MinInterval: 10 * time.Second, Burst: 2},

	// Finnish Meteorological Institute. Academic, and their R-index JSON
	// claims a five-minute update.
	"space.fmi.fi": {MinInterval: 10 * time.Second, Burst: 2},

	// INTERMAGNET via the BGS geomagnetism node. Each observatory query
	// returns roughly 87 KB, and we curate a handful of stations rather than
	// iterating all 154.
	"imag-data.bgs.ac.uk": {MinInterval: 5 * time.Second, Burst: 4},

	// DLR Neustrelitz. Their acceptable use policy states plainly that they
	// monitor traffic and will block without notice, so this is the strictest
	// of the government-adjacent hosts.
	"data.impc.dlr.de": {MinInterval: 15 * time.Second, Burst: 2},

	// NMDB neutron monitor database. NEST is a PHP page hitting MySQL per
	// request, so one multi-station query rather than several single-station
	// ones, well spaced.
	"www.nmdb.eu": {MinInterval: 30 * time.Second, Burst: 1},

	// hamqsl.com. The operator asks for hourly polling and has stated he was
	// shut down by his ISP over automated clients. The source-level schedule
	// already enforces an interval floor; this is the belt to that braces.
	"www.hamqsl.com": {MinInterval: 5 * time.Minute, Burst: 1},

	// prop.kc2g.com. No published limit and no cache headers at all, so
	// self-throttling is the only control. Maps regenerate every 15 minutes.
	"prop.kc2g.com": {MinInterval: 60 * time.Second, Burst: 2},

	// PSK Reporter. The operator's own words: "Users are encouraged to
	// retrieve reception data no more often than once every five minutes",
	// and he reserves the right to block anybody imposing significant load.
	"pskreporter.info":          {MinInterval: 5 * time.Minute, Burst: 1},
	"retrieve.pskreporter.info": {MinInterval: 5 * time.Minute, Burst: 1},

	// wspr.live. Documented limit is 20 requests per minute; we need one
	// query every few minutes, so this sits far inside it while leaving the
	// shared budget for everyone else using the service.
	"db1.wspr.live": {MinInterval: 30 * time.Second, Burst: 2},

	// Parks on the Air. Undocumented API behind CloudFront, and spots carry
	// their own expiry, so a minute is plenty.
	"api.pota.app": {MinInterval: 30 * time.Second, Burst: 2},

	// ARRL Logbook of the World. The activity file is 6 MB and rebuilt about
	// weekly; it is fetched daily with a conditional request that should
	// answer 304 six days out of seven.
	"lotw.arrl.org": {MinInterval: 5 * time.Minute, Burst: 1},

	// Celestrak. Their policy prohibits high-frequency automated retrieval
	// and they block IPs that poll aggressively. Element sets change daily.
	"celestrak.org": {MinInterval: 5 * time.Minute, Burst: 1},

	// Radio Meteor Observing Bulletin. Volunteer-run, files rewritten hourly.
	"www.rmob.org": {MinInterval: 60 * time.Second, Burst: 2},

	// Kyoto WDC. Academic, non-commercial, file updated roughly half-hourly.
	"wdc.kugi.kyoto-u.ac.jp": {MinInterval: 60 * time.Second, Burst: 1},
}

// DefaultFallback applies to any host not listed above.
var DefaultFallback = HostPolicy{MinInterval: 30 * time.Second, Burst: 1}

// NewDefault returns a client configured with the policy table above.
func NewDefault(timeout time.Duration, log *slog.Logger) *Client {
	return New(Options{
		Timeout:  timeout,
		Policies: DefaultPolicies,
		Fallback: DefaultFallback,
	}, log)
}
