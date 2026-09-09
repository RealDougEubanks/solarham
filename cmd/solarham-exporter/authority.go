package main

// sourceAuthority ranks sources against each other when two of them publish
// the same series.
//
// Several quantities in this exporter are available from more than one
// upstream, and they are not equally good. Without an explicit ordering the
// winner would be whichever source last polled with a newer upstream
// timestamp, so a dashboard would show a number that flapped between two
// subtly different values for no visible reason. That is the worst of the
// available outcomes: not obviously broken, just quietly wrong.
//
// The rule applied here is that the more authoritative and more current source
// wins. Where one upstream operates the instrument and another republishes its
// figure, the operator wins. Where both are relays of the same third party, the
// one with the shorter path or the better cadence wins. A higher number wins;
// zero means unranked, which is correct for the great majority of sources
// because they publish quantities nobody else touches.
//
// Only the contested metrics matter here. Losing a collision costs a source
// that one series and nothing else -- NOAA loses the 10.7 cm flux to DRAO and
// remains the only source for the G/S/R scales.
var sourceAuthority = map[string]int{
	// The 10.7 cm solar flux: solar_flux_sfu.
	//
	// DRAO Penticton is the observatory whose instrument makes this
	// measurement, three times per UT day, and it publishes the raw
	// observations plus the URSI-normalised series. NOAA republishes a single
	// daily value derived from it. The observatory wins.
	"drao": 30,

	// Kp, ap and Hp30: solar_k_index{station="planetary"},
	// solar_ap_index, solar_k_index_half_hourly.
	//
	// GFZ Potsdam is the institute that defines and publishes Kp. NOAA's
	// planetary K is a separate estimate from a US subnetwork, and iSWA
	// relays GFZ's own Hp30 through a third party. GFZ direct wins on both
	// authority and path length.
	"gfz": 30,

	// Ground magnetometer vectors: solar_geomagnetic_field_nanotesla,
	// solar_geomagnetic_field_range_nanotesla.
	//
	// USGS operates BOU and FRD, which appear in both its own default station
	// list and INTERMAGNET's. INTERMAGNET is a relay for those two, so the
	// operator wins. For the stations only INTERMAGNET carries there is no
	// collision and this ranking is irrelevant.
	"usgs-geomag": 20,
	"intermagnet": 10,

	// Solar wind and Dst: solar_wind_speed_kilometers_per_second,
	// solar_wind_density_protons_per_cubic_centimeter,
	// solar_wind_magnetic_field_nanotesla, solar_dst_nanotesla.
	//
	// This one is close, and it goes to iSWA on currency rather than
	// authority. Both are relays -- NOAA's summary products and iSWA both
	// carry L1 monitor data, and both carry Kyoto's Dst. iSWA wins because it
	// exposes the primary-spacecraft flag, so it publishes whichever monitor
	// is actually current rather than a summary that does not say, and because
	// its RTSW datasets are the live replacement for the NOAA products that
	// were retired.
	//
	// Note that iSWA publishes only the primary monitor. Making ACE, Wind and
	// IMAP available as genuine independent cross-checks would need a
	// spacecraft label on those three descriptors, which is a change to the
	// metric vocabulary rather than to this table.
	"iswa": 20,

	// SWPC is left unranked at zero deliberately. It loses the four contests
	// above and remains the sole source for everything else it publishes:
	// the NOAA scales, alerts, GOES particle flux, X-ray class, sunspot
	// number, auroral hemispheric power and D-RAP.
}

// f107Owner reports whether DRAO is the configured owner of the 10.7 cm flux.
//
// LASP's LISIRD collection includes two datasets that carry the same
// measurement -- one a relay of DRAO's own figures, the other the CLS
// homogenised series -- and both would write solar_radio_flux_sfu at 10.7 cm.
// Rather than let the authority table arbitrate a collision that need not
// happen, the LASP source simply does not request those datasets unless DRAO is
// switched off, which saves two requests as well as the ambiguity.
func f107Owner(draoEnabled bool) bool { return draoEnabled }
