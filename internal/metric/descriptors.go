package metric

import "slices"

// The descriptor table. Every metric this exporter can publish is declared
// here and nowhere else.
//
// Help text is written for someone reading a Grafana tooltip who does not
// already know the field, because that is who reads it. Where a value has a
// conventional range or a threshold that matters operationally, it is stated:
// "K-index" alone tells an operator nothing, "0 to 9, above 4 is a geomagnetic
// storm" tells them whether to care.
var (
	// Solar activity.

	FluxSFU = &Descriptor{
		Name: "flux_sfu",
		Help: "10.7 cm solar radio flux in solar flux units. Roughly 65 at solar minimum, above 200 at an active maximum; higher values favour the higher HF bands.",
		Unit: "{sfu}",
		Kind: KindGauge,
	}

	FluxNinetyDayMeanSFU = &Descriptor{
		Name: "flux_ninety_day_mean_sfu",
		Help: "Ninety-day mean of the 10.7 cm solar radio flux, in solar flux units.",
		Unit: "{sfu}",
		Kind: KindGauge,
	}

	SunspotNumber = &Descriptor{
		Name: "sunspot_number",
		Help: "SESC sunspot number reported by NOAA SWPC.",
		Unit: "{count}",
		Kind: KindGauge,
	}

	XRayFlux = &Descriptor{
		Name:   "xray_flux_watts_per_square_meter",
		Help:   "GOES X-ray flux. The 0.1-0.8 nm band is the one flare classes are derived from: 1e-6 is class C, 1e-5 is M, 1e-4 is X.",
		Unit:   "W/m2",
		Kind:   KindGauge,
		Labels: []string{"band"},
	}

	XRayClassInfo = &Descriptor{
		Name:   "xray_class_info",
		Help:   "Current GOES X-ray flare class in the conventional letter notation, for example B3.8 or M1.2. Always 1; read the class label.",
		Kind:   KindInfo,
		Labels: []string{"class"},
	}

	ProtonFlux = &Descriptor{
		Name:   "proton_flux_particles",
		Help:   "GOES integral proton flux in particles per square centimetre per second per steradian. The >=10 MeV channel drives polar cap absorption and NOAA S-scale radiation storms.",
		Unit:   "{particles}/(cm2.s.sr)",
		Kind:   KindGauge,
		Labels: []string{"energy"},
	}

	ElectronFlux = &Descriptor{
		Name:   "electron_flux_particles",
		Help:   "GOES integral electron flux in particles per square centimetre per second per steradian, for the >=2 MeV channel.",
		Unit:   "{particles}/(cm2.s.sr)",
		Kind:   KindGauge,
		Labels: []string{"energy"},
	}

	// Geomagnetic conditions.

	AIndex = &Descriptor{
		Name:   "a_index",
		Help:   "Daily geomagnetic A-index, a linear 0 to 400 scale derived from the day's K values. Above about 30 indicates disturbed conditions that degrade HF propagation.",
		Unit:   "{index}",
		Kind:   KindGauge,
		Labels: []string{"station"},
	}

	KIndex = &Descriptor{
		Name:   "k_index",
		Help:   "Three-hourly geomagnetic K-index on a 0 to 9 quasi-logarithmic scale. 5 and above is a geomagnetic storm.",
		Unit:   "{index}",
		Kind:   KindGauge,
		Labels: []string{"station"},
	}

	KIndexEstimated = &Descriptor{
		Name: "k_index_estimated",
		Help: "Estimated planetary K-index updated every minute, ahead of the definitive three-hourly value.",
		Unit: "{index}",
		Kind: KindGauge,
	}

	DstNanotesla = &Descriptor{
		Name: "dst_nanotesla",
		Help: "Kyoto Dst index in nanotesla, measuring ring current strength. Large negative values indicate a geomagnetic storm; below -100 nT is severe.",
		Unit: "nT",
		Kind: KindGauge,
	}

	GeomagneticFieldInfo = &Descriptor{
		Name:   "geomagnetic_field_info",
		Help:   "Geomagnetic field condition as worded by hamqsl.com, for example QUIET, UNSETTLD or STORM. Always 1; read the state label.",
		Kind:   KindInfo,
		Labels: []string{"state"},
	}

	// Solar wind.

	WindSpeed = &Descriptor{
		Name: "wind_speed_kilometers_per_second",
		Help: "Solar wind bulk speed at the L1 point. Typically 300 to 500 km/s; sustained values above 600 km/s often precede geomagnetic disturbance.",
		Unit: "km/s",
		Kind: KindGauge,
	}

	WindDensity = &Descriptor{
		Name: "wind_density_protons_per_cubic_centimeter",
		Help: "Solar wind proton density at the L1 point.",
		Unit: "/cm3",
		Kind: KindGauge,
	}

	WindMagneticField = &Descriptor{
		Name:   "wind_magnetic_field_nanotesla",
		Help:   "Interplanetary magnetic field at L1 in nanotesla. A sustained southward Bz, meaning a negative value, couples solar wind energy into the magnetosphere and drives storms.",
		Unit:   "nT",
		Kind:   KindGauge,
		Labels: []string{"component"},
	}

	// Aurora.

	AuroraHemisphericPower = &Descriptor{
		Name:   "aurora_hemispheric_power_gigawatts",
		Help:   "OVATION estimated hemispheric power deposited by auroral particles. Above about 50 GW the aurora is visible well equatorward of its usual latitudes.",
		Unit:   "GW",
		Kind:   KindGauge,
		Labels: []string{"hemisphere"},
	}

	// AuroraBoundary is declared but NOT currently published by any source.
	//
	// SWPC's OVATION product states no boundary anywhere: it is a grid of
	// energy flux by magnetic local time and latitude. Deriving a boundary
	// from it means choosing a flux threshold, and plausible thresholds move
	// the answer by several degrees of latitude with nothing in the data to
	// arbitrate between them. Publishing an invented number under this name
	// would be worse than publishing nothing.
	//
	// The descriptor is kept so the metric name and unit are already agreed if
	// a defensible source appears. See parseHemisphericPower in the swpc
	// package for the full reasoning.
	AuroraBoundary = &Descriptor{
		Name:   "aurora_equatorward_boundary_degrees",
		Help:   "Estimated equatorward boundary of the auroral oval in degrees of magnetic latitude. Lower values mean the aurora has expanded towards the equator.",
		Unit:   "deg",
		Kind:   KindGauge,
		Labels: []string{"hemisphere"},
	}

	// NOAA scales and alerts.

	GeomagneticStormScale = &Descriptor{
		Name: "geomagnetic_storm_scale",
		Help: "NOAA G-scale for geomagnetic storms, 0 for none through 5 for extreme.",
		Unit: "{scale}",
		Kind: KindGauge,
	}

	RadioBlackoutScale = &Descriptor{
		Name: "radio_blackout_scale",
		Help: "NOAA R-scale for HF radio blackouts, 0 for none through 5 for extreme. Directly relevant to HF operating: R1 already degrades the low bands on the sunlit side.",
		Unit: "{scale}",
		Kind: KindGauge,
	}

	RadiationStormScale = &Descriptor{
		Name: "radiation_storm_scale",
		Help: "NOAA S-scale for solar radiation storms, 0 for none through 5 for extreme.",
		Unit: "{scale}",
		Kind: KindGauge,
	}

	AlertActive = &Descriptor{
		Name:   "alert_active",
		Help:   "An active NOAA SWPC alert, warning or watch. Always 1; read the labels.",
		Kind:   KindInfo,
		Labels: []string{"product_id", "serial"},
	}

	// HF absorption.

	DRAPMaxFrequency = &Descriptor{
		Name:   "drap_max_absorbed_frequency_megahertz",
		Help:   "D-Region Absorption Prediction: the highest frequency substantially absorbed by the D layer at this point. Frequencies below this are unusable there.",
		Unit:   "MHz",
		Kind:   KindGauge,
		Labels: []string{"latitude", "longitude"},
	}

	// hamqsl model output.

	BandCondition = &Descriptor{
		Name:   "band_condition",
		Help:   "HF band condition from the hamqsl.com model as an ordinal: 0 Poor, 1 Fair, 2 Good.",
		Unit:   "{ordinal}",
		Kind:   KindGauge,
		Labels: []string{"band", "period"},
	}

	BandConditionInfo = &Descriptor{
		Name:   "band_condition_info",
		Help:   "HF band condition from the hamqsl.com model as the original wording. Always 1; read the condition label.",
		Kind:   KindInfo,
		Labels: []string{"band", "period", "condition"},
	}

	VHFCondition = &Descriptor{
		Name:   "vhf_condition",
		Help:   "VHF propagation phenomenon from the hamqsl.com model as an ordinal: 0 closed, 1 open.",
		Unit:   "{ordinal}",
		Kind:   KindGauge,
		Labels: []string{"phenomenon", "location"},
	}

	VHFConditionInfo = &Descriptor{
		Name:   "vhf_condition_info",
		Help:   "VHF propagation phenomenon from the hamqsl.com model as the original wording. Always 1; read the condition label.",
		Kind:   KindInfo,
		Labels: []string{"phenomenon", "location", "condition"},
	}

	SignalNoise = &Descriptor{
		Name:   "signal_noise_s_units",
		Help:   "Background noise level in S-units as estimated by hamqsl.com. Reported as a range, so the min and max bounds are separate series.",
		Unit:   "{s_unit}",
		Kind:   KindGauge,
		Labels: []string{"bound"},
	}

	// Ionosphere, from kc2g.

	FoF2 = &Descriptor{
		Name:   "fof2_megahertz",
		Help:   "F2 layer critical frequency measured by an ionosonde. Signals below this reflect at vertical incidence; oblique paths support proportionally higher frequencies.",
		Unit:   "MHz",
		Kind:   KindGauge,
		Labels: []string{"station", "station_name"},
	}

	MUF = &Descriptor{
		Name:   "muf_megahertz",
		Help:   "Maximum usable frequency for a 3000 km path from this ionosonde.",
		Unit:   "MHz",
		Kind:   KindGauge,
		Labels: []string{"station", "station_name"},
	}

	MUFFactor = &Descriptor{
		Name:   "muf_factor",
		Help:   "Ratio of the 3000 km MUF to foF2, conventionally M(3000)F2. Typically between 2.5 and 3.5.",
		Unit:   "1",
		Kind:   KindGauge,
		Labels: []string{"station", "station_name"},
	}

	HmF2 = &Descriptor{
		Name:   "hmf2_kilometers",
		Help:   "Height of the F2 layer peak electron density.",
		Unit:   "km",
		Kind:   KindGauge,
		Labels: []string{"station", "station_name"},
	}

	StationConfidence = &Descriptor{
		Name:   "station_confidence_score",
		Help:   "Ionosonde autoscaling confidence score, 0 to 100. Low scores indicate the automatic scaling of the ionogram is unreliable.",
		Unit:   "1",
		Kind:   KindGauge,
		Labels: []string{"station"},
	}

	EffectiveSunspotNumber = &Descriptor{
		Name: "effective_sunspot_number",
		Help: "Effective sunspot number derived from ionosonde measurements. Fits observed ionisation rather than counting spots, so it tracks propagation better than the raw sunspot number.",
		Unit: "{count}",
		Kind: KindGauge,
	}

	EffectiveFluxSFU = &Descriptor{
		Name: "effective_flux_sfu",
		Help: "Effective 10.7 cm flux derived from ionosonde measurements, in solar flux units.",
		Unit: "{sfu}",
		Kind: KindGauge,
	}
)

// All lists every descriptor. Sinks that must declare their metrics up front,
// and the tests that check the table for consistency, iterate this.
//
// The table is split across files by domain rather than kept in one list,
// because a single slice that every new source appends to is a merge conflict
// waiting to happen.
var All = slices.Concat(
	coreDescriptors,
	forecastDescriptors,
	ionosphereDescriptors,
	activityDescriptors,
)

// coreDescriptors are the solar, geomagnetic and propagation quantities the
// exporter started with.
var coreDescriptors = []*Descriptor{
	FluxSFU,
	FluxNinetyDayMeanSFU,
	SunspotNumber,
	XRayFlux,
	XRayClassInfo,
	ProtonFlux,
	ElectronFlux,
	AIndex,
	KIndex,
	KIndexEstimated,
	DstNanotesla,
	GeomagneticFieldInfo,
	WindSpeed,
	WindDensity,
	WindMagneticField,
	AuroraHemisphericPower,
	AuroraBoundary,
	GeomagneticStormScale,
	RadioBlackoutScale,
	RadiationStormScale,
	AlertActive,
	DRAPMaxFrequency,
	BandCondition,
	BandConditionInfo,
	VHFCondition,
	VHFConditionInfo,
	SignalNoise,
	FoF2,
	MUF,
	MUFFactor,
	HmF2,
	StationConfidence,
	EffectiveSunspotNumber,
	EffectiveFluxSFU,
}
