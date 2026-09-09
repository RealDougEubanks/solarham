package metric

// Ionospheric, geomagnetic-observatory and RF-environment descriptors.
var (
	// Total electron content, from the GloTEC assimilative model.
	//
	// The native product is a 5,184-point global grid. Publishing it whole
	// would be more series than everything else in this exporter combined, so
	// the grid is opt-in and the normal path is point samples at configured
	// locations plus latitude-band statistics.

	TotalElectronContent = &Descriptor{
		Name:   "total_electron_content_tecu",
		Help:   "Vertical total electron content in TEC units. Higher values support higher usable frequencies; the gradient matters as much as the value for oblique paths.",
		Unit:   "{tecu}",
		Kind:   KindGauge,
		Labels: []string{"point"},
	}

	TECAnomaly = &Descriptor{
		Name:   "total_electron_content_anomaly_ratio",
		Help:   "Total electron content relative to the quiet-time expectation. Values well above or below 1 indicate a disturbed ionosphere without needing a baseline of your own.",
		Unit:   "1",
		Kind:   KindGauge,
		Labels: []string{"point"},
	}

	TECStatistic = &Descriptor{
		Name:   "total_electron_content_tecu_stat",
		Help:   "Summary statistic of total electron content over a latitude band, in TEC units.",
		Unit:   "{tecu}",
		Kind:   KindGauge,
		Labels: []string{"statistic", "latitude_band"},
	}

	PeakHeightF2 = &Descriptor{
		Name:   "f2_peak_height_kilometers",
		Help:   "Height of the F2 layer peak electron density. A raised layer lengthens single-hop distances.",
		Unit:   "km",
		Kind:   KindGauge,
		Labels: []string{"point"},
	}

	PeakDensityF2 = &Descriptor{
		Name:   "f2_peak_density_per_cubic_meter",
		Help:   "Peak electron density of the F2 layer.",
		Unit:   "/m3",
		Kind:   KindGauge,
		Labels: []string{"point"},
	}

	// Sporadic E, measured by ionosondes.
	//
	// This is the real quantity behind every sporadic-E prediction: a
	// measurement, not a model. Everything else on offer is either a
	// probability model or a count of spots after the fact.

	FoEs = &Descriptor{
		Name:   "foes_megahertz",
		Help:   "Sporadic-E critical frequency measured by an ionosonde. Values above about 8 MHz suggest 6 m openings are possible; the usable frequency on an oblique path is several times this.",
		Unit:   "MHz",
		Kind:   KindGauge,
		Labels: []string{"station", "station_name"},
	}

	FbEs = &Descriptor{
		Name:   "fbes_megahertz",
		Help:   "Blanketing frequency of the sporadic-E layer. Below this the Es layer screens the F region entirely, so HF paths that normally use F2 stop working.",
		Unit:   "MHz",
		Kind:   KindGauge,
		Labels: []string{"station", "station_name"},
	}

	FoE = &Descriptor{
		Name:   "foe_megahertz",
		Help:   "Ordinary E layer critical frequency measured by an ionosonde.",
		Unit:   "MHz",
		Kind:   KindGauge,
		Labels: []string{"station", "station_name"},
	}

	StationTEC = &Descriptor{
		Name:   "station_total_electron_content_tecu",
		Help:   "Total electron content derived from an ionosonde's own electron density profile, in TEC units.",
		Unit:   "{tecu}",
		Kind:   KindGauge,
		Labels: []string{"station", "station_name"},
	}

	StationSourceInfo = &Descriptor{
		Name:   "station_source_info",
		Help:   "Which network an ionosonde's measurement came from, for example giro, ingv or aus-sws. Always 1; read the source label.",
		Kind:   KindInfo,
		Labels: []string{"station", "source"},
	}

	// Ionospheric irregularity, from GNSS observations.

	ROTI = &Descriptor{
		Name:   "roti_tecu_per_minute",
		Help:   "Rate of TEC index: how fast total electron content is fluctuating, in TEC units per minute. High values mean ionospheric irregularities that scatter and fade HF and GNSS signals. This is a measurement, where D-RAP is a model.",
		Unit:   "{tecu}/min",
		Kind:   KindGauge,
		Labels: []string{"region", "statistic"},
	}

	ROTIDisturbedCells = &Descriptor{
		Name:   "roti_disturbed_cells",
		Help:   "Number of grid cells whose rate-of-TEC index exceeds this threshold. A rising count means irregularity is spreading geographically, not just intensifying at one point.",
		Unit:   "{count}",
		Kind:   KindGauge,
		Labels: []string{"region", "threshold"},
	}

	// Geomagnetic observatories.

	GeomagneticFieldComponent = &Descriptor{
		Name:   "geomagnetic_field_nanotesla",
		Help:   "Magnetic field component measured at a ground observatory, in nanotesla. This is the raw input from which Kp and Dst are derived.",
		Unit:   "nT",
		Kind:   KindGauge,
		Labels: []string{"observatory", "component"},
	}

	GeomagneticFieldRange = &Descriptor{
		Name:   "geomagnetic_field_range_nanotesla",
		Help:   "Peak-to-peak variation of the horizontal field at an observatory over the last hour. A local, immediate measure of disturbance, ahead of the three-hourly K index.",
		Unit:   "nT",
		Kind:   KindGauge,
		Labels: []string{"observatory"},
	}

	GeomagneticFieldRate = &Descriptor{
		Name:   "geomagnetic_field_rate_nanotesla_per_minute",
		Help:   "Rate of change of the magnetic field at an observatory. Large values indicate substorm activity and drive geomagnetically induced currents.",
		Unit:   "nT/min",
		Kind:   KindGauge,
		Labels: []string{"observatory", "component"},
	}

	KIndexHalfHourly = &Descriptor{
		Name:   "k_index_half_hourly",
		Help:   "GFZ Potsdam Hp30 or Hp60 index: the Kp scale at thirty or sixty minute resolution. Resolves substorms that the three-hourly Kp averages away.",
		Unit:   "{index}",
		Kind:   KindGauge,
		Labels: []string{"window"},
	}

	APIndex = &Descriptor{
		Name:   "ap_index",
		Help:   "Linear equivalent of the K index, in nanotesla-like units. Unlike K it can be meaningfully averaged.",
		Unit:   "{index}",
		Kind:   KindGauge,
		Labels: []string{"window"},
	}

	IndexStatusInfo = &Descriptor{
		Name:   "index_status_info",
		Help:   "Whether a geomagnetic index value is definitive or still preliminary. Always 1; read the status label.",
		Kind:   KindInfo,
		Labels: []string{"index", "status"},
	}

	// Regional auroral activity.

	AuroraRegionalIndex = &Descriptor{
		Name:   "aurora_regional_index",
		Help:   "Regional auroral activity index derived from ground magnetometers. Unlike a model of particle precipitation this is a measurement of the ground response.",
		Unit:   "{index}",
		Kind:   KindGauge,
		Labels: []string{"station"},
	}

	AuroraProbabilityInfo = &Descriptor{
		Name:   "aurora_probability_info",
		Help:   "Worded auroral visibility likelihood at a station. Always 1; read the probability label.",
		Kind:   KindInfo,
		Labels: []string{"station", "probability"},
	}

	// The RF environment, measured rather than modelled.

	NoiseFloor = &Descriptor{
		Name:   "noise_floor_dbm",
		Help:   "Measured median received power in this HF sub-band, in dBm. This is the actual noise floor at the receiver, which no regulator publishes anywhere.",
		Unit:   "dB[mW]",
		Kind:   KindGauge,
		Labels: []string{"receiver", "band"},
	}

	NoiseFloorPeak = &Descriptor{
		Name:   "noise_floor_p95_dbm",
		Help:   "Ninety-fifth percentile received power in this HF sub-band, in dBm. Together with the median this separates a raised noise floor from strong individual signals.",
		Unit:   "dB[mW]",
		Kind:   KindGauge,
		Labels: []string{"receiver", "band"},
	}

	BandSignalToNoise = &Descriptor{
		Name:   "band_signal_to_noise_db",
		Help:   "Difference between the ninety-fifth percentile and median power in this HF sub-band. A crude but real measure of how much signal is above the noise.",
		Unit:   "dB",
		Kind:   KindGauge,
		Labels: []string{"receiver", "band"},
	}

	// Data freshness.
	//
	// This descriptor exists because HTTP 200 with stale content is the
	// dominant failure mode across every upstream here. Several publishers
	// rewrite a file on a timer while its contents stay hours, or in one
	// observed case years, out of date. Age is computed from the payload's own
	// timestamp, never from Last-Modified.

	SourceDataAge = &Descriptor{
		Name:   "source_data_age_seconds",
		Help:   "Age of the newest observation a source returned, measured from the upstream's own timestamp. A rising value with no polling failures means the publisher has gone quiet while still serving a fresh-looking response.",
		Unit:   "s",
		Kind:   KindGauge,
		Labels: []string{"source"},
	}

	StationDataAge = &Descriptor{
		Name:   "station_data_age_seconds",
		Help:   "Age of an individual station's newest observation. Networks routinely serve a current document containing stations that stopped reporting months ago.",
		Unit:   "s",
		Kind:   KindGauge,
		Labels: []string{"source", "station"},
	}

	StationsFiltered = &Descriptor{
		Name:   "stations_filtered",
		Help:   "Stations dropped from a network's response for this reason. Without it an operator seeing no data cannot tell whether the filter or the upstream is responsible.",
		Unit:   "{count}",
		Kind:   KindGauge,
		Labels: []string{"source", "reason"},
	}
)

var ionosphereDescriptors = []*Descriptor{
	TotalElectronContent,
	TECAnomaly,
	TECStatistic,
	PeakHeightF2,
	PeakDensityF2,
	FoEs,
	FbEs,
	FoE,
	StationTEC,
	StationSourceInfo,
	ROTI,
	ROTIDisturbedCells,
	GeomagneticFieldComponent,
	GeomagneticFieldRange,
	GeomagneticFieldRate,
	KIndexHalfHourly,
	APIndex,
	IndexStatusInfo,
	AuroraRegionalIndex,
	AuroraProbabilityInfo,
	NoiseFloor,
	NoiseFloorPeak,
	BandSignalToNoise,
	SourceDataAge,
	StationDataAge,
	StationsFiltered,
}
