package metric

// Forecast and solar-region descriptors.
//
// A word on forecasts. Prometheus stamps every sample at scrape time, so a
// value predicted for a future moment cannot be stored as a future point. The
// horizon therefore becomes a label — solar_flare_probability{horizon="1d"} —
// and the sample time stays the moment the forecast was issued. That is a
// deliberate compromise: it keeps rate() and delta() meaningful over the
// forecast's own history, which is what you actually want to ask ("has the
// X-class probability been climbing?"), at the cost of not being able to plot
// a forecast curve into the future. Plot that from the table instead.
var (
	// Flare and particle-event probabilities.

	FlareProbability = &Descriptor{
		Name:   "flare_probability_percent",
		Help:   "NOAA SWPC probability of at least one solar flare of this class within the horizon. An X-class flare is a direct forecast of an HF radio blackout on the sunlit side.",
		Unit:   "%",
		Kind:   KindGauge,
		Labels: []string{"class", "horizon"},
	}

	ProtonEventProbability = &Descriptor{
		Name:   "proton_event_probability_percent",
		Help:   "NOAA SWPC probability of a >=10 MeV proton event within the horizon. Such events drive polar cap absorption, which closes polar HF paths entirely.",
		Unit:   "%",
		Kind:   KindGauge,
		Labels: []string{"horizon"},
	}

	PolarCapAbsorptionInfo = &Descriptor{
		Name:   "polar_cap_absorption_info",
		Help:   "NOAA SWPC polar cap absorption status, for example green, yellow or red. Always 1; read the status label.",
		Kind:   KindInfo,
		Labels: []string{"status"},
	}

	// Index forecasts.

	KIndexForecast = &Descriptor{
		Name:   "k_index_forecast",
		Help:   "Forecast planetary K-index at this horizon, on the 0 to 9 scale. The sample time is when the forecast was issued, not the time it describes.",
		Unit:   "{index}",
		Kind:   KindGauge,
		Labels: []string{"horizon"},
	}

	FluxForecastSFU = &Descriptor{
		Name:   "flux_forecast_sfu",
		Help:   "Forecast 10.7 cm solar radio flux at this horizon, in solar flux units.",
		Unit:   "{sfu}",
		Kind:   KindGauge,
		Labels: []string{"horizon"},
	}

	AIndexForecast = &Descriptor{
		Name:   "a_index_forecast",
		Help:   "Forecast Fredericksburg A-index at this horizon.",
		Unit:   "{index}",
		Kind:   KindGauge,
		Labels: []string{"horizon"},
	}

	// Active regions.
	//
	// Region numbers churn: a region visible today will have rotated off the
	// disc within a fortnight, taking its label value with it. Per-region
	// series are therefore opt-in, and the aggregates below are what a
	// dashboard should normally use.

	ActiveRegionCount = &Descriptor{
		Name: "active_region_count",
		Help: "Number of numbered active regions on the visible solar disc.",
		Unit: "{count}",
		Kind: KindGauge,
	}

	ActiveRegionSpotTotal = &Descriptor{
		Name: "active_region_spot_total",
		Help: "Total sunspot count across all numbered active regions on the visible disc.",
		Unit: "{count}",
		Kind: KindGauge,
	}

	ActiveRegionAreaTotal = &Descriptor{
		Name: "active_region_area_total_millionths",
		Help: "Combined area of all active regions in millionths of the solar hemisphere.",
		Unit: "{millionths}",
		Kind: KindGauge,
	}

	ActiveRegionMaxFlareProbability = &Descriptor{
		Name:   "active_region_max_flare_probability_percent",
		Help:   "Highest per-region flare probability of this class across all active regions. Answers the operational question directly: is any region on the disc likely to produce one?",
		Unit:   "%",
		Kind:   KindGauge,
		Labels: []string{"class"},
	}

	ActiveRegionFlareProbability = &Descriptor{
		Name:   "active_region_flare_probability_percent",
		Help:   "Per-region flare probability of this class. Region numbers change as regions rotate on and off the disc, so this series set is not stable over weeks.",
		Unit:   "%",
		Kind:   KindGauge,
		Labels: []string{"region", "class"},
	}

	// Solar cycle and long-baseline indices.

	SunspotNumberSmoothed = &Descriptor{
		Name: "sunspot_number_smoothed",
		Help: "Thirteen-month smoothed international sunspot number from SILSO. The standard measure of where we are in the solar cycle.",
		Unit: "{count}",
		Kind: KindGauge,
	}

	SunspotNumberEISN = &Descriptor{
		Name: "sunspot_number_estimated",
		Help: "SILSO estimated international sunspot number for today, provisional and revised later in the month.",
		Unit: "{count}",
		Kind: KindGauge,
	}

	SunspotNumberEISNDeviation = &Descriptor{
		Name: "sunspot_number_estimated_stddev",
		Help: "Standard deviation of today's SILSO estimated sunspot number across reporting stations.",
		Unit: "{count}",
		Kind: KindGauge,
	}

	SunspotStationCount = &Descriptor{
		Name:   "sunspot_station_count",
		Help:   "Number of SILSO reporting stations, as calculated and as total. A low calculated-to-total ratio means today's estimate rests on thin coverage.",
		Unit:   "{count}",
		Kind:   KindGauge,
		Labels: []string{"state"},
	}

	// Coronal mass ejections, from the DONKI catalogue.

	CMESpeed = &Descriptor{
		Name:   "cme_speed_kilometers_per_second",
		Help:   "Plane-of-sky speed of the most recent analysed coronal mass ejection. Above about 1000 km/s an Earth-directed CME is likely to cause a geomagnetic storm.",
		Unit:   "km/s",
		Kind:   KindGauge,
		Labels: []string{"accuracy"},
	}

	CMEHalfAngle = &Descriptor{
		Name:   "cme_half_angle_degrees",
		Help:   "Angular half-width of the most recent analysed coronal mass ejection. A wide halo CME is more likely to have an Earth-directed component.",
		Unit:   "deg",
		Kind:   KindGauge,
		Labels: []string{"accuracy"},
	}

	CMESourceLatitude = &Descriptor{
		Name: "cme_source_latitude_degrees",
		Help: "Heliographic latitude of the most recent analysed coronal mass ejection's source.",
		Unit: "deg",
		Kind: KindGauge,
	}

	CMESourceLongitude = &Descriptor{
		Name: "cme_source_longitude_degrees",
		Help: "Heliographic longitude of the most recent analysed coronal mass ejection's source. Values near zero are close to the Sun-Earth line.",
		Unit: "deg",
		Kind: KindGauge,
	}

	CMEEventCount = &Descriptor{
		Name: "cme_events_recent_count",
		Help: "Coronal mass ejections catalogued in the recent window.",
		Unit: "{count}",
		Kind: KindGauge,
	}

	FlareEventCount = &Descriptor{
		Name:   "flare_events_recent_count",
		Help:   "Flares of this class catalogued in the recent window.",
		Unit:   "{count}",
		Kind:   KindGauge,
		Labels: []string{"class"},
	}

	// Irradiance and spectral indices.

	TotalSolarIrradiance = &Descriptor{
		Name: "total_solar_irradiance_watts_per_square_meter",
		Help: "Total solar irradiance at one astronomical unit. Varies by only about 0.1% over a solar cycle, so small changes are significant.",
		Unit: "W/m2",
		Kind: KindGauge,
	}

	MgIIIndex = &Descriptor{
		Name: "mg_ii_index",
		Help: "Bremen composite Mg II core-to-wing ratio, a proxy for chromospheric and upper-photospheric activity that tracks EUV better than sunspot number.",
		Unit: "1",
		Kind: KindGauge,
	}

	LymanAlphaIrradiance = &Descriptor{
		Name: "lyman_alpha_irradiance",
		Help: "Composite Lyman-alpha irradiance at 121.6 nm, a driver of D-layer ionisation and therefore of daytime HF absorption.",
		Unit: "W/m2/nm",
		Kind: KindGauge,
	}

	RadioFluxMultiFrequency = &Descriptor{
		Name:   "radio_flux_sfu",
		Help:   "Solar radio flux at this wavelength, in solar flux units. The ratio of the 30 cm to the 10.7 cm flux is a spectral-hardness diagnostic.",
		Unit:   "{sfu}",
		Kind:   KindGauge,
		Labels: []string{"wavelength_cm", "adjustment"},
	}

	EUVIrradiance = &Descriptor{
		Name:   "euv_irradiance",
		Help:   "SDO/EVE extreme-ultraviolet irradiance in this band.",
		Unit:   "W/m2",
		Kind:   KindGauge,
		Labels: []string{"band"},
	}

	EUVSourceLatitude = &Descriptor{
		Name: "euv_source_latitude_degrees",
		Help: "Heliographic latitude of the centroid of EUV emission, from SDO/EVE. Effectively where on the disc the dominant activity is right now.",
		Unit: "deg",
		Kind: KindGauge,
	}

	EUVSourceLongitude = &Descriptor{
		Name: "euv_source_longitude_degrees",
		Help: "Heliographic longitude of the centroid of EUV emission, from SDO/EVE. Values near zero are close to the Sun-Earth line, where a flare's effects reach Earth most directly.",
		Unit: "deg",
		Kind: KindGauge,
	}

	// Cosmic rays.

	NeutronMonitorRate = &Descriptor{
		Name:   "neutron_monitor_counts_per_minute",
		Help:   "Ground-level neutron monitor count rate, corrected for detector efficiency. Tracks galactic cosmic ray intensity, which is anticorrelated with solar activity; a sudden drop is a Forbush decrease following a CME.",
		Unit:   "{count}/min",
		Kind:   KindGauge,
		Labels: []string{"station"},
	}
)

var forecastDescriptors = []*Descriptor{
	FlareProbability,
	ProtonEventProbability,
	PolarCapAbsorptionInfo,
	KIndexForecast,
	FluxForecastSFU,
	AIndexForecast,
	ActiveRegionCount,
	ActiveRegionSpotTotal,
	ActiveRegionAreaTotal,
	ActiveRegionMaxFlareProbability,
	ActiveRegionFlareProbability,
	SunspotNumberSmoothed,
	SunspotNumberEISN,
	SunspotNumberEISNDeviation,
	SunspotStationCount,
	CMESpeed,
	CMEHalfAngle,
	CMESourceLatitude,
	CMESourceLongitude,
	CMEEventCount,
	FlareEventCount,
	TotalSolarIrradiance,
	MgIIIndex,
	LymanAlphaIrradiance,
	RadioFluxMultiFrequency,
	EUVIrradiance,
	EUVSourceLatitude,
	EUVSourceLongitude,
	NeutronMonitorRate,
}
