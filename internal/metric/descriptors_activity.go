package metric

// Amateur radio activity descriptors.
//
// These measure what operators are actually working, which is the ground truth
// a propagation forecast is trying to predict. They are observations, not
// models, and that is why they belong here.
//
// The cardinality discipline is stricter than elsewhere: the underlying feeds
// carry callsigns, grid squares and DXCC entities, none of which may become
// labels. A callsign label is unbounded, and DXCC alone is roughly 340 values
// before it multiplies against band and mode.
var (
	// Digital-mode reception reports.

	SpotRate = &Descriptor{
		Name:   "spots_per_minute",
		Help:   "Reception reports per minute on this band, aggregated from a reporting network. Rising counts mean the band is open and being used; they cannot distinguish propagation from popularity.",
		Unit:   "{count}/min",
		Kind:   KindGauge,
		Labels: []string{"network", "band"},
	}

	SpotCount = &Descriptor{
		Name:   "spots_recent_count",
		Help:   "Reception reports on this band in the recent window.",
		Unit:   "{count}",
		Kind:   KindGauge,
		Labels: []string{"network", "band"},
	}

	SpotSNRMean = &Descriptor{
		Name:   "spot_snr_mean_db",
		Help:   "Mean reported signal-to-noise ratio on this band. Reported in the reference bandwidth of the mode, so WSPR and FT8 figures are not directly comparable.",
		Unit:   "dB",
		Kind:   KindGauge,
		Labels: []string{"network", "band"},
	}

	SpotDistanceMean = &Descriptor{
		Name:   "spot_distance_mean_kilometers",
		Help:   "Mean great-circle distance of reception reports on this band. A jump indicates the band has opened to longer paths rather than simply getting busier.",
		Unit:   "km",
		Kind:   KindGauge,
		Labels: []string{"network", "band"},
	}

	SpotStationCount = &Descriptor{
		Name:   "spot_stations_count",
		Help:   "Distinct transmitting or receiving stations seen on this band in the recent window. A more honest activity measure than raw spot count, which one loud station can dominate.",
		Unit:   "{count}",
		Kind:   KindGauge,
		Labels: []string{"network", "band", "role"},
	}

	BandActivityScore = &Descriptor{
		Name:   "band_activity_score",
		Help:   "Reporting network's own weighted activity score for this band and grid square.",
		Unit:   "1",
		Kind:   KindGauge,
		Labels: []string{"network", "band", "grid"},
	}

	// Operating activity.

	ActivationSpots = &Descriptor{
		Name:   "activation_spots",
		Help:   "Currently spotted portable activations. Spots carry their own expiry upstream, so this is a snapshot of what is on the air rather than a running total.",
		Unit:   "{count}",
		Kind:   KindGauge,
		Labels: []string{"program", "band", "mode"},
	}

	ActivationSpotsTotal = &Descriptor{
		Name:   "activation_spots_total",
		Help:   "Currently spotted portable activations across all bands and modes.",
		Unit:   "{count}",
		Kind:   KindGauge,
		Labels: []string{"program"},
	}

	ScheduledActivations = &Descriptor{
		Name:   "activations_scheduled",
		Help:   "Activations scheduled to be underway now, from a program's own calendar.",
		Unit:   "{count}",
		Kind:   KindGauge,
		Labels: []string{"program"},
	}

	// Population measures. These move over months, not minutes, and are the
	// long-baseline companion to the solar cycle series.

	ActiveOperators = &Descriptor{
		Name:   "active_operators",
		Help:   "Operators who have uploaded a log within this window. A slow measure of how many people are actually on the air worldwide.",
		Unit:   "{count}",
		Kind:   KindGauge,
		Labels: []string{"service", "window"},
	}

	RegisteredOperators = &Descriptor{
		Name:   "registered_operators",
		Help:   "Callsigns known to a logging service, active or not.",
		Unit:   "{count}",
		Kind:   KindGauge,
		Labels: []string{"service"},
	}

	// Meteor scatter.

	MeteorEchoRate = &Descriptor{
		Name:   "meteor_echo_count",
		Help:   "Radio meteor echoes counted by an observer in the last complete hour. Counts are raw and uncalibrated: antennas, frequencies and detection thresholds differ wildly between observers, so these are comparable over time at one station but not between stations.",
		Unit:   "{count}",
		Kind:   KindGauge,
		Labels: []string{"observer", "country"},
	}

	MeteorObserversReporting = &Descriptor{
		Name: "meteor_observers_reporting",
		Help: "Observers who reported meteor counts for the current month. The cheapest single indicator of whether the network is healthy.",
		Unit: "{count}",
		Kind: KindGauge,
	}

	// Satellite element sets.
	//
	// Pass predictions are deliberately absent. A countdown to the next pass
	// is a computed function of time, wrong between scrapes and quantised to
	// the scrape interval; it reconstructs badly in any range query and cannot
	// be usefully averaged. Element-set freshness, by contrast, is a real
	// observation about the outside world and stale elements are a genuine
	// operational problem worth alerting on.

	TLEAge = &Descriptor{
		Name:   "tle_age_seconds",
		Help:   "Age of the newest orbital element set in a catalogue. Element sets are normally regenerated daily; a value beyond a few days means pass predictions computed from them are drifting.",
		Unit:   "s",
		Kind:   KindGauge,
		Labels: []string{"catalogue"},
	}

	TLECount = &Descriptor{
		Name:   "tle_object_count",
		Help:   "Objects in an orbital element catalogue. Changes when satellites are launched, added to the list, or decay.",
		Unit:   "{count}",
		Kind:   KindGauge,
		Labels: []string{"catalogue"},
	}
)

var activityDescriptors = []*Descriptor{
	SpotRate,
	SpotCount,
	SpotSNRMean,
	SpotDistanceMean,
	SpotStationCount,
	BandActivityScore,
	ActivationSpots,
	ActivationSpotsTotal,
	ScheduledActivations,
	ActiveOperators,
	RegisteredOperators,
	MeteorEchoRate,
	MeteorObserversReporting,
	TLEAge,
	TLECount,
}
