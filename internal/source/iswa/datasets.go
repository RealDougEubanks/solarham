package iswa

import (
	"strings"
	"time"
)

// datasetKind groups datasets by the quantity they carry, so that several
// spacecraft measuring the same thing can be compared and one chosen.
type datasetKind uint8

const (
	kindMag datasetKind = iota
	kindPlasma
	kindDst
	kindGeomag
)

// column is a logical quantity this package publishes. Each dataset maps its
// own parameter names onto these, because the same measurement is called B_t on
// one instrument and b_magnitude on another.
type column string

const (
	colBt      column = "bt"
	colBz      column = "bz"
	colSpeed   column = "speed"
	colDensity column = "density"
	colDst     column = "dst"
	colHp30    column = "hp30"
	colAp30    column = "ap30"
)

// Parameter names shared by the SWPC and ACE real-time products.
const (
	paramTime      = "Time"
	paramIsPrimary = "isPrimary"
	paramSource    = "source"
)

// dataset describes one HAPI dataset well enough to fetch it and read it.
type dataset struct {
	// id is the HAPI dataset id.
	id string

	kind datasetKind

	// craft is the cfg.Spacecraft value that selects this dataset, or "" for a
	// dataset that is not a spacecraft measurement.
	craft string

	// cadence is how often the dataset publishes a new row. It is also the
	// minimum spacing between fetches: re-requesting an hourly Dst sixty times
	// an hour returns the same number fifty-nine times and is rude for no gain.
	cadence time.Duration

	// window is the trailing time range to request. It is deliberately a few
	// multiples of the cadence and no more. Asking for a day of one-minute data
	// to read one current value transfers 1,440 rows to use one of them; asking
	// for a day of imap_mag's four-second data transfers over 20,000.
	window time.Duration

	// cols maps the quantities this package publishes onto this dataset's own
	// parameter names.
	cols map[column]string

	// timeColumn is the dataset's time parameter. It is Time everywhere
	// observed, but naming it per dataset costs nothing and this server has
	// already been caught declaring it twice.
	timeColumn string

	// hasPrimaryFlag reports whether the dataset carries the isPrimary and
	// source parameters that say whether SWPC is currently using it.
	hasPrimaryFlag bool
}

// datasets is the table of everything this source knows how to read.
//
// Every entry was verified live against iswa.gsfc.nasa.gov on 2026-09-09.
//
// Three datasets that look useful are deliberately absent, because they serve a
// fresh-looking HTTP 200 over content that has not moved in months:
// Predicted_KP_P1M (about six months stale), SWMF2023_RT_GMlog_P1M and
// NMDB_p1m_NMDB-APTY. Publishing a six-month-old predicted Kp as the current
// one is worse than publishing nothing, so they are not in the table at all
// rather than being configurable.
var datasets = []dataset{
	// NOAA/SWPC real-time solar wind. This is the primary product: SWPC
	// switches which spacecraft feeds it without notice and the isPrimary flag
	// says which one is live right now.
	{
		id:             "swpc_rtsw_mag_P1M",
		kind:           kindMag,
		craft:          "swpc",
		cadence:        time.Minute,
		window:         10 * time.Minute,
		timeColumn:     paramTime,
		hasPrimaryFlag: true,
		cols:           map[column]string{colBt: "B_t", colBz: "B_z"},
	},
	{
		id:             "swpc_rtsw_plasma_P1M",
		kind:           kindPlasma,
		craft:          "swpc",
		cadence:        time.Minute,
		window:         10 * time.Minute,
		timeColumn:     paramTime,
		hasPrimaryFlag: true,
		cols:           map[column]string{colSpeed: "BulkSpeed", colDensity: "ProtonDensity"},
	},

	// ACE. Still flying, still measuring, and reported here with the same
	// isPrimary flag, so it is a genuine cross-check rather than a duplicate.
	{
		id:             "ace_mag_P1M",
		kind:           kindMag,
		craft:          "ace",
		cadence:        time.Minute,
		window:         10 * time.Minute,
		timeColumn:     paramTime,
		hasPrimaryFlag: true,
		cols:           map[column]string{colBt: "B_t", colBz: "B_z"},
	},
	{
		id:             "ace_swepam_P1M",
		kind:           kindPlasma,
		craft:          "ace",
		cadence:        time.Minute,
		window:         10 * time.Minute,
		timeColumn:     paramTime,
		hasPrimaryFlag: true,
		cols:           map[column]string{colSpeed: "BulkSpeed", colDensity: "ProtonDensity"},
	},

	// Wind. Verified present but its /info reported a stopDate of
	// 2026-03-08 — six months behind — so a trailing-window request answers
	// "no data" rather than returning anything. It stays in the table because
	// the archive is real and may resume; a poll simply produces nothing while
	// it is quiet, which is the correct outcome.
	{
		id:         "WIND_MFI_P2M",
		kind:       kindMag,
		craft:      "wind",
		cadence:    92 * time.Second,
		window:     15 * time.Minute,
		timeColumn: paramTime,
		cols:       map[column]string{colBt: "B_t", colBz: "B_z"},
	},
	{
		id:         "WIND_SWE_P2M",
		kind:       kindPlasma,
		craft:      "wind",
		cadence:    92 * time.Second,
		window:     15 * time.Minute,
		timeColumn: paramTime,
		cols:       map[column]string{colSpeed: "BulkSpeed", colDensity: "ProtonDensity"},
	},

	// IMAP, at a four-second cadence — by far the fastest magnetometer here.
	// The window is two minutes rather than ten precisely because of that: ten
	// minutes is 150 rows to read one, and the whole point of a trailing window
	// is to stop asking for data we throw away.
	//
	// It carries no isPrimary flag, and its /info declares Time twice; see
	// newResponse for how the duplicate is handled.
	{
		id:         "imap_mag",
		kind:       kindMag,
		craft:      "imap",
		cadence:    4 * time.Second,
		window:     2 * time.Minute,
		timeColumn: paramTime,
		cols:       map[column]string{colBt: "b_magnitude", colBz: "bz_gsm"},
	},

	// Kyoto Dst quicklook, hourly. The window is six hours rather than ten
	// minutes: a ten-minute window on an hourly product straddles no
	// publication boundary most of the time and answers "no data".
	{
		id:         "dst_quicklook",
		kind:       kindDst,
		cadence:    time.Hour,
		window:     6 * time.Hour,
		timeColumn: paramTime,
		cols:       map[column]string{colDst: "Dst"},
	},

	// GFZ Hp30 and ap30. Thirty-minute geomagnetic resolution, which NOAA does
	// not publish at any cadence.
	{
		id:         "gfz_obs_geo_30m_indices",
		kind:       kindGeomag,
		cadence:    30 * time.Minute,
		window:     3 * time.Hour,
		timeColumn: paramTime,
		cols:       map[column]string{colHp30: "Hp30", colAp30: "ap30"},
	},
}

// knownSpacecraft lists the selectable L1 monitors, in the order they are
// preferred when none of them reports itself as primary.
var knownSpacecraft = []string{"swpc", "ace", "wind", "imap"}

// selectDatasets returns the datasets to poll for the chosen spacecraft, plus
// the geomagnetic datasets, which are not spacecraft measurements and are
// always included.
//
// Order is significant: it is the tie-break when no dataset claims to be
// primary.
func selectDatasets(craft []string) []dataset {
	want := make(map[string]struct{}, len(craft))
	for _, c := range craft {
		want[normaliseSpacecraft(c)] = struct{}{}
	}

	var out []dataset
	for _, name := range knownSpacecraft {
		if _, ok := want[name]; !ok {
			continue
		}
		for _, d := range datasets {
			if d.craft == name {
				out = append(out, d)
			}
		}
	}
	for _, d := range datasets {
		if d.craft == "" {
			out = append(out, d)
		}
	}
	return out
}

// normaliseSpacecraft folds the aliases an operator is likely to write.
func normaliseSpacecraft(name string) string {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "swpc", "rtsw", "dscovr", "noaa":
		// DSCOVR and GOES both appear behind the SWPC real-time product
		// depending on which one SWPC has switched to, so the selector names
		// the product rather than the spacecraft.
		return "swpc"
	case "ace":
		return "ace"
	case "wind":
		return "wind"
	case "imap":
		return "imap"
	default:
		return strings.ToLower(strings.TrimSpace(name))
	}
}

// knownSpacecraftName reports whether a selector names a dataset we have.
func knownSpacecraftName(name string) bool {
	name = normaliseSpacecraft(name)
	for _, k := range knownSpacecraft {
		if k == name {
			return true
		}
	}
	return false
}
