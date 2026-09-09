package pskreporter

// Band derivation from a dial frequency in hertz.
//
// PSK Reporter reports the frequency, not the band, so the band label has to be
// derived. This is the same problem the POTA source has and it is solved the
// same way — a local table — rather than shared, because the two feeds report
// frequencies with different precision and different conventions about which
// edge of a band a digital sub-segment sits in, and a single shared table would
// have to be a compromise between them.
//
// The edges below are the ITU amateur allocations, widened to the widest
// regional variant wherever the three regions differ, because a report can come
// from any of them. 40 m is 7.000–7.300 MHz because Region 2 goes to 7.300 even
// though Regions 1 and 3 stop at 7.200; 10 m runs to 29.700; 2 m runs to 148
// even though Region 1 stops at 146. Being generous at the edges is the right
// error: a slightly wide band puts a report in the band an operator would call
// it, where a strict band drops it.
//
// A frequency outside every allocation is skipped rather than published under a
// synthesised label. Unlike wspr.live's band column — where an unknown code is
// a band the network has added and the data is real — a digital reception report
// outside every amateur allocation is a malformed report or a receiver with a
// broken frequency readout, and inventing a series for it would put permanent
// junk in the label space.

// bandRange is one allocation, in hertz, inclusive of both edges.
type bandRange struct {
	label string
	loHz  float64
	hiHz  float64
}

// bandRanges is ordered by frequency so that a linear scan reads in the order
// an operator thinks about the spectrum. There are two dozen entries; a sorted
// binary search would be faster and less obvious.
var bandRanges = []bandRange{
	{"2200m", 135_700, 137_800},
	{"630m", 472_000, 479_000},
	{"160m", 1_800_000, 2_000_000},
	{"80m", 3_500_000, 4_000_000},
	{"60m", 5_250_000, 5_450_000},
	{"40m", 7_000_000, 7_300_000},
	{"30m", 10_100_000, 10_150_000},
	{"20m", 14_000_000, 14_350_000},
	{"17m", 18_068_000, 18_168_000},
	{"15m", 21_000_000, 21_450_000},
	{"12m", 24_890_000, 24_990_000},
	{"10m", 28_000_000, 29_700_000},
	{"8m", 40_660_000, 40_700_000},
	{"6m", 50_000_000, 54_000_000},
	{"4m", 70_000_000, 70_500_000},
	{"2m", 144_000_000, 148_000_000},
	{"1.25m", 222_000_000, 225_000_000},
	{"70cm", 420_000_000, 450_000_000},
	{"33cm", 902_000_000, 928_000_000},
	{"23cm", 1_240_000_000, 1_300_000_000},
	{"13cm", 2_300_000_000, 2_450_000_000},
	{"9cm", 3_300_000_000, 3_500_000_000},
	{"5cm", 5_650_000_000, 5_925_000_000},
	{"3cm", 10_000_000_000, 10_500_000_000},
}

// bandForHz returns the band label for a frequency in hertz, or the empty
// string when it falls outside every allocation.
func bandForHz(hz float64) string {
	for _, b := range bandRanges {
		if hz >= b.loHz && hz <= b.hiHz {
			return b.label
		}
	}
	return ""
}
