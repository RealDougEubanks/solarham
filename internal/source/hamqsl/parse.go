package hamqsl

import (
	"encoding/xml"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// solarDocument is the root <solar> element.
//
// Declaring XMLName makes encoding/xml reject a document whose root is
// something else — a CDN error page, a captive portal, an HTML holding page —
// rather than silently unmarshalling into an empty struct and reporting a
// successful poll with no samples.
type solarDocument struct {
	XMLName xml.Name  `xml:"solar"`
	Data    solarData `xml:"solardata"`
}

// solarData is the single <solardata> child, and the whole payload.
//
// Every struct tag here matches the feed's own spelling on the wire, including
// its mistakes. The corrected spellings live in the metric vocabulary, not
// here. See the package comment for why this distinction cost the previous
// exporter four fields.
type solarData struct {
	// Attribution and provenance.
	Source  string `xml:"source"`
	Updated string `xml:"updated"`

	// Fields NOAA SWPC publishes first-hand. They are parsed so the document is
	// fully modelled — and so the misspellings above are pinned down by a test
	// rather than by a comment — but they are deliberately not emitted as
	// samples. The swpc source owns these series; publishing them from here as
	// well would create a second, disagreeing copy of each.
	SolarFlux     string `xml:"solarflux"`
	AIndex        string `xml:"aindex"`
	KIndex        string `xml:"kindex"`
	KIndexNT      string `xml:"kindexnt"`
	XRay          string `xml:"xray"`
	Sunspots      string `xml:"sunspots"`
	HeliumLine    string `xml:"heliumline"`
	ProtonFlux    string `xml:"protonflux"`
	ElectonFlux   string `xml:"electonflux"` // sic: the feed omits the 'r'.
	Aurora        string `xml:"aurora"`
	Normalization string `xml:"normalization"`
	LatDegree     string `xml:"latdegree"`
	SolarWind     string `xml:"solarwind"`
	MagneticField string `xml:"magneticfield"`

	// Fields this source publishes.
	Bands       []bandElement       `xml:"calculatedconditions>band"`
	VHF         []phenomenonElement `xml:"calculatedvhfconditions>phenomenon"`
	GeomagField string              `xml:"geomagfield"` // not "geomagneticfield".
	SignalNoise string              `xml:"signalnoise"`

	// Ionospheric fields. kc2g measures these per station; hamqsl's copies are
	// usually empty, and muf carries the literal "NoRpt" when unavailable.
	FoF2      string `xml:"fof2"`
	MUFFactor string `xml:"muffactor"` // two f's, not three.
	MUF       string `xml:"muf"`       // "muf", not "muff".
}

// bandElement is one <band name="80m-40m" time="day">Poor</band>.
type bandElement struct {
	Name      string `xml:"name,attr"`
	Time      string `xml:"time,attr"`
	Condition string `xml:",chardata"`
}

// phenomenonElement is one entry in <calculatedvhfconditions>.
type phenomenonElement struct {
	Name      string `xml:"name,attr"`
	Location  string `xml:"location,attr"`
	Condition string `xml:",chardata"`
}

// parse unmarshals the feed.
//
// A truncated or malformed body is an error rather than a partial result. Half
// a document is not half the truth: encoding/xml fills what it managed to read
// and leaves the rest zero, and a zero band condition means "Poor", which is a
// specific and wrong claim about propagation.
func parse(body []byte) (*solarData, error) {
	var doc solarDocument
	if err := xml.Unmarshal(body, &doc); err != nil {
		return nil, fmt.Errorf("hamqsl: parsing solar XML: %w", err)
	}
	return &doc.Data, nil
}

// updatedLayouts are the forms seen in <updated>.
//
// The observed value is " 09 Sep 2026 0322 GMT" — a leading space, a
// four-digit time with no separator, and a zone abbreviation. The zone is
// stripped before parsing rather than matched with MST, because Go resolves an
// unknown abbreviation to a fabricated zone with a zero offset, which looks
// like it worked and silently misplaces the sample by hours if the feed ever
// switches to a real one. Only GMT and UTC are accepted, and both mean UTC.
var updatedLayouts = []string{
	"02 Jan 2006 1504",
	"02 Jan 2006 15:04",
	"02 Jan 2006 150405",
	"2 Jan 2006 1504",
}

// parseUpdated interprets the feed's own generation timestamp.
//
// It reports failure rather than an error because a bad timestamp must not fail
// a poll: the band conditions in the same document are still good, and the
// caller falls back to the response receipt time.
func parseUpdated(raw string) (time.Time, bool) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return time.Time{}, false
	}

	upper := strings.ToUpper(s)
	switch {
	case strings.HasSuffix(upper, " GMT"):
		s = strings.TrimSpace(s[:len(s)-4])
	case strings.HasSuffix(upper, " UTC"):
		s = strings.TrimSpace(s[:len(s)-4])
	case strings.HasSuffix(upper, "Z"):
		s = strings.TrimSpace(s[:len(s)-1])
	default:
		// Anything else carries a zone we cannot honestly resolve, and guessing
		// UTC for, say, "EST" would be wrong by five hours.
		return time.Time{}, false
	}

	// Collapse the runs of whitespace the feed sometimes emits between fields.
	s = strings.Join(strings.Fields(s), " ")

	for _, layout := range updatedLayouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), true
		}
	}
	return time.Time{}, false
}

// bandOrdinal maps an HF band rating to a graphable number.
//
// The comparison is case-insensitive because the feed's own wording has varied,
// but it is exact beyond that: an unrecognised word returns false so the caller
// can publish the wording without inventing a value for it.
func bandOrdinal(condition string) (float64, bool) {
	switch strings.ToLower(strings.TrimSpace(condition)) {
	case "poor":
		return 0, true
	case "fair":
		return 1, true
	case "good":
		return 2, true
	}
	return 0, false
}

// vhfOrdinal maps a VHF phenomenon state to a graphable number.
func vhfOrdinal(condition string) (float64, bool) {
	switch normalizeSpaces(strings.ToLower(condition)) {
	case "band closed", "closed":
		return 0, true
	case "band open", "open":
		return 1, true
	}
	return 0, false
}

// normalizeSpaces trims and collapses whitespace, so " Band  Closed " and
// "Band Closed" compare equal.
func normalizeSpaces(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

// parseSignalNoise reads an S-meter reading, which the feed publishes either as
// a range ("S2-S3") or as a single value ("S4").
//
// A single value yields equal bounds rather than only a "min" series, so a
// dashboard querying bound="max" does not develop a hole whenever the noise
// floor happens to be flat.
func parseSignalNoise(raw string) (float64, float64, bool) {
	s := normalizeSpaces(raw)
	if s == "" || isPlaceholder(s) {
		return 0, 0, false
	}

	parts := strings.SplitN(s, "-", 2)
	lower, ok := parseSUnit(parts[0])
	if !ok {
		return 0, 0, false
	}
	if len(parts) == 1 {
		return lower, lower, true
	}
	upper, ok := parseSUnit(parts[1])
	if !ok {
		// "S2-" is malformed rather than a half-open range, and publishing the
		// lower bound alone would misrepresent it as a flat noise floor.
		return 0, 0, false
	}
	if upper < lower {
		lower, upper = upper, lower
	}
	return lower, upper, true
}

// parseSUnit reads one S-meter value such as "S3" or "3".
func parseSUnit(raw string) (float64, bool) {
	s := strings.TrimSpace(raw)
	s = strings.TrimPrefix(strings.TrimPrefix(s, "S"), "s")
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, false
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

// isPlaceholder reports whether a value is one of the feed's ways of saying "no
// data". These must produce no sample at all — not a zero, and not an error.
//
// The comparison is on the whitespace-collapsed, case-folded text because the
// feed pads its fields inconsistently.
func isPlaceholder(raw string) bool {
	switch normalizeSpaces(strings.ToLower(raw)) {
	case "", "norpt", "no rpt", "no report", "n/a", "na", "none", "no data", "-":
		return true
	}
	return false
}
