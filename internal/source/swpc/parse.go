package swpc

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// timeLayouts are every timestamp shape SWPC has been observed to publish.
//
// Two families are in use and both are common enough that a parser handling
// only one is wrong roughly half the time. The JSON summary and GOES products
// use RFC 3339 with a Z ("2026-09-09T03:46:00Z"); the older products serve a
// bare local-looking string with no zone at all ("2026-09-08T20:00:00"). The
// zoneless form is UTC — SWPC publishes nothing in any other zone — and
// time.Parse defaults to UTC for a layout with no zone, so it needs no special
// handling beyond being listed.
//
// The remaining layouts are the text products: alerts use a space separator
// with milliseconds, the aurora table uses an underscore, and D-RAP writes a
// trailing "UTC" that is stripped before parsing.
var timeLayouts = []string{
	time.RFC3339Nano,
	time.RFC3339,
	"2006-01-02T15:04:05.999999999",
	"2006-01-02T15:04:05",
	"2006-01-02T15:04",
	"2006-01-02 15:04:05.999999999",
	"2006-01-02 15:04:05",
	"2006-01-02 15:04",
	"2006-01-02_15:04:05",
	"2006-01-02_15:04",
	"2006-01-02",
}

// parseTime interprets any SWPC timestamp, always yielding UTC.
func parseTime(raw string) (time.Time, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return time.Time{}, fmt.Errorf("empty timestamp")
	}
	// D-RAP writes "2026-09-09 03:49 UTC"; the suffix is redundant with the
	// layouts and would only fail to match.
	s = strings.TrimSuffix(s, " UTC")
	s = strings.TrimSuffix(s, "Z UTC")

	for _, layout := range timeLayouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("unrecognised timestamp %q", raw)
}

// sentinels are the values NOAA writes to mean "no data".
//
// They must never become samples. -999.9 published as a solar wind speed is not
// a slow solar wind, it is a gap, and a dashboard that draws it as a number is
// actively misleading — worse than a dashboard with a hole in it, because the
// hole is obviously a hole.
var sentinels = []float64{-9999, -999.9, -999, -99999, -1e31}

// isSentinel reports whether v is a NOAA no-data marker or otherwise unusable.
func isSentinel(v float64) bool {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return true
	}
	for _, s := range sentinels {
		if math.Abs(v-s) < 1e-6 {
			return true
		}
	}
	return false
}

// number is a JSON value that may arrive as a number, as a string, or as null.
//
// SWPC is inconsistent about this within a single document: the NOAA scales
// product carries its scale levels as strings ("0") and uses null for "none",
// while the same file's probabilities are strings too. Decoding into float64
// fails on the strings and decoding into string fails on the numbers, so
// everything numeric goes through this type.
type number struct {
	Value float64
	Valid bool
}

// UnmarshalJSON accepts a number, a numeric string, or null.
func (n *number) UnmarshalJSON(data []byte) error {
	trimmed := strings.TrimSpace(string(data))
	if trimmed == "null" || trimmed == `""` {
		*n = number{}
		return nil
	}
	if trimmed != "" && trimmed[0] == '"' {
		var s string
		if err := json.Unmarshal(data, &s); err != nil {
			return err
		}
		s = strings.TrimSpace(s)
		if s == "" {
			*n = number{}
			return nil
		}
		v, err := strconv.ParseFloat(s, 64)
		if err != nil {
			// A non-numeric string is missing data, not a malformed document.
			// SWPC writes "*" and "n/a" in fields that are otherwise numeric.
			*n = number{}
			return nil
		}
		*n = number{Value: v, Valid: true}
		return nil
	}
	var v float64
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}
	*n = number{Value: v, Valid: true}
	return nil
}

// ok reports whether the value is present and is not a no-data sentinel.
func (n number) ok() bool { return n.Valid && !isSentinel(n.Value) }

// flareClassMagnitude maps the flare class letter to the decade of X-ray flux
// it denotes, in W/m² over the 0.1-0.8 nm band.
var flareClassMagnitude = map[byte]float64{
	'A': 1e-8,
	'B': 1e-7,
	'C': 1e-6,
	'M': 1e-5,
	'X': 1e-4,
}

// decodeFlareClass converts flare-class notation to W/m².
//
// "B3.5" is 3.5 × 10⁻⁷. The letter is the decade and the number is the
// mantissa, so the notation is a base-10 float in disguise and there is no
// reason to publish it only as a string when the quantity it names is a
// perfectly ordinary flux. The class itself is still published separately as an
// info metric, because "M1.2" is what operators say to each other.
//
// A bare letter with no mantissa means 1.0 in that decade, which is how SWPC
// writes the very bottom of a class.
func decodeFlareClass(class string) (float64, error) {
	s := strings.ToUpper(strings.TrimSpace(class))
	if s == "" {
		return 0, fmt.Errorf("empty flare class")
	}
	magnitude, ok := flareClassMagnitude[s[0]]
	if !ok {
		return 0, fmt.Errorf("unknown flare class letter in %q", class)
	}
	mantissa := 1.0
	if rest := strings.TrimSpace(s[1:]); rest != "" {
		v, err := strconv.ParseFloat(rest, 64)
		if err != nil {
			return 0, fmt.Errorf("unparseable flare class %q: %w", class, err)
		}
		mantissa = v
	}
	return mantissa * magnitude, nil
}

// row is one record from a SWPC product, whatever shape the document used.
type row map[string]any

// decodeRows decodes a SWPC array product into rows.
//
// Two shapes are in circulation and the same product has been seen in both, so
// both are supported rather than one being assumed. The modern shape is an
// array of objects. The older "products" shape is an array of arrays whose
// first element is a header row naming the columns:
//
//	[["time_tag","Kp","a_running","station_count"],["2026-09-08T21:00:00","3.00",...]]
//
// Assuming the object shape against the array shape does not fail loudly; it
// fails by decoding nothing, which is why this is worth the extra code.
func decodeRows(body []byte) ([]row, error) {
	var raw []json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("decoding array: %w", err)
	}
	if len(raw) == 0 {
		return nil, nil
	}

	first := strings.TrimLeft(string(raw[0]), " \t\r\n")
	if strings.HasPrefix(first, "[") {
		return decodeHeaderedRows(raw)
	}

	rows := make([]row, 0, len(raw))
	for i, r := range raw {
		var m row
		if err := json.Unmarshal(r, &m); err != nil {
			return nil, fmt.Errorf("decoding element %d: %w", i, err)
		}
		rows = append(rows, m)
	}
	return rows, nil
}

// decodeHeaderedRows handles the array-of-arrays shape with a leading header.
func decodeHeaderedRows(raw []json.RawMessage) ([]row, error) {
	var header []string
	if err := json.Unmarshal(raw[0], &header); err != nil {
		return nil, fmt.Errorf("decoding header row: %w", err)
	}
	rows := make([]row, 0, len(raw)-1)
	for i, r := range raw[1:] {
		var values []any
		if err := json.Unmarshal(r, &values); err != nil {
			return nil, fmt.Errorf("decoding element %d: %w", i+1, err)
		}
		m := make(row, len(header))
		for j, name := range header {
			if j < len(values) {
				m[name] = values[j]
			}
		}
		rows = append(rows, m)
	}
	return rows, nil
}

// numberField reads a numeric field that may be stored as a number or a string.
func (r row) numberField(name string) (float64, bool) {
	v, ok := r[name]
	if !ok || v == nil {
		return 0, false
	}
	switch typed := v.(type) {
	case float64:
		if isSentinel(typed) {
			return 0, false
		}
		return typed, true
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(typed), 64)
		if err != nil || isSentinel(f) {
			return 0, false
		}
		return f, true
	default:
		return 0, false
	}
}

// timeField reads a timestamp field.
func (r row) timeField(name string) (time.Time, bool) {
	s, ok := r[name].(string)
	if !ok {
		return time.Time{}, false
	}
	t, err := parseTime(s)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

// textLines splits a text product into its data lines, dropping comments and
// blanks.
//
// SWPC text products comment with '#', and the older ':Product:' headers use a
// leading colon. Neither is data.
func textLines(body []byte) []string {
	raw := strings.Split(strings.ReplaceAll(string(body), "\r\n", "\n"), "\n")
	out := make([]string, 0, len(raw))
	for _, line := range raw {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") || strings.HasPrefix(trimmed, ":") {
			continue
		}
		out = append(out, line)
	}
	return out
}

// headerValue finds a labelled value in a text product's comment header, such
// as OVATION's "Hemispheric Power:     12.9" or D-RAP's "Product Valid At :".
func headerValue(body []byte, label string) (string, bool) {
	for _, line := range strings.Split(strings.ReplaceAll(string(body), "\r\n", "\n"), "\n") {
		clean := strings.TrimSpace(strings.TrimLeft(line, "#: \t"))
		lower := strings.ToLower(clean)
		want := strings.ToLower(label)
		if !strings.HasPrefix(lower, want) {
			continue
		}
		rest := strings.TrimSpace(clean[len(label):])
		rest = strings.TrimSpace(strings.TrimPrefix(rest, ":"))
		if rest == "" {
			continue
		}
		return rest, true
	}
	return "", false
}

// formatCoordinate renders a grid coordinate as a label value.
//
// Trailing zeros are dropped so 67.5 is "67.5" and 68 is "68" rather than
// "68.0". Label values are strings, and a series whose latitude label changes
// spelling between polls is a different series to Prometheus, so this has to be
// deterministic.
func formatCoordinate(v float64) string {
	return strconv.FormatFloat(v, 'f', -1, 64)
}
