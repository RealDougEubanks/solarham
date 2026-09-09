package donki

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// cmeRecord is one entry from the CME endpoint.
//
// The endpoint returns a great deal more per record — instrument lists, free
// text notes, and the full nested analysis array — all of which is ignored.
// Only the identity and the start time are needed to count events in a window.
type cmeRecord struct {
	ActivityID string `json:"activityID"`
	StartTime  string `json:"startTime"`
}

// cmeAnalysisRecord is one entry from the CMEAnalysis endpoint.
//
// time21_5 is when the leading edge passed 21.5 solar radii, which is the
// model's own reference surface and the timestamp DONKI orders analyses by. It
// is used as this record's observation time in preference to the submission
// time: the submission time says when a human got round to it.
type cmeAnalysisRecord struct {
	Time215        string   `json:"time21_5"`
	Latitude       *float64 `json:"latitude"`
	Longitude      *float64 `json:"longitude"`
	HalfAngle      *float64 `json:"halfAngle"`
	Speed          *float64 `json:"speed"`
	Type           string   `json:"type"`
	IsMostAccurate bool     `json:"isMostAccurate"`
}

// flareRecord is one entry from the FLR endpoint.
type flareRecord struct {
	FlrID     string `json:"flrID"`
	BeginTime string `json:"beginTime"`
	PeakTime  string `json:"peakTime"`
	ClassType string `json:"classType"`
}

// observedAt is the flare's peak time where DONKI has recorded one, falling
// back to its begin time. A flare in progress has a begin time and no peak.
func (f flareRecord) observedAt() (time.Time, bool) {
	if t, err := parseDONKITime(f.PeakTime); err == nil {
		return t, true
	}
	if t, err := parseDONKITime(f.BeginTime); err == nil {
		return t, true
	}
	return time.Time{}, false
}

// flareClass is the first letter of a GOES class designation: B, C, M or X.
//
// Only the letter is published. The mantissa — the 2.4 in C2.4 — is a
// continuous quantity that already has a home in the GOES X-ray flux metric
// SWPC publishes; what a catalogue count adds is how many flares of each
// magnitude class occurred, which is the letter.
func flareClass(classType string) (string, bool) {
	classType = strings.TrimSpace(strings.ToUpper(classType))
	if classType == "" {
		return "", false
	}
	letter := classType[:1]
	switch letter {
	case "A", "B", "C", "M", "X":
		return letter, true
	default:
		return "", false
	}
}

// decodeJSON reads a DONKI response into v.
//
// DONKI answers a window with no events as the two-byte document "[]" rather
// than as an error or an empty body, which unmarshals to a nil slice and needs
// no special case. A body that is not JSON at all does need one: it is
// typically an HTML error page from an intermediary, and reporting "invalid
// character '<'" without saying what was being read is not much use in a log.
func decodeJSON(what string, body []byte, v any) error {
	if len(body) == 0 {
		return fmt.Errorf("%s returned an empty body", what)
	}
	if err := json.Unmarshal(body, v); err != nil {
		return fmt.Errorf("%s returned a body that is not JSON: %w", what, err)
	}
	return nil
}

// donkiTimeLayouts are the forms DONKI writes timestamps in. Minute precision
// with a Z suffix is what every observed record uses, but the catalogue is
// curated by hand and a stray seconds field should not lose an event.
var donkiTimeLayouts = []string{
	"2006-01-02T15:04Z",
	"2006-01-02T15:04:05Z",
	"2006-01-02T15:04:05Z07:00",
	"2006-01-02T15:04Z07:00",
	"2006-01-02T15:04:05",
	"2006-01-02T15:04",
}

// parseDONKITime reads a DONKI timestamp as UTC.
func parseDONKITime(raw string) (time.Time, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, fmt.Errorf("empty timestamp")
	}
	for _, layout := range donkiTimeLayouts {
		if t, err := time.Parse(layout, raw); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("timestamp %q is not a DONKI time", raw)
}
