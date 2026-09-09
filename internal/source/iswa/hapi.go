package iswa

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// HAPI status codes this package treats specially.
//
// The HAPI specification puts the real outcome of a request in a JSON status
// object, and permits a server to return that object with HTTP 200. That is not
// a hypothetical: a malformed request to a HAPI server can arrive as a 200
// whose body says 1411, and an empty time range can arrive as a 200 whose body
// says 1201. Trusting the HTTP code alone therefore produces two distinct
// wrong answers — an error read as data, and no-data read as an error.
const (
	// statusOK is 1200, a normal successful response.
	statusOK = 1200

	// statusNoData is 1201, "OK - no data for time range". This is a success:
	// the request was valid and the answer is that the window is empty. It
	// happens routinely at the leading edge of a dataset, where the newest
	// minute has not been ingested yet.
	statusNoData = 1201

	// statusNoDataAlt is 1406, "No data available for the specified ID and
	// time range". iswa.gsfc.nasa.gov returns this rather than 1201, and pairs
	// it with HTTP 404. It means the same thing.
	statusNoDataAlt = 1406
)

// hapiStatus is the status object every HAPI response carries.
type hapiStatus struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// ok reports whether the status means the request succeeded, with or without
// rows.
func (s hapiStatus) ok() bool {
	return s.Code == 0 || s.Code == statusOK || s.noData()
}

// noData reports whether the status means "valid request, empty answer".
func (s hapiStatus) noData() bool {
	return s.Code == statusNoData || s.Code == statusNoDataAlt
}

// hapiColumn is one declared parameter of a dataset, in declaration order.
//
// Fill is the sentinel the dataset uses for a missing value. The magnetic field
// parameters declare "-999.9", which read as a real measurement rather than a
// gap if the fill is ignored — a southward Bz of minus a thousand nanotesla
// would be the largest storm ever recorded.
type hapiColumn struct {
	Name string
	Fill string
}

// hapiResponse is a parsed HAPI data response.
type hapiResponse struct {
	Status  hapiStatus
	Columns []hapiColumn
	Rows    [][]string

	// index maps a column name to its position. A name declared twice keeps
	// the first position; see parseHAPI.
	index map[string]int
}

// parseHAPI reads a HAPI data response requested as format=csv&include=header.
//
// That request form returns the dataset's JSON header as "#"-prefixed lines
// followed by bare CSV rows, which is why this package asks for it: the header
// and the rows are then guaranteed to describe each other, and one request does
// the work of a separate /info call plus a /data call. HAPI CSV carries no
// column names of its own, so without the header the columns can only be
// guessed at.
//
// An error response is a bare JSON document with no "#" prefixes and no rows,
// so both shapes are handled here.
func parseHAPI(body []byte) (*hapiResponse, error) {
	text := strings.TrimSpace(string(body))
	if text == "" {
		return nil, fmt.Errorf("empty response body")
	}

	// A bare JSON document is an error or status-only response.
	if strings.HasPrefix(text, "{") {
		var doc struct {
			Status     hapiStatus   `json:"status"`
			Parameters []hapiColumn `json:"parameters"`
		}
		if err := json.Unmarshal([]byte(text), &doc); err != nil {
			return nil, fmt.Errorf("body is neither HAPI CSV nor HAPI JSON: %w", err)
		}
		return newResponse(doc.Status, doc.Parameters, nil), nil
	}

	var header strings.Builder
	var data strings.Builder
	for line := range strings.SplitSeq(text, "\n") {
		trimmed := strings.TrimRight(line, "\r")
		if strings.HasPrefix(trimmed, "#") {
			header.WriteString(strings.TrimPrefix(trimmed, "#"))
			header.WriteByte('\n')
			continue
		}
		if strings.TrimSpace(trimmed) == "" {
			continue
		}
		data.WriteString(trimmed)
		data.WriteByte('\n')
	}

	if header.Len() == 0 {
		return nil, fmt.Errorf("response carried no HAPI header; cannot name the columns")
	}

	var doc struct {
		Status     hapiStatus   `json:"status"`
		Parameters []hapiColumn `json:"parameters"`
	}
	if err := json.Unmarshal([]byte(header.String()), &doc); err != nil {
		return nil, fmt.Errorf("HAPI header is not JSON: %w", err)
	}
	if len(doc.Parameters) == 0 {
		return nil, fmt.Errorf("HAPI header declared no parameters")
	}

	// FieldsPerRecord is disabled so that a short or long row is reported by
	// this package, naming the dataset, rather than as an opaque csv error.
	reader := csv.NewReader(strings.NewReader(data.String()))
	reader.FieldsPerRecord = -1
	reader.ReuseRecord = false
	rows, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("malformed HAPI CSV: %w", err)
	}
	for i, row := range rows {
		if len(row) != len(doc.Parameters) {
			return nil, fmt.Errorf("HAPI CSV row %d has %d fields but the header declares %d parameters",
				i+1, len(row), len(doc.Parameters))
		}
	}

	return newResponse(doc.Status, doc.Parameters, rows), nil
}

// newResponse builds the name index.
//
// Duplicate column names are tolerated rather than rejected. The imap_mag
// dataset declares its Time parameter twice — a server-side bug, verified live
// — and emits two identical time columns to match. Rejecting that would lose a
// working four-second magnetometer over a cosmetic defect, so the first
// occurrence of a name wins and the duplicate is simply never addressed.
func newResponse(status hapiStatus, cols []hapiColumn, rows [][]string) *hapiResponse {
	index := make(map[string]int, len(cols))
	for i, c := range cols {
		if _, seen := index[c.Name]; seen {
			continue
		}
		index[c.Name] = i
	}
	return &hapiResponse{Status: status, Columns: cols, Rows: rows, index: index}
}

// statusError describes a non-success HAPI status, naming both the code and the
// server's message. The code is the part worth grepping for; the message is the
// part worth reading.
func (r *hapiResponse) statusError() error {
	return fmt.Errorf("HAPI status %d: %s", r.Status.Code, r.Status.Message)
}

// duplicateColumns lists names the header declared more than once, for logging.
func (r *hapiResponse) duplicateColumns() []string {
	seen := make(map[string]int, len(r.Columns))
	var dupes []string
	for _, c := range r.Columns {
		seen[c.Name]++
		if seen[c.Name] == 2 {
			dupes = append(dupes, c.Name)
		}
	}
	return dupes
}

// newest returns the row with the largest value in the named time column, and
// that time.
//
// The rows arrive in ascending order in practice, but "in practice" is not a
// guarantee any HAPI server makes, and taking the last row of a
// reverse-ordered response would publish the oldest reading in the window as
// the current one.
func (r *hapiResponse) newest(timeColumn string) ([]string, time.Time, bool) {
	idx, ok := r.index[timeColumn]
	if !ok {
		return nil, time.Time{}, false
	}

	var best []string
	var bestAt time.Time
	for _, row := range r.Rows {
		at, err := parseHAPITime(row[idx])
		if err != nil {
			continue
		}
		if best == nil || at.After(bestAt) {
			best, bestAt = row, at
		}
	}
	if best == nil {
		return nil, time.Time{}, false
	}
	return best, bestAt, true
}

// value reads a named column out of a row, reporting whether it held a real
// measurement. A fill value, an empty field or a literal "null" is a gap.
func (r *hapiResponse) value(row []string, name string) (string, bool) {
	idx, ok := r.index[name]
	if !ok || idx >= len(row) {
		return "", false
	}
	raw := strings.TrimSpace(row[idx])
	if raw == "" || strings.EqualFold(raw, "null") || strings.EqualFold(raw, "nan") {
		return "", false
	}
	if fill := strings.TrimSpace(r.Columns[idx].Fill); fill != "" && !strings.EqualFold(fill, "null") {
		if raw == fill {
			return "", false
		}
		// Compare numerically too, so that a fill declared as "-999.9" also
		// matches a field written as "-999.90".
		if f, err := strconv.ParseFloat(fill, 64); err == nil {
			if v, err := strconv.ParseFloat(raw, 64); err == nil && v == f {
				return "", false
			}
		}
	}
	return raw, true
}

// float reads a named column as a number.
func (r *hapiResponse) float(row []string, name string) (float64, bool) {
	raw, ok := r.value(row, name)
	if !ok {
		return 0, false
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

// hapiTimeLayouts are the ISO 8601 forms observed from this server. HAPI
// permits several precisions, and a parser that only accepts one of them fails
// on a dataset that happens to publish another.
var hapiTimeLayouts = []string{
	"2006-01-02T15:04:05.999999999Z07:00",
	"2006-01-02T15:04:05Z07:00",
	"2006-01-02T15:04Z07:00",
	"2006-01-02T15:04:05.999999999Z",
	"2006-01-02T15:04:05Z",
	"2006-01-02T15:04Z",
	"2006-01-02T15:04:05",
	"2006-01-02",
}

// parseHAPITime reads a HAPI isotime field as UTC.
func parseHAPITime(raw string) (time.Time, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, fmt.Errorf("empty timestamp")
	}
	for _, layout := range hapiTimeLayouts {
		if t, err := time.Parse(layout, raw); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("timestamp %q is not ISO 8601", raw)
}
