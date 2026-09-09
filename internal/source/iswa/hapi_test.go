package iswa

import (
	"strings"
	"testing"
	"time"
)

func TestParseHAPIReadsTheHeaderAndTheRowsFromOneResponse(t *testing.T) {
	got, err := parseHAPI(fixture(t, "swpc_rtsw_mag_P1M.csv"))
	if err != nil {
		t.Fatalf("parseHAPI: %v", err)
	}
	if got.Status.Code != statusOK {
		t.Errorf("status = %d, want %d", got.Status.Code, statusOK)
	}
	wantColumns := []string{"Time", "B_x", "B_y", "B_z", "B_t", "Latitude", "Longitude", "isPrimary", "source"}
	if len(got.Columns) != len(wantColumns) {
		t.Fatalf("parsed %d columns, want %d", len(got.Columns), len(wantColumns))
	}
	for i, name := range wantColumns {
		if got.Columns[i].Name != name {
			t.Errorf("column %d = %q, want %q", i, got.Columns[i].Name, name)
		}
	}
	if len(got.Rows) == 0 {
		t.Fatal("parsed no rows")
	}
}

func TestParseHAPIReadsAStatusFromABareJSONBody(t *testing.T) {
	got, err := parseHAPI(fixture(t, "error_1411.json"))
	if err != nil {
		t.Fatalf("parseHAPI: %v", err)
	}
	if got.Status.ok() {
		t.Error("a 1411 status was read as a success")
	}
	if msg := got.statusError().Error(); !strings.Contains(msg, "1411") {
		t.Errorf("statusError = %q, want it to name the code", msg)
	}
}

func TestStatus1201And1406MeanValidRequestEmptyAnswer(t *testing.T) {
	for _, name := range []string{"status_1201.json", "no_data_1406.json"} {
		got, err := parseHAPI(fixture(t, name))
		if err != nil {
			t.Fatalf("parseHAPI(%s): %v", name, err)
		}
		if !got.Status.ok() {
			t.Errorf("%s: status %d read as a failure, want a success with no rows", name, got.Status.Code)
		}
		if !got.Status.noData() {
			t.Errorf("%s: status %d not recognised as no-data", name, got.Status.Code)
		}
	}
}

// TestDuplicateColumnNamesKeepTheFirstPosition covers imap_mag, whose header
// declares Time twice.
func TestDuplicateColumnNamesKeepTheFirstPosition(t *testing.T) {
	got, err := parseHAPI(fixture(t, "imap_mag.csv"))
	if err != nil {
		t.Fatalf("parseHAPI: %v", err)
	}
	if dupes := got.duplicateColumns(); len(dupes) != 1 || dupes[0] != paramTime {
		t.Errorf("duplicateColumns() = %v, want [%s]", dupes, paramTime)
	}
	if idx := got.index[paramTime]; idx != 0 {
		t.Errorf("Time resolved to column %d, want the first occurrence at 0", idx)
	}
	if _, _, ok := got.newest(paramTime); !ok {
		t.Error("the duplicated time column could not be read")
	}
}

// TestNewestIgnoresRowOrder proves the newest row is chosen by its timestamp
// rather than by its position, so a reverse-ordered response does not publish
// the oldest reading in the window as the current one.
func TestNewestIgnoresRowOrder(t *testing.T) {
	body := "#{\"status\":{\"code\":1200,\"message\":\"OK\"}," +
		"\"parameters\":[{\"name\":\"Time\"},{\"name\":\"B_t\"}]}\n" +
		"2026-09-09T15:40:00Z,7.09\n" +
		"2026-09-09T15:38:00Z,6.89\n" +
		"2026-09-09T15:39:00Z,7.26\n"

	got, err := parseHAPI([]byte(body))
	if err != nil {
		t.Fatalf("parseHAPI: %v", err)
	}
	row, at, ok := got.newest(paramTime)
	if !ok {
		t.Fatal("no newest row found")
	}
	want := time.Date(2026, 9, 9, 15, 40, 0, 0, time.UTC)
	if !at.Equal(want) {
		t.Errorf("newest row is at %s, want %s", at, want)
	}
	if v, ok := got.float(row, "B_t"); !ok || v != 7.09 {
		t.Errorf("newest B_t = (%v, %v), want (7.09, true)", v, ok)
	}
}

// TestFillValuesAreReadAsGapsRatherThanMeasurements matters because the
// magnetic field parameters declare a fill of -999.9. Taken literally that is
// the largest southward Bz ever recorded.
func TestFillValuesAreReadAsGapsRatherThanMeasurements(t *testing.T) {
	body := "#{\"status\":{\"code\":1200,\"message\":\"OK\"},\"parameters\":[" +
		"{\"name\":\"Time\",\"fill\":\"null\"}," +
		"{\"name\":\"B_z\",\"fill\":\"-999.9\"}," +
		"{\"name\":\"B_t\",\"fill\":\"-999.9\"}," +
		"{\"name\":\"Latitude\",\"fill\":\"null\"}]}\n" +
		"2026-09-09T15:40:00Z,-999.9,-999.90,null\n"

	got, err := parseHAPI([]byte(body))
	if err != nil {
		t.Fatalf("parseHAPI: %v", err)
	}
	row := got.Rows[0]

	if v, ok := got.float(row, "B_z"); ok {
		t.Errorf("B_z fill value read as the measurement %v", v)
	}
	if v, ok := got.float(row, "B_t"); ok {
		t.Errorf("B_t fill written as -999.90 read as the measurement %v", v)
	}
	if v, ok := got.value(row, "Latitude"); ok {
		t.Errorf("a null field read as the value %q", v)
	}
}

func TestParseHAPIRejectsBodiesItCannotName(t *testing.T) {
	cases := map[string]string{
		"an empty body":            "",
		"CSV with no header":       "2026-09-09T15:40:00Z,7.09\n",
		"a header that isn't JSON": "#nonsense\n2026-09-09T15:40:00Z,7.09\n",
		"a header with no parameters": "#{\"status\":{\"code\":1200,\"message\":\"OK\"}}\n" +
			"2026-09-09T15:40:00Z,7.09\n",
		"a row with the wrong field count": "#{\"status\":{\"code\":1200,\"message\":\"OK\"}," +
			"\"parameters\":[{\"name\":\"Time\"},{\"name\":\"B_t\"}]}\n" +
			"2026-09-09T15:40:00Z,7.09,4.77\n",
	}
	for name, body := range cases {
		if _, err := parseHAPI([]byte(body)); err == nil {
			t.Errorf("parseHAPI accepted %s", name)
		}
	}
}

func TestParseHAPITimeAcceptsTheServersPrecisions(t *testing.T) {
	for _, raw := range []string{
		"2026-09-09T15:40:00Z",
		"2026-09-09T15:43:32Z",
		"2026-09-09T15:40Z",
		"2026-09-09T15:40:00.123Z",
		"2026-09-09",
	} {
		if _, err := parseHAPITime(raw); err != nil {
			t.Errorf("parseHAPITime(%q): %v", raw, err)
		}
	}
	for _, raw := range []string{"", "not-a-time", "15:40:00"} {
		if _, err := parseHAPITime(raw); err == nil {
			t.Errorf("parseHAPITime(%q) succeeded", raw)
		}
	}
}

func TestSelectDatasetsAlwaysIncludesTheGeomagneticDatasets(t *testing.T) {
	got := selectDatasets([]string{"swpc"})

	var haveDst, haveGeomag bool
	for _, d := range got {
		switch d.kind {
		case kindDst:
			haveDst = true
		case kindGeomag:
			haveGeomag = true
		}
		if d.craft != "" && d.craft != "swpc" {
			t.Errorf("selected %s for spacecraft %q", d.id, d.craft)
		}
	}
	if !haveDst {
		t.Error("Dst is not a spacecraft measurement and must always be selected")
	}
	if !haveGeomag {
		t.Error("the half-hourly geomagnetic indices must always be selected")
	}
}

func TestSelectDatasetsExcludesTheKnownStaleProducts(t *testing.T) {
	// Predicted_KP_P1M and friends serve a fresh-looking 200 over content that
	// has not moved in months. They must not be reachable by configuration.
	stale := []string{"Predicted_KP_P1M", "SWMF2023_RT_GMlog_P1M", "NMDB_p1m_NMDB-APTY"}
	for _, d := range datasets {
		for _, bad := range stale {
			if d.id == bad {
				t.Errorf("the dataset table includes %s, which was measured stale", bad)
			}
		}
	}
	all := selectDatasets(knownSpacecraft)
	if len(all) != len(datasets) {
		t.Errorf("selecting every monitor gave %d datasets, want all %d", len(all), len(datasets))
	}
}

func TestEveryDatasetRequestsAWindowProportionateToItsCadence(t *testing.T) {
	for _, d := range datasets {
		if d.window <= 0 {
			t.Errorf("dataset %s has no trailing window", d.id)
			continue
		}
		if d.cadence <= 0 {
			t.Errorf("dataset %s declares no cadence", d.id)
			continue
		}
		// A window is meant to catch a handful of published rows, not a day of
		// them. Sixty rows is the ceiling; imap_mag's four-second cadence is
		// the case this guards.
		if rows := d.window / d.cadence; rows > 60 {
			t.Errorf("dataset %s asks for about %d rows per poll (%s window at a %s cadence)",
				d.id, rows, d.window, d.cadence)
		}
		if _, ok := d.cols[colBt]; !ok {
			if d.kind == kindMag {
				t.Errorf("magnetic field dataset %s names no B_t equivalent", d.id)
			}
		}
	}
}

func TestNormaliseSpacecraftFoldsTheAliasesAnOperatorWillWrite(t *testing.T) {
	for in, want := range map[string]string{
		"SWPC":     "swpc",
		" dscovr ": "swpc",
		"noaa":     "swpc",
		"rtsw":     "swpc",
		"ACE":      "ace",
		"Wind":     "wind",
		"IMAP":     "imap",
	} {
		if got := normaliseSpacecraft(in); got != want {
			t.Errorf("normaliseSpacecraft(%q) = %q, want %q", in, got, want)
		}
		if !knownSpacecraftName(in) {
			t.Errorf("knownSpacecraftName(%q) = false", in)
		}
	}
	if knownSpacecraftName("voyager") {
		t.Error("knownSpacecraftName accepted a spacecraft with no dataset")
	}
}
