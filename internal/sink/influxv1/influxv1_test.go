package influxv1

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/RealDougEubanks/solarham/internal/config"
	"github.com/RealDougEubanks/solarham/internal/metric"
	"github.com/RealDougEubanks/solarham/internal/redact"
	"github.com/RealDougEubanks/solarham/internal/sink"
)

// testPassword is deliberately unlike anything else in a log line, so a test
// can assert its absence without matching something innocent.
const testPassword = "canary-p4ssw0rd-do-not-log"

// testUser is the account those credentials belong to.
const testUser = "canary-user-do-not-log"

// sampleTime is a fixed instant so expected line protocol can be written out in
// full rather than recomputed by the test.
var sampleTime = time.Unix(1700000000, 0).UTC()

// Descriptors mirroring the real table's shapes: a bare gauge, a gauge with
// labels, and an info metric whose last label carries the category.
var (
	fluxDesc = &metric.Descriptor{
		Name: "flux_sfu",
		Kind: metric.KindGauge,
	}

	foF2Desc = &metric.Descriptor{
		Name:   "fof2_megahertz",
		Kind:   metric.KindGauge,
		Labels: []string{"station", "station_name"},
	}

	xrayClassDesc = &metric.Descriptor{
		Name:   "xray_class_info",
		Kind:   metric.KindInfo,
		Labels: []string{"class"},
	}
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func testConfig(serverURL string) config.InfluxV1 {
	return config.InfluxV1{
		Enabled:     true,
		URL:         serverURL,
		Database:    "solarham",
		Username:    testUser,
		Password:    redact.New(testPassword),
		Measurement: "solar",
		Timeout:     2 * time.Second,
		Retries:     2,
	}
}

// wantBasicAuth is the header the configured credentials must produce.
func wantBasicAuth() string {
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(testUser+":"+testPassword))
}

// sampleBatch is one batch covering all three descriptor shapes, including a
// station name with commas and spaces in it.
func sampleBatch() metric.Batch {
	return metric.Batch{
		Source:  "swpc-slow",
		Fetched: sampleTime,
		Samples: []metric.Sample{
			{Desc: fluxDesc, Value: 137.5, Time: sampleTime},
			{
				Desc:   foF2Desc,
				Labels: []string{"AU930", "Austin, TX, USA"},
				Value:  8.25,
				Time:   sampleTime,
			},
			{
				Desc:   xrayClassDesc,
				Labels: []string{"B3.8"},
				Value:  1,
				Time:   sampleTime,
			},
		},
	}
}

// wantSampleBody is the exact line protocol sampleBatch must produce. Tags are
// sorted by key, so the metric and source tags interleave with the
// descriptor's own labels.
const wantSampleBody = `solar,metric=flux_sfu,source=swpc-slow value=137.5 1700000000000000000
solar,metric=fof2_megahertz,source=swpc-slow,station=AU930,station_name=Austin\,\ TX\,\ USA value=8.25 1700000000000000000
solar,class=B3.8,metric=xray_class_info,source=swpc-slow value=1,category="B3.8" 1700000000000000000`

func newSink(t *testing.T, cfg config.InfluxV1, log *slog.Logger) *Sink {
	t.Helper()
	s, err := New(cfg, log)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// Retry pacing is not what most of these tests are measuring.
	s.backoff = time.Millisecond
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// bodyRecorder answers every write with the given status and remembers what it
// was sent.
type bodyRecorder struct {
	body   string
	path   string
	query  string
	method string
	auth   string
	ctype  string
	calls  atomic.Int32
}

func (rec *bodyRecorder) handler(status int) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rec.calls.Add(1)
		body, _ := io.ReadAll(r.Body)
		rec.body = string(body)
		rec.path = r.URL.Path
		rec.query = r.URL.RawQuery
		rec.method = r.Method
		rec.auth = r.Header.Get("Authorization")
		rec.ctype = r.Header.Get("Content-Type")
		w.WriteHeader(status)
	}
}

func TestPublishSkipsAnEmptyBatch(t *testing.T) {
	var rec bodyRecorder
	srv := httptest.NewServer(rec.handler(http.StatusNoContent))
	defer srv.Close()

	s := newSink(t, testConfig(srv.URL), discardLogger())
	err := s.Publish(context.Background(), metric.Batch{Source: "swpc-fast", Fetched: sampleTime})
	if !errors.Is(err, sink.ErrSkipped) {
		t.Errorf("Publish returned %v, want sink.ErrSkipped", err)
	}
	if got := rec.calls.Load(); got != 0 {
		t.Errorf("server saw %d requests for an empty batch, want 0", got)
	}
}

func TestPublishSendsTheExpectedLineProtocolInOneRequest(t *testing.T) {
	var rec bodyRecorder
	srv := httptest.NewServer(rec.handler(http.StatusNoContent))
	defer srv.Close()

	s := newSink(t, testConfig(srv.URL), discardLogger())
	if err := s.Publish(context.Background(), sampleBatch()); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	if rec.body != wantSampleBody {
		t.Errorf("body:\n got %q\nwant %q", rec.body, wantSampleBody)
	}
	if got := rec.calls.Load(); got != 1 {
		t.Errorf("server saw %d requests, want 1: a batch is one POST", got)
	}
	if rec.method != http.MethodPost {
		t.Errorf("method = %q, want POST", rec.method)
	}
	if rec.path != "/write" {
		t.Errorf("path = %q, want /write", rec.path)
	}
	if !strings.HasPrefix(rec.ctype, "text/plain") {
		t.Errorf("Content-Type = %q, want text/plain", rec.ctype)
	}
}

func TestPublishSendsTheConfiguredDatabaseAndNanosecondPrecision(t *testing.T) {
	var rec bodyRecorder
	srv := httptest.NewServer(rec.handler(http.StatusNoContent))
	defer srv.Close()

	s := newSink(t, testConfig(srv.URL), discardLogger())
	if err := s.Publish(context.Background(), sampleBatch()); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	q, err := url.ParseQuery(rec.query)
	if err != nil {
		t.Fatalf("parse query: %v", err)
	}
	if q.Get("db") != "solarham" {
		t.Errorf("query db = %q, want solarham", q.Get("db"))
	}
	if q.Get("precision") != "ns" {
		t.Errorf("query precision = %q, want ns", q.Get("precision"))
	}
}

// TestPublishSendsCredentialsAsBasicAuthNotQueryParameters records the choice
// this sink makes over the u=/p= parameters InfluxDB 1.x also accepts: a
// password in a query string lands in the server's access log, in any proxy in
// front of it, and in the *url.Error this sink logs on every retry.
func TestPublishSendsCredentialsAsBasicAuthNotQueryParameters(t *testing.T) {
	var rec bodyRecorder
	srv := httptest.NewServer(rec.handler(http.StatusNoContent))
	defer srv.Close()

	s := newSink(t, testConfig(srv.URL), discardLogger())
	if err := s.Publish(context.Background(), sampleBatch()); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	if rec.auth != wantBasicAuth() {
		t.Errorf("Authorization = %q, want %q", rec.auth, wantBasicAuth())
	}

	q, err := url.ParseQuery(rec.query)
	if err != nil {
		t.Fatalf("parse query: %v", err)
	}
	if q.Has("u") || q.Has("p") {
		t.Errorf("credentials were sent as query parameters: %q", rec.query)
	}
	if strings.Contains(rec.query, testPassword) || strings.Contains(rec.query, testUser) {
		t.Errorf("a credential appeared in the query string: %q", rec.query)
	}
}

func TestPublishSendsNoAuthorizationHeaderWhenNoCredentialsAreConfigured(t *testing.T) {
	var rec bodyRecorder
	srv := httptest.NewServer(rec.handler(http.StatusNoContent))
	defer srv.Close()

	cfg := testConfig(srv.URL)
	cfg.Username = ""
	cfg.Password = redact.Secret{}

	s := newSink(t, cfg, discardLogger())
	if err := s.Publish(context.Background(), sampleBatch()); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	// Many home installations run InfluxDB unauthenticated; an empty Basic
	// header against such a server is not harmless.
	if rec.auth != "" {
		t.Errorf("Authorization = %q, want no header at all", rec.auth)
	}
}

func TestPublishIncludesTheRetentionPolicyOnlyWhenOneIsConfigured(t *testing.T) {
	t.Run("configured", func(t *testing.T) {
		var rec bodyRecorder
		srv := httptest.NewServer(rec.handler(http.StatusNoContent))
		defer srv.Close()

		cfg := testConfig(srv.URL)
		cfg.RetentionPolicy = "one_year"

		s := newSink(t, cfg, discardLogger())
		if err := s.Publish(context.Background(), sampleBatch()); err != nil {
			t.Fatalf("Publish: %v", err)
		}
		q, err := url.ParseQuery(rec.query)
		if err != nil {
			t.Fatalf("parse query: %v", err)
		}
		if q.Get("rp") != "one_year" {
			t.Errorf("query rp = %q, want one_year", q.Get("rp"))
		}
	})

	t.Run("not configured", func(t *testing.T) {
		var rec bodyRecorder
		srv := httptest.NewServer(rec.handler(http.StatusNoContent))
		defer srv.Close()

		s := newSink(t, testConfig(srv.URL), discardLogger())
		if err := s.Publish(context.Background(), sampleBatch()); err != nil {
			t.Fatalf("Publish: %v", err)
		}
		q, err := url.ParseQuery(rec.query)
		if err != nil {
			t.Fatalf("parse query: %v", err)
		}
		// An empty rp= names a policy called "", which does not exist; the
		// parameter must be absent so the database's default applies.
		if q.Has("rp") {
			t.Errorf("query carried rp=%q with no retention policy configured", q.Get("rp"))
		}
	})
}

func TestPublishUsesTheSampleTimestampRatherThanWallClock(t *testing.T) {
	var rec bodyRecorder
	srv := httptest.NewServer(rec.handler(http.StatusNoContent))
	defer srv.Close()

	// An instant well in the past, with sub-second precision that must survive.
	observed := time.Unix(1600000000, 123456789).UTC()
	batch := metric.Batch{
		Source:  "swpc-slow",
		Fetched: time.Now(),
		Samples: []metric.Sample{{Desc: fluxDesc, Value: 70, Time: observed}},
	}

	s := newSink(t, testConfig(srv.URL), discardLogger())
	if err := s.Publish(context.Background(), batch); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	want := "solar,metric=flux_sfu,source=swpc-slow value=70 1600000000123456789"
	if rec.body != want {
		t.Errorf("body:\n got %q\nwant %q", rec.body, want)
	}
}

func TestPublishTagsEverySampleWithItsMetricNameAndSource(t *testing.T) {
	var rec bodyRecorder
	srv := httptest.NewServer(rec.handler(http.StatusNoContent))
	defer srv.Close()

	s := newSink(t, testConfig(srv.URL), discardLogger())
	if err := s.Publish(context.Background(), sampleBatch()); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	lines := strings.Split(rec.body, "\n")
	if len(lines) != 3 {
		t.Fatalf("got %d lines, want 3: %q", len(lines), rec.body)
	}
	wantMetrics := []string{"flux_sfu", "fof2_megahertz", "xray_class_info"}
	for i, line := range lines {
		if !strings.Contains(line, "metric="+wantMetrics[i]) {
			t.Errorf("line %d has no metric=%s tag: %q", i, wantMetrics[i], line)
		}
		// The solar_ prefix is a Prometheus convention and must not appear here.
		if strings.Contains(line, "metric=solar_") {
			t.Errorf("line %d carries the metric prefix in its tag: %q", i, line)
		}
		if !strings.Contains(line, "source=swpc-slow") {
			t.Errorf("line %d has no source tag: %q", i, line)
		}
	}
}

func TestPublishWritesBothTheNumericAndTheStringFieldForInfoSamples(t *testing.T) {
	var rec bodyRecorder
	srv := httptest.NewServer(rec.handler(http.StatusNoContent))
	defer srv.Close()

	batch := metric.Batch{
		Source:  "hamqsl",
		Fetched: sampleTime,
		Samples: []metric.Sample{{
			Desc: &metric.Descriptor{
				Name:   "band_condition_info",
				Kind:   metric.KindInfo,
				Labels: []string{"band", "period", "condition"},
			},
			Labels: []string{"40m-30m", "day", "Good"},
			Value:  1,
			Time:   sampleTime,
		}},
	}

	s := newSink(t, testConfig(srv.URL), discardLogger())
	if err := s.Publish(context.Background(), batch); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// The category comes from the descriptor's last label, so a query returns
	// the wording rather than a column of ones.
	want := `solar,band=40m-30m,condition=Good,metric=band_condition_info,period=day,source=hamqsl value=1,category="Good" 1700000000000000000`
	if rec.body != want {
		t.Errorf("body:\n got %q\nwant %q", rec.body, want)
	}
}

func TestPublishWritesNoStringFieldForGaugeSamples(t *testing.T) {
	var rec bodyRecorder
	srv := httptest.NewServer(rec.handler(http.StatusNoContent))
	defer srv.Close()

	batch := metric.Batch{
		Source:  "kc2g",
		Fetched: sampleTime,
		Samples: []metric.Sample{{
			Desc:   foF2Desc,
			Labels: []string{"AU930", "Austin"},
			Value:  8.25,
			Time:   sampleTime,
		}},
	}

	s := newSink(t, testConfig(srv.URL), discardLogger())
	if err := s.Publish(context.Background(), batch); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if strings.Contains(rec.body, "category=") {
		t.Errorf("a gauge sample gained a category field: %q", rec.body)
	}
}

func TestEscapingCoversEveryDelimiterInItsOwnContext(t *testing.T) {
	tests := []struct {
		name            string
		in              string
		wantMeasurement string
		wantTag         string
		wantStringField string
	}{
		{
			name:            "plain",
			in:              "solar",
			wantMeasurement: "solar",
			wantTag:         "solar",
			wantStringField: "solar",
		},
		{
			name:            "comma",
			in:              "a,b",
			wantMeasurement: `a\,b`,
			wantTag:         `a\,b`,
			wantStringField: "a,b",
		},
		{
			name:            "space",
			in:              "a b",
			wantMeasurement: `a\ b`,
			wantTag:         `a\ b`,
			wantStringField: "a b",
		},
		{
			name: "equals",
			in:   "a=b",
			// An equals sign is not special in a measurement name, and escaping
			// it there would change the stored name.
			wantMeasurement: "a=b",
			wantTag:         `a\=b`,
			wantStringField: "a=b",
		},
		{
			name:            "double quote",
			in:              `a"b`,
			wantMeasurement: `a"b`,
			wantTag:         `a"b`,
			wantStringField: `a\"b`,
		},
		{
			name:            "backslash",
			in:              `a\b`,
			wantMeasurement: `a\\b`,
			wantTag:         `a\\b`,
			wantStringField: `a\\b`,
		},
		{
			name:            "trailing backslash",
			in:              `ab\`,
			wantMeasurement: `ab\\`,
			wantTag:         `ab\\`,
			wantStringField: `ab\\`,
		},
		{
			name:            "newline",
			in:              "a\nb",
			wantMeasurement: `a\nb`,
			wantTag:         `a\nb`,
			wantStringField: `a\nb`,
		},
		{
			name:            "station name with commas and spaces",
			in:              "Austin, TX, USA",
			wantMeasurement: `Austin\,\ TX\,\ USA`,
			wantTag:         `Austin\,\ TX\,\ USA`,
			wantStringField: "Austin, TX, USA",
		},
		{
			name:            "all at once",
			in:              `a, b="c"\`,
			wantMeasurement: `a\,\ b="c"\\`,
			wantTag:         `a\,\ b\="c"\\`,
			wantStringField: `a, b=\"c\"\\`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := escapeMeasurement(tc.in); got != tc.wantMeasurement {
				t.Errorf("escapeMeasurement(%q) = %q, want %q", tc.in, got, tc.wantMeasurement)
			}
			if got := escapeTag(tc.in); got != tc.wantTag {
				t.Errorf("escapeTag(%q) = %q, want %q", tc.in, got, tc.wantTag)
			}
			if got := escapeStringField(tc.in); got != tc.wantStringField {
				t.Errorf("escapeStringField(%q) = %q, want %q", tc.in, got, tc.wantStringField)
			}
			if got, want := quoteStringField(tc.in), `"`+tc.wantStringField+`"`; got != want {
				t.Errorf("quoteStringField(%q) = %q, want %q", tc.in, got, want)
			}
		})
	}
}

func TestPublishEscapesTheMeasurementTagKeysTagValuesAndStringFields(t *testing.T) {
	var rec bodyRecorder
	srv := httptest.NewServer(rec.handler(http.StatusNoContent))
	defer srv.Close()

	cfg := testConfig(srv.URL)
	cfg.Measurement = "odd name,with=stuff"

	batch := metric.Batch{
		Source:  "swpc-fast",
		Fetched: sampleTime,
		Samples: []metric.Sample{
			{
				Desc: &metric.Descriptor{
					Name:   "weird_metric",
					Kind:   metric.KindGauge,
					Labels: []string{"sta,tion"},
				},
				Labels: []string{`a b"c\`},
				Value:  3,
				Time:   sampleTime,
			},
			{
				Desc: &metric.Descriptor{
					Name:   "weird_info",
					Kind:   metric.KindInfo,
					Labels: []string{"class"},
				},
				Labels: []string{`x"y\z`},
				Value:  1,
				Time:   sampleTime,
			},
		},
	}

	s := newSink(t, cfg, discardLogger())
	if err := s.Publish(context.Background(), batch); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	want := `odd\ name\,with=stuff,metric=weird_metric,source=swpc-fast,sta\,tion=a\ b"c\\ value=3 1700000000000000000
odd\ name\,with=stuff,class=x"y\\z,metric=weird_info,source=swpc-fast value=1,category="x\"y\\z" 1700000000000000000`
	if rec.body != want {
		t.Errorf("body:\n got %q\nwant %q", rec.body, want)
	}
}

// TestPublishRoundTripsAStationNameContainingAComma is the concrete case the
// escaping exists for: kc2g station names look like "Austin, TX, USA", and an
// unescaped comma there silently turns the rest of the name into another tag.
func TestPublishRoundTripsAStationNameContainingAComma(t *testing.T) {
	var rec bodyRecorder
	srv := httptest.NewServer(rec.handler(http.StatusNoContent))
	defer srv.Close()

	batch := metric.Batch{
		Source:  "kc2g",
		Fetched: sampleTime,
		Samples: []metric.Sample{{
			Desc:   foF2Desc,
			Labels: []string{"AU930", "Austin, TX, USA"},
			Value:  8.25,
			Time:   sampleTime,
		}},
	}

	s := newSink(t, testConfig(srv.URL), discardLogger())
	if err := s.Publish(context.Background(), batch); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	want := `solar,metric=fof2_megahertz,source=kc2g,station=AU930,station_name=Austin\,\ TX\,\ USA value=8.25 1700000000000000000`
	if rec.body != want {
		t.Errorf("body:\n got %q\nwant %q", rec.body, want)
	}

	// Decoding the tag set the way a parser does must give back four tags with
	// the name intact, not six with it split apart.
	tags := splitUnescaped(splitUnescaped(rec.body, ' ')[0], ',')
	if len(tags) != 5 {
		t.Fatalf("tag set parsed into %d parts, want measurement plus 4 tags: %v", len(tags), tags)
	}
	if got, want := unescapeLineProtocol(tags[4]), "station_name=Austin, TX, USA"; got != want {
		t.Errorf("station name round-tripped as %q, want %q", got, want)
	}
}

// splitUnescaped splits on a delimiter that is not preceded by a backslash,
// which is how a line protocol parser reads a tag set.
func splitUnescaped(s string, sep byte) []string {
	var (
		out   []string
		cur   strings.Builder
		esc   bool
		flush = func() { out = append(out, cur.String()); cur.Reset() }
	)
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case esc:
			cur.WriteByte('\\')
			cur.WriteByte(c)
			esc = false
		case c == '\\':
			esc = true
		case c == sep:
			flush()
		default:
			cur.WriteByte(c)
		}
	}
	if esc {
		cur.WriteByte('\\')
	}
	flush()
	return out
}

// unescapeLineProtocol reverses the escaping a parser would undo.
func unescapeLineProtocol(s string) string {
	return strings.NewReplacer(`\,`, ",", `\ `, " ", `\=`, "=", `\\`, `\`).Replace(s)
}

func TestPublishSkipsSamplesWithAnEmptyTagValue(t *testing.T) {
	var rec bodyRecorder
	srv := httptest.NewServer(rec.handler(http.StatusNoContent))
	defer srv.Close()

	batch := metric.Batch{
		Source:  "kc2g",
		Fetched: sampleTime,
		Samples: []metric.Sample{
			// An empty tag value is dropped silently by InfluxDB, which would
			// quietly cost the series a dimension.
			{Desc: foF2Desc, Labels: []string{"AU930", ""}, Value: 8.25, Time: sampleTime},
			{Desc: fluxDesc, Value: 137.5, Time: sampleTime},
		},
	}

	s := newSink(t, testConfig(srv.URL), discardLogger())
	if err := s.Publish(context.Background(), batch); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	want := "solar,metric=flux_sfu,source=kc2g value=137.5 1700000000000000000"
	if rec.body != want {
		t.Errorf("body:\n got %q\nwant %q", rec.body, want)
	}
}

func TestPublishSkipsSamplesWithANewlineInALabel(t *testing.T) {
	var rec bodyRecorder
	srv := httptest.NewServer(rec.handler(http.StatusNoContent))
	defer srv.Close()

	batch := metric.Batch{
		Source:  "kc2g",
		Fetched: sampleTime,
		Samples: []metric.Sample{
			// A newline would end the point and turn the remainder into a
			// second, malformed one.
			{Desc: foF2Desc, Labels: []string{"AU930", "Austin\nTX"}, Value: 8.25, Time: sampleTime},
			{Desc: fluxDesc, Value: 137.5, Time: sampleTime},
		},
	}

	s := newSink(t, testConfig(srv.URL), discardLogger())
	if err := s.Publish(context.Background(), batch); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if strings.Count(rec.body, "\n") != 0 {
		t.Errorf("body contains more than one line: %q", rec.body)
	}
	if !strings.Contains(rec.body, "metric=flux_sfu") {
		t.Errorf("the good sample was lost with the bad one: %q", rec.body)
	}
}

func TestPublishSkipsNonFiniteValuesWithoutFailingTheBatch(t *testing.T) {
	var rec bodyRecorder
	srv := httptest.NewServer(rec.handler(http.StatusNoContent))
	defer srv.Close()

	batch := metric.Batch{
		Source:  "swpc-fast",
		Fetched: sampleTime,
		Samples: []metric.Sample{
			{Desc: fluxDesc, Value: math.NaN(), Time: sampleTime},
			{Desc: fluxDesc, Value: math.Inf(1), Time: sampleTime},
			{Desc: fluxDesc, Value: math.Inf(-1), Time: sampleTime},
			{Desc: fluxDesc, Value: 137.5, Time: sampleTime},
		},
	}

	s := newSink(t, testConfig(srv.URL), discardLogger())
	if err := s.Publish(context.Background(), batch); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	want := "solar,metric=flux_sfu,source=swpc-fast value=137.5 1700000000000000000"
	if rec.body != want {
		t.Errorf("body:\n got %q\nwant %q\nInflux rejects the whole request over one NaN", rec.body, want)
	}
}

func TestPublishSkipsABatchWhoseSamplesAreAllUnwritable(t *testing.T) {
	var rec bodyRecorder
	srv := httptest.NewServer(rec.handler(http.StatusNoContent))
	defer srv.Close()

	batch := metric.Batch{
		Source:  "swpc-fast",
		Fetched: sampleTime,
		Samples: []metric.Sample{{Desc: fluxDesc, Value: math.NaN(), Time: sampleTime}},
	}

	s := newSink(t, testConfig(srv.URL), discardLogger())
	err := s.Publish(context.Background(), batch)
	if !errors.Is(err, sink.ErrSkipped) {
		t.Errorf("Publish returned %v, want sink.ErrSkipped", err)
	}
	if got := rec.calls.Load(); got != 0 {
		t.Errorf("server saw %d requests with nothing to write, want 0", got)
	}
}

func TestPublishAcceptsBothNoContentAndOK(t *testing.T) {
	for _, status := range []int{http.StatusNoContent, http.StatusOK} {
		var rec bodyRecorder
		srv := httptest.NewServer(rec.handler(status))

		s := newSink(t, testConfig(srv.URL), discardLogger())
		if err := s.Publish(context.Background(), sampleBatch()); err != nil {
			t.Errorf("Publish with status %d: %v", status, err)
		}
		if got := rec.calls.Load(); got != 1 {
			t.Errorf("status %d: server saw %d requests, want 1", status, got)
		}
		srv.Close()
	}
}

func TestPublishDoesNotRetryBadRequestAndSurfacesTheResponseBody(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, `{"error":"unable to parse 'solar ': missing fields"}`)
	}))
	defer srv.Close()

	s := newSink(t, testConfig(srv.URL), discardLogger())
	err := s.Publish(context.Background(), sampleBatch())
	if err == nil {
		t.Fatal("Publish succeeded, want failure")
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("server saw %d attempts, want 1: malformed line protocol never becomes valid", got)
	}
	if !strings.Contains(err.Error(), "missing fields") {
		t.Errorf("error does not surface the response body: %v", err)
	}
}

func TestPublishTruncatesAVeryLongResponseBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, strings.Repeat("x", 4096))
	}))
	defer srv.Close()

	s := newSink(t, testConfig(srv.URL), discardLogger())
	err := s.Publish(context.Background(), sampleBatch())
	if err == nil {
		t.Fatal("Publish succeeded, want failure")
	}
	if len(err.Error()) > maxBodySnippet+512 {
		t.Errorf("error is %d bytes; the body should have been truncated", len(err.Error()))
	}
	if !strings.HasSuffix(err.Error(), "...") {
		t.Errorf("truncated error should be marked as truncated: %v", err)
	}
}

func TestPublishDoesNotRetryAnUnauthorizedResponse(t *testing.T) {
	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden} {
		var calls atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			w.WriteHeader(status)
			_, _ = io.WriteString(w, `{"error":"authorization failed"}`)
		}))

		s := newSink(t, testConfig(srv.URL), discardLogger())
		if err := s.Publish(context.Background(), sampleBatch()); err == nil {
			t.Errorf("status %d: Publish succeeded, want failure", status)
		}
		if got := calls.Load(); got != 1 {
			t.Errorf("status %d: server saw %d attempts, want 1", status, got)
		}
		srv.Close()
	}
}

// TestPublishDoesNotRetryNotFoundAndNamesTheTarget guards the defect this
// exporter replaces: the predecessor wrote to a target that did not exist for
// the life of its container and nothing surfaced it. Here the equivalent
// mistake is a misnamed database or retention policy.
func TestPublishDoesNotRetryNotFoundAndNamesTheTarget(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, `{"error":"database not found: \"solarham\""}`)
	}))
	defer srv.Close()

	var logs bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))

	cfg := testConfig(srv.URL)
	cfg.RetentionPolicy = "one_year"

	s := newSink(t, cfg, log)
	err := s.Publish(context.Background(), sampleBatch())
	if err == nil {
		t.Fatal("Publish succeeded against a 404, want failure")
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("server saw %d attempts, want 1: a missing database does not appear by retrying", got)
	}
	for _, want := range []string{"solarham", "one_year"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error does not name %q, so an operator cannot see which names were used: %v", want, err)
		}
		if !strings.Contains(logs.String(), want) {
			t.Errorf("log does not name %q: %s", want, logs.String())
		}
	}
	if !strings.Contains(logs.String(), `"level":"ERROR"`) {
		t.Errorf("a 404 must be logged loudly, at error level: %s", logs.String())
	}
}

func TestPublishDoesNotRetryAPayloadTooLargeResponse(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusRequestEntityTooLarge)
		_, _ = io.WriteString(w, `{"error":"write request too large"}`)
	}))
	defer srv.Close()

	var logs bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))

	s := newSink(t, testConfig(srv.URL), log)
	err := s.Publish(context.Background(), sampleBatch())
	if err == nil {
		t.Fatal("Publish succeeded against a 413, want failure")
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("server saw %d attempts, want 1", got)
	}
	if !strings.Contains(logs.String(), "too large") {
		t.Errorf("log does not say the batch was too large: %s", logs.String())
	}
}

func TestPublishRetriesTooManyRequestsHonouringRetryAfter(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	// The sink's own backoff is a millisecond, so any wait near a second can
	// only have come from the header.
	s := newSink(t, testConfig(srv.URL), discardLogger())

	start := time.Now()
	if err := s.Publish(context.Background(), sampleBatch()); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	elapsed := time.Since(start)

	if got := calls.Load(); got != 2 {
		t.Errorf("server saw %d attempts, want 2", got)
	}
	if elapsed < 900*time.Millisecond {
		t.Errorf("retried after %v, want at least the second the server asked for", elapsed)
	}
}

func TestPublishRetriesAServiceUnavailableResponse(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	s := newSink(t, testConfig(srv.URL), discardLogger())
	if err := s.Publish(context.Background(), sampleBatch()); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if got := calls.Load(); got != 2 {
		t.Errorf("server saw %d attempts, want 2", got)
	}
}

func TestPublishRetriesAServerErrorThenSucceeds(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	s := newSink(t, testConfig(srv.URL), discardLogger())
	if err := s.Publish(context.Background(), sampleBatch()); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if got := calls.Load(); got != 3 {
		t.Errorf("server saw %d attempts, want 3", got)
	}
}

func TestPublishReturnsAnErrorWhenRetriesAreExhausted(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(w, `{"error":"engine: shard is disabled"}`)
	}))
	defer srv.Close()

	s := newSink(t, testConfig(srv.URL), discardLogger())
	err := s.Publish(context.Background(), sampleBatch())
	if err == nil {
		t.Fatal("Publish succeeded, want failure")
	}
	// Retries is 2, so three attempts in total.
	if got := calls.Load(); got != 3 {
		t.Errorf("server saw %d attempts, want 3", got)
	}
	if !strings.Contains(err.Error(), "engine: shard is disabled") {
		t.Errorf("error does not surface the response body: %v", err)
	}
	if !strings.Contains(err.Error(), Name) {
		t.Errorf("error does not name the sink: %v", err)
	}
}

func TestPublishHonoursAnAlreadyCancelledContext(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	s := newSink(t, testConfig(srv.URL), discardLogger())
	err := s.Publish(ctx, sampleBatch())
	if err == nil {
		t.Fatal("Publish succeeded with a cancelled context, want failure")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("error does not report cancellation: %v", err)
	}
	if got := calls.Load(); got != 0 {
		t.Errorf("server saw %d requests, want 0", got)
	}
}

func TestPublishHonoursCancellationWhileWaitingToRetry(t *testing.T) {
	var calls atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Cancel once the first attempt has failed, so cancellation lands while
		// the sink is waiting to retry.
		calls.Add(1)
		cancel()
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	cfg := testConfig(srv.URL)
	cfg.Retries = 5
	s := newSink(t, cfg, discardLogger())
	// Long enough that the sink must actually observe the cancellation rather
	// than time out into the next attempt.
	s.backoff = 5 * time.Second

	done := make(chan error, 1)
	go func() { done <- s.Publish(ctx, sampleBatch()) }()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Publish succeeded, want failure")
		}
		if !errors.Is(err, context.Canceled) {
			t.Errorf("error does not report cancellation: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Publish ignored context cancellation")
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("server saw %d attempts, want 1", got)
	}
}

func TestCloseIsSafeOnASinkThatNeverPublished(t *testing.T) {
	s, err := New(testConfig("http://localhost:8086"), discardLogger())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
	// Closing twice must also be harmless; the fan-out closes on shutdown
	// regardless of what happened before it.
	if err := s.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
}

// TestThePasswordNeverAppearsInAnErrorOrLogLine guards the log hygiene rule. A
// transport failure hands back a *url.Error holding the whole request URL, and
// the sink logs that error on every retry; nothing about the password may
// survive that path.
func TestThePasswordNeverAppearsInAnErrorOrLogLine(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	addr := srv.URL
	// Closing the server forces the transport failure that carries the URL.
	srv.Close()

	var logs bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))

	s := newSink(t, testConfig(addr), log)
	err := s.Publish(context.Background(), sampleBatch())
	if err == nil {
		t.Fatal("Publish succeeded against a closed server, want failure")
	}

	if strings.Contains(err.Error(), testPassword) {
		t.Errorf("password leaked into the returned error: %v", err)
	}
	if strings.Contains(logs.String(), testPassword) {
		t.Errorf("password leaked into log output: %s", logs.String())
	}
	if logs.Len() == 0 {
		t.Fatal("no retry was logged, so this test proved nothing")
	}
	// The endpoint must still be identifiable, or the redaction has made the
	// failure undiagnosable.
	if !strings.Contains(logs.String(), "/write") {
		t.Errorf("log does not identify the endpoint: %s", logs.String())
	}
}

// TestThePasswordIsNotLoggableByAccident records that the password is held as a
// redact.Secret, so even a careless structured log of the value prints a
// placeholder.
func TestThePasswordIsNotLoggableByAccident(t *testing.T) {
	var logs bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&logs, nil))
	log.Info("careless", "password", redact.New(testPassword))
	if strings.Contains(logs.String(), testPassword) {
		t.Errorf("redact.Secret printed its value: %s", logs.String())
	}
}

func TestNewRejectsConfigurationThatCouldOnlyFail(t *testing.T) {
	valid := config.InfluxV1{
		URL:      "http://localhost:8086",
		Database: "solarham",
	}
	tests := []struct {
		name   string
		mutate func(*config.InfluxV1)
	}{
		{"no URL", func(c *config.InfluxV1) { c.URL = "" }},
		{"no database", func(c *config.InfluxV1) { c.Database = "" }},
		{"wrong scheme", func(c *config.InfluxV1) { c.URL = "udp://localhost:8089" }},
		{"newline in measurement", func(c *config.InfluxV1) { c.Measurement = "sol\nar" }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := valid
			tc.mutate(&cfg)
			if _, err := New(cfg, discardLogger()); err == nil {
				t.Error("New accepted configuration that can only 404 or fail")
			}
		})
	}
}

func TestNewDoesNotRevealThePasswordInConfigurationErrors(t *testing.T) {
	_, err := New(config.InfluxV1{
		URL:      "http://%zz",
		Database: "solarham",
		Username: testUser,
		Password: redact.New(testPassword),
	}, discardLogger())
	if err == nil {
		t.Fatal("New accepted an unparseable URL")
	}
	if strings.Contains(err.Error(), testPassword) {
		t.Errorf("password leaked into a configuration error: %v", err)
	}
}

func TestParseRetryAfterReadsSecondsAndDates(t *testing.T) {
	now := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name  string
		value string
		want  time.Duration
	}{
		{"absent", "", 0},
		{"seconds", "3", 3 * time.Second},
		{"zero seconds", "0", 0},
		{"negative seconds", "-5", 0},
		{"capped", "3600", maxBackoff},
		{"http date", "Tue, 08 Sep 2026 12:00:02 GMT", 2 * time.Second},
		{"http date in the past", "Tue, 08 Sep 2026 11:59:00 GMT", 0},
		{"nonsense", "soon", 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := parseRetryAfter(tc.value, now); got != tc.want {
				t.Errorf("parseRetryAfter(%q) = %v, want %v", tc.value, got, tc.want)
			}
		})
	}
}
