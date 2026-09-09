package mqtt

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"

	"github.com/RealDougEubanks/solarham/internal/config"
	"github.com/RealDougEubanks/solarham/internal/metric"
	"github.com/RealDougEubanks/solarham/internal/redact"
	"github.com/RealDougEubanks/solarham/internal/sink"
)

// canaryPassword is deliberately unlike anything else in this package, so a
// substring search for it in an error or in captured log output cannot match by
// accident.
const canaryPassword = "Zq7-CANARY-mqtt-password-3f9x"

// --- fakes -----------------------------------------------------------------

// fakeToken satisfies paho.Token without any network involvement. It is always
// already complete, since the fake client does its work synchronously.
type fakeToken struct {
	err  error
	done chan struct{}
}

func newFakeToken(err error) *fakeToken {
	done := make(chan struct{})
	close(done)
	return &fakeToken{err: err, done: done}
}

func (t *fakeToken) Wait() bool                     { return true }
func (t *fakeToken) WaitTimeout(time.Duration) bool { return true }
func (t *fakeToken) Done() <-chan struct{}          { return t.done }
func (t *fakeToken) Error() error                   { return t.err }

// pendingToken never completes, so a test can exercise the deadline path
// without waiting on a real broker.
type pendingToken struct{ done chan struct{} }

func (t *pendingToken) Wait() bool                     { return false }
func (t *pendingToken) WaitTimeout(time.Duration) bool { return false }
func (t *pendingToken) Done() <-chan struct{}          { return t.done }
func (t *pendingToken) Error() error                   { return nil }

// published records one call to Publish.
type published struct {
	topic    string
	qos      byte
	retained bool
	payload  string
}

// fakeClient records publishes instead of sending them. Keeping paho behind the
// client interface is what makes every test in this file run without a broker.
type fakeClient struct {
	mu         sync.Mutex
	connected  bool
	connectErr error
	failWith   error
	failTopic  string
	stall      bool
	messages   []published
	connects   int
	disconnect int
}

func (c *fakeClient) Connect() paho.Token {
	c.mu.Lock()
	c.connects++
	if c.stall {
		c.mu.Unlock()
		return &pendingToken{done: make(chan struct{})}
	}
	err := c.connectErr
	if err == nil {
		c.connected = true
	}
	c.mu.Unlock()

	return newFakeToken(err)
}

func (c *fakeClient) Publish(topic string, qos byte, retained bool, payload any) paho.Token {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.stall {
		return &pendingToken{done: make(chan struct{})}
	}

	body, _ := payload.(string)
	c.messages = append(c.messages, published{topic: topic, qos: qos, retained: retained, payload: body})

	if c.failWith != nil && (c.failTopic == "" || c.failTopic == topic) {
		return newFakeToken(c.failWith)
	}
	return newFakeToken(nil)
}

func (c *fakeClient) IsConnectionOpen() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.connected
}

func (c *fakeClient) Disconnect(uint) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.disconnect++
	c.connected = false
}

func (c *fakeClient) sent() []published {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]published, len(c.messages))
	copy(out, c.messages)
	return out
}

func (c *fakeClient) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.messages = nil
}

func (c *fakeClient) messageFor(topic string) (published, bool) {
	for _, m := range c.sent() {
		if m.topic == topic {
			return m, true
		}
	}
	return published{}, false
}

func (c *fakeClient) topics() []string {
	sent := c.sent()
	out := make([]string, 0, len(sent))
	for _, m := range sent {
		out = append(out, m.topic)
	}
	return out
}

// syncBuffer makes a bytes.Buffer safe for paho's callback goroutines and the
// test goroutine to write concurrently, so -race stays quiet.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// --- helpers ---------------------------------------------------------------

func discardLogger() *slog.Logger { return slog.New(slog.DiscardHandler) }

func testConfig() config.MQTT {
	return config.MQTT{
		Enabled:          true,
		Broker:           "192.168.52.2:1883",
		ClientID:         "solarham",
		Username:         "solarham",
		Password:         redact.New(canaryPassword),
		Topic:            "solarham",
		QoS:              1,
		Retain:           true,
		Timeout:          2 * time.Second,
		DiscoveryEnabled: true,
		DiscoveryPrefix:  "homeassistant",
		DeviceID:         "solarham",
		DeviceName:       "Space Weather",
	}
}

// newTestSink builds a Sink wired to a connected fake client.
func newTestSink(t *testing.T, cfg config.MQTT) (*Sink, *fakeClient) {
	t.Helper()
	s, client, _ := newTestSinkWithLogs(t, cfg)
	return s, client
}

// newTestSinkWithLogs additionally captures the sink's log output, for the
// tests that assert on what does and does not reach a log.
func newTestSinkWithLogs(t *testing.T, cfg config.MQTT) (*Sink, *fakeClient, *syncBuffer) {
	t.Helper()

	logs := &syncBuffer{}
	logger := slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	s, err := newSink(cfg, logger)
	if err != nil {
		t.Fatalf("newSink: %v", err)
	}
	client := &fakeClient{connected: true}
	s.client = client
	return s, client, logs
}

// gaugeSample builds a labelled gauge sample from the real descriptor table, so
// the tests exercise the units and label names the exporter actually publishes.
func gaugeSample(band, period string, value float64) metric.Sample {
	return metric.Sample{
		Desc:   metric.BandCondition,
		Labels: []string{band, period},
		Value:  value,
		Time:   time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC),
	}
}

func batchOf(samples ...metric.Sample) metric.Batch {
	return metric.Batch{
		Source:  "hamqsl",
		Fetched: time.Date(2026, 9, 8, 12, 0, 5, 0, time.UTC),
		Samples: samples,
	}
}

// decodeJSON decodes a payload as generic JSON, so assertions are about what
// Home Assistant actually receives rather than about this package's structs.
func decodeJSON(t *testing.T, payload string) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal([]byte(payload), &out); err != nil {
		t.Fatalf("payload is not valid JSON: %v\n%s", err, payload)
	}
	return out
}

// --- publishing ------------------------------------------------------------

func TestPublishReturnsErrSkippedForAnEmptyBatch(t *testing.T) {
	s, client := newTestSink(t, testConfig())

	err := s.Publish(context.Background(), metric.Batch{Source: "swpc"})
	if !errors.Is(err, sink.ErrSkipped) {
		t.Errorf("Publish of an empty batch returned %v, want sink.ErrSkipped", err)
	}
	if len(client.sent()) != 0 {
		t.Errorf("published %d messages for an empty batch, want 0", len(client.sent()))
	}
}

func TestPublishWritesTheExpectedTopicAndJSONPayload(t *testing.T) {
	cfg := testConfig()
	cfg.DiscoveryEnabled = false

	s, client := newTestSink(t, cfg)
	if err := s.Publish(context.Background(), batchOf(gaugeSample("30m-20m", "day", 2))); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	msg, ok := client.messageFor("solarham/band_condition/30m-20m/day")
	if !ok {
		t.Fatalf("no publish to solarham/band_condition/30m-20m/day, got %v", client.topics())
	}

	got := decodeJSON(t, msg.payload)
	want := map[string]any{
		"value":  float64(2),
		"unit":   "{ordinal}",
		"time":   "2026-09-08T12:00:00Z",
		"source": "hamqsl",
		"labels": map[string]any{"band": "30m-20m", "period": "day"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("state payload:\ngot  %#v\nwant %#v", got, want)
	}
}

func TestPublishSkipsTheSolarPrefixOnStateTopics(t *testing.T) {
	cfg := testConfig()
	cfg.DiscoveryEnabled = false

	s, client := newTestSink(t, cfg)
	sample := metric.Sample{Desc: metric.FluxSFU, Value: 142, Time: time.Now()}
	if err := s.Publish(context.Background(), batchOf(sample)); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// The base topic already namespaces everything, so repeating the Prometheus
	// prefix would say "solar" twice in one topic.
	if _, ok := client.messageFor("solarham/flux_sfu"); !ok {
		t.Errorf("no publish to solarham/flux_sfu, got %v", client.topics())
	}
}

func TestPublishUsesTheConfiguredQoSAndRetain(t *testing.T) {
	cfg := testConfig()
	cfg.QoS = 2
	cfg.Retain = false

	s, client := newTestSink(t, cfg)
	if err := s.Publish(context.Background(), batchOf(gaugeSample("30m-20m", "day", 2))); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	sent := client.sent()
	if len(sent) == 0 {
		t.Fatal("nothing published")
	}
	for _, m := range sent {
		if m.qos != 2 {
			t.Errorf("%s published with qos %d, want 2", m.topic, m.qos)
		}
		// Discovery is always retained; only state messages follow the config.
		if !strings.HasPrefix(m.topic, "homeassistant/") && m.retained {
			t.Errorf("%s published retained, want retain false from config", m.topic)
		}
	}
}

func TestPublishHonoursACustomBaseTopic(t *testing.T) {
	cfg := testConfig()
	cfg.DiscoveryEnabled = false
	cfg.Topic = "/house/space weather/"

	s, client := newTestSink(t, cfg)
	if err := s.Publish(context.Background(), batchOf(gaugeSample("30m-20m", "day", 2))); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	if _, ok := client.messageFor("house/space_weather/band_condition/30m-20m/day"); !ok {
		t.Errorf("custom base topic not honoured, got %v", client.topics())
	}
}

func TestPublishOfAKindInfoSampleCarriesTheCategoryString(t *testing.T) {
	cfg := testConfig()
	cfg.DiscoveryEnabled = false

	s, client := newTestSink(t, cfg)
	sample := metric.Sample{
		Desc:   metric.BandConditionInfo,
		Labels: []string{"30m-20m", "day", "Good"},
		Value:  1,
		Time:   time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC),
	}
	if err := s.Publish(context.Background(), batchOf(sample)); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	msg, ok := client.messageFor("solarham/band_condition_info/30m-20m/day/Good")
	if !ok {
		t.Fatalf("no publish for the info sample, got %v", client.topics())
	}
	payload := decodeJSON(t, msg.payload)
	if payload["category"] != "Good" {
		t.Errorf("category = %v, want %q", payload["category"], "Good")
	}
	if payload["value"] != float64(1) {
		t.Errorf("value = %v, want 1", payload["value"])
	}
	if _, present := payload["unit"]; present {
		t.Errorf("info payload carries unit %v, want it omitted", payload["unit"])
	}
}

func TestPublishSkipsNonFiniteValues(t *testing.T) {
	cfg := testConfig()
	cfg.DiscoveryEnabled = false

	s, client := newTestSink(t, cfg)
	nan := gaugeSample("30m-20m", "day", 0)
	nan.Value = math.NaN()

	err := s.Publish(context.Background(), batchOf(nan))
	if !errors.Is(err, sink.ErrSkipped) {
		t.Errorf("Publish of a NaN-only batch returned %v, want sink.ErrSkipped", err)
	}
	// Retaining "NaN" leaves the Home Assistant entity broken until the next
	// good value, which for a three-hourly index can be hours away.
	if len(client.sent()) != 0 {
		t.Errorf("published %d messages for a NaN sample, want 0", len(client.sent()))
	}
}

// --- topic sanitising ------------------------------------------------------

func TestSanitizeSegmentReplacesEverythingMQTTForbids(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"30m-20m", "30m-20m"},
		{"day", "day"},
		{"Austin, TX, USA", "Austin_TX_USA"},
		{">=10 MeV", "10_MeV"},
		{"a/b", "a_b"},
		{"a+b", "a_b"},
		{"a#b", "a_b"},
		{"a b  c", "a_b_c"},
		{"  padded  ", "padded"},
		{"1.5", "1.5"},
		{"", emptySegment},
		{"///", emptySegment},
		{"+#", emptySegment},
	}
	for _, tt := range tests {
		if got := sanitizeSegment(tt.in); got != tt.want {
			t.Errorf("sanitizeSegment(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestSanitizeTopicDropsEmptySegmentsAndFallsBack(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"solarham", "solarham"},
		{"/house/solar/", "house/solar"},
		{"house//solar", "house/solar"},
		{"bad/#/topic", "bad/topic"},
		{"  ", "fallback"},
		{"", "fallback"},
	}
	for _, tt := range tests {
		if got := sanitizeTopic(tt.in, "fallback"); got != tt.want {
			t.Errorf("sanitizeTopic(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestPublishSanitisesAwkwardLabelValuesIntoTopicSegments(t *testing.T) {
	cfg := testConfig()
	cfg.DiscoveryEnabled = false

	s, client := newTestSink(t, cfg)
	sample := metric.Sample{
		Desc:   metric.ProtonFlux,
		Labels: []string{">=10 MeV"},
		Value:  0.31,
		Time:   time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC),
	}
	if err := s.Publish(context.Background(), batchOf(sample)); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	msg, ok := client.messageFor("solarham/proton_flux_particles/10_MeV")
	if !ok {
		t.Fatalf("no publish to the sanitised topic, got %v", client.topics())
	}
	// The topic is sanitised; the payload keeps the label value verbatim, so
	// nothing is actually lost.
	labels := decodeJSON(t, msg.payload)["labels"].(map[string]any)
	if labels["energy"] != ">=10 MeV" {
		t.Errorf("payload label energy = %v, want %q", labels["energy"], ">=10 MeV")
	}
}

// --- connection ------------------------------------------------------------

func TestPublishConnectsLazilyOnTheFirstBatch(t *testing.T) {
	cfg := testConfig()
	cfg.DiscoveryEnabled = false

	s, client := newTestSink(t, cfg)
	client.connected = false

	if err := s.Publish(context.Background(), batchOf(gaugeSample("30m-20m", "day", 2))); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if client.connects != 1 {
		t.Errorf("Connect called %d times on the first publish, want 1", client.connects)
	}

	if err := s.Publish(context.Background(), batchOf(gaugeSample("30m-20m", "day", 2))); err != nil {
		t.Fatalf("second Publish: %v", err)
	}
	// A healthy process dials once; paho's auto-reconnect covers the rest.
	if client.connects != 1 {
		t.Errorf("Connect called %d times over two publishes, want 1", client.connects)
	}
}

func TestOnConnectPublishesOnlineRetainedToTheAvailabilityTopic(t *testing.T) {
	s, client := newTestSink(t, testConfig())
	s.onConnected()

	msg, ok := client.messageFor("solarham/availability")
	if !ok {
		t.Fatalf("onConnected published nothing to the availability topic, got %v", client.topics())
	}
	if msg.payload != payloadOnline {
		t.Errorf("availability payload = %q, want %q", msg.payload, payloadOnline)
	}
	if !msg.retained {
		// Not retained means a Home Assistant that starts later never learns
		// the exporter is alive.
		t.Error("availability published without the retain flag")
	}
}

func TestClientOptionsSetALastWillOfOffline(t *testing.T) {
	s, _ := newTestSink(t, testConfig())
	opts := s.clientOptions()

	if !opts.WillEnabled {
		t.Fatal("no last will configured; Home Assistant would show stale values forever")
	}
	if opts.WillTopic != "solarham/availability" {
		t.Errorf("will topic = %q, want %q", opts.WillTopic, "solarham/availability")
	}
	if string(opts.WillPayload) != payloadOffline {
		t.Errorf("will payload = %q, want %q", opts.WillPayload, payloadOffline)
	}
	if !opts.WillRetained {
		t.Error("will published without the retain flag")
	}
	if opts.WillQos != 1 {
		t.Errorf("will qos = %d, want the configured 1", opts.WillQos)
	}
}

func TestConnectFailureReturnsAnErrorWithoutHanging(t *testing.T) {
	s, client := newTestSink(t, testConfig())
	client.connected = false
	client.connectErr = errors.New("connection refused")

	err := s.Publish(context.Background(), batchOf(gaugeSample("30m-20m", "day", 2)))
	if err == nil {
		t.Fatal("Publish returned nil against an unreachable broker, want a transient error")
	}
	if len(client.sent()) != 0 {
		t.Errorf("published %d messages while disconnected, want 0", len(client.sent()))
	}
}

// --- failure containment ---------------------------------------------------

func TestPublishFailureReturnsAnErrorThatDoesNotLeakThePassword(t *testing.T) {
	s, client, logs := newTestSinkWithLogs(t, testConfig())
	client.failWith = errors.New("broker refused the message")

	err := s.Publish(context.Background(), batchOf(gaugeSample("30m-20m", "day", 2)))
	if err == nil {
		t.Fatal("Publish returned nil after a broker failure, want an error")
	}
	if !strings.Contains(err.Error(), "broker refused the message") {
		t.Errorf("error = %v, want it to wrap the broker failure", err)
	}
	if strings.Contains(err.Error(), canaryPassword) {
		t.Errorf("error contains the password: %v", err)
	}
	if strings.Contains(logs.String(), canaryPassword) {
		t.Error("the password reached a log line")
	}
	// A whole-struct format is the accident this guards against: fmt walks
	// unexported fields by reflection and would print a plain string password.
	if formatted := fmt.Sprintf("%+v", s); strings.Contains(formatted, canaryPassword) {
		t.Errorf("formatting the sink leaked the password: %s", formatted)
	}
}

func TestPublishStopsAtTheFirstFailure(t *testing.T) {
	cfg := testConfig()
	cfg.DiscoveryEnabled = false

	s, client := newTestSink(t, cfg)
	client.failWith = errors.New("nope")
	client.failTopic = "solarham/band_condition/30m-20m/day"

	batch := batchOf(
		gaugeSample("30m-20m", "day", 2),
		gaugeSample("30m-20m", "night", 1),
		gaugeSample("17m-15m", "day", 2),
	)
	if err := s.Publish(context.Background(), batch); err == nil {
		t.Fatal("Publish returned nil after a broker failure, want an error")
	}
	// One timeout per outage, not one per series: a DRAP grid would otherwise
	// spend thousands of timeouts discovering the same dead broker.
	if len(client.sent()) != 1 {
		t.Errorf("published %d messages after the first failure, want 1", len(client.sent()))
	}
}

func TestPublishReturnsWhenItsContextIsCancelled(t *testing.T) {
	s, client := newTestSink(t, testConfig())
	client.stall = true

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan error, 1)
	go func() { done <- s.Publish(ctx, batchOf(gaugeSample("30m-20m", "day", 2))) }()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("error = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Publish did not return after its context was cancelled")
	}
}

func TestPublishTimesOutRatherThanHangingForever(t *testing.T) {
	cfg := testConfig()
	// Paho tokens block until the message reaches the broker, which during an
	// outage is never. The configured timeout is what stops a shutdown hanging.
	cfg.Timeout = 100 * time.Millisecond

	s, client := newTestSink(t, cfg)
	client.stall = true

	done := make(chan error, 1)
	go func() { done <- s.Publish(context.Background(), batchOf(gaugeSample("30m-20m", "day", 2))) }()

	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("error = %v, want context.DeadlineExceeded", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Publish did not return after its timeout expired")
	}
}

func TestPublishAfterCloseReturnsAnError(t *testing.T) {
	s, _ := newTestSink(t, testConfig())
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := s.Publish(context.Background(), batchOf(gaugeSample("30m-20m", "day", 2))); err == nil {
		t.Error("Publish returned nil after Close, want an error")
	}
}

// --- shutdown --------------------------------------------------------------

func TestCloseAnnouncesOfflineAndDisconnects(t *testing.T) {
	s, client := newTestSink(t, testConfig())

	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	sent := client.sent()
	want := published{topic: "solarham/availability", qos: 1, retained: true, payload: payloadOffline}
	if len(sent) != 1 || sent[0] != want {
		t.Errorf("Close published %+v, want exactly [%+v]", sent, want)
	}
	if client.disconnect != 1 {
		t.Errorf("Disconnect called %d times, want 1", client.disconnect)
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	s, client := newTestSink(t, testConfig())

	for i := 0; i < 3; i++ {
		if err := s.Close(); err != nil {
			t.Fatalf("Close %d: %v", i, err)
		}
	}
	if len(client.sent()) != 1 {
		t.Errorf("published %d offline messages over three Closes, want 1", len(client.sent()))
	}
}

func TestCloseSucceedsOnASinkThatNeverConnected(t *testing.T) {
	s, client := newTestSink(t, testConfig())
	client.connected = false

	if err := s.Close(); err != nil {
		t.Fatalf("Close on a sink that never connected: %v", err)
	}
	if len(client.sent()) != 0 {
		t.Errorf("published %d messages while disconnected, want 0", len(client.sent()))
	}
	if client.disconnect != 1 {
		t.Errorf("Disconnect called %d times, want 1", client.disconnect)
	}
}

func TestCloseDisconnectsEvenWhenTheFarewellFails(t *testing.T) {
	s, client := newTestSink(t, testConfig())
	client.failWith = errors.New("broker gone")

	if err := s.Close(); err == nil {
		t.Error("Close returned nil after a failed offline publish, want an error")
	}
	if client.disconnect != 1 {
		t.Errorf("Disconnect called %d times, want 1 even after a publish failure", client.disconnect)
	}
}

// --- construction ----------------------------------------------------------

func TestNewRejectsAMissingBroker(t *testing.T) {
	cfg := testConfig()
	cfg.Broker = "   "

	if _, err := New(cfg, discardLogger()); err == nil {
		t.Fatal("New succeeded without a broker, want an error")
	}
}

func TestNewRejectsAnInvalidQoS(t *testing.T) {
	cfg := testConfig()
	cfg.QoS = 3

	if _, err := New(cfg, discardLogger()); err == nil {
		t.Fatal("New accepted qos 3, want an error")
	}
}

func TestNewSucceedsAgainstAnUnreachableBrokerAndDoesNotDial(t *testing.T) {
	cfg := testConfig()
	// Port 1 on loopback refuses connections immediately, standing in for a
	// broker that is not up yet when the container starts.
	cfg.Broker = "127.0.0.1:1"
	cfg.Timeout = time.Second

	s, err := New(cfg, discardLogger())
	if err != nil {
		// Refusing to start here would turn a broker restart into an exporter
		// outage, and the Prometheus endpoint would never come up.
		t.Fatalf("New failed against an unreachable broker: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
}

func TestNameIsStable(t *testing.T) {
	s, _ := newTestSink(t, testConfig())
	// The name becomes a metric label, so changing it breaks dashboards.
	if s.Name() != "mqtt" {
		t.Errorf("Name = %q, want %q", s.Name(), "mqtt")
	}
}

func TestBrokerURLAcceptsHostHostPortAndFullURL(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"192.168.52.2:1883", "tcp://192.168.52.2:1883"},
		{"mqtt.lan", "tcp://mqtt.lan:1883"},
		{"tcp://mqtt.lan:1883", "tcp://mqtt.lan:1883"},
		{"ssl://mqtt.lan:8883", "ssl://mqtt.lan:8883"},
		{"  mqtt.lan  ", "tcp://mqtt.lan:1883"},
	}
	for _, tt := range tests {
		if got := brokerURL(tt.in); got != tt.want {
			t.Errorf("brokerURL(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestZeroValuedConfigStillProducesWellFormedTopics(t *testing.T) {
	s, err := newSink(config.MQTT{Broker: "mqtt.lan"}, discardLogger())
	if err != nil {
		t.Fatalf("newSink: %v", err)
	}
	if s.baseTopic != defaultBaseTopic {
		t.Errorf("base topic = %q, want %q", s.baseTopic, defaultBaseTopic)
	}
	if s.availTopic != defaultBaseTopic+"/availability" {
		t.Errorf("availability topic = %q, want %q", s.availTopic, defaultBaseTopic+"/availability")
	}
	if s.cfg.Timeout <= 0 {
		t.Error("timeout left at zero; paho tokens would block forever")
	}
}

func TestCategoryOfPrefersTheNamedCategoryLabel(t *testing.T) {
	tests := []struct {
		name   string
		sample metric.Sample
		want   string
	}{
		{
			name:   "condition",
			sample: metric.Sample{Desc: metric.BandConditionInfo, Labels: []string{"30m-20m", "day", "Good"}},
			want:   "Good",
		},
		{
			name:   "class",
			sample: metric.Sample{Desc: metric.XRayClassInfo, Labels: []string{"B3.8"}},
			want:   "B3.8",
		},
		{
			name:   "state",
			sample: metric.Sample{Desc: metric.GeomagneticFieldInfo, Labels: []string{"QUIET"}},
			want:   "QUIET",
		},
		{
			// The SWPC alert has no category-named label, so the most specific
			// one wins.
			name:   "fallback to the last label",
			sample: metric.Sample{Desc: metric.AlertActive, Labels: []string{"WARK04", "1234"}},
			want:   "1234",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := categoryOf(tt.sample); got != tt.want {
				t.Errorf("categoryOf = %q, want %q", got, tt.want)
			}
		})
	}
}
