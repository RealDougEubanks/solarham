// Package mqtt publishes samples to an MQTT broker, optionally advertising
// every series to Home Assistant through MQTT discovery.
//
// The broker lives elsewhere on the network — in the deployment this was
// written for, Mosquitto and Home Assistant share a host — so nothing here
// assumes the broker is reachable at any particular moment. The connection is
// established lazily, on the first publish, and paho reconnects in the
// background afterwards. A broker that is down when the container starts
// therefore costs MQTT data and nothing else: the Prometheus endpoint still
// serves, the other sinks still write, and the poll loop never blocks on a
// socket that is not coming up.
//
// # Topics
//
// Every topic sits under cfg.Topic, default "solarham":
//
//	{base}/{metric_name}                 a metric with no labels
//	{base}/{metric_name}/{label}/{label} one segment per label value, in
//	                                     descriptor order
//	{base}/availability                  online/offline, also the last will
//
// metric_name is Descriptor.Name without the "solar_" prefix, because the
// prefix exists to namespace a flat Prometheus registry and the base topic
// already does that job here.
//
// # Payload
//
// Each state message is a JSON object rather than a bare number. The extra
// bytes buy the context both Home Assistant templates and a human with
// mosquitto_sub want, and the key names are a stable contract:
//
//	{
//	  "value":    23.4,                    // always present, always numeric
//	  "unit":     "MHz",                   // UCUM, omitted when the descriptor has none
//	  "time":     "2026-09-08T12:00:00Z",  // the upstream's own observation time, RFC3339
//	  "source":   "swpc",                  // which source produced the batch
//	  "labels":   {"station": "AU930"},    // label name to value, omitted when there are none
//	  "category": "Good"                   // KindInfo only: the categorical value
//	}
//
// For a KindInfo sample "value" is the constant 1, exactly as in Prometheus,
// and "category" carries the string that actually matters. Discovery points
// those sensors at value_json.category so Home Assistant shows the word rather
// than a meaningless 1.
package mqtt

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strings"
	"sync"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"

	"github.com/RealDougEubanks/solarham/internal/config"
	"github.com/RealDougEubanks/solarham/internal/metric"
	"github.com/RealDougEubanks/solarham/internal/redact"
	"github.com/RealDougEubanks/solarham/internal/sink"
)

// Sink implements sink.Sink. Asserting it here means a change to the interface
// breaks the build rather than the wiring in main.
var _ sink.Sink = (*Sink)(nil)

// Name is the sink's stable identifier, used as a metric label.
const Name = "mqtt"

// Version is reported as the Home Assistant device's sw_version. It is a
// package variable so main can stamp it at build time with -X without this
// package having to import a build-info package that would drag the whole
// binary's version wiring into every sink.
var Version = "dev"

// Availability payloads. These are Home Assistant's defaults, and the discovery
// documents state them explicitly so the contract does not rest on a default
// that could change under us.
const (
	payloadOnline  = "online"
	payloadOffline = "offline"
)

// Defaults for a zero-valued config. config.Load supplies all of these, but a
// Sink constructed directly must still produce well-formed topics rather than
// ones beginning with a slash.
const (
	defaultBaseTopic       = "solarham"
	defaultDiscoveryPrefix = "homeassistant"
	defaultClientID        = "solarham"
	defaultDeviceID        = "solarham"
	defaultDeviceName      = "Space Weather"
	defaultTimeout         = 15 * time.Second
)

// defaultPort is appended when cfg.Broker names a host with no port. 1883 is
// the IANA-registered plaintext MQTT port and the one Mosquitto listens on out
// of the box.
const defaultPort = "1883"

// Reconnect bounds. The first retry is short so a broker that is merely
// restarting is picked up quickly; the cap stops a multi-day outage from
// generating a connection attempt every few seconds for its whole length.
const (
	connectRetryInterval = 5 * time.Second
	maxReconnectInterval = 2 * time.Minute
)

// emptySegment replaces a topic segment that sanitising emptied. An empty
// segment is illegal in MQTT — "a//b" is not a valid publish topic — and a
// placeholder that says why is easier to diagnose in mosquitto_sub than a
// publish that silently fails at the broker.
const emptySegment = "unknown"

// client is the slice of paho.Client this sink actually uses.
//
// Narrowing the dependency to four methods is what makes the sink testable
// without a broker: the tests inject a fake that records every publish instead
// of opening a socket.
//
// Readiness is IsConnectionOpen rather than IsConnected because paho reports
// IsConnected true while it is still retrying a connection that has never
// succeeded, which would let Publish queue messages for a broker that was never
// reached.
type client interface {
	Connect() paho.Token
	Publish(topic string, qos byte, retained bool, payload any) paho.Token
	IsConnectionOpen() bool
	Disconnect(quiesce uint)
}

// Sink publishes batches to an MQTT broker.
type Sink struct {
	cfg config.MQTT
	log *slog.Logger

	client client
	qos    byte

	// baseTopic and availTopic are resolved once, so publishing builds only
	// the per-series suffix.
	baseTopic  string
	availTopic string

	// deviceID is the sanitised identifier shared by every discovery topic and
	// every unique_id. It must be stable across restarts or Home Assistant
	// creates a second device on every deploy.
	deviceID string

	// brokerURL is safe to log: paho carries credentials in its options struct,
	// never in the URL.
	brokerURL string

	// connMu serialises connection attempts so a batch arriving while another
	// goroutine is still dialling does not open a second socket.
	connMu sync.Mutex

	// mu guards closed and announced.
	mu     sync.Mutex
	closed bool

	// announced records which series have had a discovery document published.
	//
	// Discovery documents are retained and immutable, so republishing them on
	// every batch would push thousands of identical retained messages at the
	// broker every poll for no benefit. Each series is announced once, on first
	// sight, and the set is cleared on connect: a broker that restarted lost
	// every retained message it held, so a fresh connection has to be treated
	// as a fresh broker.
	announced map[string]bool
}

// New creates a Sink. It does not connect.
//
// Connecting lazily rather than at construction is deliberate. This exporter's
// primary job is to serve Prometheus, and a broker that is not up yet — a
// common state for thirty seconds after a host reboots, when every container
// starts at once — must not be able to delay or fail that. New therefore
// returns an error only for configuration that can never work, and the first
// Publish does the dialling. Paho's own auto-reconnect covers everything after
// that, so the lazy path runs exactly once in a healthy process.
func New(cfg config.MQTT, log *slog.Logger) (*Sink, error) {
	if log == nil {
		log = slog.Default()
	}

	s, err := newSink(cfg, log)
	if err != nil {
		return nil, err
	}

	s.client = paho.NewClient(s.clientOptions())

	s.log.Info("mqtt sink configured",
		"broker", s.brokerURL,
		"base_topic", s.baseTopic,
		"discovery", s.cfg.DiscoveryEnabled)
	return s, nil
}

// newSink builds the sink without a client, so New and the tests share one
// definition of how configuration maps to topics.
func newSink(cfg config.MQTT, log *slog.Logger) (*Sink, error) {
	if strings.TrimSpace(cfg.Broker) == "" {
		return nil, errors.New("mqtt: broker is required")
	}
	if cfg.QoS > 2 {
		return nil, fmt.Errorf("mqtt: qos must be 0, 1 or 2, got %d", cfg.QoS)
	}

	cfg = withDefaults(cfg)
	base := sanitizeTopic(cfg.Topic, defaultBaseTopic)

	return &Sink{
		cfg:        cfg,
		log:        log.With("sink", Name),
		qos:        cfg.QoS,
		baseTopic:  base,
		availTopic: base + "/availability",
		deviceID:   sanitizeID(cfg.DeviceID, defaultDeviceID),
		brokerURL:  brokerURL(cfg.Broker),
		announced:  make(map[string]bool),
	}, nil
}

// withDefaults fills the fields a zero-valued config leaves empty.
func withDefaults(cfg config.MQTT) config.MQTT {
	cfg.Topic = sanitizeTopic(cfg.Topic, defaultBaseTopic)
	cfg.DiscoveryPrefix = sanitizeTopic(cfg.DiscoveryPrefix, defaultDiscoveryPrefix)
	cfg.ClientID = firstNonEmpty(strings.TrimSpace(cfg.ClientID), defaultClientID)
	cfg.DeviceID = firstNonEmpty(strings.TrimSpace(cfg.DeviceID), defaultDeviceID)
	cfg.DeviceName = firstNonEmpty(strings.TrimSpace(cfg.DeviceName), defaultDeviceName)
	if cfg.Timeout <= 0 {
		cfg.Timeout = defaultTimeout
	}
	return cfg
}

// brokerURL renders the address paho dials.
//
// A broker is configured as a host, a host:port, or a full URL, because all
// three appear in the wild and rejecting two of them would be a configuration
// error nobody expects. Anything already carrying a scheme is passed through
// untouched, so ssl:// and ws:// keep working without this function having to
// know about them.
func brokerURL(broker string) string {
	broker = strings.TrimSpace(broker)
	if strings.Contains(broker, "://") {
		return broker
	}
	if !strings.Contains(broker, ":") {
		broker += ":" + defaultPort
	}
	return "tcp://" + broker
}

// clientOptions translates config into paho's options, including the last will
// that makes Home Assistant mark the entities unavailable when this process
// dies.
func (s *Sink) clientOptions() *paho.ClientOptions {
	opts := paho.NewClientOptions().
		AddBroker(s.brokerURL).
		SetClientID(s.cfg.ClientID).
		SetConnectTimeout(s.cfg.Timeout).
		SetWriteTimeout(s.cfg.Timeout).
		// A clean session stops the broker queueing hours of stale space
		// weather for a client that was offline; the retained topics already
		// carry the current value of everything.
		SetCleanSession(true).
		SetAutoReconnect(true).
		SetConnectRetry(true).
		SetConnectRetryInterval(connectRetryInterval).
		SetMaxReconnectInterval(maxReconnectInterval).
		SetOrderMatters(false)

	// The will is retained so a subscriber connecting after this process died
	// still sees "offline". Without it Home Assistant shows the last K-index
	// forever, and a stale space weather reading looks exactly like a live one.
	opts.SetWill(s.availTopic, payloadOffline, s.qos, true)

	if s.cfg.Username != "" {
		opts.SetUsername(s.cfg.Username)
	}
	if !s.cfg.Password.IsZero() {
		// The only place the password is revealed, and it goes to the client
		// rather than anywhere near a log line.
		opts.SetPassword(s.cfg.Password.Reveal())
	}

	opts.SetOnConnectHandler(func(paho.Client) { s.onConnected() })
	opts.SetConnectionLostHandler(func(_ paho.Client, err error) {
		// Logged and dropped: paho reconnects on its own and the poll loop must
		// not learn about broker trouble.
		s.log.Warn("mqtt connection lost", "broker", s.brokerURL, "error", redact.Error(err))
	})
	opts.SetReconnectingHandler(func(paho.Client, *paho.ClientOptions) {
		s.log.Info("mqtt reconnecting", "broker", s.brokerURL)
	})
	return opts
}

// Name identifies the sink.
func (s *Sink) Name() string { return Name }

// Publish sends one batch, one message per sample.
//
// An empty batch is sink.ErrSkipped rather than an error: a source that polled
// successfully and found nothing new — a 304, or a feed whose optional fields
// were all empty — has not failed.
func (s *Sink) Publish(ctx context.Context, b metric.Batch) error {
	if b.Len() == 0 {
		return fmt.Errorf("%w: empty batch from %s", sink.ErrSkipped, b.Source)
	}
	if s.isClosed() {
		return errors.New("mqtt: sink is closed")
	}
	if err := s.ensureConnected(ctx); err != nil {
		return err
	}

	published := 0
	for i := range b.Samples {
		sample := b.Samples[i]
		if sample.Desc == nil {
			// A sample with no descriptor cannot be named, let alone
			// advertised. Drop it rather than publish to a topic built from an
			// empty string.
			s.log.Warn("skipping sample with no descriptor", "source", b.Source)
			continue
		}
		if math.IsNaN(sample.Value) || math.IsInf(sample.Value, 0) {
			// Retaining "NaN" on a topic Home Assistant parses as a number
			// leaves the entity broken until the next good value, which for a
			// three-hourly index can be hours away.
			s.log.Warn("skipping non-finite value", "metric", sample.Desc.FullName())
			continue
		}

		topic := s.stateTopic(sample)

		if s.cfg.DiscoveryEnabled && !s.isAnnounced(sample) {
			if err := s.announce(ctx, sample, topic); err != nil {
				// Discovery failing costs auto-configuration, not data, so the
				// state message still goes out. The series stays unannounced
				// and is retried on the next batch.
				s.log.Error("publish discovery failed",
					"metric", sample.Desc.FullName(), "error", err)
			}
		}

		payload, err := renderPayload(b, sample)
		if err != nil {
			s.log.Error("render payload failed", "metric", sample.Desc.FullName(), "error", err)
			continue
		}
		if err := s.publish(ctx, topic, payload, s.cfg.Retain); err != nil {
			// Returning on the first failure keeps a broker outage bounded by
			// one timeout instead of one per series, which for a DRAP grid
			// would be thousands.
			return err
		}
		published++
	}

	if published == 0 {
		return fmt.Errorf("%w: no publishable samples in batch from %s", sink.ErrSkipped, b.Source)
	}
	return nil
}

// Close announces the shutdown and disconnects.
//
// The retained "offline" is the whole point: it is what tells Home Assistant to
// stop presenting the last reading as though it were current. Close is
// idempotent and safe on a sink that never connected.
func (s *Sink) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()

	if s.client == nil {
		return nil
	}

	var err error
	if s.client.IsConnectionOpen() {
		ctx, cancel := context.WithTimeout(context.Background(), s.cfg.Timeout)
		err = s.publish(ctx, s.availTopic, payloadOffline, true)
		cancel()
		if err != nil {
			err = fmt.Errorf("mqtt: publish offline: %w", err)
		}
	}

	// Disconnect regardless, so a broker that would not accept the farewell
	// still gets the socket closed.
	s.client.Disconnect(uint(s.cfg.Timeout / time.Millisecond))
	return err
}

// ensureConnected dials the broker if it is not already up.
//
// The double check around the mutex is not premature optimisation: in the
// healthy case this runs on every sample-bearing batch from every source, and
// the connection is open, so the fast path must not serialise them all behind
// one lock.
func (s *Sink) ensureConnected(ctx context.Context) error {
	if s.client.IsConnectionOpen() {
		return nil
	}

	s.connMu.Lock()
	defer s.connMu.Unlock()
	if s.client.IsConnectionOpen() {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, s.cfg.Timeout)
	defer cancel()

	token := s.client.Connect()
	if token == nil {
		return fmt.Errorf("mqtt: connect to %s: client returned no token", s.brokerURL)
	}

	select {
	case <-token.Done():
		if err := token.Error(); err != nil {
			// Transient by construction: paho keeps retrying in the background
			// and the next batch tries again.
			return fmt.Errorf("mqtt: connect to %s: %w", s.brokerURL, redact.Error(err))
		}
	case <-ctx.Done():
		// Paho's token would otherwise block for the length of the outage, and
		// a sink that blocks forever is a shutdown that hangs forever.
		return fmt.Errorf("mqtt: connect to %s: %w", s.brokerURL, ctx.Err())
	}

	if !s.client.IsConnectionOpen() {
		return fmt.Errorf("mqtt: connect to %s: connection not open after connect", s.brokerURL)
	}
	return nil
}

// onConnected runs on every successful connect, including paho's own
// reconnects.
//
// It republishes availability and forgets which series have been announced. A
// broker that restarted lost every retained message it was holding, including
// the discovery documents, so the announcements have to be made again; a
// broker that merely dropped one client did not, and republishing a handful of
// identical retained configs after a reconnect is cheap.
func (s *Sink) onConnected() {
	if s.isClosed() {
		return
	}

	s.mu.Lock()
	s.announced = make(map[string]bool)
	s.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), s.cfg.Timeout)
	defer cancel()

	if err := s.publish(ctx, s.availTopic, payloadOnline, true); err != nil {
		s.log.Error("publish availability failed", "topic", s.availTopic, "error", err)
		return
	}
	s.log.Info("mqtt connected", "broker", s.brokerURL, "availability_topic", s.availTopic)
}

// publish sends one message and waits for the broker.
//
// The wait is bounded by the caller's context and, independently, by the
// configured timeout. Paho accepts a publish while it is still reconnecting and
// completes the token only once the message reaches the broker, so an unbounded
// wait here would block for the whole length of an outage.
//
// Every error goes through redact.Error, because a broker URL carrying
// credentials is exactly the kind of thing that ends up in a transport error's
// message.
func (s *Sink) publish(ctx context.Context, topic, payload string, retained bool) error {
	ctx, cancel := context.WithTimeout(ctx, s.cfg.Timeout)
	defer cancel()

	token := s.client.Publish(topic, s.qos, retained, payload)
	if token == nil {
		return fmt.Errorf("mqtt: publish %s: client returned no token", topic)
	}

	select {
	case <-token.Done():
		if err := token.Error(); err != nil {
			return fmt.Errorf("mqtt: publish %s: %w", topic, redact.Error(err))
		}
		return nil
	case <-ctx.Done():
		return fmt.Errorf("mqtt: publish %s: %w", topic, ctx.Err())
	}
}

func (s *Sink) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

// --- topics ----------------------------------------------------------------

// stateTopic is where one series' values are published.
//
// The metric name is used without metric.Prefix: the prefix exists to namespace
// a flat Prometheus registry, and the base topic already namespaces everything
// here, so "solarham/solar_k_index/Boulder" would say "solar" twice.
func (s *Sink) stateTopic(sample metric.Sample) string {
	var b strings.Builder
	b.WriteString(s.baseTopic)
	b.WriteByte('/')
	b.WriteString(sanitizeSegment(sample.Desc.Name))
	for _, v := range sample.Labels {
		b.WriteByte('/')
		b.WriteString(sanitizeSegment(v))
	}
	return b.String()
}

// sanitizeSegment makes one label or metric name safe to use as a single topic
// segment.
//
// The rule: ASCII letters, digits, '-', '_' and '.' are kept as they are;
// every other byte, including the wildcards '+' and '#', the separator '/',
// spaces, commas and the '>' and '=' of ">=10 MeV", becomes '_'; runs of '_'
// collapse to one; leading and trailing '_' are trimmed; and a segment left
// empty becomes "unknown", since MQTT has no empty segments.
//
// So "Austin, TX, USA" publishes as "Austin_TX_USA" and ">=10 MeV" as
// "10_MeV". Case is preserved because MQTT topics are case-sensitive and
// lowercasing would make the topic harder to match back to the label value an
// operator sees in Grafana.
//
// This is lossy, and deliberately so: two label values that differ only in
// punctuation share a topic. Discovery does not rely on the topic for identity
// — unique_id is derived from the raw label values — so a collision affects
// only which topic a value lands on, and no real label set in this exporter's
// vocabulary collides.
func sanitizeSegment(s string) string {
	if out := sanitizeSegmentOrEmpty(s); out != "" {
		return out
	}
	return emptySegment
}

// sanitizeSegmentOrEmpty is sanitizeSegment without the placeholder, so callers
// building a multi-segment prefix can drop a segment that had nothing usable in
// it rather than writing "unknown" into the middle of a topic.
func sanitizeSegmentOrEmpty(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	underscore := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9',
			c == '-', c == '_', c == '.':
			b.WriteByte(c)
			underscore = false
		default:
			if !underscore {
				b.WriteByte('_')
				underscore = true
			}
		}
	}
	return strings.Trim(b.String(), "_")
}

// sanitizeTopic cleans a multi-segment prefix such as the base topic or the
// discovery prefix, sanitising each segment and dropping empty ones so
// "/home//solar/" becomes "home/solar".
//
// Wildcards are legal in a subscription and illegal in a publish, so a base
// topic containing one would make every publish fail at the broker with an
// error that is hard to trace back to configuration.
func sanitizeTopic(topic, fallback string) string {
	parts := strings.Split(strings.TrimSpace(topic), "/")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		seg := sanitizeSegmentOrEmpty(p)
		if seg == "" {
			continue
		}
		out = append(out, seg)
	}
	if len(out) == 0 {
		return fallback
	}
	return strings.Join(out, "/")
}

// sanitizeID reduces an identifier to the characters Home Assistant accepts in
// a discovery topic's node id and object id, and in a unique_id.
func sanitizeID(id, fallback string) string {
	var b strings.Builder
	b.Grow(len(id))
	underscore := false
	for i := 0; i < len(id); i++ {
		c := id[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9',
			c == '-', c == '_':
			b.WriteByte(c)
			underscore = false
		default:
			if !underscore {
				b.WriteByte('_')
				underscore = true
			}
		}
	}
	out := strings.Trim(b.String(), "_")
	if out == "" {
		return fallback
	}
	return out
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

// --- payload ---------------------------------------------------------------

// statePayload is the JSON published to a state topic. The key names are part
// of the contract with Home Assistant's value templates, so they change only
// with the templates in discovery.go.
type statePayload struct {
	Value    float64           `json:"value"`
	Unit     string            `json:"unit,omitempty"`
	Time     string            `json:"time"`
	Source   string            `json:"source"`
	Labels   map[string]string `json:"labels,omitempty"`
	Category string            `json:"category,omitempty"`
}

// renderPayload builds one state message.
//
// The timestamp is the sample's own, never time.Now(): a three-hourly K-index
// republished every minute under a fresh timestamp is how the exporter this
// replaces turned one observation into hundreds of fictitious ones.
func renderPayload(b metric.Batch, sample metric.Sample) (string, error) {
	p := statePayload{
		Value:  sample.Value,
		Unit:   sample.Desc.Unit,
		Time:   sample.Time.UTC().Format(time.RFC3339),
		Source: b.Source,
		Labels: labelMap(sample),
	}
	if sample.Desc.Kind == metric.KindInfo {
		p.Category = categoryOf(sample)
	}

	out, err := json.Marshal(p)
	if err != nil {
		return "", fmt.Errorf("mqtt: marshal payload for %s: %w", sample.Desc.FullName(), err)
	}
	return string(out), nil
}

// labelMap pairs the descriptor's label names with the sample's values.
func labelMap(sample metric.Sample) map[string]string {
	if len(sample.Desc.Labels) == 0 || len(sample.Labels) == 0 {
		return nil
	}
	out := make(map[string]string, len(sample.Labels))
	for i, name := range sample.Desc.Labels {
		if i >= len(sample.Labels) {
			break
		}
		out[name] = sample.Labels[i]
	}
	return out
}

// categoryLabels are the label names that carry the categorical value of a
// KindInfo metric, in preference order.
//
// A KindInfo sample is the constant 1 with the interesting part in a label, and
// which label that is differs per metric: "condition" for band and VHF states,
// "class" for the X-ray flare class, "state" for the geomagnetic field wording.
// Naming them here rather than positionally means adding an info metric with an
// existing label name needs no change at all.
var categoryLabels = []string{"condition", "class", "state", "category", "status"}

// categoryOf extracts the categorical value from a KindInfo sample.
//
// A metric whose labels match none of the known names — the SWPC alert, whose
// labels are a product id and a serial — falls back to the last label value,
// which is the most specific one by descriptor convention.
func categoryOf(sample metric.Sample) string {
	for _, name := range categoryLabels {
		if v, ok := sample.LabelFor(name); ok && v != "" {
			return v
		}
	}
	if n := len(sample.Labels); n > 0 {
		return sample.Labels[n-1]
	}
	return ""
}

// seriesKey identifies one series for the announced set. It is the raw key from
// metric.Sample, not the sanitised topic, so two label sets that sanitise to
// the same topic are still two series.
func seriesKey(sample metric.Sample) string { return sample.Key() }

// seriesHash is a short, stable digest of a series' identity, used to guarantee
// that two different label sets can never share a unique_id even when their
// sanitised forms are identical.
func seriesHash(sample metric.Sample) string {
	sum := sha256.Sum256([]byte(seriesKey(sample)))
	return hex.EncodeToString(sum[:4])
}
