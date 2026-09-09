// Package config resolves every setting the exporter takes, from flags,
// environment variables, an optional file, and built-in defaults.
package config

import (
	"log/slog"
	"time"

	"github.com/RealDougEubanks/solarham/internal/redact"
)

// Config is every resolved setting.
//
// It is flat at the top level with one nested struct per concern, so that a
// sink or source constructor takes exactly the settings it needs and cannot
// reach the ones it does not.
type Config struct {
	Log        Log
	HTTP       HTTP
	Hamqsl     Hamqsl
	SWPC       SWPC
	KC2G       KC2G
	Prometheus Prometheus
	InfluxV1   InfluxV1
	InfluxV2   InfluxV2
	OTLP       OTLP
	MQTT       MQTT

	// resolved records every setting's provenance for the startup log.
	resolved []resolved

	// configFile is the file settings were read from, if any.
	configFile string

	// notes carries deprecation warnings and similar operator-facing messages
	// accumulated during loading, emitted after the logger exists.
	notes []string
}

// Log configures the structured logger.
type Log struct {
	Level  slog.Level
	Format string // "json" or "text"
}

// HTTP configures the metrics and health server.
type HTTP struct {
	Addr            string
	ReadTimeout     time.Duration
	ShutdownTimeout time.Duration

	// StaleAfter overrides the per-source staleness window used by /readyz.
	// Zero means each source uses three of its own intervals, which is almost
	// always what you want given how widely the intervals differ.
	StaleAfter time.Duration
}

// Hamqsl configures the hamqsl.com solar XML source.
//
// This feed is one person's server behind a CDN, and its operator has publicly
// asked for hourly polling after being shut down by his ISP over exactly this
// load. Interval is therefore clamped to a floor rather than being freely
// configurable downwards.
type Hamqsl struct {
	Enabled  bool
	URL      string
	Interval time.Duration
	Timeout  time.Duration
	Retries  int
}

// SWPC configures the NOAA Space Weather Prediction Center source.
//
// SWPC publishes at very different cadences, so it is polled in three tiers
// rather than on one interval. Everything is public domain and needs no
// credentials.
type SWPC struct {
	Enabled bool
	BaseURL string

	FastInterval   time.Duration
	MediumInterval time.Duration
	SlowInterval   time.Duration

	Timeout time.Duration
	Retries int

	// DRAP controls whether the D-Region Absorption Prediction grid is
	// published. It is a global lat/lon grid and produces far more series than
	// everything else combined, so it is opt-in.
	DRAPEnabled bool

	// DRAPGridStep subsamples the D-RAP grid, publishing every Nth cell in each
	// axis. 1 publishes the full grid.
	DRAPGridStep int
}

// KC2G configures the prop.kc2g.com ionosonde source.
//
// The data is CC BY-NC-SA and non-commercial only, which is why this source is
// disabled by default: a licence restriction should be something an operator
// opts into knowingly, not something they inherit from a default.
type KC2G struct {
	Enabled  bool
	BaseURL  string
	Interval time.Duration
	Timeout  time.Duration
	Retries  int

	// MinConfidence drops stations whose autoscaling confidence score is below
	// this. Ionosonde autoscaling is unreliable enough that low-confidence
	// readings are worse than no reading.
	MinConfidence float64

	// MaxAge drops stations whose own timestamp is older than this. The
	// document is served fresh even when individual stations within it have
	// been silent for months, so per-station freshness must be checked
	// separately.
	MaxAge time.Duration

	// Stations, when non-empty, restricts publication to these URSI codes.
	// Publishing every station worldwide is a lot of series for a dashboard
	// that probably cares about a handful nearby.
	Stations []string

	// EffectiveIndices controls whether the effective SSN and flux series from
	// the essn endpoint are published.
	EffectiveIndices bool
}

// Prometheus configures the pull-based metrics endpoint.
type Prometheus struct {
	Enabled bool
	Path    string

	// Retention bounds how long a sample stays on /metrics after it was last
	// observed. Zero derives it from the owning source's interval.
	Retention time.Duration
}

// InfluxV1 configures an InfluxDB 1.x write target.
type InfluxV1 struct {
	Enabled         bool
	URL             string
	Database        string
	RetentionPolicy string
	Username        string
	Password        redact.Secret
	Measurement     string
	Timeout         time.Duration
	Retries         int
}

// InfluxV2 configures an InfluxDB 2.x write target.
//
// Both Org and OrgID are accepted. The token this replaces a script for is
// bucket-scoped and cannot resolve an org by name, which is half of why that
// script never wrote a single point, so addressing the org by ID has to be
// possible.
type InfluxV2 struct {
	Enabled     bool
	URL         string
	Org         string
	OrgID       string
	Bucket      string
	Token       redact.Secret
	Measurement string
	Timeout     time.Duration
	Retries     int
}

// OTLP configures the OpenTelemetry metrics exporter.
type OTLP struct {
	Enabled  bool
	Protocol string // "grpc" or "http"
	Endpoint string
	Insecure bool
	Headers  map[string]string
	Interval time.Duration
	Timeout  time.Duration
}

// MQTT configures the MQTT publisher and its Home Assistant discovery.
type MQTT struct {
	Enabled  bool
	Broker   string
	ClientID string
	Username string
	Password redact.Secret

	// Topic is the prefix every published topic sits under.
	Topic   string
	QoS     byte
	Retain  bool
	Timeout time.Duration

	// DiscoveryEnabled publishes Home Assistant MQTT discovery documents so the
	// entities appear without manual configuration.
	DiscoveryEnabled bool
	DiscoveryPrefix  string
	DeviceID         string
	DeviceName       string
}

// ConfigFile reports which file settings were read from, or "" if none.
func (c *Config) ConfigFile() string { return c.configFile }

// Notes returns operator-facing messages accumulated while loading, such as
// deprecation warnings for the legacy environment variables.
func (c *Config) Notes() []string { return c.notes }

// AnySinkEnabled reports whether at least one sink is configured.
func AnySinkEnabled(c *Config) bool {
	return c.Prometheus.Enabled ||
		c.InfluxV1.Enabled ||
		c.InfluxV2.Enabled ||
		c.OTLP.Enabled ||
		c.MQTT.Enabled
}

// AnySourceEnabled reports whether at least one source is configured.
func AnySourceEnabled(c *Config) bool {
	return c.Hamqsl.Enabled || c.SWPC.Enabled || c.KC2G.Enabled
}
