package config

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"strings"
	"time"
)

// ErrVersionRequested is returned when --version was given. It is not a
// failure: the caller prints the build information and exits zero.
var ErrVersionRequested = errors.New("version requested")

// legacyAliases are the lowercase environment variables the container this
// replaces passes, mapped to their modern equivalents.
//
// Accepting them is what makes this image a drop-in: swapping the image and
// changing nothing else keeps writing to the same InfluxDB. Each one is
// deprecated rather than supported, so using one appends an operator-facing
// note naming the variable to migrate to.
var legacyAliases = []struct {
	// legacy is the variable name exactly as the old container passes it,
	// lowercase and unprefixed.
	legacy string
	// modern is the setting name it stands in for, without the prefix.
	modern string
}{
	{legacy: "url", modern: "INFLUX2_URL"},
	{legacy: "token", modern: "INFLUX2_TOKEN"},
}

// Load reads and validates configuration from flags, the environment, an
// optional file, and built-in defaults, in that order of precedence.
//
// lookupEnv is injected rather than read from the process so tests never have
// to mutate the real environment; passing nil uses os.LookupEnv.
//
// Every problem found is reported together, not just the first, so a
// misconfigured deployment can be fixed in one pass.
func Load(args []string, lookupEnv func(string) (string, bool)) (*Config, error) {
	return load(args, lookupEnv, os.ReadFile)
}

// load is Load with an injectable file reader, covering both secret files and
// the config file.
func load(args []string, lookupEnv func(string) (string, bool), readFile func(string) ([]byte, error)) (*Config, error) {
	configPath, err := parseFlags(args)
	if err != nil {
		return nil, err
	}

	l := newLoader()
	if lookupEnv != nil {
		l.lookupEnv = lookupEnv
	}
	if readFile != nil {
		l.readFile = readFile
	}

	cfg := &Config{}

	if err := applyConfigFile(l, cfg, configPath); err != nil {
		return nil, err
	}
	applyLegacyEnv(l, cfg)

	loadLog(l, cfg)
	loadHTTP(l, cfg)

	loadHamqsl(l, cfg)
	loadSWPC(l, cfg)
	loadKC2G(l, cfg)
	loadSources(l, cfg)

	loadPrometheus(l, cfg)
	loadInfluxV1(l, cfg)
	loadInfluxV2(l, cfg)
	loadOTLP(l, cfg)
	loadMQTT(l, cfg)

	validate(l, cfg)

	cfg.resolved = l.resolved
	if len(l.errs) > 0 {
		return nil, fmt.Errorf("invalid configuration:\n  - %s",
			strings.Join(errorStrings(l.errs), "\n  - "))
	}
	return cfg, nil
}

// parseFlags handles the two flags the exporter takes and rejects anything
// else. A positional argument is a hard error rather than something ignored,
// because a mistyped flag that lands as one would otherwise start the process
// with the setting silently unapplied.
func parseFlags(args []string) (string, error) {
	fs := flag.NewFlagSet("solarham-exporter", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	configPath := fs.String("config", "", "path to an INI configuration file")
	version := fs.Bool("version", false, "print build information and exit")

	if err := fs.Parse(args); err != nil {
		return "", fmt.Errorf("invalid flags: %w", err)
	}
	if *version {
		return "", ErrVersionRequested
	}
	if rest := fs.Args(); len(rest) > 0 {
		return "", fmt.Errorf("unexpected argument %q; this exporter is configured "+
			"through %s* environment variables and takes no positional arguments",
			rest[0], EnvPrefix)
	}
	return *configPath, nil
}

// applyConfigFile locates and reads the optional INI file.
//
// A file named explicitly must exist; one found by searching the default
// locations is optional. Silently ignoring a file the operator mounted is how a
// deployment ends up running on defaults with nobody noticing.
func applyConfigFile(l *loader, cfg *Config, configPath string) error {
	explicit := configPath != ""
	if !explicit {
		found, err := FindConfigFile()
		if err != nil {
			return err
		}
		configPath = found
	}
	if configPath == "" {
		return nil
	}

	settings, err := loadConfigFile(configPath, l.readFile)
	if err != nil {
		if explicit || !errors.Is(err, fs.ErrNotExist) {
			return err
		}
		return nil
	}
	l.fileValues = settings.values
	cfg.configFile = settings.path
	return nil
}

// applyLegacyEnv accepts the lowercase variables the predecessor container
// passes, and turns each one into a deprecation note.
//
// Setting both an alias and its modern equivalent is an error rather than a
// silent precedence rule: the two would be pointing the same sink at two
// different places, and guessing which the operator meant is how data lands in
// the wrong database.
func applyLegacyEnv(l *loader, cfg *Config) {
	used := false

	for _, alias := range legacyAliases {
		value, ok := l.lookupEnv(alias.legacy)
		if !ok {
			continue
		}

		modern := EnvPrefix + alias.modern
		_, hasModern := l.lookupEnv(modern)
		_, hasModernFile := l.lookupEnv(modern + FileSuffix)
		if hasModern || hasModernFile {
			// Neither the legacy value nor the modern one is quoted here: one
			// of the pair is a token.
			l.errf("the legacy %q environment variable and %s are both set; "+
				"remove %q and keep %s", alias.legacy, modern, alias.legacy, modern)
			continue
		}

		l.legacy[alias.modern] = value
		used = true
		cfg.notes = append(cfg.notes, fmt.Sprintf(
			"the lowercase %q environment variable is deprecated; set %s instead",
			alias.legacy, modern))
	}

	if !used {
		return
	}

	// A legacy variable is only ever passed by a deployment that wanted the
	// InfluxDB 2.x sink, so it implies the sink. An explicit setting still
	// wins, which is why this goes in as a value rather than as a default.
	enabled := EnvPrefix + "INFLUX2_ENABLED"
	if _, ok := l.lookupEnv(enabled); !ok {
		l.legacy["INFLUX2_ENABLED"] = "true"
		cfg.notes = append(cfg.notes, fmt.Sprintf(
			"a legacy environment variable enabled the InfluxDB 2.x sink; set %s "+
				"explicitly to be rid of this note", enabled))
	}
}

func loadLog(l *loader, cfg *Config) {
	cfg.Log = Log{
		Level:  parseLevel(l, l.Enum("LOG_LEVEL", "info", "debug", "info", "warn", "error")),
		Format: l.Enum("LOG_FORMAT", "json", "json", "text"),
	}
}

func loadHTTP(l *loader, cfg *Config) {
	cfg.HTTP = HTTP{
		Addr:            l.String("HTTP_ADDR", "0.0.0.0:9102"),
		ReadTimeout:     l.Duration("HTTP_READ_TIMEOUT", 10*time.Second, time.Second, 5*time.Minute),
		ShutdownTimeout: l.Duration("HTTP_SHUTDOWN_TIMEOUT", 10*time.Second, time.Second, 5*time.Minute),
		StaleAfter:      l.Duration("HTTP_STALE_AFTER", 0, 0, 24*time.Hour),
	}
}

// loadHamqsl reads the hamqsl.com source.
//
// The interval floor of fifteen minutes is not a guess at a sensible value. The
// feed's operator was shut down by his ISP over exactly this load and asked
// publicly for hourly polling, so the default is an hour and the floor exists
// so a copy-pasted compose file cannot drive it lower.
func loadHamqsl(l *loader, cfg *Config) {
	cfg.Hamqsl = Hamqsl{
		Enabled:  l.Bool("HAMQSL_ENABLED", true),
		URL:      l.String("HAMQSL_URL", "https://www.hamqsl.com/solarxml.php"),
		Interval: l.Duration("HAMQSL_INTERVAL", time.Hour, 15*time.Minute, 24*time.Hour),
		Timeout:  l.Duration("HAMQSL_TIMEOUT", 15*time.Second, time.Second, 2*time.Minute),
		Retries:  l.Int("HAMQSL_RETRIES", 2, 0, 10),
	}
	requireURL(l, cfg.Hamqsl.Enabled, "hamqsl", "HAMQSL_URL", cfg.Hamqsl.URL)
}

func loadSWPC(l *loader, cfg *Config) {
	cfg.SWPC = SWPC{
		Enabled: l.Bool("SWPC_ENABLED", true),
		BaseURL: l.String("SWPC_BASE_URL", "https://services.swpc.noaa.gov"),

		FastInterval:   l.Duration("SWPC_FAST_INTERVAL", time.Minute, time.Minute, time.Hour),
		MediumInterval: l.Duration("SWPC_MEDIUM_INTERVAL", 5*time.Minute, time.Minute, 6*time.Hour),
		SlowInterval:   l.Duration("SWPC_SLOW_INTERVAL", time.Hour, 5*time.Minute, 24*time.Hour),

		Timeout: l.Duration("SWPC_TIMEOUT", 20*time.Second, time.Second, 2*time.Minute),
		Retries: l.Int("SWPC_RETRIES", 2, 0, 10),

		DRAPEnabled:  l.Bool("SWPC_DRAP_ENABLED", false),
		DRAPGridStep: l.Int("SWPC_DRAP_GRID_STEP", 10, 1, 90),
	}
	requireURL(l, cfg.SWPC.Enabled, "SWPC", "SWPC_BASE_URL", cfg.SWPC.BaseURL)
}

// loadKC2G reads the prop.kc2g.com source.
//
// It is off by default because the data is CC BY-NC-SA and non-commercial only.
// A licence restriction should be something an operator opts into knowingly,
// not something they inherit from a default they never read.
func loadKC2G(l *loader, cfg *Config) {
	cfg.KC2G = KC2G{
		Enabled:  l.Bool("KC2G_ENABLED", false),
		BaseURL:  l.String("KC2G_BASE_URL", "https://prop.kc2g.com"),
		Interval: l.Duration("KC2G_INTERVAL", 15*time.Minute, 5*time.Minute, 6*time.Hour),
		Timeout:  l.Duration("KC2G_TIMEOUT", 20*time.Second, time.Second, 2*time.Minute),
		Retries:  l.Int("KC2G_RETRIES", 2, 0, 10),

		MinConfidence:    l.Float("KC2G_MIN_CONFIDENCE", 25, 0, 100),
		MaxAge:           l.Duration("KC2G_MAX_AGE", 3*time.Hour, 5*time.Minute, 72*time.Hour),
		Stations:         l.StringSlice("KC2G_STATIONS"),
		EffectiveIndices: l.Bool("KC2G_EFFECTIVE_INDICES", true),
	}
	requireURL(l, cfg.KC2G.Enabled, "kc2g", "KC2G_BASE_URL", cfg.KC2G.BaseURL)
}

func loadPrometheus(l *loader, cfg *Config) {
	cfg.Prometheus = Prometheus{
		Enabled:   l.Bool("PROMETHEUS_ENABLED", true),
		Path:      l.String("PROMETHEUS_PATH", "/metrics"),
		Retention: l.Duration("PROMETHEUS_RETENTION", 0, 0, 24*time.Hour),
	}
}

func loadInfluxV1(l *loader, cfg *Config) {
	cfg.InfluxV1 = InfluxV1{
		Enabled:         l.Bool("INFLUX1_ENABLED", false),
		URL:             l.String("INFLUX1_URL", ""),
		Database:        l.String("INFLUX1_DATABASE", ""),
		RetentionPolicy: l.String("INFLUX1_RETENTION_POLICY", ""),
		Username:        l.String("INFLUX1_USERNAME", ""),
		Password:        l.Secret("INFLUX1_PASSWORD"),
		Measurement:     l.String("INFLUX1_MEASUREMENT", "solar"),
		Timeout:         l.Duration("INFLUX1_TIMEOUT", 10*time.Second, time.Second, 2*time.Minute),
		Retries:         l.Int("INFLUX1_RETRIES", 2, 0, 10),
	}
}

func loadInfluxV2(l *loader, cfg *Config) {
	cfg.InfluxV2 = InfluxV2{
		Enabled:     l.Bool("INFLUX2_ENABLED", false),
		URL:         l.String("INFLUX2_URL", ""),
		Org:         l.String("INFLUX2_ORG", ""),
		OrgID:       l.String("INFLUX2_ORG_ID", ""),
		Bucket:      l.String("INFLUX2_BUCKET", "SolarHAM"),
		Token:       l.Secret("INFLUX2_TOKEN"),
		Measurement: l.String("INFLUX2_MEASUREMENT", "solar"),
		Timeout:     l.Duration("INFLUX2_TIMEOUT", 10*time.Second, time.Second, 2*time.Minute),
		Retries:     l.Int("INFLUX2_RETRIES", 2, 0, 10),
	}
}

func loadOTLP(l *loader, cfg *Config) {
	cfg.OTLP = OTLP{
		Enabled:  l.Bool("OTLP_ENABLED", false),
		Protocol: l.Enum("OTLP_PROTOCOL", "grpc", "grpc", "http"),
		Endpoint: l.String("OTLP_ENDPOINT", ""),
		Insecure: l.Bool("OTLP_INSECURE", false),
		Headers:  l.StringMap("OTLP_HEADERS"),
		Interval: l.Duration("OTLP_INTERVAL", 60*time.Second, 10*time.Second, time.Hour),
		Timeout:  l.Duration("OTLP_TIMEOUT", 10*time.Second, time.Second, 2*time.Minute),
	}
}

func loadMQTT(l *loader, cfg *Config) {
	cfg.MQTT = MQTT{
		Enabled:  l.Bool("MQTT_ENABLED", false),
		Broker:   l.String("MQTT_BROKER", ""),
		ClientID: l.String("MQTT_CLIENT_ID", "solarham-exporter"),
		Username: l.String("MQTT_USERNAME", ""),
		Password: l.Secret("MQTT_PASSWORD"),

		Topic:   l.String("MQTT_TOPIC", "solarham"),
		QoS:     byte(l.Int("MQTT_QOS", 0, 0, 2)),
		Retain:  l.Bool("MQTT_RETAIN", true),
		Timeout: l.Duration("MQTT_TIMEOUT", 10*time.Second, time.Second, 2*time.Minute),

		DiscoveryEnabled: l.Bool("MQTT_DISCOVERY_ENABLED", true),
		DiscoveryPrefix:  l.String("MQTT_DISCOVERY_PREFIX", "homeassistant"),
		DeviceID:         l.String("MQTT_DEVICE_ID", "solarham"),
		DeviceName:       l.String("MQTT_DEVICE_NAME", "SolarHAM"),
	}
}

// validate applies the cross-cutting rules, the ones that span more than one
// setting and so cannot be checked as each value is read.
func validate(l *loader, cfg *Config) {
	if !AnySinkEnabled(cfg) {
		l.errf("no sinks are enabled, so nothing would be published; enable at "+
			"least one, for example %sPROMETHEUS_ENABLED=true", EnvPrefix)
	}
	if !AnySourceEnabled(cfg) {
		l.errf("no sources are enabled, so nothing would be collected; enable at "+
			"least one, for example %sSWPC_ENABLED=true", EnvPrefix)
	}

	if cfg.Prometheus.Enabled {
		if !strings.HasPrefix(cfg.Prometheus.Path, "/") {
			l.errf("%sPROMETHEUS_PATH must start with a slash, got %q",
				EnvPrefix, cfg.Prometheus.Path)
		}
		l.require("Prometheus", "HTTP_ADDR", cfg.HTTP.Addr)
	}

	if cfg.InfluxV1.Enabled {
		l.require("InfluxDB 1.x", "INFLUX1_URL", cfg.InfluxV1.URL)
		l.require("InfluxDB 1.x", "INFLUX1_DATABASE", cfg.InfluxV1.Database)
	}

	if cfg.InfluxV2.Enabled {
		l.require("InfluxDB 2.x", "INFLUX2_URL", cfg.InfluxV2.URL)
		l.require("InfluxDB 2.x", "INFLUX2_BUCKET", cfg.InfluxV2.Bucket)
		l.requireSecret("InfluxDB 2.x", "INFLUX2_TOKEN", cfg.InfluxV2.Token)
		validateInfluxV2Org(l, cfg)
	}

	if cfg.OTLP.Enabled {
		l.require("OTLP", "OTLP_ENDPOINT", cfg.OTLP.Endpoint)
	}

	if cfg.MQTT.Enabled {
		l.require("MQTT", "MQTT_BROKER", cfg.MQTT.Broker)
	}

	// A timeout at or above the interval means a slow poll is still running
	// when the next one is due, which turns one slow upstream into a growing
	// backlog rather than a skipped sample.
	requireTimeoutBelowInterval(l, cfg.Hamqsl.Enabled,
		"HAMQSL_TIMEOUT", cfg.Hamqsl.Timeout, "HAMQSL_INTERVAL", cfg.Hamqsl.Interval)
	requireTimeoutBelowInterval(l, cfg.SWPC.Enabled,
		"SWPC_TIMEOUT", cfg.SWPC.Timeout, "SWPC_FAST_INTERVAL", cfg.SWPC.FastInterval)
	requireTimeoutBelowInterval(l, cfg.KC2G.Enabled,
		"KC2G_TIMEOUT", cfg.KC2G.Timeout, "KC2G_INTERVAL", cfg.KC2G.Interval)

	validateSources(l, cfg)
}

// validateInfluxV2Org enforces exactly one of org or orgID.
//
// Requiring neither is precisely the bug that made the predecessor script 404
// on every single write for the life of the container: the write was accepted
// by the client, rejected by the server, and nothing ever said so. The error
// therefore names both settings rather than failing quietly at run time.
func validateInfluxV2Org(l *loader, cfg *Config) {
	hasOrg := strings.TrimSpace(cfg.InfluxV2.Org) != ""
	hasOrgID := strings.TrimSpace(cfg.InfluxV2.OrgID) != ""

	switch {
	case hasOrg && hasOrgID:
		l.errf("%sINFLUX2_ORG and %sINFLUX2_ORG_ID are both set; supply exactly one",
			EnvPrefix, EnvPrefix)
	case !hasOrg && !hasOrgID:
		l.errf("InfluxDB 2.x is enabled but neither %sINFLUX2_ORG nor %sINFLUX2_ORG_ID "+
			"is set; exactly one of the two is required, and writing without an org "+
			"is rejected by the server on every request",
			EnvPrefix, EnvPrefix)
	}
}

// requireTimeoutBelowInterval reports a poll budget that does not fit inside
// its own schedule.
func requireTimeoutBelowInterval(l *loader, enabled bool, timeoutName string, timeout time.Duration, intervalName string, interval time.Duration) {
	if !enabled || timeout < interval {
		return
	}
	l.errf("%s%s (%s) must be shorter than %s%s (%s)",
		EnvPrefix, timeoutName, timeout, EnvPrefix, intervalName, interval)
}

// parseLevel converts a validated level name to a slog.Level.
func parseLevel(l *loader, name string) slog.Level {
	switch name {
	case "debug":
		return slog.LevelDebug
	case "info":
		return slog.LevelInfo
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		l.errf("unknown log level %q", name)
		return slog.LevelInfo
	}
}
