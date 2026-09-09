package config

import (
	"bytes"
	"errors"
	"io/fs"
	"log/slog"
	"strings"
	"testing"
	"time"
)

// loadEnv runs Load against exactly the supplied settings and files.
//
// Nothing here touches the real process environment or filesystem, which is the
// reason lookupEnv and the file reader are injected: a developer with the
// exporter configured in their shell must see the same results as CI.
//
// Keys in env are given without the SOLARHAM_ prefix, except the lowercase
// legacy aliases, which have none.
func loadEnv(t *testing.T, env, files map[string]string, args ...string) (*Config, error) {
	t.Helper()
	return load(args,
		func(key string) (string, bool) {
			v, ok := env[strings.TrimPrefix(key, EnvPrefix)]
			return v, ok
		},
		func(path string) ([]byte, error) {
			content, ok := files[path]
			if !ok {
				return nil, &fs.PathError{Op: "open", Path: path, Err: fs.ErrNotExist}
			}
			return []byte(content), nil
		},
	)
}

// problems splits an aggregated Load error back into its individual complaints.
func problems(t *testing.T, err error) []string {
	t.Helper()
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	text := err.Error()
	if !strings.HasPrefix(text, "invalid configuration:\n  - ") {
		t.Fatalf("error is not an aggregated report: %q", text)
	}
	return strings.Split(strings.TrimPrefix(text, "invalid configuration:\n  - "), "\n  - ")
}

// assertProblem finds the single complaint containing every fragment.
func assertProblem(t *testing.T, err error, fragments ...string) {
	t.Helper()
	found := 0
	for _, p := range problems(t, err) {
		matches := true
		for _, f := range fragments {
			if !strings.Contains(p, f) {
				matches = false
				break
			}
		}
		if matches {
			found++
		}
	}
	if found != 1 {
		t.Fatalf("found %d problems matching %v, want 1; got:\n%s", found, fragments, err)
	}
}

func TestLoadWithNothingSetProducesTheDocumentedDefaults(t *testing.T) {
	cfg, err := loadEnv(t, nil, nil)
	if err != nil {
		t.Fatalf("Load = %v, want a valid configuration", err)
	}

	if cfg.Log.Level != slog.LevelInfo || cfg.Log.Format != "json" {
		t.Fatalf("log defaults = %+v", cfg.Log)
	}
	if cfg.HTTP.Addr != "0.0.0.0:9102" || cfg.HTTP.ReadTimeout != 10*time.Second ||
		cfg.HTTP.ShutdownTimeout != 10*time.Second || cfg.HTTP.StaleAfter != 0 {
		t.Fatalf("http defaults = %+v", cfg.HTTP)
	}

	if !cfg.Hamqsl.Enabled || cfg.Hamqsl.URL != "https://www.hamqsl.com/solarxml.php" ||
		cfg.Hamqsl.Interval != time.Hour || cfg.Hamqsl.Timeout != 15*time.Second ||
		cfg.Hamqsl.Retries != 2 {
		t.Fatalf("hamqsl defaults = %+v", cfg.Hamqsl)
	}
	if !cfg.SWPC.Enabled || cfg.SWPC.BaseURL != "https://services.swpc.noaa.gov" ||
		cfg.SWPC.FastInterval != time.Minute || cfg.SWPC.MediumInterval != 5*time.Minute ||
		cfg.SWPC.SlowInterval != time.Hour || cfg.SWPC.DRAPGridStep != 10 {
		t.Fatalf("swpc defaults = %+v", cfg.SWPC)
	}
	// The D-RAP grid produces more series than everything else combined, so it
	// is opt-in even though its source is on by default.
	if cfg.SWPC.DRAPEnabled {
		t.Fatal("the D-RAP grid defaulted to enabled")
	}

	// kc2g is CC BY-NC-SA, so its default must be an informed opt-in.
	if cfg.KC2G.Enabled {
		t.Fatal("kc2g defaulted to enabled despite its non-commercial licence")
	}
	if cfg.KC2G.Interval != 15*time.Minute || cfg.KC2G.MinConfidence != 25 ||
		cfg.KC2G.MaxAge != 3*time.Hour || !cfg.KC2G.EffectiveIndices ||
		cfg.KC2G.Stations != nil {
		t.Fatalf("kc2g defaults = %+v", cfg.KC2G)
	}

	if !cfg.Prometheus.Enabled || cfg.Prometheus.Path != "/metrics" || cfg.Prometheus.Retention != 0 {
		t.Fatalf("prometheus defaults = %+v", cfg.Prometheus)
	}
	if cfg.InfluxV1.Enabled || cfg.InfluxV2.Enabled || cfg.OTLP.Enabled || cfg.MQTT.Enabled {
		t.Fatal("a push sink defaulted to enabled")
	}
	if cfg.InfluxV2.Bucket != "SolarHAM" || cfg.InfluxV2.Measurement != "solar" {
		t.Fatalf("influx2 defaults = %+v", cfg.InfluxV2)
	}
	if cfg.MQTT.ClientID != "solarham-exporter" || cfg.MQTT.Topic != "solarham" ||
		!cfg.MQTT.Retain || !cfg.MQTT.DiscoveryEnabled ||
		cfg.MQTT.DiscoveryPrefix != "homeassistant" || cfg.MQTT.DeviceName != "SolarHAM" {
		t.Fatalf("mqtt defaults = %+v", cfg.MQTT)
	}

	if len(cfg.resolved) == 0 {
		t.Fatal("no settings were recorded for the startup log")
	}
	if cfg.ConfigFile() != "" || len(cfg.Notes()) != 0 {
		t.Fatalf("config file %q and notes %v, want neither", cfg.ConfigFile(), cfg.Notes())
	}
}

func TestEnvironmentOverridesEveryDefault(t *testing.T) {
	cfg, err := loadEnv(t, map[string]string{
		"LOG_LEVEL":              "debug",
		"LOG_FORMAT":             "text",
		"HTTP_ADDR":              "127.0.0.1:8080",
		"HTTP_STALE_AFTER":       "30m",
		"HAMQSL_INTERVAL":        "2h",
		"SWPC_DRAP_ENABLED":      "true",
		"SWPC_DRAP_GRID_STEP":    "5",
		"KC2G_ENABLED":           "true",
		"KC2G_STATIONS":          "EA036,AL945",
		"KC2G_MIN_CONFIDENCE":    "50",
		"PROMETHEUS_PATH":        "/solar",
		"MQTT_ENABLED":           "true",
		"MQTT_BROKER":            "tcp://mosquitto:1883",
		"MQTT_QOS":               "2",
		"MQTT_DISCOVERY_ENABLED": "false",
		"OTLP_ENABLED":           "true",
		"OTLP_ENDPOINT":          "otel:4317",
		"OTLP_PROTOCOL":          "http",
		"OTLP_HEADERS":           "authorization=Bearer abc",
		"OTLP_INTERVAL":          "30s",
	}, nil)
	if err != nil {
		t.Fatalf("Load = %v", err)
	}

	if cfg.Log.Level != slog.LevelDebug || cfg.Log.Format != "text" {
		t.Fatalf("log = %+v", cfg.Log)
	}
	if cfg.HTTP.Addr != "127.0.0.1:8080" || cfg.HTTP.StaleAfter != 30*time.Minute {
		t.Fatalf("http = %+v", cfg.HTTP)
	}
	if cfg.Hamqsl.Interval != 2*time.Hour {
		t.Fatalf("hamqsl interval = %s", cfg.Hamqsl.Interval)
	}
	if !cfg.SWPC.DRAPEnabled || cfg.SWPC.DRAPGridStep != 5 {
		t.Fatalf("swpc = %+v", cfg.SWPC)
	}
	if !cfg.KC2G.Enabled || len(cfg.KC2G.Stations) != 2 || cfg.KC2G.MinConfidence != 50 {
		t.Fatalf("kc2g = %+v", cfg.KC2G)
	}
	if cfg.Prometheus.Path != "/solar" {
		t.Fatalf("prometheus path = %q", cfg.Prometheus.Path)
	}
	if cfg.MQTT.QoS != 2 || cfg.MQTT.DiscoveryEnabled {
		t.Fatalf("mqtt = %+v", cfg.MQTT)
	}
	if cfg.OTLP.Protocol != "http" || cfg.OTLP.Interval != 30*time.Second ||
		cfg.OTLP.Headers["authorization"] != "Bearer abc" {
		t.Fatalf("otlp = %+v", cfg.OTLP)
	}
}

func TestSecretsAreReadFromTheirFileVariant(t *testing.T) {
	cfg, err := loadEnv(t,
		map[string]string{
			"INFLUX2_ENABLED":            "true",
			"INFLUX2_URL":                "http://influx:8086",
			"INFLUX2_ORG":                "home",
			"INFLUX2_TOKEN" + FileSuffix: "/run/secrets/influx-token",
			"MQTT_ENABLED":               "true",
			"MQTT_BROKER":                "tcp://mosquitto:1883",
			"MQTT_PASSWORD" + FileSuffix: "/run/secrets/mqtt",
		},
		map[string]string{
			"/run/secrets/influx-token": "tok3n\n",
			"/run/secrets/mqtt":         "p4ss\n",
		},
	)
	if err != nil {
		t.Fatalf("Load = %v", err)
	}
	if got := cfg.InfluxV2.Token.Reveal(); got != "tok3n" {
		t.Fatalf("token = %q, want tok3n", got)
	}
	if got := cfg.MQTT.Password.Reveal(); got != "p4ss" {
		t.Fatalf("mqtt password = %q, want p4ss", got)
	}
}

func TestSettingBothASecretAndItsFileVariantIsAnError(t *testing.T) {
	_, err := loadEnv(t,
		map[string]string{
			"INFLUX2_ENABLED":            "true",
			"INFLUX2_URL":                "http://influx:8086",
			"INFLUX2_ORG":                "home",
			"INFLUX2_TOKEN":              "from-env",
			"INFLUX2_TOKEN" + FileSuffix: "/run/secrets/influx-token",
		},
		map[string]string{"/run/secrets/influx-token": "from-file"},
	)
	assertProblem(t, err, EnvPrefix+"INFLUX2_TOKEN", "both set", "supply exactly one")
	if text := err.Error(); strings.Contains(text, "from-env") || strings.Contains(text, "from-file") {
		t.Fatalf("the conflict error leaked a credential: %s", text)
	}
}

func TestAMissingSecretFileNamesThePathAndNotItsContents(t *testing.T) {
	_, err := loadEnv(t, map[string]string{
		"INFLUX2_ENABLED":            "true",
		"INFLUX2_URL":                "http://influx:8086",
		"INFLUX2_ORG":                "home",
		"INFLUX2_TOKEN" + FileSuffix: "/run/secrets/absent",
	}, nil)
	assertProblem(t, err, EnvPrefix+"INFLUX2_TOKEN"+FileSuffix, "/run/secrets/absent")
}

// TestEveryProblemIsReportedTogether is the point of accumulating errors: an
// operator fixing a container's environment should not have to restart it six
// times to discover six mistakes.
func TestEveryProblemIsReportedTogether(t *testing.T) {
	_, err := loadEnv(t, map[string]string{
		"LOG_LEVEL":        "verbose",
		"HAMQSL_INTERVAL":  "30s",
		"KC2G_RETRIES":     "99",
		"PROMETHEUS_PATH":  "metrics",
		"INFLUX1_ENABLED":  "true",
		"OTLP_ENABLED":     "true",
		"MQTT_ENABLED":     "true",
		"SWPC_DRAP_ENABLE": "true",
	}, nil)

	got := problems(t, err)
	if len(got) < 6 {
		t.Fatalf("got %d problems, want at least six reported at once:\n%s", len(got), err)
	}

	assertProblem(t, err, EnvPrefix+"LOG_LEVEL", "is not one of")
	assertProblem(t, err, EnvPrefix+"HAMQSL_INTERVAL", "outside the accepted range")
	assertProblem(t, err, EnvPrefix+"KC2G_RETRIES", "outside the accepted range")
	assertProblem(t, err, EnvPrefix+"PROMETHEUS_PATH", "must start with a slash")
	assertProblem(t, err, EnvPrefix+"INFLUX1_URL")
	assertProblem(t, err, EnvPrefix+"INFLUX1_DATABASE")
	assertProblem(t, err, EnvPrefix+"OTLP_ENDPOINT")
	assertProblem(t, err, EnvPrefix+"MQTT_BROKER")

	// Every complaint has to name something the operator can search for.
	for _, p := range got {
		if !strings.Contains(p, EnvPrefix) {
			t.Fatalf("problem %q names no setting", p)
		}
	}
}

// TestLegacyAliasesKeepTheOldContainerWorking covers the whole point of the
// drop-in claim: the predecessor passes two lowercase variables and nothing
// else, and swapping the image must keep writing to the same database.
func TestLegacyAliasesKeepTheOldContainerWorking(t *testing.T) {
	cfg, err := loadEnv(t, map[string]string{
		"url":         "http://influx.example.lan:8086",
		"token":       "legacy-t0ken",
		"INFLUX2_ORG": "home",
	}, nil)
	if err != nil {
		t.Fatalf("Load = %v", err)
	}

	if !cfg.InfluxV2.Enabled {
		t.Fatal("a legacy variable did not imply the InfluxDB 2.x sink")
	}
	if cfg.InfluxV2.URL != "http://influx.example.lan:8086" {
		t.Fatalf("url = %q, want the legacy value", cfg.InfluxV2.URL)
	}
	if cfg.InfluxV2.Token.Reveal() != "legacy-t0ken" {
		t.Fatalf("token = %q, want the legacy value", cfg.InfluxV2.Token.Reveal())
	}
	// The bucket bug the predecessor never recovered from is fixed by a default.
	if cfg.InfluxV2.Bucket != "SolarHAM" {
		t.Fatalf("bucket = %q, want SolarHAM", cfg.InfluxV2.Bucket)
	}

	for _, key := range []string{"INFLUX2_URL", "INFLUX2_TOKEN", "INFLUX2_ENABLED"} {
		if src := recordedSource(t, cfg, EnvPrefix+key); src != SourceLegacyEnv {
			t.Fatalf("%s provenance = %q, want %q", key, src, SourceLegacyEnv)
		}
	}

	notes := strings.Join(cfg.Notes(), "\n")
	for _, want := range []string{"url", "token", EnvPrefix + "INFLUX2_URL", EnvPrefix + "INFLUX2_TOKEN"} {
		if !strings.Contains(notes, want) {
			t.Fatalf("notes %q do not mention %q", notes, want)
		}
	}
	if strings.Contains(notes, "legacy-t0ken") {
		t.Fatalf("a deprecation note leaked the token: %s", notes)
	}
}

func TestSettingALegacyAliasAndItsModernEquivalentIsAnError(t *testing.T) {
	_, err := loadEnv(t, map[string]string{
		"url":             "http://old:8086",
		"INFLUX2_URL":     "http://new:8086",
		"token":           "legacy-t0ken",
		"INFLUX2_ENABLED": "true",
		"INFLUX2_ORG":     "home",
	}, nil)

	assertProblem(t, err, `"url"`, EnvPrefix+"INFLUX2_URL", "both set")
}

func TestSettingTheLegacyTokenAndItsFileVariantIsAnError(t *testing.T) {
	_, err := loadEnv(t,
		map[string]string{
			"token":                      "legacy-t0ken",
			"INFLUX2_TOKEN" + FileSuffix: "/run/secrets/influx-token",
			"INFLUX2_URL":                "http://influx:8086",
			"INFLUX2_ENABLED":            "true",
			"INFLUX2_ORG":                "home",
		},
		map[string]string{"/run/secrets/influx-token": "file-t0ken"},
	)

	assertProblem(t, err, `"token"`, EnvPrefix+"INFLUX2_TOKEN", "both set")
	if text := err.Error(); strings.Contains(text, "legacy-t0ken") || strings.Contains(text, "file-t0ken") {
		t.Fatalf("the conflict error leaked a credential: %s", text)
	}
}

// TestAnExplicitEnableStillBeatsTheImpliedOne keeps the legacy convenience from
// becoming something an operator cannot turn off.
func TestAnExplicitEnableStillBeatsTheImpliedOne(t *testing.T) {
	cfg, err := loadEnv(t, map[string]string{
		"url":             "http://influx:8086",
		"token":           "legacy-t0ken",
		"INFLUX2_ENABLED": "false",
	}, nil)
	if err != nil {
		t.Fatalf("Load = %v", err)
	}
	if cfg.InfluxV2.Enabled {
		t.Fatal("an explicit false was overridden by the implied enable")
	}
}

// TestNoCredentialReachesTheStartupLog greps the entire effective-configuration
// output for a canary. This is the one place the whole configuration is
// printed, so it is the one place a credential could most easily escape.
func TestNoCredentialReachesTheStartupLog(t *testing.T) {
	const canary = "canary-t0ken-do-not-log"

	cfg, err := loadEnv(t, map[string]string{
		"url":               "http://influx:8086",
		"token":             canary,
		"INFLUX2_ORG":       "home",
		"INFLUX1_ENABLED":   "true",
		"INFLUX1_URL":       "http://influx:8086",
		"INFLUX1_DATABASE":  "solar",
		"INFLUX1_PASSWORD":  canary,
		"MQTT_ENABLED":      "true",
		"MQTT_BROKER":       "tcp://mosquitto:1883",
		"MQTT_PASSWORD":     canary,
		"OTLP_ENABLED":      "true",
		"OTLP_ENDPOINT":     "otel:4317",
		"OTLP_HEADERS":      "authorization=Bearer " + canary,
		"KC2G_ENABLED":      "true",
		"SWPC_DRAP_ENABLED": "true",
	}, nil)
	if err != nil {
		t.Fatalf("Load = %v", err)
	}

	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	cfg.LogEffective(log)

	if strings.Contains(buf.String(), canary) {
		t.Fatalf("the effective configuration log leaked a credential:\n%s", buf.String())
	}
	if strings.Contains(strings.Join(cfg.Notes(), "\n"), canary) {
		t.Fatalf("a note leaked a credential: %v", cfg.Notes())
	}
	// The secrets did survive to the caller; the redaction is in the display
	// string, not in the value.
	if cfg.InfluxV2.Token.Reveal() != canary || cfg.MQTT.Password.Reveal() != canary {
		t.Fatal("a secret was lost on the way to the caller")
	}
}

func TestNoErrorTextEverContainsACredential(t *testing.T) {
	const canary = "canary-t0ken-do-not-log"

	_, err := loadEnv(t, map[string]string{
		"INFLUX2_ENABLED": "true",
		"INFLUX2_TOKEN":   canary,
		"MQTT_ENABLED":    "true",
		"MQTT_PASSWORD":   canary,
		"OTLP_ENABLED":    "true",
		"OTLP_HEADERS":    "authorization",
		"LOG_LEVEL":       "verbose",
	}, nil)

	if err == nil {
		t.Fatal("expected this deliberately broken configuration to fail")
	}
	if strings.Contains(err.Error(), canary) {
		t.Fatalf("an error leaked a credential:\n%s", err)
	}
}

func TestOutOfRangeAndUnparseableValuesNameTheirSetting(t *testing.T) {
	cases := []struct {
		name      string
		env       map[string]string
		fragments []string
	}{
		{
			name:      "the hamqsl floor protects a feed whose operator asked for it",
			env:       map[string]string{"HAMQSL_INTERVAL": "1m"},
			fragments: []string{EnvPrefix + "HAMQSL_INTERVAL", "outside the accepted range"},
		},
		{
			name:      "the kc2g floor",
			env:       map[string]string{"KC2G_INTERVAL": "1m"},
			fragments: []string{EnvPrefix + "KC2G_INTERVAL", "outside the accepted range"},
		},
		{
			name:      "an out of range grid step",
			env:       map[string]string{"SWPC_DRAP_GRID_STEP": "0"},
			fragments: []string{EnvPrefix + "SWPC_DRAP_GRID_STEP", "outside the accepted range 1..90"},
		},
		{
			name:      "an out of range confidence",
			env:       map[string]string{"KC2G_MIN_CONFIDENCE": "101"},
			fragments: []string{EnvPrefix + "KC2G_MIN_CONFIDENCE", "outside the accepted range"},
		},
		{
			name:      "an out of range QoS",
			env:       map[string]string{"MQTT_QOS": "3"},
			fragments: []string{EnvPrefix + "MQTT_QOS", "outside the accepted range 0..2"},
		},
		{
			name:      "an unknown log format",
			env:       map[string]string{"LOG_FORMAT": "logfmt"},
			fragments: []string{EnvPrefix + "LOG_FORMAT", "is not one of", "json, text"},
		},
		{
			name:      "an unknown OTLP protocol",
			env:       map[string]string{"OTLP_PROTOCOL": "thrift"},
			fragments: []string{EnvPrefix + "OTLP_PROTOCOL", "is not one of", "grpc, http"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := loadEnv(t, tc.env, nil)
			assertProblem(t, err, tc.fragments...)
		})
	}
}

// TestInfluxV2RequiresExactlyOneOfOrgAndOrgID covers the bug that made the
// predecessor 404 on every single write for the life of the container.
func TestInfluxV2RequiresExactlyOneOfOrgAndOrgID(t *testing.T) {
	base := map[string]string{
		"INFLUX2_ENABLED": "true",
		"INFLUX2_URL":     "http://influx:8086",
		"INFLUX2_TOKEN":   "tok3n",
	}
	with := func(extra map[string]string) map[string]string {
		env := map[string]string{}
		for k, v := range base {
			env[k] = v
		}
		for k, v := range extra {
			env[k] = v
		}
		return env
	}

	_, err := loadEnv(t, base, nil)
	assertProblem(t, err, EnvPrefix+"INFLUX2_ORG", EnvPrefix+"INFLUX2_ORG_ID", "required")

	_, err = loadEnv(t, with(map[string]string{"INFLUX2_ORG": "home", "INFLUX2_ORG_ID": "0123456789abcdef"}), nil)
	assertProblem(t, err, EnvPrefix+"INFLUX2_ORG", EnvPrefix+"INFLUX2_ORG_ID", "supply exactly one")

	for _, extra := range []map[string]string{
		{"INFLUX2_ORG": "home"},
		{"INFLUX2_ORG_ID": "0123456789abcdef"},
	} {
		if _, err := loadEnv(t, with(extra), nil); err != nil {
			t.Fatalf("Load with %v = %v, want a valid configuration", extra, err)
		}
	}
}

func TestASinkAndASourceAreBothRequired(t *testing.T) {
	_, err := loadEnv(t, map[string]string{"PROMETHEUS_ENABLED": "false"}, nil)
	assertProblem(t, err, "no sinks are enabled", EnvPrefix+"PROMETHEUS_ENABLED=true")

	_, err = loadEnv(t, map[string]string{
		"HAMQSL_ENABLED": "false",
		"SWPC_ENABLED":   "false",
	}, nil)
	assertProblem(t, err, "no sources are enabled", EnvPrefix+"SWPC_ENABLED=true")
}

// TestATimeoutMustFitInsideItsOwnInterval catches the configuration that turns
// one slow upstream into a growing backlog of overlapping polls.
func TestATimeoutMustFitInsideItsOwnInterval(t *testing.T) {
	_, err := loadEnv(t, map[string]string{
		"SWPC_FAST_INTERVAL": "1m",
		"SWPC_TIMEOUT":       "1m",
	}, nil)
	assertProblem(t, err, EnvPrefix+"SWPC_TIMEOUT", EnvPrefix+"SWPC_FAST_INTERVAL", "must be shorter")
}

func TestConfigFileSuppliesSettingsAndTheEnvironmentOverridesIt(t *testing.T) {
	const file = `
# Values may be written with or without a section.
[hamqsl]
interval = 3h

[kc2g]
enabled = true
stations = EA036, AL945

[mqtt]
enabled = true
broker = "tcp://mosquitto:1883"

SOLARHAM_PROMETHEUS_PATH = /solar
`

	cfg, err := loadEnv(t,
		map[string]string{"HAMQSL_INTERVAL": "4h"},
		map[string]string{"/etc/solarham-exporter/config.ini": file},
		"--config", "/etc/solarham-exporter/config.ini",
	)
	if err != nil {
		t.Fatalf("Load = %v", err)
	}

	if cfg.ConfigFile() != "/etc/solarham-exporter/config.ini" {
		t.Fatalf("ConfigFile = %q", cfg.ConfigFile())
	}
	if !cfg.KC2G.Enabled || len(cfg.KC2G.Stations) != 2 {
		t.Fatalf("kc2g from the file = %+v", cfg.KC2G)
	}
	if !cfg.MQTT.Enabled || cfg.MQTT.Broker != "tcp://mosquitto:1883" {
		t.Fatalf("mqtt from the file = %+v", cfg.MQTT)
	}
	if cfg.Prometheus.Path != "/solar" {
		t.Fatalf("a fully qualified key in the file did not apply: %q", cfg.Prometheus.Path)
	}

	// The environment wins, so a container can override a mounted file without
	// editing it.
	if cfg.Hamqsl.Interval != 4*time.Hour {
		t.Fatalf("hamqsl interval = %s, want the environment's 4h", cfg.Hamqsl.Interval)
	}
	if src := recordedSource(t, cfg, EnvPrefix+"KC2G_ENABLED"); src != SourceConfigFile {
		t.Fatalf("KC2G_ENABLED provenance = %q, want %q", src, SourceConfigFile)
	}
}

// TestAnExplicitlyNamedConfigFileMustExist keeps a mounted-but-mistyped path
// from starting the exporter on defaults with nobody noticing.
func TestAnExplicitlyNamedConfigFileMustExist(t *testing.T) {
	_, err := loadEnv(t, nil, nil, "--config", "/etc/solarham-exporter/absent.ini")
	if err == nil {
		t.Fatal("a missing explicit config file was ignored")
	}
	if !errors.Is(err, fs.ErrNotExist) || !strings.Contains(err.Error(), "absent.ini") {
		t.Fatalf("error = %v, want one naming the missing file", err)
	}
}

func TestFlagsAreParsedAndAnythingElseIsRejected(t *testing.T) {
	if _, err := loadEnv(t, nil, nil, "--version"); !errors.Is(err, ErrVersionRequested) {
		t.Fatalf("--version = %v, want ErrVersionRequested", err)
	}

	_, err := loadEnv(t, nil, nil, "start")
	if err == nil || !strings.Contains(err.Error(), "unexpected argument") {
		t.Fatalf("a positional argument returned %v, want a hard error", err)
	}

	_, err = loadEnv(t, nil, nil, "--nonsense")
	if err == nil || !strings.Contains(err.Error(), "invalid flags") {
		t.Fatalf("an unknown flag returned %v, want a hard error", err)
	}
}

// recordedSource returns the provenance recorded for one setting.
func recordedSource(t *testing.T, cfg *Config, key string) Source {
	t.Helper()
	for _, r := range cfg.resolved {
		if r.Key == key {
			return r.Source
		}
	}
	t.Fatalf("no provenance was recorded for %s", key)
	return ""
}
