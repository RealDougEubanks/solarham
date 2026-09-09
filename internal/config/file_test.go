package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeTemp writes a config file and returns its path.
func writeTemp(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("writing %s: %v", path, err)
	}
	return path
}

func TestConfigFileParsesSectionsCommentsAndQuotes(t *testing.T) {
	t.Parallel()

	path := writeTemp(t, "config.ini", `
; a semicolon comment
# a hash comment

[hamqsl]
  interval = 3h
  url = "https://www.hamqsl.com/solarxml.php"

[swpc]
  drap_enabled = true

[kc2g]
  enabled = 'true'
  stations = EA036, AL945

[influx2]
  url = http://influx.example.lan:8086
  org_id = 0123456789abcdef

; a key already carrying its section prefix is not prefixed twice
[mqtt]
  MQTT_BROKER = tcp://mosquitto:1883

; a fully qualified key is taken as it stands
SOLARHAM_PROMETHEUS_PATH = /solar
`)

	settings, err := LoadConfigFile(path)
	if err != nil {
		t.Fatalf("LoadConfigFile: %v", err)
	}
	if settings.path != path {
		t.Fatalf("path = %q, want %q", settings.path, path)
	}

	want := map[string]string{
		"HAMQSL_INTERVAL":   "3h",
		"HAMQSL_URL":        "https://www.hamqsl.com/solarxml.php",
		"SWPC_DRAP_ENABLED": "true",
		"KC2G_ENABLED":      "true",
		"KC2G_STATIONS":     "EA036, AL945",
		"INFLUX2_URL":       "http://influx.example.lan:8086",
		"INFLUX2_ORG_ID":    "0123456789abcdef",
		"MQTT_BROKER":       "tcp://mosquitto:1883",
		"PROMETHEUS_PATH":   "/solar",
	}
	for key, value := range want {
		if got := settings.values[key]; got != value {
			t.Fatalf("values[%s] = %q, want %q", key, got, value)
		}
	}
	if len(settings.values) != len(want) {
		t.Fatalf("parsed %d settings, want %d: %v", len(settings.values), len(want), settings.values)
	}
}

// TestConfigFileRejectsALineThatIsNotAPair names the line, because a config
// file that is half-applied is worse than one that is refused.
func TestConfigFileRejectsALineThatIsNotAPair(t *testing.T) {
	t.Parallel()

	path := writeTemp(t, "config.ini", "[hamqsl]\ninterval 3h\n")
	_, err := LoadConfigFile(path)
	if err == nil {
		t.Fatal("a malformed line was accepted")
	}
	if !strings.Contains(err.Error(), "line 2") || !strings.Contains(err.Error(), "key = value") {
		t.Fatalf("error = %v, want one naming the line", err)
	}
}

func TestFindConfigFileReturnsEmptyWhenNoneOfTheDefaultsExist(t *testing.T) {
	t.Parallel()

	// The defaults are absolute container paths, which a developer machine and
	// CI both lack; this asserts the absence is not itself an error.
	path, err := FindConfigFile()
	if err != nil {
		t.Fatalf("FindConfigFile = %v", err)
	}
	if path != "" && path != DefaultConfigPaths[0] && path != DefaultConfigPaths[1] {
		t.Fatalf("FindConfigFile = %q, want one of %v or empty", path, DefaultConfigPaths)
	}
}

func TestDefaultConfigPathsIncludeThePredecessorsMountPoint(t *testing.T) {
	t.Parallel()

	// /config.ini is where the container this replaces mounted its file, so an
	// existing deployment keeps working after swapping the image.
	want := []string{"/etc/solarham-exporter/config.ini", "/config.ini"}
	if len(DefaultConfigPaths) != len(want) {
		t.Fatalf("DefaultConfigPaths = %v, want %v", DefaultConfigPaths, want)
	}
	for i := range want {
		if DefaultConfigPaths[i] != want[i] {
			t.Fatalf("DefaultConfigPaths = %v, want %v", DefaultConfigPaths, want)
		}
	}
}
