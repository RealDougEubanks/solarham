package config

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"strings"
)

// DefaultConfigPaths are searched when no config file is named explicitly.
//
// /config.ini is included because that is where the container this replaces
// mounted its file, so an existing deployment keeps working after swapping the
// image rather than starting up with nothing but defaults.
var DefaultConfigPaths = []string{
	"/etc/solarham-exporter/config.ini",
	"/config.ini",
}

// fileSettings is a parsed config file: setting names without the SOLARHAM_
// prefix, mapped to their values.
type fileSettings struct {
	// values maps a setting name to its value.
	values map[string]string
	// path is the file the values came from, for logging.
	path string
}

// LoadConfigFile reads an INI file into settings.
//
// The schema is this exporter's own setting names, so a file is a direct
// transcription of the environment variables. Sections are a convenience only:
// they supply the prefix a key does not already carry, which means these two
// describe the same setting.
//
//	[kc2g]
//	enabled = true
//
//	KC2G_ENABLED = true
func LoadConfigFile(path string) (*fileSettings, error) {
	return loadConfigFile(path, os.ReadFile)
}

// loadConfigFile is LoadConfigFile with an injectable reader, so tests never
// have to touch the real filesystem.
func loadConfigFile(path string, readFile func(string) ([]byte, error)) (*fileSettings, error) {
	data, err := readFile(path)
	if err != nil {
		// Returned unwrapped enough for errors.Is(err, fs.ErrNotExist) to keep
		// working, since a missing default-location file is not a failure.
		return nil, fmt.Errorf("reading config file %s: %w", path, err)
	}
	return parseConfigFile(path, data)
}

// parseConfigFile turns INI text into settings.
func parseConfigFile(path string, data []byte) (*fileSettings, error) {
	raw := map[string]map[string]string{}
	section := ""

	scanner := bufio.NewScanner(bytes.NewReader(data))
	line := 0
	for scanner.Scan() {
		line++
		text := strings.TrimSpace(scanner.Text())
		if text == "" || strings.HasPrefix(text, "#") || strings.HasPrefix(text, ";") {
			continue
		}

		if strings.HasPrefix(text, "[") && strings.HasSuffix(text, "]") {
			section = strings.ToLower(strings.TrimSpace(text[1 : len(text)-1]))
			continue
		}

		key, value, found := strings.Cut(text, "=")
		if !found {
			return nil, fmt.Errorf("%s line %d: %q is not a key = value pair", path, line, text)
		}
		key = strings.ToLower(strings.TrimSpace(key))
		value = strings.TrimSpace(value)

		// Values are sometimes quoted in hand-written files.
		if len(value) >= 2 && (value[0] == '"' && value[len(value)-1] == '"' ||
			value[0] == '\'' && value[len(value)-1] == '\'') {
			value = value[1 : len(value)-1]
		}

		if raw[section] == nil {
			raw[section] = map[string]string{}
		}
		raw[section][key] = value
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("reading config file %s: %w", path, err)
	}

	return &fileSettings{values: qualify(raw), path: path}, nil
}

// qualify flattens sections onto fully qualified setting names.
func qualify(raw map[string]map[string]string) map[string]string {
	out := map[string]string{}
	for section, kv := range raw {
		prefix := strings.ToUpper(strings.ReplaceAll(section, ".", "_"))
		for key, value := range kv {
			name := strings.ToUpper(key)

			// A key written with the full SOLARHAM_ prefix is already fully
			// qualified. Prefixing it with the enclosing section as well would
			// silently rename it to something that does not exist.
			if strings.HasPrefix(name, EnvPrefix) {
				out[strings.TrimPrefix(name, EnvPrefix)] = value
				continue
			}

			if prefix != "" && !strings.HasPrefix(name, prefix+"_") {
				name = prefix + "_" + name
			}
			out[name] = value
		}
	}
	return out
}

// FindConfigFile returns the first of DefaultConfigPaths that exists, or an
// empty string. A path that exists but cannot be examined is reported rather
// than skipped, since silently ignoring a config file the operator mounted is
// how a deployment ends up running on defaults without anyone noticing.
func FindConfigFile() (string, error) {
	for _, path := range DefaultConfigPaths {
		info, err := os.Stat(path)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return "", fmt.Errorf("checking for config file %s: %w", path, err)
		}
		if info.IsDir() {
			continue
		}
		return path, nil
	}
	return "", nil
}
