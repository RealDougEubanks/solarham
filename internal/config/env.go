package config

import (
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/RealDougEubanks/solarham/internal/redact"
)

// EnvPrefix is prepended to every setting name.
const EnvPrefix = "SOLARHAM_"

// FileSuffix marks the variant of a setting that names a file to read the value
// from, rather than carrying the value itself.
//
// This matters for secrets. A plain environment variable is visible to anyone
// who can run `docker inspect`, and is inherited by every child process. Docker
// and Kubernetes secrets are delivered as files, so SOLARHAM_INFLUX2_TOKEN_FILE
// is the correct way to supply a token to a container.
const FileSuffix = "_FILE"

// loader reads settings from the environment, accumulating errors rather than
// failing on the first one.
//
// Reporting every configuration problem at once is deliberate: an operator
// fixing a container's environment should not have to restart it six times to
// discover six mistakes.
type loader struct {
	errs     []error
	resolved []resolved

	// fileValues holds settings read from a config file. Environment
	// variables take precedence over these, per the documented order of
	// flags, then environment, then file, then defaults.
	fileValues map[string]string

	// legacy holds values recovered from the lowercase environment variables
	// the container this replaces passes. They sit below the modern
	// environment and above the config file, though in practice a conflict
	// with the modern variable is reported as an error rather than resolved
	// by precedence.
	legacy map[string]string

	// lookupEnv is injectable so tests do not have to mutate the real
	// process environment.
	lookupEnv func(string) (string, bool)

	// readFile is injectable for the same reason.
	readFile func(string) ([]byte, error)
}

// newLoader builds a loader over the real process environment and filesystem.
func newLoader() *loader {
	return &loader{
		legacy:    map[string]string{},
		lookupEnv: os.LookupEnv,
		readFile:  os.ReadFile,
	}
}

// raw returns the value for a setting, honouring the _FILE variant.
//
// Precedence is: NAME_FILE, then NAME, then a legacy alias, then the config
// file, then the caller's default. Supplying both NAME and NAME_FILE is an
// error rather than a silent preference, because guessing which one the
// operator meant is how the wrong credential ends up in production.
func (l *loader) raw(name string) (string, Source, bool) {
	key := EnvPrefix + name
	fileKey := key + FileSuffix

	fileVal, hasFile := l.lookupEnv(fileKey)
	envVal, hasEnv := l.lookupEnv(key)

	switch {
	case hasFile && hasEnv:
		l.errf("%s and %s are both set; supply exactly one", key, fileKey)
		return "", SourceDefault, false

	case hasFile:
		path := strings.TrimSpace(fileVal)
		if path == "" {
			l.errf("%s is set but empty", fileKey)
			return "", SourceDefault, false
		}
		content, err := l.readFile(path)
		if err != nil {
			// The path is safe to log; the contents are not.
			l.errf("%s: reading %s: %v", fileKey, path, err)
			return "", SourceDefault, false
		}
		// Trailing newlines are near-universal in secret files and are
		// almost never part of the credential.
		return strings.TrimRight(string(content), "\r\n"), SourceSecretFile, true

	case hasEnv:
		return envVal, SourceEnv, true
	}

	if v, ok := l.legacy[name]; ok {
		return v, SourceLegacyEnv, true
	}

	// The config file is consulted only after the environment, so a container
	// can override a mounted file without editing it.
	if v, ok := l.fileValues[name]; ok {
		return v, SourceConfigFile, true
	}
	return "", SourceDefault, false
}

// record notes a setting's effective value for the startup log.
func (l *loader) record(name string, src Source, display string) {
	l.resolved = append(l.resolved, resolved{Key: EnvPrefix + name, Source: src, Display: display})
}

// String reads a plain string setting.
func (l *loader) String(name, def string) string {
	v, src, ok := l.raw(name)
	if !ok {
		l.record(name, SourceDefault, def)
		return def
	}
	l.record(name, src, v)
	return v
}

// Secret reads a credential. Its value is never recorded for logging.
func (l *loader) Secret(name string) redact.Secret {
	v, src, ok := l.raw(name)
	if !ok {
		l.record(name, SourceDefault, "(unset)")
		return redact.Secret{}
	}
	l.record(name, src, redact.Placeholder)
	return redact.New(v)
}

// Bool reads a boolean setting.
func (l *loader) Bool(name string, def bool) bool {
	v, src, ok := l.raw(name)
	if !ok {
		l.record(name, SourceDefault, strconv.FormatBool(def))
		return def
	}
	parsed, err := strconv.ParseBool(strings.TrimSpace(v))
	if err != nil {
		l.errf("%s%s: %q is not a boolean (use true or false)", EnvPrefix, name, v)
		return def
	}
	l.record(name, src, strconv.FormatBool(parsed))
	return parsed
}

// Int reads an integer setting and enforces an inclusive range.
func (l *loader) Int(name string, def, min, max int) int {
	v, src, ok := l.raw(name)
	if !ok {
		l.record(name, SourceDefault, strconv.Itoa(def))
		return def
	}
	parsed, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil {
		l.errf("%s%s: %q is not a whole number", EnvPrefix, name, v)
		return def
	}
	if parsed < min || parsed > max {
		l.errf("%s%s: %d is outside the accepted range %d..%d", EnvPrefix, name, parsed, min, max)
		return def
	}
	l.record(name, src, strconv.Itoa(parsed))
	return parsed
}

// Float reads a floating point setting and enforces an inclusive range.
func (l *loader) Float(name string, def, min, max float64) float64 {
	v, src, ok := l.raw(name)
	if !ok {
		l.record(name, SourceDefault, strconv.FormatFloat(def, 'g', -1, 64))
		return def
	}
	parsed, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
	if err != nil {
		l.errf("%s%s: %q is not a number", EnvPrefix, name, v)
		return def
	}
	if parsed < min || parsed > max {
		l.errf("%s%s: %v is outside the accepted range %v..%v", EnvPrefix, name, parsed, min, max)
		return def
	}
	l.record(name, src, strconv.FormatFloat(parsed, 'g', -1, 64))
	return parsed
}

// Duration reads a duration setting such as "60s" or "2m".
//
// Several of the intervals here have a floor rather than a free range, because
// the upstream they poll is one person's server and has asked for a cadence.
// The floor is enforced here so the request survives a copy-pasted compose file.
func (l *loader) Duration(name string, def, min, max time.Duration) time.Duration {
	v, src, ok := l.raw(name)
	if !ok {
		l.record(name, SourceDefault, def.String())
		return def
	}
	parsed, err := time.ParseDuration(strings.TrimSpace(v))
	if err != nil {
		l.errf("%s%s: %q is not a duration (try 60s, 2m, 1h)", EnvPrefix, name, v)
		return def
	}
	if parsed < min || parsed > max {
		l.errf("%s%s: %s is outside the accepted range %s..%s", EnvPrefix, name, parsed, min, max)
		return def
	}
	l.record(name, src, parsed.String())
	return parsed
}

// Enum reads a setting constrained to a fixed set of values.
func (l *loader) Enum(name, def string, allowed ...string) string {
	v, src, ok := l.raw(name)
	if !ok {
		l.record(name, SourceDefault, def)
		return def
	}
	got := strings.ToLower(strings.TrimSpace(v))
	for _, a := range allowed {
		if got == a {
			l.record(name, src, got)
			return got
		}
	}
	l.errf("%s%s: %q is not one of %s", EnvPrefix, name, v, strings.Join(allowed, ", "))
	return def
}

// StringSlice reads a comma-separated list, used for station filters.
//
// An empty or unset value yields nil rather than a one-element slice holding
// the empty string, since "no filter" and "filter on nothing" would otherwise
// be indistinguishable and the second one publishes nothing at all.
func (l *loader) StringSlice(name string) []string {
	return l.StringSliceDefault(name, nil)
}

// StringSliceDefault is StringSlice with a built-in list.
//
// Several of the observatory and station networks list more members than anyone
// wants to poll — INTERMAGNET has 154 — so the useful default for those is a
// curated handful rather than "all of them". A nil default keeps the
// no-filter meaning that StringSlice documents.
func (l *loader) StringSliceDefault(name string, def []string) []string {
	fallback := func(src Source) []string {
		if len(def) == 0 {
			l.record(name, src, "(all)")
			return nil
		}
		l.record(name, src, strings.Join(def, ", "))
		return def
	}

	v, src, ok := l.raw(name)
	if !ok {
		return fallback(SourceDefault)
	}
	if strings.TrimSpace(v) == "" {
		// An explicitly empty value is a deliberate "no filter", so it must be
		// able to clear a built-in default rather than being overridden by it.
		l.record(name, src, "(all)")
		return nil
	}

	var out []string
	for _, item := range strings.Split(v, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		out = append(out, item)
	}
	if len(out) == 0 {
		l.record(name, src, "(all)")
		return nil
	}
	l.record(name, src, strings.Join(out, ", "))
	return out
}

// StringMap reads comma-separated key=value pairs, used for OTLP headers,
// GloTEC sample points and KiwiSDR receivers.
//
// Values are not logged: headers commonly carry authorization tokens, and a
// receiver URL can name somebody's home address on a hostname.
func (l *loader) StringMap(name string) map[string]string {
	return l.StringMapDelim(name, ",")
}

// StringMapDelim is StringMap with the pair separator chosen by the caller.
//
// A comma is the right separator almost everywhere, but not where the value
// itself contains one: GloTEC's sample points are "lat,lon" pairs, and
// splitting those on a comma would tear every coordinate in half. Those use a
// semicolon between pairs and keep the comma inside the value, which is the way
// the coordinate is written everywhere else.
func (l *loader) StringMapDelim(name, delim string) map[string]string {
	v, src, ok := l.raw(name)
	if !ok || strings.TrimSpace(v) == "" {
		l.record(name, SourceDefault, "(unset)")
		return nil
	}

	out := map[string]string{}
	var keys []string
	for _, pair := range strings.Split(v, delim) {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		key, value, found := strings.Cut(pair, "=")
		if !found {
			l.errf("%s%s: %q is not in key=value form", EnvPrefix, name, pair)
			return nil
		}
		key = strings.TrimSpace(key)
		if key == "" {
			l.errf("%s%s: empty key in %q", EnvPrefix, name, pair)
			return nil
		}
		out[key] = strings.TrimSpace(value)
		keys = append(keys, key)
	}
	if len(out) == 0 {
		l.record(name, SourceDefault, "(unset)")
		return nil
	}
	l.record(name, src, fmt.Sprintf("%d entries: %s", len(out), strings.Join(keys, ", ")))
	return out
}

// sortedKeys returns a map's keys in a stable order.
//
// Error messages are iterated over maps in a few places here, and a report that
// lists the same three problems in a different order on every restart is
// noticeably harder to work through than one that does not.
func sortedKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

// errf records a configuration problem.
func (l *loader) errf(format string, args ...any) {
	l.errs = append(l.errs, fmt.Errorf(format, args...))
}

// require reports a missing mandatory setting for an enabled sink or source.
func (l *loader) require(name, settingName, value string) {
	if strings.TrimSpace(value) == "" {
		l.errf("%s is enabled but %s%s is not set", name, EnvPrefix, settingName)
	}
}

// requireSecret is require for credentials, without touching the value.
func (l *loader) requireSecret(name, settingName string, value redact.Secret) {
	if value.IsZero() {
		l.errf("%s is enabled but neither %s%s nor %s%s%s is set",
			name, EnvPrefix, settingName, EnvPrefix, settingName, FileSuffix)
	}
}

// errorStrings renders errors for the aggregated message.
func errorStrings(errs []error) []string {
	out := make([]string, 0, len(errs))
	for _, err := range errs {
		out = append(out, err.Error())
	}
	return out
}
