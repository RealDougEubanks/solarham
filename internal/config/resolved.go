package config

import "log/slog"

// Source names where a setting's value came from.
type Source string

const (
	SourceDefault    Source = "default"
	SourceEnv        Source = "env"
	SourceSecretFile Source = "secret-file"
	SourceConfigFile Source = "config-file"
	SourceFlag       Source = "flag"
	SourceLegacyEnv  Source = "legacy-env"
)

// resolved records one setting's provenance, so an operator can find out why a
// value is what they are seeing rather than guessing which of four layers won.
type resolved struct {
	Key     string
	Source  Source
	Display string
}

// LogEffective writes the resolved configuration to the log: a summary at info
// level and the full listing at debug.
//
// Secrets appear as their redacted placeholder. This is the one place the whole
// configuration is printed, so it is the one place a credential could most
// easily escape, which is why the display string is built during loading rather
// than reflected off the struct here.
func (c *Config) LogEffective(log *slog.Logger) {
	if log == nil {
		log = slog.Default()
	}
	if c.configFile != "" {
		log.Info("configuration loaded", "settings", len(c.resolved), "file", c.configFile)
	} else {
		log.Info("configuration loaded", "settings", len(c.resolved))
	}
	for _, r := range c.resolved {
		log.Debug("setting", "key", r.Key, "value", r.Display, "source", string(r.Source))
	}
}
