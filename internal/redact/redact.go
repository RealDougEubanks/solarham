// Package redact keeps credentials out of logs.
//
// This is not a nicety. Several of the APIs this exporter talks to accept
// credentials only as URL query parameters, so a request URL routinely contains
// a password. Go's net/http returns transport failures as *url.Error, which
// embeds the full URL including its query string. Logging such an error
// directly — or letting one reach an error message that is later logged — is
// enough to write a plaintext password into container logs on every failure.
//
// To be accurate about the risk: over HTTPS the query string is encrypted in
// transit, so this is not about interception. It is about log hygiene. Logs get
// copied into tickets, pasted into chat, and shipped to aggregators.
package redact

import (
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
)

// Placeholder replaces any redacted value.
const Placeholder = "REDACTED"

// Secret is a credential that will not print.
//
// The value is held in a closure rather than in a string field, and that is the
// entire point of the design. Implementing Stringer and Formatter is enough to
// protect a Secret that is formatted directly, but not one reached through the
// struct that holds it: fmt walks unexported fields by reflection and cannot
// call methods on what it finds there, so a plain `type Secret string` inside a
// sink struct prints in the clear the moment anyone logs the whole struct.
//
//	fmt.Sprintf("%v", sink)  ->  {radmon.org doug hunter2}
//
// A func field has no readable contents, so reflection can only print its
// address. That closes the hole centrally, for every struct that holds a
// Secret, instead of relying on each one to remember to define a String method.
//
// The cost is that Secret is no longer comparable and no longer convertible
// from a string, so it must be built with New.
type Secret struct {
	reveal func() string
}

// New wraps a value as a Secret.
func New(value string) Secret {
	if value == "" {
		return Secret{}
	}
	return Secret{reveal: func() string { return value }}
}

// String implements fmt.Stringer.
func (s Secret) String() string { return Placeholder }

// GoString implements fmt.GoStringer, covering %#v.
func (s Secret) GoString() string { return Placeholder }

// Format implements fmt.Formatter so every verb, including %s and %q, is
// redacted rather than only the ones Stringer covers.
func (s Secret) Format(f fmt.State, verb rune) {
	_, _ = f.Write([]byte(Placeholder))
	_ = verb
}

// LogValue implements slog.LogValuer.
func (s Secret) LogValue() slog.Value { return slog.StringValue(Placeholder) }

// MarshalJSON ensures a Secret cannot leak through serialisation.
func (s Secret) MarshalJSON() ([]byte, error) { return []byte(`"` + Placeholder + `"`), nil }

// MarshalText covers encoders that prefer TextMarshaler.
func (s Secret) MarshalText() ([]byte, error) { return []byte(Placeholder), nil }

// Reveal returns the underlying value. Every call site is a deliberate decision
// to expose the secret, and should be handing it to a server rather than to a
// log.
func (s Secret) Reveal() string {
	if s.reveal == nil {
		return ""
	}
	return s.reveal()
}

// IsZero reports whether the secret is empty, without revealing it.
func (s Secret) IsZero() bool { return s.reveal == nil || s.reveal() == "" }

// URL strips the query string and any userinfo from a URL, keeping enough to
// identify which endpoint was involved.
//
// A parse failure returns the placeholder rather than the input, because a URL
// that cannot be parsed is exactly the sort of malformed value most likely to
// carry something sensitive.
func URL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return Placeholder
	}
	return sanitizeURL(u)
}

// sanitizeURL rebuilds a URL without credentials.
func sanitizeURL(u *url.URL) string {
	if u == nil {
		return Placeholder
	}
	clean := &url.URL{
		Scheme: u.Scheme,
		Host:   u.Host,
		Path:   u.Path,
	}
	out := clean.String()
	if u.RawQuery != "" {
		// Keep the parameter names, which are useful when debugging, but
		// never their values.
		out += "?" + redactQuery(u.RawQuery)
	}
	if out == "" {
		return Placeholder
	}
	return out
}

// redactQuery keeps query parameter names and replaces every value.
func redactQuery(rawQuery string) string {
	values, err := url.ParseQuery(rawQuery)
	if err != nil {
		return Placeholder
	}
	keys := make([]string, 0, len(values))
	for k := range values {
		keys = append(keys, k)
	}
	// Sort for stable log lines.
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j] < keys[j-1]; j-- {
			keys[j], keys[j-1] = keys[j-1], keys[j]
		}
	}
	var b strings.Builder
	for i, k := range keys {
		if i > 0 {
			b.WriteByte('&')
		}
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(Placeholder)
	}
	return b.String()
}

// Error returns an error safe to log.
//
// If err is or wraps a *url.Error, the embedded URL is replaced with a
// credential-free form. Any other error is returned unchanged, since it did not
// come from a URL-carrying transport.
//
// Every HTTP client call in this project passes its error through here before
// the error is returned, wrapped, or logged.
func Error(err error) error {
	if err == nil {
		return nil
	}

	var urlErr *url.Error
	if !errors.As(err, &urlErr) {
		return err
	}

	safe := &url.Error{
		Op:  urlErr.Op,
		URL: URL(urlErr.URL),
		Err: urlErr.Err,
	}

	// errors.As found the *url.Error possibly nested inside wrapping. Rebuild
	// the message rather than the chain, so no wrapper can still be holding
	// the original URL in its own formatted text.
	return fmt.Errorf("%s", safe.Error())
}
