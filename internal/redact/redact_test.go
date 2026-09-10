package redact

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// thePassword is the canary. No test in this file may allow it to appear in
// any output that could reach a log.
const thePassword = "hunter2-SUPER-SECRET"

// TestSecretNeverPrints checks every formatting route a secret could escape by.
func TestSecretNeverPrints(t *testing.T) {
	t.Parallel()

	s := New(thePassword)

	cases := map[string]string{
		"%v":              fmt.Sprintf("%v", s),
		"%s":              fmt.Sprintf("%s", s),
		"%q":              fmt.Sprintf("%q", s),
		"%#v":             fmt.Sprintf("%#v", s),
		"%+v":             fmt.Sprintf("%+v", s),
		"String()":        s.String(),
		"inside struct":   fmt.Sprintf("%v", struct{ P Secret }{s}),
		"inside slice":    fmt.Sprintf("%v", []Secret{s}),
		"inside map":      fmt.Sprintf("%v", map[string]Secret{"p": s}),
		"error wrapping":  fmt.Errorf("login failed for %v", s).Error(),
		"string concat":   "password=" + s.String(),
		"Errorf with %s":  fmt.Errorf("auth: %s", s).Error(),
		"nested pointers": fmt.Sprintf("%v", &struct{ P *Secret }{&s}),
	}

	for name, got := range cases {
		if strings.Contains(got, thePassword) {
			t.Errorf("%s leaked the secret: %q", name, got)
		}
	}

	// JSON is a separate escape route.
	encoded, err := json.Marshal(struct {
		User     string `json:"user"`
		Password Secret `json:"password"`
	}{User: "doug", Password: s})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if bytes.Contains(encoded, []byte(thePassword)) {
		t.Errorf("JSON leaked the secret: %s", encoded)
	}

	// And the value must still be usable when deliberately revealed.
	if s.Reveal() != thePassword {
		t.Error("Reveal did not return the underlying value")
	}
}

// TestSecretSurvivesReflection is the reason Secret holds its value in a
// closure rather than in a string field.
//
// fmt walks unexported struct fields by reflection and cannot call methods on
// what it finds, so Stringer and Formatter protect a Secret formatted directly
// but not one reached through the struct that holds it. With `type Secret
// string` this test fails: formatting a sink prints its password in the clear.
//
// Every sink in this project keeps its credential in an unexported field, so
// this is the case that actually matters. If someone later simplifies Secret
// back to a string type, this test is what stops it reaching production.
func TestSecretSurvivesReflection(t *testing.T) {
	t.Parallel()

	// Deliberately unexported, mirroring how every sink stores its credential.
	type sinkLike struct {
		name     string
		user     string
		password Secret
	}

	s := sinkLike{name: "radmon.org", user: "doug", password: New(thePassword)}

	for _, format := range []string{"%v", "%+v", "%#v", "%s", "%q"} {
		got := fmt.Sprintf(format, s)
		if strings.Contains(got, thePassword) {
			t.Errorf("formatting the containing struct with %s leaked the secret: %s", format, got)
		}
	}

	// Pointers and nesting are the same hazard one level down.
	nested := struct{ Inner *sinkLike }{Inner: &s}
	if got := fmt.Sprintf("%+v", nested); strings.Contains(got, thePassword) {
		t.Errorf("nested struct leaked the secret: %s", got)
	}

	// A zero Secret must be safe to use rather than panicking on a nil func.
	var zero Secret
	if !zero.IsZero() {
		t.Error("a zero Secret should report itself empty")
	}
	if zero.Reveal() != "" {
		t.Error("a zero Secret should reveal an empty string")
	}
	if got := fmt.Sprintf("%v", zero); got != Placeholder {
		t.Errorf("a zero Secret formatted as %q, want %q", got, Placeholder)
	}
}

// TestSecretNeverReachesStructuredLogs checks slog specifically, since that is
// what this project logs with.
func TestSecretNeverReachesStructuredLogs(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	s := New(thePassword)
	log.Info("configured", "password", s)
	log.Info("configured group", slog.Group("radmon", "user", "doug", "password", s))
	log.Error("request failed", "error", fmt.Errorf("bad credentials for %v", s))

	if strings.Contains(buf.String(), thePassword) {
		t.Errorf("slog leaked the secret:\n%s", buf.String())
	}
	if !strings.Contains(buf.String(), Placeholder) {
		t.Errorf("expected the placeholder to appear:\n%s", buf.String())
	}
}

// TestURLRedaction checks that query values are removed while the endpoint
// stays identifiable.
func TestURLRedaction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		in          string
		mustContain []string
		mustNotHave []string
	}{
		{
			name:        "radmon submit with password in query",
			in:          "https://radmon.org/radmon.php?function=submit&user=doug&password=" + thePassword + "&value=28&unit=CPM",
			mustContain: []string{"radmon.org", "/radmon.php", "password=", "user="},
			mustNotHave: []string{thePassword, "doug", "28"},
		},
		{
			name:        "userinfo in authority",
			in:          "https://doug:" + thePassword + "@influx.example.com:8086/write?db=radiation",
			mustContain: []string{"influx.example.com:8086", "/write"},
			mustNotHave: []string{thePassword, "doug"},
		},
		{
			name:        "no query string",
			in:          "https://api.safecast.org/measurements.json",
			mustContain: []string{"api.safecast.org", "/measurements.json"},
			mustNotHave: []string{"?"},
		},
		{
			name:        "unparseable input yields the placeholder",
			in:          "://not a url\x7f?password=" + thePassword,
			mustContain: []string{Placeholder},
			mustNotHave: []string{thePassword},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := URL(tc.in)
			for _, want := range tc.mustContain {
				if !strings.Contains(got, want) {
					t.Errorf("URL(%q) = %q, expected it to contain %q", tc.in, got, want)
				}
			}
			for _, bad := range tc.mustNotHave {
				if strings.Contains(got, bad) {
					t.Errorf("URL(%q) = %q, leaked %q", tc.in, got, bad)
				}
			}
		})
	}
}

// TestErrorRedactsTransportFailures is the case that matters most: a real
// http.Client failure against a URL carrying a password.
func TestErrorRedactsTransportFailures(t *testing.T) {
	t.Parallel()

	// A server that is closed immediately, so the request genuinely fails at
	// the transport layer and produces a real *url.Error.
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	endpoint := srv.URL + "/radmon.php?function=submit&user=doug&password=" + thePassword
	srv.Close()

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, endpoint, nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	_, err = http.DefaultClient.Do(req) //nolint:bodyclose // the request always fails
	if err == nil {
		t.Fatal("expected the request to fail against a closed server")
	}

	// Confirm the premise: the raw error really does carry the password.
	if !strings.Contains(err.Error(), thePassword) {
		t.Fatalf("premise failed, raw error did not contain the password: %v", err)
	}

	safe := Error(err)
	if strings.Contains(safe.Error(), thePassword) {
		t.Errorf("redacted error still leaked the password: %v", safe)
	}
	if !strings.Contains(safe.Error(), "Get") {
		t.Errorf("expected the operation to survive redaction: %v", safe)
	}
}

// TestErrorRedactsWrappedTransportFailures covers the realistic case where the
// transport error has already been wrapped with context before redaction.
func TestErrorRedactsWrappedTransportFailures(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	endpoint := srv.URL + "/submit?password=" + thePassword
	srv.Close()

	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, endpoint, nil)
	_, rawErr := http.DefaultClient.Do(req) //nolint:bodyclose // the request always fails
	if rawErr == nil {
		t.Fatal("expected a transport failure")
	}

	wrapped := fmt.Errorf("publishing to radmon: %w", rawErr)
	if safe := Error(wrapped); strings.Contains(safe.Error(), thePassword) {
		t.Errorf("wrapped error leaked the password: %v", safe)
	}
}

// TestErrorPassesThroughNonURLErrors confirms redaction does not destroy
// unrelated diagnostics.
func TestErrorPassesThroughNonURLErrors(t *testing.T) {
	t.Parallel()

	original := errors.New("connection reset by peer")
	if got := Error(original); !errors.Is(got, original) {
		t.Errorf("Error() = %v, expected the original error to be preserved", got)
	}
	if Error(nil) != nil {
		t.Error("Error(nil) should be nil")
	}
}
