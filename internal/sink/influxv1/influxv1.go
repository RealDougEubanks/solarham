// Package influxv1 publishes batches to InfluxDB 1.x as line protocol.
//
// The 1.x write API is one endpoint that accepts line protocol as a plain
// request body and answers with a status code, so this sink speaks to it with
// net/http directly instead of pulling in a client library. The exporter ships
// in an image measured in tens of megabytes, and a dependency that exists to
// concatenate strings and POST them does not earn its size.
//
// InfluxDB 1.x also accepts its credentials as u= and p= query parameters, and
// this sink deliberately does not use them: see New. Every error leaving this
// package still passes through redact.Error, because the database and retention
// policy travel in the query string and Go embeds the whole request URL in the
// *url.Error it returns from a transport failure.
package influxv1

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/RealDougEubanks/solarham/internal/config"
	"github.com/RealDougEubanks/solarham/internal/metric"
	"github.com/RealDougEubanks/solarham/internal/redact"
	"github.com/RealDougEubanks/solarham/internal/sink"
)

// Name is the stable identifier this sink reports, which also becomes a metric
// label.
const Name = "influxv1"

// defaultMeasurement is used when the operator names none. Every metric goes
// into one measurement, distinguished by the metric tag, because the metric
// vocabulary is already the schema and duplicating it as measurement names
// would make cross-metric queries a union.
const defaultMeasurement = "solar"

// maxBodySnippet bounds how much of a failure response is kept. Influx explains
// a rejected write in its body, and that explanation is the difference between
// a diagnosable failure and a bare status code, but an unbounded body from a
// misdirected request could be a whole HTML page.
const maxBodySnippet = 512

// defaultBackoff is the pause before the first retry, doubling from there.
const defaultBackoff = 500 * time.Millisecond

// maxBackoff caps the exponential growth so a high retry count cannot outlive
// the poll interval.
const maxBackoff = 10 * time.Second

// defaultTimeout bounds a single write when configuration names no timeout.
const defaultTimeout = 15 * time.Second

// Sink writes batches to a single InfluxDB 1.x database.
type Sink struct {
	measurement string
	attempts    int
	backoff     time.Duration

	// target names the database and retention policy actually addressed. It is
	// credential-free by construction and exists so a 404 can say which names
	// were used rather than leaving an operator to guess.
	target string

	writeURL string
	// safeURL is the endpoint with every query value replaced, and is what logs
	// and error messages are allowed to name.
	safeURL string
	// authHeader is the fully formed Basic header value, empty when the server
	// is unauthenticated. It is built once so the password is revealed in
	// exactly one place.
	authHeader string

	client *http.Client
	log    *slog.Logger
}

// A sink must satisfy the interface the fan-out publishes through.
var _ sink.Sink = (*Sink)(nil)

// New builds a sink from validated configuration.
//
// Credentials go in an Authorization header rather than in the u= and p= query
// parameters InfluxDB 1.x also accepts. Both are equally encrypted in transit
// over HTTPS, so this is not about interception: it is that a password in a
// query string is written verbatim into the server's access log, into any proxy
// in front of it, and into the *url.Error Go returns from a transport failure —
// which this sink logs on every retry. Basic auth keeps the password out of all
// three. redact.Error still guards the error path as defence in depth.
func New(cfg config.InfluxV1, log *slog.Logger) (*Sink, error) {
	if log == nil {
		log = slog.Default()
	}

	raw := strings.TrimSpace(cfg.URL)
	if raw == "" {
		return nil, fmt.Errorf("%s: URL is required", Name)
	}
	u, err := url.Parse(raw)
	if err != nil {
		// The parse error quotes the input, and an unparseable URL is exactly
		// the kind that might carry credentials, so it is not repeated here.
		return nil, fmt.Errorf("%s: URL is not a valid URL", Name)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("%s: URL scheme must be http or https, got %q", Name, u.Scheme)
	}

	database := strings.TrimSpace(cfg.Database)
	if database == "" {
		return nil, fmt.Errorf("%s: database is required", Name)
	}

	measurement := strings.TrimSpace(cfg.Measurement)
	if measurement == "" {
		measurement = defaultMeasurement
	}
	if strings.ContainsAny(measurement, "\n\r") {
		return nil, fmt.Errorf("%s: measurement must not contain a newline", Name)
	}

	q := url.Values{}
	q.Set("db", database)
	// Nanosecond precision matches the timestamps this exporter emits; sending
	// a nanosecond timestamp while Influx assumes its default would misplace
	// every point by decades.
	q.Set("precision", "ns")

	target := "db=" + database
	// An absent rp= means the database's default retention policy, which is
	// what almost every installation wants. Sending an empty one instead names
	// a policy called "", which does not exist.
	rp := strings.TrimSpace(cfg.RetentionPolicy)
	if rp != "" {
		q.Set("rp", rp)
		target += ", rp=" + rp
	}

	endpoint := *u
	endpoint.Path = path.Join(u.Path, "write")
	endpoint.RawQuery = q.Encode()

	// Many home installations run InfluxDB unauthenticated. Sending an empty
	// Basic header to such a server is not harmless, so it is omitted rather
	// than sent blank.
	authHeader := ""
	if cfg.Username != "" || !cfg.Password.IsZero() {
		authHeader = "Basic " + base64.StdEncoding.EncodeToString(
			[]byte(cfg.Username+":"+cfg.Password.Reveal()))
	}

	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = defaultTimeout
	}

	return &Sink{
		measurement: measurement,
		// Retries counts attempts after the first, so a configured zero still
		// makes one attempt.
		attempts:   cfg.Retries + 1,
		backoff:    defaultBackoff,
		target:     target,
		writeURL:   endpoint.String(),
		safeURL:    redact.URL(endpoint.String()),
		authHeader: authHeader,
		client:     &http.Client{Timeout: timeout},
		log:        log,
	}, nil
}

// Name identifies the sink in logs and metrics.
func (s *Sink) Name() string { return Name }

// Publish writes every sample in the batch as one POST.
//
// One request per batch rather than one per sample: a batch is what the source
// observed in a single poll, the server accepts an arbitrary number of lines in
// one body, and a per-sample request would multiply a 30-sample poll into 30
// round trips for no gain.
func (s *Sink) Publish(ctx context.Context, b metric.Batch) error {
	if b.Len() == 0 {
		return fmt.Errorf("%w: batch has no samples", sink.ErrSkipped)
	}

	body, skipped := s.encode(b)
	if skipped > 0 {
		s.log.Warn("skipped samples that cannot be represented in line protocol",
			"sink", Name, "source", b.Source, "skipped", skipped, "samples", b.Len())
	}
	if body == "" {
		return fmt.Errorf("%w: no writable samples in batch", sink.ErrSkipped)
	}

	return s.write(ctx, body)
}

// Close releases pooled connections. It is safe on a sink that never published.
func (s *Sink) Close() error {
	s.client.CloseIdleConnections()
	return nil
}

// write posts the body, retrying failures that could plausibly succeed later.
func (s *Sink) write(ctx context.Context, body string) error {
	backoff := s.backoff
	var lastErr error

	for attempt := 1; attempt <= s.attempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("%s: write to %s abandoned: %w", Name, s.safeURL, err)
		}

		retryable, retryAfter, err := s.attempt(ctx, body)
		if err == nil {
			return nil
		}
		lastErr = err

		if !retryable || attempt == s.attempts {
			break
		}

		wait := backoff
		// A server that states how long to wait knows better than a doubling
		// constant does, so Retry-After overrides the backoff outright.
		if retryAfter > 0 {
			wait = retryAfter
		}

		s.log.Warn("influxdb 1.x write failed, retrying",
			"sink", Name, "url", s.safeURL, "attempt", attempt,
			"backoff", wait, "error", err)

		select {
		case <-ctx.Done():
			return fmt.Errorf("%s: write to %s abandoned: %w: %w",
				Name, s.safeURL, ctx.Err(), lastErr)
		case <-time.After(wait):
		}
		if backoff *= 2; backoff > maxBackoff {
			backoff = maxBackoff
		}
	}

	return fmt.Errorf("%s: write to %s failed: %w", Name, s.safeURL, lastErr)
}

// attempt performs one write and reports whether the failure is worth
// repeating, and how long the server asked us to wait before doing so.
func (s *Sink) attempt(ctx context.Context, body string) (retryable bool, retryAfter time.Duration, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.writeURL, strings.NewReader(body))
	if err != nil {
		return false, 0, redact.Error(err)
	}
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	if s.authHeader != "" {
		req.Header.Set("Authorization", s.authHeader)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		// This error embeds the request URL until it has been through
		// redact.Error.
		return true, 0, redact.Error(err)
	}
	defer func() { _ = resp.Body.Close() }()

	snippet := readSnippet(resp.Body)

	// Influx answers a successful write with 204 No Content, but any 2xx means
	// the points were accepted.
	if resp.StatusCode/100 == 2 {
		return false, 0, nil
	}

	return s.classify(resp, snippet)
}

// classify turns a non-2xx response into an error, a retry decision and, where
// the server supplied one, a delay.
//
// The rules are about whether repeating the identical request could change the
// answer. Malformed line protocol will never parse, a rejected password will
// never be accepted, and a nonexistent database will not appear; repeating any
// of those just repeats the load. Throttling and server errors are transient by
// nature.
func (s *Sink) classify(resp *http.Response, snippet string) (retryable bool, retryAfter time.Duration, err error) {
	switch resp.StatusCode {
	case http.StatusBadRequest:
		// Influx names the offending line and column, which is the only way to
		// find a bad point in a batch of thirty, so it is logged as well as
		// returned.
		s.log.Error("influxdb 1.x rejected the batch as malformed line protocol",
			"sink", Name, "url", s.safeURL, "target", s.target, "response", snippet)
		return false, 0, fmt.Errorf("unexpected status %s (malformed line protocol, not retried): %s",
			resp.Status, snippet)

	case http.StatusUnauthorized, http.StatusForbidden:
		s.log.Error("influxdb 1.x rejected the credentials",
			"sink", Name, "url", s.safeURL, "target", s.target, "response", snippet)
		return false, 0, fmt.Errorf("unexpected status %s (credentials rejected for %s, not retried): %s",
			resp.Status, s.target, snippet)

	case http.StatusNotFound:
		// This is the failure that motivated the rewrite, in its 1.x form. The
		// predecessor script wrote to an InfluxDB 2.x org that did not exist
		// and a bucket whose name was wrongly cased, so every write 404'd for
		// the life of the container — and because the client batched
		// asynchronously and nothing inspected the result, the process stayed
		// "healthy" while dropping every point. The same shape of mistake here
		// is a misnamed database or retention policy, so a 404 must be loud and
		// must name the database and policy actually used.
		s.log.Error("influxdb 1.x write target does not exist; every write will fail until this is corrected",
			"sink", Name, "url", s.safeURL, "target", s.target, "response", snippet)
		return false, 0, fmt.Errorf("unexpected status %s: write target not found (%s), not retried: %s",
			resp.Status, s.target, snippet)

	case http.StatusRequestEntityTooLarge:
		s.log.Error("influxdb 1.x rejected the batch as too large",
			"sink", Name, "url", s.safeURL, "target", s.target, "response", snippet)
		return false, 0, fmt.Errorf("unexpected status %s (batch too large, not retried): %s",
			resp.Status, snippet)

	case http.StatusTooManyRequests, http.StatusServiceUnavailable:
		return true, parseRetryAfter(resp.Header.Get("Retry-After"), time.Now()),
			fmt.Errorf("unexpected status %s: %s", resp.Status, snippet)
	}

	if resp.StatusCode >= 500 {
		return true, 0, fmt.Errorf("unexpected status %s: %s", resp.Status, snippet)
	}

	// Any other 4xx is a client-side problem that will not fix itself.
	return false, 0, fmt.Errorf("unexpected status %s: %s", resp.Status, snippet)
}

// parseRetryAfter reads a Retry-After header, which is either a count of
// seconds or an HTTP date. A value that is absent, unparseable or in the past
// yields zero, meaning "use the ordinary backoff".
func parseRetryAfter(value string, now time.Time) time.Duration {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	if secs, err := strconv.Atoi(value); err == nil {
		if secs <= 0 {
			return 0
		}
		return capDelay(time.Duration(secs) * time.Second)
	}
	if when, err := http.ParseTime(value); err == nil {
		if d := when.Sub(now); d > 0 {
			return capDelay(d)
		}
	}
	return 0
}

// capDelay bounds a server-supplied delay. A Retry-After of an hour would
// otherwise hold a publish open far past the next poll, which is worse than
// giving up and letting that next poll try.
func capDelay(d time.Duration) time.Duration {
	if d > maxBackoff {
		return maxBackoff
	}
	return d
}

// readSnippet reads a bounded prefix of a response body and drains the rest so
// the connection can be reused.
func readSnippet(body io.Reader) string {
	buf, err := io.ReadAll(io.LimitReader(body, maxBodySnippet+1))
	if err != nil && len(buf) == 0 {
		return "<unreadable body>"
	}
	_, _ = io.Copy(io.Discard, body)

	if len(buf) > maxBodySnippet {
		return strings.TrimSpace(string(buf[:maxBodySnippet])) + "..."
	}
	snippet := strings.TrimSpace(string(buf))
	if snippet == "" {
		return "<empty body>"
	}
	return snippet
}

// encode renders the batch as line protocol, one line per sample, and reports
// how many samples were dropped as unrepresentable.
func (s *Sink) encode(b metric.Batch) (body string, skipped int) {
	var out strings.Builder

	for i := range b.Samples {
		line, ok := encodePoint(s.measurement, b.Source, b.Samples[i])
		if !ok {
			skipped++
			continue
		}
		if out.Len() > 0 {
			out.WriteByte('\n')
		}
		out.WriteString(line)
	}

	return out.String(), skipped
}

// encodePoint renders one sample, reporting false if the sample cannot be
// written.
//
// A sample is dropped rather than mangled. One unwritable point makes the whole
// batch fail with a 400, so a value that cannot be represented — a NaN, an
// empty tag value, an embedded newline — must not reach the wire.
func encodePoint(measurement, source string, s metric.Sample) (string, bool) {
	if s.Desc == nil {
		return "", false
	}
	// NaN and infinity have no line protocol representation. Influx rejects
	// the entire request when one appears, so a single divide-by-zero upstream
	// would otherwise discard every other sample in the poll.
	if math.IsNaN(s.Value) || math.IsInf(s.Value, 0) {
		return "", false
	}

	tags, ok := pointTags(source, s)
	if !ok {
		return "", false
	}

	var b strings.Builder
	b.WriteString(escapeMeasurement(measurement))
	for _, t := range tags {
		b.WriteByte(',')
		b.WriteString(escapeTag(t.key))
		b.WriteByte('=')
		b.WriteString(escapeTag(t.value))
	}

	b.WriteByte(' ')
	b.WriteString("value=")
	b.WriteString(strconv.FormatFloat(s.Value, 'f', -1, 64))

	// A KindInfo sample carries its meaning in a label and always has the value
	// 1, which is the only shape Prometheus can express. InfluxDB stores text,
	// so the category is written as a string field too: without it a query
	// against this measurement returns a column of ones. The category is the
	// descriptor's last label by convention — the info descriptors in package
	// metric append the categorical label after the identifying ones, so
	// xray_class_info ends in "class" and band_condition_info in "condition".
	// It is written under the field key "category" rather than under the label
	// name, because a tag and a field sharing a key is legal but ambiguous to
	// query.
	if s.Desc.Kind == metric.KindInfo && len(s.Desc.Labels) > 0 {
		if category, ok := categoryOf(s); ok {
			b.WriteString(",category=")
			b.WriteString(quoteStringField(category))
		}
	}

	// A zero timestamp means nothing recorded when the observation was made.
	// Omitting it lets the server stamp arrival time, which is approximately
	// right, whereas the zero time is year 1 and any retention policy would
	// drop it silently.
	if !s.Time.IsZero() {
		b.WriteByte(' ')
		b.WriteString(strconv.FormatInt(s.Time.UnixNano(), 10))
	}

	return b.String(), true
}

// categoryOf returns the categorical value of a KindInfo sample: the value of
// the descriptor's last label.
func categoryOf(s metric.Sample) (string, bool) {
	last := len(s.Desc.Labels) - 1
	if last < 0 || last >= len(s.Labels) {
		return "", false
	}
	v := s.Labels[last]
	if v == "" || containsNewline(v) {
		return "", false
	}
	return v, true
}

// tag is one tag key and value, before escaping.
type tag struct{ key, value string }

// pointTags builds the tag set: the descriptor's labels, plus the metric name
// and the source that produced the batch.
//
// The metric name is a tag rather than the measurement so that every metric
// lands in one measurement and can be queried together; source records which
// upstream observed it, which matters when two sources publish overlapping
// quantities.
//
// Tags are sorted by key, which is what InfluxDB's own storage engine wants and
// which also makes the encoding deterministic regardless of label order.
//
// An empty tag value is not the same as an absent tag: InfluxDB drops it
// silently, so the series quietly loses a dimension and nothing reports why.
// The whole sample is dropped instead.
func pointTags(source string, s metric.Sample) ([]tag, bool) {
	tags := make([]tag, 0, len(s.Desc.Labels)+2)

	if len(s.Labels) != len(s.Desc.Labels) {
		return nil, false
	}
	for i, name := range s.Desc.Labels {
		value := s.Labels[i]
		if name == "" || value == "" {
			return nil, false
		}
		if containsNewline(name) || containsNewline(value) {
			return nil, false
		}
		tags = append(tags, tag{key: name, value: value})
	}

	// Desc.Name already excludes the solar_ prefix; the prefix is a Prometheus
	// naming convention and carries no information here.
	if s.Desc.Name == "" || containsNewline(s.Desc.Name) {
		return nil, false
	}
	tags = append(tags, tag{key: "metric", value: s.Desc.Name})

	// A batch with no source still describes real observations, so the tag is
	// omitted rather than the sample dropped.
	if source != "" && !containsNewline(source) {
		tags = append(tags, tag{key: "source", value: source})
	}

	sort.Slice(tags, func(i, j int) bool { return tags[i].key < tags[j].key })
	return tags, true
}

// containsNewline reports whether a value would break the one-point-per-line
// framing. Such a value is refused rather than escaped, because a label with a
// newline in it is upstream corruption rather than a legitimate value.
func containsNewline(s string) bool { return strings.ContainsAny(s, "\n\r") }

// Line protocol delimits with unescaped commas, spaces and equals signs, so an
// unescaped one in a name silently reshapes the point: a comma in a tag value
// turns the remainder into another tag, and a space ends the tag set early.
// This is a correctness problem with real data, not a theoretical one — kc2g
// station names look like "Austin, TX, USA".
//
// These replacers are single-pass, so an already-escaped backslash is not
// doubled a second time.
var (
	// measurementEscaper covers a measurement name, which is ended by a comma
	// or a space but may contain an equals sign.
	measurementEscaper = strings.NewReplacer(
		`\`, `\\`,
		`,`, `\,`,
		` `, `\ `,
		"\n", `\n`,
	)

	// tagEscaper covers tag keys, tag values and field keys, which share one
	// escape set.
	tagEscaper = strings.NewReplacer(
		`\`, `\\`,
		`,`, `\,`,
		`=`, `\=`,
		` `, `\ `,
		"\n", `\n`,
	)

	// stringFieldEscaper covers the inside of a quoted string field value,
	// where only the closing quote and the escape character itself are special.
	stringFieldEscaper = strings.NewReplacer(
		`\`, `\\`,
		`"`, `\"`,
		"\n", `\n`,
	)
)

// escapeMeasurement escapes a measurement name.
func escapeMeasurement(s string) string { return measurementEscaper.Replace(s) }

// escapeTag escapes a tag key, tag value or field key.
func escapeTag(s string) string { return tagEscaper.Replace(s) }

// escapeStringField escapes the contents of a string field value, without the
// surrounding quotes.
func escapeStringField(s string) string { return stringFieldEscaper.Replace(s) }

// quoteStringField renders a string field value ready to place after an equals
// sign.
func quoteStringField(s string) string { return `"` + escapeStringField(s) + `"` }
