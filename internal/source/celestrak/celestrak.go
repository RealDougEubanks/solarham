// Package celestrak publishes the freshness of an orbital element-set
// catalogue, from celestrak.org's GP query endpoint.
//
// It fetches one document:
//
//	/NORAD/elements/gp.php?GROUP=amateur&FORMAT=json
//
// which is about 40 KB of OMM records — OBJECT_NAME, NORAD_CAT_ID, EPOCH,
// MEAN_MOTION and the rest — for the amateur-satellite group. Two numbers are
// published from it: how many objects the catalogue holds, and how old its
// newest element set is.
//
// # Why there are no pass predictions here
//
// This is the entire scope decision, and it is deliberate rather than
// unfinished.
//
// A countdown to the next pass of AO-91 is not an observation. It is a computed
// function of the current time, and storing a computed function of time as a
// time series goes wrong in every direction at once. It is wrong the moment it
// is written and stays wrong until the next scrape, so its error is bounded
// only by the scrape interval. It quantises: sampled once a minute, a
// twelve-minute pass is a staircase. It reconstructs badly under any range
// query, because a downsampled average of a sawtooth countdown is a number that
// describes nothing. And it cannot be averaged, rate()-d or summed — the three
// things a time-series database is for.
//
// The consumer that wants a pass prediction should compute it at the moment it
// needs it, from element sets, which is what every tracking program already
// does. What such a program cannot easily tell you is whether its elements have
// gone stale.
//
// Element-set age, by contrast, is a genuine fact about the outside world.
// Element sets are regenerated as new tracking data arrives, normally daily. If
// the newest epoch in a catalogue is four days old, something upstream has
// stopped — and every pass prediction anybody computes from it is quietly
// drifting, by minutes for a low orbit. That is a real operational problem,
// worth an alert, and it is invisible unless somebody measures it.
//
// # Polling policy
//
// Celestrak's terms prohibit high-frequency automated retrieval and its
// operator blocks addresses that poll aggressively; the shared politeness table
// gives celestrak.org a five-minute floor with burst 1 on top of what this
// source asks for.
//
// This source asks for twice a day, with a conditional request. The endpoint
// serves validators, so an unchanged catalogue costs a 304 and no body. Element
// sets are regenerated roughly daily and asynchronously, so two polls bound how
// stale our view can be at twelve hours while making four requests a day where
// a naive exporter would make 288.
//
// # Licence
//
// Celestrak requires attribution. Credit celestrak.org — Dr T S Kelso — and,
// through it, the US Space Force's space-track.org, which is the origin of the
// underlying tracking data.
package celestrak

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"time"

	"github.com/RealDougEubanks/solarham/internal/config"
	"github.com/RealDougEubanks/solarham/internal/httpx"
	"github.com/RealDougEubanks/solarham/internal/metric"
	"github.com/RealDougEubanks/solarham/internal/redact"
	"github.com/RealDougEubanks/solarham/internal/source"
)

// Name identifies this source in logs, metrics and the health endpoints. It is
// stable because it becomes a metric label.
const Name = "celestrak"

const (
	// DefaultBaseURL is the public site.
	DefaultBaseURL = "https://celestrak.org"

	// Path is the general-perturbations query endpoint.
	Path = "/NORAD/elements/gp.php"

	// DefaultGroup is the amateur-satellite group, which is the one that
	// matters for this exporter's audience. Any group name celestrak accepts
	// works; the group name becomes the "catalogue" label, so it is the one
	// piece of configuration that changes the published series.
	DefaultGroup = "amateur"

	// format asks for OMM JSON rather than the three-line TLE text.
	//
	// Only JSON is supported, and there is no format setting. The TLE form
	// carries the same epoch in a fixed-column two-digit-year encoding that has
	// to be pivoted around 1957, for no benefit: the JSON is 41 KB against 16 KB
	// and both are trivial. One parser that can fail one way is better than two.
	format = "json"

	// publishLag is added to each scheduled time. See Schedule.
	publishLag = 20 * time.Minute

	// defaultRetries is the number of retries after the first attempt. At two
	// polls a day, a transient 5xx is worth another try rather than waiting
	// twelve hours.
	defaultRetries = 2

	// maxBodyBytes bounds the response. The live amateur group was 41,011 bytes
	// for 97 objects. 8 MiB covers the largest groups celestrak publishes with
	// room to spare, and is small enough that a broken origin cannot hurt us.
	maxBodyBytes = 8 << 20
)

// Source polls one celestrak element-set group.
type Source struct {
	url   string
	group string
	// retries is the number of retries after the first attempt.
	retries int
	client  *httpx.Client
	log     *slog.Logger

	// now is time.Now except in tests. Element-set age is by definition
	// measured against the current instant, so this is the one source where the
	// clock is part of the published value rather than only of its metadata,
	// and pinning it in tests is what makes the age assertable.
	now func() time.Time
}

// Compile-time proof the source is both pollable and schedulable.
var (
	_ source.Source    = (*Source)(nil)
	_ source.Scheduled = (*Source)(nil)
)

// New builds the source from validated configuration.
//
// The client is passed in rather than constructed here because politeness is
// enforced across sources sharing a host, and celestrak is a host where getting
// that wrong means being blocked.
//
// cfg.Timeout is not read; request timeouts belong to the shared client.
func New(cfg config.Celestrak, client *httpx.Client, log *slog.Logger) (*Source, error) {
	if client == nil {
		return nil, errors.New("celestrak: a shared httpx client is required")
	}
	if log == nil {
		log = slog.Default()
	}

	base := strings.TrimRight(strings.TrimSpace(cfg.BaseURL), "/")
	if base == "" {
		base = DefaultBaseURL
	}
	parsed, err := url.Parse(base)
	if err != nil {
		return nil, fmt.Errorf("celestrak: base URL %q is not a URL: %w", redact.URL(base), err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, fmt.Errorf("celestrak: base URL must be http or https, got %q", parsed.Scheme)
	}
	if parsed.Host == "" {
		return nil, errors.New("celestrak: base URL has no host")
	}

	group := strings.TrimSpace(cfg.Group)
	if group == "" {
		group = DefaultGroup
	}
	// The group is remote-ish configuration that becomes both a query parameter
	// and a metric label, so it is escaped for the URL and rejected outright if
	// it could not be a label value.
	if strings.ContainsAny(group, " \t\n\"") {
		return nil, fmt.Errorf("celestrak: group %q contains whitespace or quotes; "+
			"it becomes a metric label value", group)
	}

	retries := cfg.Retries
	if retries < 0 {
		retries = 0
	}
	if cfg.Retries == 0 {
		retries = defaultRetries
	}

	query := url.Values{"GROUP": {group}, "FORMAT": {format}}
	return &Source{
		url:     base + Path + "?" + query.Encode(),
		group:   group,
		retries: retries,
		client:  client,
		log:     log.With("source", Name),
		now:     time.Now,
	}, nil
}

// Name identifies the source.
func (s *Source) Name() string { return Name }

// Interval reports whatever the schedule reports, so the two can never
// disagree.
func (s *Source) Interval() time.Duration { return s.Schedule().Interval() }

// Schedule polls twice per UTC day, at 04:00 and 16:00 plus a twenty-minute
// lag: 04:20 and 16:20 UTC.
//
// The reasoning, in order:
//
// Not hourly, and not on an interval at all. Celestrak's stated policy
// prohibits high-frequency automated retrieval and its operator blocks
// addresses that ignore that. Element sets are regenerated roughly daily as new
// tracking data is processed, so an hourly poll would make 24 requests to learn
// one thing, which is precisely the behaviour the policy exists to stop.
//
// Twice rather than once, because regeneration is asynchronous and there is no
// published time for it: the observed epochs across one amateur-group fetch
// spanned six days, which is what a rolling per-object refresh looks like. A
// single daily poll makes our age figure up to 24 hours pessimistic through no
// fault of the upstream, and the age figure is the entire point of this source.
// Two polls bound that error at twelve hours for four requests a day.
//
// Twelve hours apart and off the hour, because a schedule that lands on 00:00
// UTC arrives with every other cron job on the internet.
func (s *Source) Schedule() source.Schedule {
	return source.DailyAt(publishLag,
		source.TimeOfDay{Hour: 4, Minute: 0},
		source.TimeOfDay{Hour: 16, Minute: 0},
	)
}

// object is one OMM record. Only the fields used are declared; the document
// also carries the full element set, which a consumer that wants predictions
// should fetch for itself rather than have us republish as metrics.
type object struct {
	Name  string `json:"OBJECT_NAME"`
	Epoch string `json:"EPOCH"`
	// NoradID is read only so that a record can be identified in a log line
	// when its epoch will not parse.
	NoradID json.Number `json:"NORAD_CAT_ID"`
}

// Poll fetches the group and publishes its size and freshness.
func (s *Source) Poll(ctx context.Context) (batch metric.Batch, err error) {
	fetched := s.now().UTC()
	empty := metric.Batch{Source: Name, Fetched: fetched}

	if err := ctx.Err(); err != nil {
		return empty, fmt.Errorf("celestrak: poll cancelled: %w", err)
	}

	// A parse panic would otherwise take down the poll. This is somebody else's
	// JSON, and NORAD_CAT_ID has been seen as both a number and a string in the
	// wild, so a shape nobody anticipated is a question of when.
	defer func() {
		if r := recover(); r != nil {
			batch = empty
			err = fmt.Errorf("celestrak: panic decoding the element set: %v", r)
		}
	}()

	resp, err := s.client.Get(ctx, httpx.Request{
		URL: s.url,
		// The endpoint serves validators, and the catalogue changes daily
		// against a twice-daily poll, so a fair share of polls should cost a
		// 304 and no body.
		Conditional: true,
		Retries:     s.retries,
		MaxBody:     maxBodyBytes,
	})
	switch {
	case errors.Is(err, httpx.ErrNotModified):
		return empty, source.ErrNotModified
	case errors.Is(err, httpx.ErrRateLimited):
		return empty, fmt.Errorf("%w: %w", source.ErrRateLimited, err)
	case err != nil:
		return empty, fmt.Errorf("celestrak: fetching the element set: %w", err)
	}

	var objects []object
	if err := json.Unmarshal(resp.Body, &objects); err != nil {
		return empty, fmt.Errorf("celestrak: decoding the element set: %w", redact.Error(err))
	}
	if len(objects) == 0 {
		// An empty array is a well-formed answer meaning "no such group".
		// Publishing a count of zero would read as every amateur satellite
		// having decayed overnight.
		return empty, fmt.Errorf("celestrak: group %q returned no objects; "+
			"check the configured group name", s.group)
	}

	var (
		newest   time.Time
		unparsed int
	)
	for _, o := range objects {
		epoch, ok := ParseEpoch(o.Epoch)
		if !ok {
			unparsed++
			s.log.Debug("celestrak object had an unreadable epoch",
				"norad_cat_id", o.NoradID.String(), "epoch", o.Epoch)
			continue
		}
		if epoch.After(newest) {
			newest = epoch
		}
	}
	if unparsed > 0 {
		s.log.Warn("celestrak objects had unreadable epochs",
			"objects", unparsed, "of", len(objects), "group", s.group)
	}
	if newest.IsZero() {
		return empty, fmt.Errorf("celestrak: none of the %d objects in group %q had a readable EPOCH; "+
			"the document format has probably changed", len(objects), s.group)
	}

	age := fetched.Sub(newest).Seconds()
	if age < 0 {
		// An epoch in the future is normal for propagated element sets in some
		// catalogues, and is also what clock skew looks like. Either way a
		// negative age reads as a broken exporter, so it is clamped: the
		// operational question this metric answers is "how stale", and nothing
		// is more fresh than fresh.
		age = 0
	}

	batch = metric.Batch{Source: Name, Fetched: fetched, Samples: []metric.Sample{
		{
			Desc:   metric.TLEAge,
			Labels: []string{s.group},
			Value:  age,
			// Stamped now, not at the epoch. The age is a property of this
			// moment; stamping it at the epoch would place a point in the past
			// whose value describes the present.
			Time: fetched,
		},
		{
			Desc:   metric.TLECount,
			Labels: []string{s.group},
			Value:  float64(len(objects)),
			// The count is an observation of the catalogue as of its newest
			// element set.
			Time: newest,
		},
		{
			Desc:   metric.SourceDataAge,
			Labels: []string{Name},
			Value:  age,
			Time:   fetched,
		},
	}}
	return batch, nil
}

// epochLayouts are the forms EPOCH has been seen in, most likely first.
//
// The JSON output writes "2026-09-08T14:58:28.028928" — ISO 8601 shaped, with
// microseconds, and with no zone at all. It is UTC; every epoch in the
// space-track lineage is. The rest of the list is defensive: the same value has
// been observed with a Z, with fewer or no fractional digits, and with a space
// in place of the T when the document has been through a spreadsheet on the
// way.
var epochLayouts = []string{
	"2006-01-02T15:04:05.999999999",
	"2006-01-02T15:04:05.999999999Z07:00",
	"2006-01-02T15:04:05",
	"2006-01-02 15:04:05.999999999",
	"2006-01-02 15:04:05",
	"2006-01-02",
}

// ParseEpoch reads an element-set epoch as UTC.
//
// It is tolerant by design. This field is the input to the only two numbers
// this package publishes, and losing the whole document because one catalogue
// started emitting a trailing Z would be a self-inflicted outage. A record
// whose epoch cannot be read at all is skipped and logged, which is visible
// without being fatal.
func ParseEpoch(raw string) (time.Time, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, false
	}
	for _, layout := range epochLayouts {
		if t, err := time.Parse(layout, raw); err == nil {
			return t.UTC(), true
		}
	}
	return time.Time{}, false
}
