// Package source defines the interface every upstream data provider
// implements, and the scheduler that polls each of them on its own cadence.
package source

import (
	"context"
	"errors"
	"time"

	"github.com/RealDougEubanks/solarham/internal/metric"
)

// Source fetches from one upstream and converts what it returns into samples.
//
// Implementations must be safe to call repeatedly and must not retain the
// returned batch. A source that fails must return an error rather than
// terminating the process: every upstream here is a third-party service that
// will be unreachable sometimes, and that is an ordinary condition, not a
// fatal one.
type Source interface {
	// Name identifies the source in logs and metrics. It must be stable, since
	// it becomes a metric label.
	Name() string

	// Interval is the nominal spacing between polls. It sizes the staleness
	// window and the failure backoff.
	//
	// A source whose upstream publishes on a known clock should also implement
	// Scheduled, so it is polled just after publication rather than blindly on
	// this interval.
	Interval() time.Duration

	// Poll fetches once. It must honour ctx cancellation.
	//
	// A poll that legitimately produced nothing returns an empty batch and a
	// nil error. That happens routinely: SWPC answers an unchanged document
	// with 304, and hamqsl publishes empty elements for fields it has no data
	// for. Neither is a failure.
	Poll(ctx context.Context) (metric.Batch, error)
}

// Scheduled is implemented by a source whose upstream publishes on a known
// clock rather than continuously.
//
// The scheduler prefers Schedule() over Interval() when a source implements
// this. It is a separate interface so that the common case — an upstream that
// really does change every minute — stays a one-line Interval().
type Scheduled interface {
	Source

	// Schedule decides when this source is next polled.
	Schedule() Schedule
}

// scheduleFor returns the schedule a source should be polled on, falling back
// to a fixed interval for sources that declare no publication clock.
func scheduleFor(src Source) Schedule {
	if s, ok := src.(Scheduled); ok {
		if sched := s.Schedule(); sched != nil {
			return sched
		}
	}
	return Every(src.Interval())
}

// ErrNotModified reports that the upstream answered a conditional request with
// 304 and there is nothing new to publish.
//
// It is returned alongside an empty batch and is not counted as a failure. It
// exists so the scheduler can log "unchanged" rather than "no samples", which
// are operationally different things.
var ErrNotModified = errors.New("source: not modified")

// ErrRateLimited reports that the upstream refused the request because we asked
// too often. The scheduler backs off rather than retrying, and the source is
// expected to have already recorded its own cooldown.
var ErrRateLimited = errors.New("source: rate limited")

// Status is a point-in-time view of one source, for the health endpoints.
type Status struct {
	Name        string        `json:"name"`
	Interval    string        `json:"interval"`
	LastSuccess time.Time     `json:"lastSuccess,omitzero"`
	LastFailure time.Time     `json:"lastFailure,omitzero"`
	LastError   string        `json:"lastError,omitempty"`
	Samples     int           `json:"samples"`
	Successes   uint64        `json:"successes"`
	Failures    uint64        `json:"failures"`
	Stale       bool          `json:"stale"`
	StaleAfter  time.Duration `json:"-"`
}
