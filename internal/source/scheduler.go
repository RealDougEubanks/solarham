package source

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/RealDougEubanks/solarham/internal/metric"
)

// Publisher receives every batch a source produces.
type Publisher interface {
	Publish(ctx context.Context, b metric.Batch)
}

// Observer receives the outcome of each poll so metrics can be recorded without
// the scheduler depending on a metrics implementation.
type Observer interface {
	ObservePoll(sourceName string, duration time.Duration, samples int, err error)
}

// Scheduler polls every source on its own independent ticker.
//
// This is the main structural difference from the sibling gmc-exporter, which
// has one device on one interval. Here the intervals span three orders of
// magnitude — a minute for solar wind, an hour for hamqsl — and a slow or
// failing source must not delay a fast one. Each source therefore gets its own
// goroutine and its own ticker, and nothing is shared between them but the
// publisher.
type Scheduler struct {
	sources   []Source
	publisher Publisher
	log       *slog.Logger
	observer  Observer

	mu    sync.RWMutex
	state map[string]*sourceState
}

type sourceState struct {
	lastSuccess time.Time
	lastFailure time.Time
	lastError   string
	samples     int
	successes   uint64
	failures    uint64
	// backoff is the current additional delay applied after consecutive
	// failures. It resets to zero on any success.
	backoff time.Duration
}

// NewScheduler returns a scheduler over the given sources.
func NewScheduler(sources []Source, publisher Publisher, log *slog.Logger, observer Observer) *Scheduler {
	if log == nil {
		log = slog.Default()
	}
	s := &Scheduler{
		sources:   sources,
		publisher: publisher,
		log:       log,
		observer:  observer,
		state:     make(map[string]*sourceState, len(sources)),
	}
	for _, src := range sources {
		s.state[src.Name()] = &sourceState{}
	}
	return s
}

// Names lists the scheduled sources.
func (s *Scheduler) Names() []string {
	out := make([]string, 0, len(s.sources))
	for _, src := range s.sources {
		out = append(out, src.Name())
	}
	return out
}

// Len reports how many sources are scheduled.
func (s *Scheduler) Len() int { return len(s.sources) }

// Run polls every source until ctx is cancelled, then returns.
//
// Each source polls once immediately at startup so the exporter has data to
// serve without waiting a full interval — which for the slow sources would mean
// an hour of empty metrics.
//
// Those immediate polls are spread by a small random jitter. Five sources
// firing simultaneously at every container start is a self-inflicted thundering
// herd against upstreams we have already been asked to go easy on.
func (s *Scheduler) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for _, src := range s.sources {
		wg.Add(1)
		go func(src Source) {
			defer wg.Done()
			s.runOne(ctx, src)
		}(src)
	}
	wg.Wait()
}

// runOne is the poll loop for a single source.
func (s *Scheduler) runOne(ctx context.Context, src Source) {
	interval := src.Interval()
	if interval <= 0 {
		s.log.Error("source has a non-positive interval and will not be polled",
			"source", src.Name(), "interval", interval)
		return
	}

	// Jitter the first poll across the first few seconds.
	startDelay := time.Duration(rand.Int64N(int64(3 * time.Second)))
	timer := time.NewTimer(startDelay)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}

		s.pollOnce(ctx, src)

		// The next delay is the source's interval plus whatever backoff its
		// recent failures have earned.
		timer.Reset(s.nextDelay(src, interval))
	}
}

// nextDelay returns how long to wait before polling this source again.
func (s *Scheduler) nextDelay(src Source, interval time.Duration) time.Duration {
	s.mu.RLock()
	st := s.state[src.Name()]
	backoff := time.Duration(0)
	if st != nil {
		backoff = st.backoff
	}
	s.mu.RUnlock()
	return interval + backoff
}

// pollOnce performs one poll, containing its failures.
//
// A panic in a source is recovered here for the same reason it is recovered in
// the sink fan-out: a parsing bug against a malformed upstream response must
// not take down a process whose whole purpose is to keep running unattended.
func (s *Scheduler) pollOnce(ctx context.Context, src Source) {
	start := time.Now()

	batch, err := func() (b metric.Batch, err error) {
		defer func() {
			if p := recover(); p != nil {
				err = fmt.Errorf("source %s panicked: %v", src.Name(), p)
			}
		}()
		return src.Poll(ctx)
	}()

	duration := time.Since(start)

	// A cancelled context means the process is shutting down. That is not a
	// source failure and must not be recorded as one, or every clean stop
	// would leave a failure as the last thing in the metrics.
	if err != nil && ctx.Err() != nil {
		return
	}

	s.record(src, batch, duration, err)

	if s.observer != nil {
		s.observer.ObservePoll(src.Name(), duration, batch.Len(), err)
	}

	switch {
	case errors.Is(err, ErrNotModified):
		s.log.Debug("source unchanged", "source", src.Name(), "duration", duration)
		return
	case errors.Is(err, ErrRateLimited):
		s.log.Warn("source rate limited, backing off",
			"source", src.Name(), "duration", duration, "error", err)
		return
	case err != nil:
		s.log.Error("poll failed", "source", src.Name(), "duration", duration, "error", err)
		return
	}

	s.log.Debug("polled source",
		"source", src.Name(), "samples", batch.Len(), "duration", duration)

	if batch.Len() > 0 && s.publisher != nil {
		s.publisher.Publish(ctx, batch)
	}
}

// record updates the source's state for the health endpoints and adjusts its
// backoff.
func (s *Scheduler) record(src Source, batch metric.Batch, _ time.Duration, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	st := s.state[src.Name()]
	if st == nil {
		st = &sourceState{}
		s.state[src.Name()] = st
	}

	// A 304 is a successful poll. The data is current, we simply already have
	// it, and treating it as a failure would make an efficiently-behaving
	// source look broken.
	if err == nil || errors.Is(err, ErrNotModified) {
		st.lastSuccess = time.Now()
		st.lastError = ""
		st.successes++
		st.backoff = 0
		if err == nil {
			st.samples = batch.Len()
		}
		return
	}

	st.lastFailure = time.Now()
	st.lastError = err.Error()
	st.failures++
	st.backoff = nextBackoff(st.backoff, src.Interval())
}

// nextBackoff doubles the current backoff, starting at the source's interval
// and capping at eight times it.
//
// The cap is a multiple of the interval rather than a fixed duration because
// the sources differ so widely: eight minutes of backoff is reasonable for a
// one-minute source and absurd for an hourly one.
func nextBackoff(current, interval time.Duration) time.Duration {
	maxBackoff := 8 * interval
	if current <= 0 {
		return interval
	}
	next := current * 2
	if next > maxBackoff {
		return maxBackoff
	}
	return next
}

// Statuses reports the state of every source, for the health endpoints.
func (s *Scheduler) Statuses() []Status {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]Status, 0, len(s.sources))
	for _, src := range s.sources {
		st := s.state[src.Name()]
		if st == nil {
			st = &sourceState{}
		}
		staleAfter := staleAfter(src.Interval())
		out = append(out, Status{
			Name:        src.Name(),
			Interval:    src.Interval().String(),
			LastSuccess: st.lastSuccess,
			LastFailure: st.lastFailure,
			LastError:   st.lastError,
			Samples:     st.samples,
			Successes:   st.successes,
			Failures:    st.failures,
			Stale:       st.lastSuccess.IsZero() || time.Since(st.lastSuccess) > staleAfter,
			StaleAfter:  staleAfter,
		})
	}
	return out
}

// Ready reports whether every source has a recent enough success.
//
// Readiness is per-source and relative to each source's own interval, because
// "recent" means something different for a one-minute source than an hourly
// one. A source that has never succeeded is not ready, which is what makes a
// freshly-started container correctly report itself unready until it has
// actually fetched something.
func (s *Scheduler) Ready() bool {
	for _, st := range s.Statuses() {
		if st.Stale {
			return false
		}
	}
	return len(s.sources) > 0
}

// staleAfter is how long a source may go without a success before it counts as
// stale: three intervals, so a single missed poll does not flap readiness.
func staleAfter(interval time.Duration) time.Duration {
	return 3 * interval
}
