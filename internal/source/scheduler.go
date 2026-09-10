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

	// startJitter bounds the random delay before each source's first poll.
	// Tests set it to zero; nothing else should.
	startJitter time.Duration

	// pollBudget bounds one whole poll, retries and rate-limit waits included.
	// See pollOnce.
	pollBudget time.Duration

	// clock is injectable so schedule arithmetic can be tested without
	// waiting for a wall-clock publication slot.
	clock func() time.Time

	// authority ranks sources against each other when two publish the same
	// series. See metric.Batch.Authority.
	authority map[string]int

	mu    sync.RWMutex
	state map[string]*sourceState
}

// DefaultStartJitter spreads the initial polls across a few seconds.
//
// Five sources firing simultaneously at every container start is a
// self-inflicted thundering herd against upstreams we have already been asked
// to go easy on.
const DefaultStartJitter = 3 * time.Second

// DefaultPollBudget bounds one whole poll: every retry and every rate-limit
// wait inside it.
//
// Two minutes is comfortably longer than any healthy poll here -- the slowest
// upstream in normal operation answers in a few seconds -- and short enough
// that a stuck source reports a failure while somebody is still looking at it.
//
// It is deliberately shorter than the multi-minute rate-limit floors on a few
// hosts, which means a poll that has to wait for a token will be cut off
// rather than sit through it. That is the intent: the next scheduled poll
// costs nothing and does the same work, whereas holding a poll open makes the
// source look unscheduled rather than slow.
const DefaultPollBudget = 2 * time.Minute

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

// SetAuthority ranks sources for collision resolution.
//
// Several quantities are published by more than one upstream and they are not
// equally good, so the ordering has to be declared somewhere. It lives in main
// next to its reasoning rather than being implied by which source happens to
// poll last.
func (s *Scheduler) SetAuthority(authority map[string]int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.authority = authority
}

// NewScheduler returns a scheduler over the given sources.
func NewScheduler(sources []Source, publisher Publisher, log *slog.Logger, observer Observer) *Scheduler {
	if log == nil {
		log = slog.Default()
	}
	s := &Scheduler{
		sources:     sources,
		publisher:   publisher,
		log:         log,
		observer:    observer,
		startJitter: DefaultStartJitter,
		pollBudget:  DefaultPollBudget,
		clock:       time.Now,
		state:       make(map[string]*sourceState, len(sources)),
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
	if src.Interval() <= 0 {
		s.log.Error("source has a non-positive interval and will not be polled",
			"source", src.Name(), "interval", src.Interval())
		return
	}

	sched := scheduleFor(src)

	// The first poll happens almost immediately regardless of schedule, so the
	// exporter has data to serve without waiting for the next publication slot
	// — which for a daily source would mean up to 24 hours of empty metrics
	// after every container start.
	timer := time.NewTimer(s.firstDelay())
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}

		s.pollOnce(ctx, src)
		timer.Reset(s.nextDelay(src, sched))
	}
}

// firstDelay is how long to wait before a source's very first poll: a random
// point within the start jitter, or immediately if jitter is disabled.
func (s *Scheduler) firstDelay() time.Duration {
	if s.startJitter <= 0 {
		// A timer cannot be created with a non-positive duration, so use the
		// smallest delay that fires effectively at once.
		return time.Nanosecond
	}
	return time.Duration(rand.Int64N(int64(s.startJitter)))
}

// nextDelay returns how long to wait before polling this source again.
//
// The schedule decides the base time; consecutive failures add backoff on top.
// A scheduled source that fails therefore slips past its slot rather than
// hammering a publisher that is having a bad day.
func (s *Scheduler) nextDelay(src Source, sched Schedule) time.Duration {
	s.mu.RLock()
	st := s.state[src.Name()]
	backoff := time.Duration(0)
	if st != nil {
		backoff = st.backoff
	}
	s.mu.RUnlock()

	now := s.clock()
	delay := sched.NextAfter(now).Sub(now) + backoff

	// A schedule that returns a time already past would spin this loop at full
	// speed. This is a guard against that bug, deliberately far below any real
	// poll interval — configuration already floors those at a minute — so it
	// never silently slows a legitimate schedule.
	if delay <= 0 {
		delay = 50 * time.Millisecond
	}
	return delay
}

// pollOnce performs one poll, containing its failures.
//
// A panic in a source is recovered here for the same reason it is recovered in
// the sink fan-out: a parsing bug against a malformed upstream response must
// not take down a process whose whole purpose is to keep running unattended.
func (s *Scheduler) pollOnce(ctx context.Context, src Source) {
	start := time.Now()

	// Bound the whole poll, not just the requests inside it.
	//
	// The per-request timeout in httpx bounds one HTTP call. It does not bound
	// a Poll, because retries wait on the shared per-host rate limiter: on a
	// host with a five-minute floor, three attempts is fifteen minutes inside
	// one Poll. That happened in production -- a single celestrak poll ran for
	// 420 seconds while /health reported successes=0, failures=0, which is
	// indistinguishable from a source that was never scheduled.
	//
	// Retrying past this budget is not worth what it costs anyway. On a host
	// slow enough to hit the budget, the next scheduled poll does the same job
	// without holding a goroutine and a misleading health entry open in the
	// meantime.
	pollCtx, cancel := context.WithTimeout(ctx, s.pollBudget)
	defer cancel()

	batch, err := func() (b metric.Batch, err error) {
		defer func() {
			if p := recover(); p != nil {
				err = fmt.Errorf("source %s panicked: %v", src.Name(), p)
			}
		}()
		return src.Poll(pollCtx)
	}()

	duration := time.Since(start)

	// A cancelled parent context means the process is shutting down. That is
	// not a source failure and must not be recorded as one, or every clean
	// stop would leave a failure as the last thing in the metrics.
	//
	// The poll's own budget expiring is the opposite: it is a real failure and
	// must be recorded, so the parent is what gets checked here, never pollCtx.
	if err != nil && ctx.Err() != nil {
		return
	}

	if err != nil && errors.Is(pollCtx.Err(), context.DeadlineExceeded) {
		err = fmt.Errorf("poll exceeded its %s budget: %w", s.pollBudget, err)
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
		batch.Authority = s.authorityFor(src.Name())
		s.publisher.Publish(ctx, batch)
	}
}

// authorityFor returns a source's collision rank, defaulting to zero for a
// source nobody has ranked -- which is correct for the great majority, since
// most publish quantities no other source touches.
func (s *Scheduler) authorityFor(name string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.authority[name]
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
	st.backoff = nextBackoff(st.backoff, scheduleFor(src).Interval())
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
		sched := scheduleFor(src)
		staleAfter := sched.StaleAfter()
		out = append(out, Status{
			Name:        src.Name(),
			Interval:    sched.String(),
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
