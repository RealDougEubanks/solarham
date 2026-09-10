package source

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/RealDougEubanks/solarham/internal/metric"
)

// newTestScheduler builds a scheduler with the startup jitter disabled.
//
// The jitter is right in production and wrong in tests: it would make every
// case here wait a random slice of three seconds for a first poll it is about
// to assert on.
func newTestScheduler(sources []Source, publisher Publisher, log *slog.Logger, observer Observer) *Scheduler {
	s := NewScheduler(sources, publisher, log, observer)
	s.startJitter = 0
	return s
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// fakeSource is a Source whose every poll is scripted by the test.
type fakeSource struct {
	name     string
	interval time.Duration

	mu     sync.Mutex
	calls  int
	result func(call int) (metric.Batch, error)
}

func (f *fakeSource) Name() string            { return f.name }
func (f *fakeSource) Interval() time.Duration { return f.interval }

func (f *fakeSource) Poll(ctx context.Context) (metric.Batch, error) {
	f.mu.Lock()
	f.calls++
	call := f.calls
	fn := f.result
	f.mu.Unlock()

	if fn == nil {
		return metric.Batch{Source: f.name}, nil
	}
	return fn(call)
}

func (f *fakeSource) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// capturingPublisher records every batch it is handed.
type capturingPublisher struct {
	mu      sync.Mutex
	batches []metric.Batch
}

func (p *capturingPublisher) Publish(_ context.Context, b metric.Batch) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.batches = append(p.batches, b)
}

func (p *capturingPublisher) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.batches)
}

// recordingObserver records poll outcomes.
type recordingObserver struct {
	mu       sync.Mutex
	failures int
	oks      int
}

func (o *recordingObserver) ObservePoll(_ string, _ time.Duration, _ int, err error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if err != nil {
		o.failures++
		return
	}
	o.oks++
}

func sampleBatch(source string) metric.Batch {
	return metric.Batch{
		Source: source,
		Samples: []metric.Sample{{
			Desc:  metric.FluxSFU,
			Value: 110,
			Time:  time.Date(2026, 9, 9, 3, 22, 0, 0, time.UTC),
		}},
	}
}

// waitFor polls cond until it holds or the deadline passes, so tests do not
// depend on a fixed sleep.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestEachSourcePollsImmediatelyAtStartup(t *testing.T) {
	// Waiting a full interval before the first poll would leave the hourly
	// sources reporting nothing for an hour after every container start.
	src := &fakeSource{name: "fast", interval: time.Hour}
	pub := &capturingPublisher{}
	s := newTestScheduler([]Source{src}, pub, discardLogger(), nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.Run(ctx)

	waitFor(t, "the first poll", func() bool { return src.callCount() >= 1 })
}

func TestASlowSourceDoesNotDelayAFastOne(t *testing.T) {
	slow := &fakeSource{name: "slow", interval: time.Hour, result: func(int) (metric.Batch, error) {
		time.Sleep(300 * time.Millisecond)
		return sampleBatch("slow"), nil
	}}
	fast := &fakeSource{name: "fast", interval: 10 * time.Millisecond}

	s := newTestScheduler([]Source{slow, fast}, &capturingPublisher{}, discardLogger(), nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.Run(ctx)

	// The fast source should poll repeatedly while the slow one is still in
	// its first call.
	waitFor(t, "the fast source to poll several times", func() bool { return fast.callCount() >= 3 })
}

func TestSamplesReachThePublisher(t *testing.T) {
	src := &fakeSource{name: "swpc", interval: 10 * time.Millisecond, result: func(int) (metric.Batch, error) {
		return sampleBatch("swpc"), nil
	}}
	pub := &capturingPublisher{}
	s := newTestScheduler([]Source{src}, pub, discardLogger(), nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.Run(ctx)

	waitFor(t, "a published batch", func() bool { return pub.count() >= 1 })
}

func TestAnEmptyBatchIsNotPublished(t *testing.T) {
	src := &fakeSource{name: "quiet", interval: 10 * time.Millisecond, result: func(int) (metric.Batch, error) {
		return metric.Batch{Source: "quiet"}, nil
	}}
	pub := &capturingPublisher{}
	s := newTestScheduler([]Source{src}, pub, discardLogger(), nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.Run(ctx)

	waitFor(t, "several polls", func() bool { return src.callCount() >= 3 })

	if got := pub.count(); got != 0 {
		t.Errorf("publisher received %d batches, want 0 for empty batches", got)
	}
}

// A 304 means the data is current and we already have it. Counting that as a
// failure would make a well-behaved source that uses conditional GETs look
// broken.
func TestNotModifiedCountsAsASuccess(t *testing.T) {
	src := &fakeSource{name: "swpc", interval: time.Hour, result: func(int) (metric.Batch, error) {
		return metric.Batch{Source: "swpc"}, ErrNotModified
	}}
	s := newTestScheduler([]Source{src}, &capturingPublisher{}, discardLogger(), nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.Run(ctx)

	waitFor(t, "the first poll", func() bool { return src.callCount() >= 1 })
	waitFor(t, "the success to be recorded", func() bool {
		return s.Statuses()[0].Successes >= 1
	})

	st := s.Statuses()[0]
	if st.Failures != 0 {
		t.Errorf("Failures = %d, want 0: a 304 is not a failure", st.Failures)
	}
	if st.LastSuccess.IsZero() {
		t.Error("LastSuccess is zero, want a 304 to advance it")
	}
}

func TestAPanickingSourceIsContainedAndKeepsPolling(t *testing.T) {
	// A parsing bug against a malformed upstream response must not take down a
	// process whose whole purpose is to keep running unattended.
	var panics atomic.Int32
	src := &fakeSource{name: "brittle", interval: 10 * time.Millisecond, result: func(call int) (metric.Batch, error) {
		if call == 1 {
			panics.Add(1)
			panic("upstream returned something unexpected")
		}
		return sampleBatch("brittle"), nil
	}}
	pub := &capturingPublisher{}
	s := newTestScheduler([]Source{src}, pub, discardLogger(), nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.Run(ctx)

	waitFor(t, "recovery and a subsequent successful publish", func() bool { return pub.count() >= 1 })

	if panics.Load() != 1 {
		t.Errorf("the source panicked %d times, want exactly 1", panics.Load())
	}
	if s.Statuses()[0].Failures < 1 {
		t.Error("the panic was not recorded as a failure")
	}
}

func TestOneFailingSourceDoesNotStopAnother(t *testing.T) {
	bad := &fakeSource{name: "bad", interval: 10 * time.Millisecond, result: func(int) (metric.Batch, error) {
		return metric.Batch{}, errors.New("upstream is down")
	}}
	good := &fakeSource{name: "good", interval: 10 * time.Millisecond, result: func(int) (metric.Batch, error) {
		return sampleBatch("good"), nil
	}}
	pub := &capturingPublisher{}
	s := newTestScheduler([]Source{bad, good}, pub, discardLogger(), nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.Run(ctx)

	waitFor(t, "the healthy source to publish", func() bool { return pub.count() >= 2 })
}

func TestFailuresAreObserved(t *testing.T) {
	src := &fakeSource{name: "bad", interval: 10 * time.Millisecond, result: func(int) (metric.Batch, error) {
		return metric.Batch{}, errors.New("upstream is down")
	}}
	obs := &recordingObserver{}
	s := newTestScheduler([]Source{src}, &capturingPublisher{}, discardLogger(), obs)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.Run(ctx)

	waitFor(t, "an observed failure", func() bool {
		obs.mu.Lock()
		defer obs.mu.Unlock()
		return obs.failures >= 1
	})
}

func TestAFailureRecordsItsError(t *testing.T) {
	src := &fakeSource{name: "bad", interval: time.Hour, result: func(int) (metric.Batch, error) {
		return metric.Batch{}, errors.New("upstream is down")
	}}
	s := newTestScheduler([]Source{src}, &capturingPublisher{}, discardLogger(), nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.Run(ctx)

	waitFor(t, "the failure to be recorded", func() bool { return s.Statuses()[0].Failures >= 1 })

	if got := s.Statuses()[0].LastError; got != "upstream is down" {
		t.Errorf("LastError = %q, want the upstream error text", got)
	}
}

func TestASuccessClearsAPreviousError(t *testing.T) {
	src := &fakeSource{name: "flaky", interval: 10 * time.Millisecond, result: func(call int) (metric.Batch, error) {
		if call == 1 {
			return metric.Batch{}, errors.New("transient")
		}
		return sampleBatch("flaky"), nil
	}}
	s := newTestScheduler([]Source{src}, &capturingPublisher{}, discardLogger(), nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.Run(ctx)

	waitFor(t, "a success after the failure", func() bool { return s.Statuses()[0].Successes >= 1 })

	if got := s.Statuses()[0].LastError; got != "" {
		t.Errorf("LastError = %q, want it cleared by the subsequent success", got)
	}
}

func TestRunReturnsWhenTheContextIsCancelled(t *testing.T) {
	src := &fakeSource{name: "src", interval: 10 * time.Millisecond}
	s := newTestScheduler([]Source{src}, &capturingPublisher{}, discardLogger(), nil)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		s.Run(ctx)
		close(done)
	}()

	waitFor(t, "the first poll", func() bool { return src.callCount() >= 1 })
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after the context was cancelled")
	}
}

// A clean shutdown must not leave a failure as the last thing recorded, or
// every graceful stop would look like an outage in the metrics.
func TestACancelledPollIsNotRecordedAsAFailure(t *testing.T) {
	release := make(chan struct{})
	src := &fakeSource{name: "slow", interval: time.Hour, result: func(int) (metric.Batch, error) {
		<-release
		return metric.Batch{}, context.Canceled
	}}
	s := newTestScheduler([]Source{src}, &capturingPublisher{}, discardLogger(), nil)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		s.Run(ctx)
		close(done)
	}()

	waitFor(t, "the poll to start", func() bool { return src.callCount() >= 1 })
	cancel()
	close(release)
	<-done

	if got := s.Statuses()[0].Failures; got != 0 {
		t.Errorf("Failures = %d, want 0 for a poll interrupted by shutdown", got)
	}
}

func TestASourceWithANonPositiveIntervalIsNotPolled(t *testing.T) {
	src := &fakeSource{name: "broken", interval: 0}
	s := newTestScheduler([]Source{src}, &capturingPublisher{}, discardLogger(), nil)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	s.Run(ctx)

	if got := src.callCount(); got != 0 {
		t.Errorf("a source with a zero interval was polled %d times, want 0", got)
	}
}

func TestReadyIsFalseBeforeAnySourceHasSucceeded(t *testing.T) {
	// A freshly started container must report itself unready until it has
	// actually fetched something.
	src := &fakeSource{name: "src", interval: time.Hour}
	s := newTestScheduler([]Source{src}, &capturingPublisher{}, discardLogger(), nil)

	if s.Ready() {
		t.Error("Ready() = true before any poll, want false")
	}
}

func TestReadyIsTrueAfterASuccess(t *testing.T) {
	src := &fakeSource{name: "src", interval: time.Hour, result: func(int) (metric.Batch, error) {
		return sampleBatch("src"), nil
	}}
	s := newTestScheduler([]Source{src}, &capturingPublisher{}, discardLogger(), nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.Run(ctx)

	waitFor(t, "readiness", func() bool { return s.Ready() })
}

func TestReadyIsFalseWithNoSources(t *testing.T) {
	s := newTestScheduler(nil, &capturingPublisher{}, discardLogger(), nil)
	if s.Ready() {
		t.Error("Ready() = true with no sources configured, want false")
	}
}

func TestStatusesReportEverySource(t *testing.T) {
	s := newTestScheduler([]Source{
		&fakeSource{name: "a", interval: time.Minute},
		&fakeSource{name: "b", interval: time.Hour},
	}, &capturingPublisher{}, discardLogger(), nil)

	got := s.Statuses()
	if len(got) != 2 {
		t.Fatalf("Statuses() returned %d entries, want 2", len(got))
	}
	if got[0].Name != "a" || got[1].Name != "b" {
		t.Errorf("Statuses() names = %q, %q, want a, b", got[0].Name, got[1].Name)
	}
	if got[0].StaleAfter != 3*time.Minute {
		t.Errorf("StaleAfter = %v, want three intervals", got[0].StaleAfter)
	}
}

func TestNamesAndLenReportTheConfiguredSources(t *testing.T) {
	s := newTestScheduler([]Source{
		&fakeSource{name: "hamqsl", interval: time.Hour},
		&fakeSource{name: "kc2g", interval: time.Minute},
	}, &capturingPublisher{}, discardLogger(), nil)

	if got := s.Len(); got != 2 {
		t.Errorf("Len() = %d, want 2", got)
	}
	names := s.Names()
	if len(names) != 2 || names[0] != "hamqsl" || names[1] != "kc2g" {
		t.Errorf("Names() = %v, want [hamqsl kc2g]", names)
	}
}

// Backoff is a multiple of the source's own interval, because eight minutes of
// backoff is reasonable for a one-minute source and absurd for an hourly one.
func TestBackoffStartsAtTheIntervalAndDoublesToACap(t *testing.T) {
	interval := time.Minute

	tests := []struct {
		name    string
		current time.Duration
		want    time.Duration
	}{
		{"first failure starts at one interval", 0, time.Minute},
		{"subsequent failures double", time.Minute, 2 * time.Minute},
		{"doubling continues", 2 * time.Minute, 4 * time.Minute},
		{"the cap is eight intervals", 8 * time.Minute, 8 * time.Minute},
		{"the cap is not exceeded", 6 * time.Minute, 8 * time.Minute},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := nextBackoff(tc.current, interval); got != tc.want {
				t.Errorf("nextBackoff(%v, %v) = %v, want %v", tc.current, interval, got, tc.want)
			}
		})
	}
}

func TestStaleAfterIsThreeIntervals(t *testing.T) {
	if got := Every(time.Minute).StaleAfter(); got != 3*time.Minute {
		t.Errorf("Every(1m).StaleAfter() = %v, want 3m", got)
	}
}

func TestTheStartupJitterStaysWithinItsBound(t *testing.T) {
	// The jitter must spread the first polls without ever pushing one past the
	// window, or a container start would look like a stalled source.
	s := NewScheduler(nil, nil, discardLogger(), nil)

	for range 200 {
		got := s.firstDelay()
		if got < 0 || got >= DefaultStartJitter {
			t.Fatalf("firstDelay() = %v, want within [0, %v)", got, DefaultStartJitter)
		}
	}
}

func TestDisablingTheJitterStillYieldsAUsableTimerDelay(t *testing.T) {
	// time.NewTimer rejects a non-positive duration, so zero jitter must still
	// produce something that fires effectively at once.
	s := NewScheduler(nil, nil, discardLogger(), nil)
	s.startJitter = 0

	if got := s.firstDelay(); got <= 0 {
		t.Errorf("firstDelay() = %v with jitter disabled, want a positive delay", got)
	}
}

// blockingSource hangs inside Poll until its context is done, standing in for a
// source stuck waiting on a multi-minute rate-limit token.
type blockingSource struct {
	name     string
	interval time.Duration
	entered  chan struct{}
	once     sync.Once
}

func (s *blockingSource) Name() string            { return s.name }
func (s *blockingSource) Interval() time.Duration { return s.interval }

func (s *blockingSource) Poll(ctx context.Context) (metric.Batch, error) {
	s.once.Do(func() { close(s.entered) })
	<-ctx.Done()
	return metric.Batch{}, ctx.Err()
}

// TestAPollThatHangsIsCutOffAndRecordedAsAFailure is the regression test for a
// production incident: celestrak's retries each waited on a five-minute host
// floor, so one Poll ran for 420 seconds. Nothing was logged and no counter
// moved for the whole time, so /health showed successes=0 failures=0 -- which
// is what a source that was never scheduled also looks like.
func TestAPollThatHangsIsCutOffAndRecordedAsAFailure(t *testing.T) {
	src := &blockingSource{name: "stuck", interval: time.Minute, entered: make(chan struct{})}

	s := newTestScheduler([]Source{src}, &capturingPublisher{}, discardLogger(), nil)
	s.pollBudget = 50 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		s.pollOnce(ctx, src)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("pollOnce did not return; the poll budget is not bounding Poll")
	}

	st := s.Statuses()[0]
	if st.Failures != 1 {
		t.Errorf("Failures = %d, want 1; a poll cut off by its budget is a real failure", st.Failures)
	}
	if !strings.Contains(st.LastError, "budget") {
		t.Errorf("LastError = %q, want it to say the budget was exceeded", st.LastError)
	}
}

// TestShutdownIsNotRecordedAsAPollFailure guards the other side of that change.
// The budget must not make a clean stop look like an upstream fault.
func TestShutdownIsNotRecordedAsAPollFailure(t *testing.T) {
	src := &blockingSource{name: "stuck", interval: time.Minute, entered: make(chan struct{})}

	s := newTestScheduler([]Source{src}, &capturingPublisher{}, discardLogger(), nil)
	s.pollBudget = time.Hour

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		defer close(done)
		s.pollOnce(ctx, src)
	}()

	<-src.entered
	cancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("pollOnce did not return after its context was cancelled")
	}

	if st := s.Statuses()[0]; st.Failures != 0 {
		t.Errorf("Failures = %d, want 0; shutting down is not a source failure", st.Failures)
	}
}
