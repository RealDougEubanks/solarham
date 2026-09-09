package sink

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/RealDougEubanks/solarham/internal/metric"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// fakeSink is a Sink whose behaviour each test scripts.
type fakeSink struct {
	name      string
	err       error
	delay     time.Duration
	panicWith any
	closeErr  error

	mu        sync.Mutex
	published []metric.Batch
	closed    int
}

func (f *fakeSink) Name() string { return f.name }

func (f *fakeSink) Publish(ctx context.Context, b metric.Batch) error {
	if f.panicWith != nil {
		panic(f.panicWith)
	}
	if f.delay > 0 {
		select {
		case <-time.After(f.delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	f.mu.Lock()
	f.published = append(f.published, b)
	f.mu.Unlock()
	return f.err
}

func (f *fakeSink) Close() error {
	f.mu.Lock()
	f.closed++
	f.mu.Unlock()
	return f.closeErr
}

func (f *fakeSink) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.published)
}

func (f *fakeSink) closeCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closed
}

// recordingObserver captures publish outcomes.
type recordingObserver struct {
	mu      sync.Mutex
	results map[string]error
}

func newRecordingObserver() *recordingObserver {
	return &recordingObserver{results: make(map[string]error)}
}

func (o *recordingObserver) ObservePublish(name string, _ time.Duration, err error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.results[name] = err
}

func (o *recordingObserver) get(name string) (error, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	err, ok := o.results[name]
	return err, ok
}

func testBatch() metric.Batch {
	return metric.Batch{
		Source: "swpc",
		Samples: []metric.Sample{{
			Desc:  metric.FluxSFU,
			Value: 110,
			Time:  time.Date(2026, 9, 9, 3, 22, 0, 0, time.UTC),
		}},
	}
}

func TestPublishReachesEverySink(t *testing.T) {
	a := &fakeSink{name: "a"}
	b := &fakeSink{name: "b"}
	set := NewSet([]Sink{a, b}, time.Second, discardLogger(), nil)

	set.Publish(context.Background(), testBatch())

	if a.count() != 1 || b.count() != 1 {
		t.Errorf("published to a=%d b=%d, want 1 each", a.count(), b.count())
	}
}

// The whole point of the fan-out. A broken InfluxDB must not cost you the
// Prometheus metrics as well.
func TestAFailingSinkDoesNotStopTheOthers(t *testing.T) {
	bad := &fakeSink{name: "bad", err: errors.New("influx is down")}
	good := &fakeSink{name: "good"}
	set := NewSet([]Sink{bad, good}, time.Second, discardLogger(), nil)

	set.Publish(context.Background(), testBatch())

	if good.count() != 1 {
		t.Errorf("the healthy sink published %d times, want 1", good.count())
	}
}

// A third-party client library that panics on a malformed server response must
// not be able to kill a process whose entire purpose is to keep running
// unattended.
func TestAPanickingSinkIsContained(t *testing.T) {
	boom := &fakeSink{name: "boom", panicWith: "client library exploded"}
	good := &fakeSink{name: "good"}
	obs := newRecordingObserver()
	set := NewSet([]Sink{boom, good}, time.Second, discardLogger(), obs)

	set.Publish(context.Background(), testBatch())

	if good.count() != 1 {
		t.Errorf("the healthy sink published %d times, want 1", good.count())
	}
	err, ok := obs.get("boom")
	if !ok || err == nil {
		t.Fatal("the panic was not observed as a failure")
	}
	if !strings.Contains(err.Error(), "panicked") {
		t.Errorf("observed error %q does not mention the panic", err)
	}
}

// One slow backend must not hold the others, or the next poll, hostage.
func TestASlowSinkIsBoundedByTheTimeout(t *testing.T) {
	slow := &fakeSink{name: "slow", delay: 5 * time.Second}
	fast := &fakeSink{name: "fast"}
	set := NewSet([]Sink{slow, fast}, 50*time.Millisecond, discardLogger(), nil)

	start := time.Now()
	set.Publish(context.Background(), testBatch())
	elapsed := time.Since(start)

	if elapsed > 2*time.Second {
		t.Errorf("Publish took %v, want it bounded near the 50ms timeout", elapsed)
	}
	if fast.count() != 1 {
		t.Errorf("the fast sink published %d times, want 1", fast.count())
	}
}

func TestSkippedIsNotCountedAsAFailure(t *testing.T) {
	skipping := &fakeSink{name: "skipping", err: ErrSkipped}
	obs := newRecordingObserver()
	set := NewSet([]Sink{skipping}, time.Second, discardLogger(), obs)

	set.Publish(context.Background(), testBatch())

	err, ok := obs.get("skipping")
	if !ok {
		t.Fatal("the skip was not observed")
	}
	if !errors.Is(err, ErrSkipped) {
		t.Errorf("observed error %v, want it to wrap ErrSkipped", err)
	}
}

func TestASkipIsLoggedAtDebugRatherThanError(t *testing.T) {
	// A sink with nothing to send is ordinary. Logging it at error level would
	// train an operator to ignore the error level.
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))

	set := NewSet([]Sink{&fakeSink{name: "skipping", err: ErrSkipped}}, time.Second, log, nil)
	set.Publish(context.Background(), testBatch())

	if strings.Contains(buf.String(), "level=ERROR") {
		t.Errorf("a skip was logged at error level: %s", buf.String())
	}
}

func TestAFailureIsLoggedAtErrorLevel(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, nil))

	set := NewSet([]Sink{&fakeSink{name: "bad", err: errors.New("influx is down")}}, time.Second, log, nil)
	set.Publish(context.Background(), testBatch())

	if !strings.Contains(buf.String(), "level=ERROR") {
		t.Errorf("a publish failure was not logged at error level: %s", buf.String())
	}
}

func TestObserverSeesEverySink(t *testing.T) {
	obs := newRecordingObserver()
	set := NewSet([]Sink{
		&fakeSink{name: "a"},
		&fakeSink{name: "b", err: errors.New("nope")},
	}, time.Second, discardLogger(), obs)

	set.Publish(context.Background(), testBatch())

	if err, ok := obs.get("a"); !ok || err != nil {
		t.Errorf("sink a observed as %v, want a nil-error success", err)
	}
	if err, ok := obs.get("b"); !ok || err == nil {
		t.Error("sink b was not observed as a failure")
	}
}

func TestPublishWithNoSinksDoesNothing(t *testing.T) {
	set := NewSet(nil, time.Second, discardLogger(), nil)
	set.Publish(context.Background(), testBatch()) // must not panic
}

func TestNamesAndLenReportTheConfiguredSinks(t *testing.T) {
	set := NewSet([]Sink{&fakeSink{name: "prometheus"}, &fakeSink{name: "influxv2"}},
		time.Second, discardLogger(), nil)

	if got := set.Len(); got != 2 {
		t.Errorf("Len() = %d, want 2", got)
	}
	names := set.Names()
	if len(names) != 2 || names[0] != "prometheus" || names[1] != "influxv2" {
		t.Errorf("Names() = %v, want [prometheus influxv2]", names)
	}
}

func TestCloseClosesEverySinkEvenWhenOneFails(t *testing.T) {
	// One stubborn sink must not prevent the rest from shutting down cleanly.
	bad := &fakeSink{name: "bad", closeErr: errors.New("could not disconnect")}
	good := &fakeSink{name: "good"}
	set := NewSet([]Sink{bad, good}, time.Second, discardLogger(), nil)

	err := set.Close()

	if err == nil {
		t.Error("Close() = nil, want the failing sink's error reported")
	}
	if good.closeCount() != 1 {
		t.Errorf("the healthy sink was closed %d times, want 1", good.closeCount())
	}
	if !strings.Contains(err.Error(), "bad") {
		t.Errorf("Close() error %q does not name the sink that failed", err)
	}
}

func TestCloseReturnsNilWhenEverySinkClosesCleanly(t *testing.T) {
	set := NewSet([]Sink{&fakeSink{name: "a"}, &fakeSink{name: "b"}},
		time.Second, discardLogger(), nil)

	if err := set.Close(); err != nil {
		t.Errorf("Close() = %v, want nil", err)
	}
}

func TestANonPositiveTimeoutFallsBackToADefault(t *testing.T) {
	// A zero timeout would otherwise create an already-expired context and
	// every publish would fail for a reason that looks nothing like the cause.
	sk := &fakeSink{name: "a"}
	set := NewSet([]Sink{sk}, 0, discardLogger(), nil)

	set.Publish(context.Background(), testBatch())

	if sk.count() != 1 {
		t.Errorf("published %d times with a zero timeout, want 1", sk.count())
	}
}

func TestANilLoggerIsTolerated(t *testing.T) {
	set := NewSet([]Sink{&fakeSink{name: "a"}}, time.Second, nil, nil)
	set.Publish(context.Background(), testBatch()) // must not panic
}

func TestPublishHonoursACancelledContext(t *testing.T) {
	slow := &fakeSink{name: "slow", delay: 5 * time.Second}
	set := NewSet([]Sink{slow}, time.Minute, discardLogger(), nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	set.Publish(ctx, testBatch())

	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("Publish took %v with a cancelled context, want it to return promptly", elapsed)
	}
}

func TestSinksPublishConcurrentlyRatherThanInSequence(t *testing.T) {
	// Four sinks that each take 200ms must finish together in about 200ms, not
	// 800ms. Sequential fan-out would make the slowest backend set the pace for
	// every poll.
	const delay = 200 * time.Millisecond
	sinks := []Sink{
		&fakeSink{name: "a", delay: delay},
		&fakeSink{name: "b", delay: delay},
		&fakeSink{name: "c", delay: delay},
		&fakeSink{name: "d", delay: delay},
	}
	set := NewSet(sinks, time.Second, discardLogger(), nil)

	start := time.Now()
	set.Publish(context.Background(), testBatch())
	elapsed := time.Since(start)

	if elapsed > 3*delay {
		t.Errorf("Publish took %v for four %v sinks, want them to overlap", elapsed, delay)
	}
}
