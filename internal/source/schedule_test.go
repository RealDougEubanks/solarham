package source

import (
	"strings"
	"testing"
	"time"
)

func at(hour, minute int) TimeOfDay { return TimeOfDay{Hour: hour, Minute: minute} }

func utc(y int, mo time.Month, d, h, mi int) time.Time {
	return time.Date(y, mo, d, h, mi, 0, 0, time.UTC)
}

func TestEveryPollsOnAFixedSpacing(t *testing.T) {
	s := Every(5 * time.Minute)
	now := utc(2026, 9, 9, 13, 0)

	if got, want := s.NextAfter(now), now.Add(5*time.Minute); !got.Equal(want) {
		t.Errorf("NextAfter = %v, want %v", got, want)
	}
	if got := s.Interval(); got != 5*time.Minute {
		t.Errorf("Interval = %v, want 5m", got)
	}
}

func TestEveryRejectsANonPositiveInterval(t *testing.T) {
	// A zero interval would spin the poll loop, so it falls back rather than
	// being accepted and causing a busy source at runtime.
	if got := Every(0).Interval(); got != time.Hour {
		t.Errorf("Every(0).Interval() = %v, want the 1h fallback", got)
	}
}

// The whole point of the schedule abstraction: a document written once a day
// should be fetched once a day, shortly after it appears.
func TestDailyAtPicksTheNextSlotToday(t *testing.T) {
	s := DailyAt(10*time.Minute, at(17, 0), at(20, 0), at(23, 0))

	got := s.NextAfter(utc(2026, 9, 9, 13, 0))

	earliest := utc(2026, 9, 9, 17, 10)
	if got.Before(earliest) || got.After(earliest.Add(90*time.Second)) {
		t.Errorf("NextAfter = %v, want 17:10 plus at most 90s of spread", got)
	}
}

func TestDailyAtRollsOverToTomorrowAfterTheLastSlot(t *testing.T) {
	s := DailyAt(10*time.Minute, at(17, 0), at(20, 0), at(23, 0))

	got := s.NextAfter(utc(2026, 9, 9, 23, 30))

	earliest := utc(2026, 9, 10, 17, 10)
	if got.Before(earliest) || got.After(earliest.Add(90*time.Second)) {
		t.Errorf("NextAfter = %v, want tomorrow at 17:10", got)
	}
}

// A publisher's stated time is when it starts writing, not when the file is
// readable. Polling at exactly the stated minute fetches yesterday's copy and
// then waits a full day to notice.
func TestDailyAtAppliesTheLag(t *testing.T) {
	withLag := DailyAt(25*time.Minute, at(2, 25))
	got := withLag.NextAfter(utc(2026, 9, 9, 0, 0))

	earliest := utc(2026, 9, 9, 2, 50)
	if got.Before(earliest) {
		t.Errorf("NextAfter = %v, want no earlier than 02:50 with a 25m lag", got)
	}
}

func TestDailyAtNeverReturnsAPastTime(t *testing.T) {
	s := DailyAt(0, at(0, 0), at(6, 0), at(12, 0), at(18, 0))

	// Walk the whole day a minute at a time and assert monotonic progress.
	for h := range 24 {
		for _, m := range []int{0, 1, 30, 59} {
			now := utc(2026, 9, 9, h, m)
			if next := s.NextAfter(now); !next.After(now) {
				t.Fatalf("NextAfter(%v) = %v, which is not in the future", now, next)
			}
		}
	}
}

func TestDailyAtIntervalIsTheShortestGapBetweenSlots(t *testing.T) {
	tests := []struct {
		name  string
		times []TimeOfDay
		want  time.Duration
	}{
		{"a single daily slot is a day", []TimeOfDay{at(2, 25)}, 24 * time.Hour},
		{"evenly spaced slots", []TimeOfDay{at(0, 0), at(12, 0)}, 12 * time.Hour},
		{"the shortest gap wins", []TimeOfDay{at(17, 0), at(20, 0), at(23, 0)}, 3 * time.Hour},
		{"the wrap-around gap counts", []TimeOfDay{at(0, 0), at(1, 0)}, time.Hour},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := DailyAt(0, tc.times...).Interval(); got != tc.want {
				t.Errorf("Interval = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestDailyAtWithNoSlotsFallsBackToDaily(t *testing.T) {
	if got := DailyAt(0).Interval(); got != 24*time.Hour {
		t.Errorf("Interval = %v, want 24h", got)
	}
}

func TestDailyAtIsUnaffectedBySlotOrdering(t *testing.T) {
	forward := DailyAt(0, at(2, 0), at(14, 0))
	reversed := DailyAt(0, at(14, 0), at(2, 0))

	if forward.Interval() != reversed.Interval() {
		t.Errorf("interval depended on argument order: %v vs %v",
			forward.Interval(), reversed.Interval())
	}
}

// Every deployment of this exporter must not hit the same publisher in the
// same second.
func TestDailyAtSpreadsPollsAcrossDeployments(t *testing.T) {
	s := DailyAt(0, at(12, 0))
	now := utc(2026, 9, 9, 6, 0)

	seen := make(map[time.Time]bool)
	for range 100 {
		seen[s.NextAfter(now)] = true
	}
	if len(seen) < 5 {
		t.Errorf("NextAfter produced only %d distinct times over 100 calls; the spread is not working", len(seen))
	}
}

// A courtesy limit an operator can override by editing a setting is not a
// limit, so the clamp lives in the schedule.
func TestAtLeastEnforcesAFloorAgainstAFasterSchedule(t *testing.T) {
	s := AtLeast(15*time.Minute, Every(time.Minute))
	now := utc(2026, 9, 9, 13, 0)

	if got, want := s.NextAfter(now), now.Add(15*time.Minute); !got.Equal(want) {
		t.Errorf("NextAfter = %v, want the 15m floor at %v", got, want)
	}
	if got := s.Interval(); got != 15*time.Minute {
		t.Errorf("Interval = %v, want the floor to win", got)
	}
}

func TestAtLeastLeavesASlowerScheduleAlone(t *testing.T) {
	s := AtLeast(15*time.Minute, Every(time.Hour))
	now := utc(2026, 9, 9, 13, 0)

	if got, want := s.NextAfter(now), now.Add(time.Hour); !got.Equal(want) {
		t.Errorf("NextAfter = %v, want the hourly schedule preserved at %v", got, want)
	}
	if got := s.Interval(); got != time.Hour {
		t.Errorf("Interval = %v, want 1h", got)
	}
}

func TestAtLeastWithANonPositiveFloorIsAPassThrough(t *testing.T) {
	inner := Every(time.Minute)
	if got := AtLeast(0, inner); got != inner {
		t.Error("AtLeast(0, …) should return the inner schedule unchanged")
	}
}

func TestParseTimeOfDayReadsValidTimes(t *testing.T) {
	tests := map[string]TimeOfDay{
		"00:00": at(0, 0),
		"02:25": at(2, 25),
		"17:00": at(17, 0),
		"23:59": at(23, 59),
	}
	for in, want := range tests {
		got, err := ParseTimeOfDay(in)
		if err != nil {
			t.Errorf("ParseTimeOfDay(%q) errored: %v", in, err)
			continue
		}
		if got != want {
			t.Errorf("ParseTimeOfDay(%q) = %v, want %v", in, got, want)
		}
	}
}

func TestParseTimeOfDayRejectsMalformedInput(t *testing.T) {
	for _, in := range []string{"", "12", "24:00", "12:60", "-1:00", "aa:bb", "12:", ":30"} {
		if _, err := ParseTimeOfDay(in); err == nil {
			t.Errorf("ParseTimeOfDay(%q) accepted invalid input", in)
		}
	}
}

// The schedule string lands on /health, so it has to say something an operator
// can act on.
func TestScheduleStringsDescribeThemselves(t *testing.T) {
	tests := []struct {
		sched    Schedule
		contains []string
	}{
		{Every(5 * time.Minute), []string{"every", "5m"}},
		{DailyAt(10*time.Minute, at(17, 0), at(20, 0)), []string{"daily", "17:00", "20:00", "UTC", "10m"}},
		{AtLeast(15*time.Minute, Every(time.Minute)), []string{"no faster than", "15m"}},
	}
	for _, tc := range tests {
		got := tc.sched.String()
		for _, want := range tc.contains {
			if !strings.Contains(got, want) {
				t.Errorf("%q does not mention %q", got, want)
			}
		}
	}
}

// scheduledFake implements Scheduled so the scheduler's preference can be
// tested.
type scheduledFake struct {
	fakeSource
	sched Schedule
}

func (f *scheduledFake) Schedule() Schedule { return f.sched }

func TestScheduleForPrefersADeclaredSchedule(t *testing.T) {
	src := &scheduledFake{
		fakeSource: fakeSource{name: "daily", interval: time.Hour},
		sched:      DailyAt(0, at(2, 25)),
	}

	got := scheduleFor(src)
	if got.Interval() != 24*time.Hour {
		t.Errorf("scheduleFor returned interval %v, want the declared 24h schedule to win over the 1h Interval()",
			got.Interval())
	}
}

func TestScheduleForFallsBackToTheInterval(t *testing.T) {
	src := &fakeSource{name: "plain", interval: 90 * time.Second}

	if got := scheduleFor(src).Interval(); got != 90*time.Second {
		t.Errorf("scheduleFor returned %v, want the source's 90s interval", got)
	}
}

// A source that declares Scheduled but returns nil must not crash the loop.
func TestScheduleForToleratesANilSchedule(t *testing.T) {
	src := &scheduledFake{
		fakeSource: fakeSource{name: "broken", interval: 2 * time.Minute},
		sched:      nil,
	}

	if got := scheduleFor(src).Interval(); got != 2*time.Minute {
		t.Errorf("scheduleFor returned %v, want the fallback interval", got)
	}
}

func TestStatusesReportTheDeclaredSchedule(t *testing.T) {
	src := &scheduledFake{
		fakeSource: fakeSource{name: "drao", interval: time.Hour},
		sched:      DailyAt(10*time.Minute, at(17, 0), at(20, 0), at(23, 0)),
	}
	s := newTestScheduler([]Source{src}, &capturingPublisher{}, discardLogger(), nil)

	st := s.Statuses()[0]
	if !strings.Contains(st.Interval, "daily at") {
		t.Errorf("Status.Interval = %q, want it to describe the daily schedule", st.Interval)
	}
	// Staleness must size off the schedule, not the nominal interval: three
	// slots three hours apart means nine hours, not three.
	if st.StaleAfter != 9*time.Hour {
		t.Errorf("StaleAfter = %v, want 9h (three times the 3h shortest gap)", st.StaleAfter)
	}
}
