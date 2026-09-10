package source

import (
	"fmt"
	"math/rand/v2"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Schedule decides when a source is next polled.
//
// A fixed interval is the wrong model for most of these upstreams. NOAA's
// daily solar indices are regenerated once, at about 02:25 UT; DRAO Penticton
// measures the solar flux three times a day at 17:00, 20:00 and 23:00 UT; the
// ARRL's logbook activity file is rebuilt roughly weekly. Polling any of those
// every five minutes fetches an identical document hundreds of times to learn
// nothing, which is rude to the publisher and pointless for us.
//
// So a source declares when its upstream actually publishes, and the scheduler
// polls shortly afterwards.
type Schedule interface {
	// NextAfter returns the next poll time, strictly after t.
	NextAfter(t time.Time) time.Time

	// Interval is the nominal spacing between polls. It sizes the failure
	// backoff, and is what a source reports as its cadence.
	Interval() time.Duration

	// StaleAfter is how long this source may legitimately go without a
	// successful poll before something is actually wrong.
	//
	// This is deliberately not derived from Interval. A schedule whose slots
	// are unevenly spaced is quiet for its *longest* gap, not its shortest,
	// and sizing readiness off the shortest one reports a perfectly healthy
	// source as failed for hours every day.
	StaleAfter() time.Duration

	// String describes the schedule for logs and the health endpoints.
	String() string
}

// Every returns a Schedule that polls on a fixed interval.
//
// This is the right choice for an upstream that genuinely changes continuously
// — solar wind at one minute, say — or for one whose publication clock is not
// documented.
func Every(d time.Duration) Schedule {
	if d <= 0 {
		d = time.Hour
	}
	return intervalSchedule{d: d}
}

type intervalSchedule struct{ d time.Duration }

func (s intervalSchedule) NextAfter(t time.Time) time.Time { return t.Add(s.d) }
func (s intervalSchedule) Interval() time.Duration         { return s.d }
func (s intervalSchedule) String() string                  { return "every " + s.d.String() }

// StaleAfter allows two missed polls before calling a fixed-interval source
// stale, so a single blip does not flap readiness.
func (s intervalSchedule) StaleAfter() time.Duration { return 3 * s.d }

// TimeOfDay is a wall-clock time in UTC.
type TimeOfDay struct {
	Hour   int
	Minute int
}

// ParseTimeOfDay reads "HH:MM" as a UTC time of day.
func ParseTimeOfDay(s string) (TimeOfDay, error) {
	h, m, ok := strings.Cut(s, ":")
	if !ok {
		return TimeOfDay{}, fmt.Errorf("time of day %q is not HH:MM", s)
	}
	hour, err := strconv.Atoi(h)
	if err != nil || hour < 0 || hour > 23 {
		return TimeOfDay{}, fmt.Errorf("time of day %q has an invalid hour", s)
	}
	minute, err := strconv.Atoi(m)
	if err != nil || minute < 0 || minute > 59 {
		return TimeOfDay{}, fmt.Errorf("time of day %q has an invalid minute", s)
	}
	return TimeOfDay{Hour: hour, Minute: minute}, nil
}

func (t TimeOfDay) String() string { return fmt.Sprintf("%02d:%02d", t.Hour, t.Minute) }

// minutes is the time of day as minutes past midnight, for ordering.
func (t TimeOfDay) minutes() int { return t.Hour*60 + t.Minute }

// DailyAt returns a Schedule that polls once per UTC day at each listed time,
// plus lag.
//
// The lag exists because a publisher's stated time is when it starts writing,
// not when the file is readable. Polling at exactly 02:25 for a document
// generated at 02:25 fetches yesterday's copy and then waits 24 hours to
// notice. A few minutes of lag costs nothing and removes the race.
//
// Times with no lag applied are still spread by a small random offset, so that
// every deployment of this exporter does not hit the same publisher in the same
// second.
func DailyAt(lag time.Duration, at ...TimeOfDay) Schedule {
	if len(at) == 0 {
		return Every(24 * time.Hour)
	}
	times := make([]TimeOfDay, len(at))
	copy(times, at)
	sort.Slice(times, func(i, j int) bool { return times[i].minutes() < times[j].minutes() })

	if lag < 0 {
		lag = 0
	}
	return dailySchedule{times: times, lag: lag, spread: 90 * time.Second}
}

type dailySchedule struct {
	times  []TimeOfDay
	lag    time.Duration
	spread time.Duration
}

// NextAfter finds the next publication time strictly after t, then adds the
// lag and a random spread.
func (s dailySchedule) NextAfter(t time.Time) time.Time {
	utc := t.UTC()
	day := time.Date(utc.Year(), utc.Month(), utc.Day(), 0, 0, 0, 0, time.UTC)

	// Look at today's slots, then tomorrow's. Two days is always enough
	// because the slot list covers one day.
	for d := range 2 {
		base := day.AddDate(0, 0, d)
		for _, at := range s.times {
			candidate := base.Add(time.Duration(at.minutes()) * time.Minute).Add(s.lag)
			if candidate.After(utc) {
				return candidate.Add(s.jitter())
			}
		}
	}

	// Unreachable while times is non-empty, but a schedule that returns a past
	// time would spin the poll loop, so fall back to a day out.
	return utc.Add(24 * time.Hour)
}

func (s dailySchedule) jitter() time.Duration {
	if s.spread <= 0 {
		return 0
	}
	return time.Duration(rand.Int64N(int64(s.spread)))
}

// Interval is the shortest gap between consecutive slots, which is what
// staleness and backoff should be sized against. For a single daily slot that
// is 24 hours.
func (s dailySchedule) Interval() time.Duration {
	if len(s.times) == 1 {
		return 24 * time.Hour
	}
	shortest := 24 * time.Hour
	for i := range s.times {
		next := s.times[(i+1)%len(s.times)]
		gap := next.minutes() - s.times[i].minutes()
		if gap <= 0 {
			gap += 24 * 60
		}
		if d := time.Duration(gap) * time.Minute; d < shortest {
			shortest = d
		}
	}
	return shortest
}

// StaleAfter is the longest quiet period the schedule allows, plus the
// publication lag and an hour of grace.
//
// The longest gap is the load-bearing part. DRAO Penticton publishes at 17:00,
// 20:00 and 23:00 UTC: the gaps are three hours, three hours, and then
// eighteen hours until the next day. Sizing readiness off the three-hour gap
// marked a healthy source failed from 08:00 UTC every morning until its 17:00
// slot came round -- roughly nine hours of false alarm a day, on the endpoint
// the README tells operators to point an uptime monitor at.
func (s dailySchedule) StaleAfter() time.Duration {
	return s.longestGap() + s.lag + time.Hour
}

// longestGap is the longest interval between consecutive slots, wrapping
// around midnight.
func (s dailySchedule) longestGap() time.Duration {
	if len(s.times) <= 1 {
		return 24 * time.Hour
	}
	longest := time.Duration(0)
	for i := range s.times {
		next := s.times[(i+1)%len(s.times)]
		gap := next.minutes() - s.times[i].minutes()
		if gap <= 0 {
			gap += 24 * 60
		}
		if d := time.Duration(gap) * time.Minute; d > longest {
			longest = d
		}
	}
	return longest
}

func (s dailySchedule) String() string {
	parts := make([]string, 0, len(s.times))
	for _, at := range s.times {
		parts = append(parts, at.String())
	}
	out := "daily at " + strings.Join(parts, ", ") + " UTC"
	if s.lag > 0 {
		out += " +" + s.lag.String()
	}
	return out
}

// AtLeast returns sched clamped so it never polls more often than floor.
//
// This is how an upstream's stated courtesy limit is enforced against
// configuration. A limit an operator can override by editing a setting is not
// a limit, so the clamp lives in the schedule rather than in validation.
func AtLeast(floor time.Duration, sched Schedule) Schedule {
	if floor <= 0 || sched == nil {
		return sched
	}
	return flooredSchedule{floor: floor, inner: sched}
}

type flooredSchedule struct {
	floor time.Duration
	inner Schedule
}

func (s flooredSchedule) NextAfter(t time.Time) time.Time {
	next := s.inner.NextAfter(t)
	if earliest := t.Add(s.floor); next.Before(earliest) {
		return earliest
	}
	return next
}

func (s flooredSchedule) Interval() time.Duration {
	if inner := s.inner.Interval(); inner > s.floor {
		return inner
	}
	return s.floor
}

// StaleAfter takes the inner schedule's window, but never less than the floor
// would imply -- a floor that slows polling must also slow the point at which
// we call the source stale.
func (s flooredSchedule) StaleAfter() time.Duration {
	if inner := s.inner.StaleAfter(); inner > 3*s.floor {
		return inner
	}
	return 3 * s.floor
}

func (s flooredSchedule) String() string {
	return fmt.Sprintf("%s, no faster than %s", s.inner, s.floor)
}
