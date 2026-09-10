package metric

import (
	"sort"
	"sync"
	"time"
)

// Store holds the most recent sample for each series.
//
// This exists because Prometheus is a pull-based sink over push-based sources.
// Five sources poll on five schedules; a scrape can arrive at any moment and
// must be answered from whatever was most recently observed.
//
// The store expires entries rather than holding them forever. A field the
// upstream has stopped publishing must eventually disappear from /metrics, not
// linger at its last value indefinitely: a solar wind speed from six hours ago,
// presented to a dashboard as current, is worse than no reading. Expiry is
// per-sample against a TTL supplied by the caller, which sets it from the
// owning source's poll interval.
type Store struct {
	mu      sync.RWMutex
	entries map[string]entry
	ttl     time.Duration
	now     func() time.Time
}

type entry struct {
	sample    Sample
	received  time.Time
	authority int
	source    string
}

// NewStore returns a store that expires samples older than ttl.
//
// A ttl of zero or less disables expiry, which is intended for tests rather
// than production.
func NewStore(ttl time.Duration) *Store {
	return &Store{
		entries: make(map[string]entry),
		ttl:     ttl,
		now:     time.Now,
	}
}

// Replace stores every sample in the batch, superseding earlier observations of
// the same series.
//
// Samples that fail validation are dropped rather than stored. An invalid
// sample would be rejected by Prometheus at collection time, which surfaces the
// problem as a broken scrape of every metric rather than as one missing series.
func (s *Store) Replace(b Batch) {
	if len(b.Samples) == 0 {
		return
	}
	received := s.now()

	s.mu.Lock()
	defer s.mu.Unlock()
	for _, sample := range b.Samples {
		if sample.Validate() != nil {
			continue
		}
		key := sample.Key()

		if existing, ok := s.entries[key]; ok && !supersedes(existing, b, sample) {
			continue
		}
		s.entries[key] = entry{
			sample:    sample,
			received:  received,
			authority: b.Authority,
			source:    b.Source,
		}
	}
}

// supersedes reports whether an incoming sample should replace the stored one.
//
// Authority is decided first: where two upstreams publish the same quantity and
// one is better -- the observatory that took the measurement rather than an
// agency republishing it -- the better one wins regardless of which polled more
// recently. Otherwise the newer observation wins, which is what discards the
// out-of-order samples that arrive when a source returns a window of history
// rather than a single point.
func supersedes(existing entry, b Batch, sample Sample) bool {
	if b.Authority != existing.authority {
		return b.Authority > existing.authority
	}
	return !existing.sample.Time.After(sample.Time)
}

// Samples returns every unexpired sample, ordered by metric name and then by
// label values so that output is stable across calls.
func (s *Store) Samples() []Sample {
	now := s.now()

	s.mu.RLock()
	out := make([]Sample, 0, len(s.entries))
	for _, e := range s.entries {
		if s.expired(e, now) {
			continue
		}
		out = append(out, e.sample)
	}
	s.mu.RUnlock()

	sort.Slice(out, func(i, j int) bool {
		if out[i].Desc.Name != out[j].Desc.Name {
			return out[i].Desc.Name < out[j].Desc.Name
		}
		return out[i].Key() < out[j].Key()
	})
	return out
}

// Len reports how many unexpired samples the store holds.
func (s *Store) Len() int {
	now := s.now()

	s.mu.RLock()
	defer s.mu.RUnlock()
	n := 0
	for _, e := range s.entries {
		if !s.expired(e, now) {
			n++
		}
	}
	return n
}

// Sweep deletes expired entries, bounding memory for series that stop being
// reported. Samples() already hides expired entries, so this is housekeeping
// rather than correctness, and callers run it on a slow ticker.
func (s *Store) Sweep() int {
	now := s.now()

	s.mu.Lock()
	defer s.mu.Unlock()
	removed := 0
	for key, e := range s.entries {
		if s.expired(e, now) {
			delete(s.entries, key)
			removed++
		}
	}
	return removed
}

// expired reports whether an entry has outlived the store's TTL. The caller
// must hold at least a read lock.
func (s *Store) expired(e entry, now time.Time) bool {
	if s.ttl <= 0 {
		return false
	}
	return now.Sub(e.received) > s.ttl
}
