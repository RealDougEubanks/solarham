package metric

import (
	"sync"
	"testing"
	"time"
)

// newTestStore returns a store whose clock the test controls, so expiry can be
// exercised without sleeping.
func newTestStore(t *testing.T, ttl time.Duration) (*Store, func(time.Duration)) {
	t.Helper()

	var mu sync.Mutex
	now := time.Date(2026, 9, 9, 3, 0, 0, 0, time.UTC)

	s := NewStore(ttl)
	s.now = func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return now
	}
	advance := func(d time.Duration) {
		mu.Lock()
		defer mu.Unlock()
		now = now.Add(d)
	}
	return s, advance
}

func gauge(d *Descriptor, value float64, at time.Time, labels ...string) Sample {
	return Sample{Desc: d, Labels: labels, Value: value, Time: at}
}

func TestStoreReturnsWhatWasStored(t *testing.T) {
	s, _ := newTestStore(t, time.Hour)
	at := testTime()

	s.Replace(Batch{Source: "swpc", Samples: []Sample{gauge(FluxSFU, 110, at)}})

	got := s.Samples()
	if len(got) != 1 {
		t.Fatalf("Samples() returned %d samples, want 1", len(got))
	}
	if got[0].Value != 110 {
		t.Errorf("value = %v, want 110", got[0].Value)
	}
}

func TestStoreSupersedesAnEarlierObservationOfTheSameSeries(t *testing.T) {
	s, _ := newTestStore(t, time.Hour)
	at := testTime()

	s.Replace(Batch{Samples: []Sample{gauge(FluxSFU, 110, at)}})
	s.Replace(Batch{Samples: []Sample{gauge(FluxSFU, 125, at.Add(time.Minute))}})

	got := s.Samples()
	if len(got) != 1 {
		t.Fatalf("Samples() returned %d samples, want 1", len(got))
	}
	if got[0].Value != 125 {
		t.Errorf("value = %v, want the newer 125", got[0].Value)
	}
}

// Sources that return a window of history rather than a single point can emit
// an older observation after a newer one. The newest reading is the one that
// should survive.
func TestStoreKeepsTheNewestSampleWhenOneArrivesOutOfOrder(t *testing.T) {
	s, _ := newTestStore(t, time.Hour)
	at := testTime()

	s.Replace(Batch{Samples: []Sample{gauge(FluxSFU, 125, at.Add(time.Minute))}})
	s.Replace(Batch{Samples: []Sample{gauge(FluxSFU, 110, at)}})

	got := s.Samples()
	if len(got) != 1 {
		t.Fatalf("Samples() returned %d samples, want 1", len(got))
	}
	if got[0].Value != 125 {
		t.Errorf("value = %v, want the newer 125 to have survived the out-of-order write", got[0].Value)
	}
}

func TestStoreKeepsDifferentLabelSetsApart(t *testing.T) {
	s, _ := newTestStore(t, time.Hour)
	at := testTime()

	s.Replace(Batch{Samples: []Sample{
		gauge(BandCondition, 2, at, "30m-20m", "day"),
		gauge(BandCondition, 1, at, "30m-20m", "night"),
	}})

	if got := s.Len(); got != 2 {
		t.Errorf("Len() = %d, want 2 distinct series", got)
	}
}

// This is the behaviour the whole store exists for. A field the upstream has
// stopped publishing must eventually vanish rather than sitting on a dashboard
// at its last value, presented as current.
func TestAnExpiredSampleDisappears(t *testing.T) {
	s, advance := newTestStore(t, 10*time.Minute)
	s.Replace(Batch{Samples: []Sample{gauge(FluxSFU, 110, testTime())}})

	if s.Len() != 1 {
		t.Fatalf("Len() = %d before expiry, want 1", s.Len())
	}

	advance(11 * time.Minute)

	if got := s.Len(); got != 0 {
		t.Errorf("Len() = %d after the TTL elapsed, want 0", got)
	}
	if got := s.Samples(); len(got) != 0 {
		t.Errorf("Samples() returned %d samples after the TTL elapsed, want 0", len(got))
	}
}

func TestASampleSurvivesUntilItsTTLElapses(t *testing.T) {
	s, advance := newTestStore(t, 10*time.Minute)
	s.Replace(Batch{Samples: []Sample{gauge(FluxSFU, 110, testTime())}})

	advance(9 * time.Minute)

	if got := s.Len(); got != 1 {
		t.Errorf("Len() = %d just before the TTL, want 1", got)
	}
}

func TestRefreshingASampleResetsItsExpiry(t *testing.T) {
	s, advance := newTestStore(t, 10*time.Minute)
	at := testTime()

	s.Replace(Batch{Samples: []Sample{gauge(FluxSFU, 110, at)}})
	advance(9 * time.Minute)
	s.Replace(Batch{Samples: []Sample{gauge(FluxSFU, 111, at.Add(9*time.Minute))}})
	advance(9 * time.Minute)

	if got := s.Len(); got != 1 {
		t.Errorf("Len() = %d, want 1: the refresh should have reset the expiry", got)
	}
}

func TestAZeroTTLDisablesExpiry(t *testing.T) {
	s, advance := newTestStore(t, 0)
	s.Replace(Batch{Samples: []Sample{gauge(FluxSFU, 110, testTime())}})

	advance(365 * 24 * time.Hour)

	if got := s.Len(); got != 1 {
		t.Errorf("Len() = %d, want the sample retained when the TTL is disabled", got)
	}
}

// An invalid sample would be rejected by Prometheus at collection time, which
// breaks the entire scrape rather than one series. Dropping it here contains
// the damage.
func TestStoreDropsAnInvalidSample(t *testing.T) {
	s, _ := newTestStore(t, time.Hour)

	s.Replace(Batch{Samples: []Sample{
		{Desc: BandCondition, Labels: []string{"30m-20m"}, Time: testTime()}, // too few labels
		gauge(FluxSFU, 110, testTime()),                                      // valid
	}})

	got := s.Samples()
	if len(got) != 1 {
		t.Fatalf("Samples() returned %d samples, want only the valid one", len(got))
	}
	if got[0].Desc != FluxSFU {
		t.Errorf("the surviving sample was %s, want solar_flux_sfu", got[0].Desc.Name)
	}
}

func TestAnEmptyBatchChangesNothing(t *testing.T) {
	s, _ := newTestStore(t, time.Hour)
	s.Replace(Batch{Samples: []Sample{gauge(FluxSFU, 110, testTime())}})

	s.Replace(Batch{Source: "swpc"})

	if got := s.Len(); got != 1 {
		t.Errorf("Len() = %d, want the existing sample untouched by an empty batch", got)
	}
}

func TestSamplesAreOrderedStably(t *testing.T) {
	s, _ := newTestStore(t, time.Hour)
	at := testTime()

	s.Replace(Batch{Samples: []Sample{
		gauge(WindSpeed, 403.9, at),
		gauge(BandCondition, 2, at, "30m-20m", "day"),
		gauge(FluxSFU, 110, at),
		gauge(BandCondition, 1, at, "12m-10m", "day"),
	}})

	first := s.Samples()
	second := s.Samples()

	if len(first) != len(second) {
		t.Fatalf("Samples() returned %d then %d", len(first), len(second))
	}
	for i := range first {
		if first[i].Key() != second[i].Key() {
			t.Fatalf("Samples() ordering was not stable at index %d: %q then %q",
				i, first[i].Key(), second[i].Key())
		}
	}
	for i := 1; i < len(first); i++ {
		if first[i-1].Desc.Name > first[i].Desc.Name {
			t.Errorf("Samples() is not ordered by metric name: %s before %s",
				first[i-1].Desc.Name, first[i].Desc.Name)
		}
	}
}

func TestSweepRemovesExpiredEntries(t *testing.T) {
	s, advance := newTestStore(t, 10*time.Minute)
	at := testTime()

	s.Replace(Batch{Samples: []Sample{gauge(FluxSFU, 110, at), gauge(WindSpeed, 403.9, at)}})
	advance(11 * time.Minute)

	if removed := s.Sweep(); removed != 2 {
		t.Errorf("Sweep() removed %d entries, want 2", removed)
	}
	if removed := s.Sweep(); removed != 0 {
		t.Errorf("a second Sweep() removed %d entries, want 0", removed)
	}
}

func TestSweepKeepsLiveEntries(t *testing.T) {
	s, _ := newTestStore(t, 10*time.Minute)
	s.Replace(Batch{Samples: []Sample{gauge(FluxSFU, 110, testTime())}})

	if removed := s.Sweep(); removed != 0 {
		t.Errorf("Sweep() removed %d live entries, want 0", removed)
	}
	if s.Len() != 1 {
		t.Errorf("Len() = %d after sweeping live entries, want 1", s.Len())
	}
}

// The store is written by several source goroutines and read by every scrape,
// so this must hold under -race.
func TestConcurrentReplaceAndReadDoNotRace(t *testing.T) {
	s := NewStore(time.Hour)
	at := testTime()

	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := range 50 {
				s.Replace(Batch{Samples: []Sample{
					gauge(FluxSFU, float64(i*j), at.Add(time.Duration(j)*time.Second)),
					gauge(BandCondition, 1, at, "30m-20m", "day"),
				}})
			}
		}(i)
	}
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 50 {
				_ = s.Samples()
				_ = s.Len()
				_ = s.Sweep()
			}
		}()
	}
	wg.Wait()
}

// Several quantities here come from more than one upstream and they are not
// equally good: the 10.7 cm flux from the observatory that measures it versus
// an agency republishing a daily figure, Kp from GFZ as the definitive index
// versus NOAA as a US-subnetwork estimate. The better source has to win
// regardless of which happened to poll last.
func TestAHigherAuthorityWinsEvenWithAnOlderObservation(t *testing.T) {
	s, _ := newTestStore(t, time.Hour)
	at := testTime()

	// The weaker source polls second and carries a newer observation.
	s.Replace(Batch{Source: "drao", Authority: 10,
		Samples: []Sample{gauge(FluxSFU, 110, at)}})
	s.Replace(Batch{Source: "swpc", Authority: 0,
		Samples: []Sample{gauge(FluxSFU, 999, at.Add(time.Hour))}})

	got := s.Samples()
	if len(got) != 1 {
		t.Fatalf("Samples() returned %d, want 1", len(got))
	}
	if got[0].Value != 110 {
		t.Errorf("value = %v, want 110 from the authoritative source despite its older timestamp", got[0].Value)
	}
}

func TestALowerAuthorityCannotDisplaceAHigherOne(t *testing.T) {
	s, _ := newTestStore(t, time.Hour)
	at := testTime()

	// Arrive in the other order, to prove the rule is not about arrival.
	s.Replace(Batch{Source: "swpc", Authority: 0,
		Samples: []Sample{gauge(FluxSFU, 999, at.Add(time.Hour))}})
	s.Replace(Batch{Source: "drao", Authority: 10,
		Samples: []Sample{gauge(FluxSFU, 110, at)}})

	if got := s.Samples()[0].Value; got != 110 {
		t.Errorf("value = %v, want 110: authority must not depend on arrival order", got)
	}
}

// Within one authority the newer observation still wins, which is what
// discards the out-of-order samples a windowed source emits.
func TestEqualAuthorityFallsBackToTheNewerObservation(t *testing.T) {
	s, _ := newTestStore(t, time.Hour)
	at := testTime()

	s.Replace(Batch{Source: "a", Authority: 5, Samples: []Sample{gauge(FluxSFU, 110, at)}})
	s.Replace(Batch{Source: "b", Authority: 5,
		Samples: []Sample{gauge(FluxSFU, 125, at.Add(time.Minute))}})

	if got := s.Samples()[0].Value; got != 125 {
		t.Errorf("value = %v, want the newer 125 at equal authority", got)
	}
}

func TestAnUnrankedSourceStillStoresItsOwnSeries(t *testing.T) {
	// Most sources publish quantities nobody else touches, so a zero authority
	// must behave normally rather than being treated as untrusted.
	s, _ := newTestStore(t, time.Hour)

	s.Replace(Batch{Source: "kiwisdr", Samples: []Sample{
		{Desc: NoiseFloor, Labels: []string{"shack", "1800_10000"}, Value: -96, Time: testTime()},
	}})

	if s.Len() != 1 {
		t.Errorf("Len() = %d, want an unranked source's sample stored", s.Len())
	}
}

// A source that loses a collision must not have its other series suppressed --
// SWPC loses the flux to DRAO but is the only source for the NOAA scales.
func TestLosingOneSeriesDoesNotAffectAnother(t *testing.T) {
	s, _ := newTestStore(t, time.Hour)
	at := testTime()

	s.Replace(Batch{Source: "drao", Authority: 10, Samples: []Sample{gauge(FluxSFU, 110, at)}})
	s.Replace(Batch{Source: "swpc", Authority: 0, Samples: []Sample{
		gauge(FluxSFU, 999, at.Add(time.Hour)),
		gauge(GeomagneticStormScale, 2, at.Add(time.Hour)),
	}})

	var flux, scale float64
	for _, sample := range s.Samples() {
		switch sample.Desc {
		case FluxSFU:
			flux = sample.Value
		case GeomagneticStormScale:
			scale = sample.Value
		}
	}
	if flux != 110 {
		t.Errorf("flux = %v, want 110 from DRAO", flux)
	}
	if scale != 2 {
		t.Errorf("storm scale = %v, want 2: losing the flux must not suppress SWPC's other series", scale)
	}
}
