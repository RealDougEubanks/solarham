package metric

import (
	"strings"
	"testing"
	"time"
)

func testTime() time.Time {
	return time.Date(2026, 9, 9, 3, 22, 0, 0, time.UTC)
}

func TestFullNameAppliesThePrefix(t *testing.T) {
	d := &Descriptor{Name: "flux_sfu"}
	if got, want := d.FullName(), "solar_flux_sfu"; got != want {
		t.Errorf("FullName() = %q, want %q", got, want)
	}
}

func TestEveryDescriptorInTheTableIsWellFormed(t *testing.T) {
	for _, d := range All {
		if d.Name == "" {
			t.Errorf("descriptor with help %q has no name", d.Help)
			continue
		}
		if d.Help == "" {
			t.Errorf("descriptor %s has no help text", d.Name)
		}
		if strings.HasPrefix(d.Name, Prefix) {
			t.Errorf("descriptor %s already carries the prefix; Name must exclude it", d.Name)
		}
		if strings.ToLower(d.Name) != d.Name {
			t.Errorf("descriptor %s is not lower case", d.Name)
		}
		if strings.Contains(d.Name, " ") || strings.Contains(d.Name, "-") {
			t.Errorf("descriptor %s must be snake_case", d.Name)
		}
		for _, l := range d.Labels {
			if l == "" {
				t.Errorf("descriptor %s has an empty label name", d.Name)
			}
			if strings.ToLower(l) != l {
				t.Errorf("descriptor %s label %q is not lower case", d.Name, l)
			}
		}
	}
}

func TestDescriptorNamesAreUnique(t *testing.T) {
	seen := make(map[string]bool, len(All))
	for _, d := range All {
		if seen[d.Name] {
			t.Errorf("descriptor name %s appears more than once in All", d.Name)
		}
		seen[d.Name] = true
	}
}

// An info metric that carried no label would publish as a bare constant 1 with
// nothing to identify what it described, which is the one shape that makes the
// kind pointless.
func TestEveryInfoDescriptorCarriesAtLeastOneLabel(t *testing.T) {
	for _, d := range All {
		if d.Kind == KindInfo && len(d.Labels) == 0 {
			t.Errorf("info descriptor %s has no labels, so it carries no information", d.Name)
		}
	}
}

func TestGaugeDescriptorsDeclareAUnit(t *testing.T) {
	for _, d := range All {
		if d.Kind == KindGauge && d.Unit == "" {
			t.Errorf("gauge descriptor %s declares no unit, which OTLP requires", d.Name)
		}
	}
}

func TestValidateRejectsAMismatchedLabelCount(t *testing.T) {
	s := Sample{Desc: BandCondition, Labels: []string{"30m-20m"}, Time: testTime()}
	err := s.Validate()
	if err == nil {
		t.Fatal("Validate() = nil, want an error for too few label values")
	}
	if !strings.Contains(err.Error(), "solar_band_condition") {
		t.Errorf("error %q does not name the metric", err)
	}
}

func TestValidateRejectsAnEmptyLabelValue(t *testing.T) {
	// InfluxDB drops empty tags silently and Prometheus treats an empty label
	// as absent, so the same sample would mean different things in different
	// sinks. Rejecting it here keeps them consistent.
	s := Sample{Desc: BandCondition, Labels: []string{"30m-20m", ""}, Time: testTime()}
	err := s.Validate()
	if err == nil {
		t.Fatal("Validate() = nil, want an error for an empty label value")
	}
	if !strings.Contains(err.Error(), "period") {
		t.Errorf("error %q does not name the offending label", err)
	}
}

func TestValidateRejectsAMissingTimestamp(t *testing.T) {
	s := Sample{Desc: FluxSFU, Value: 110}
	if err := s.Validate(); err == nil {
		t.Fatal("Validate() = nil, want an error for a zero timestamp")
	}
}

func TestValidateRejectsAMissingDescriptor(t *testing.T) {
	s := Sample{Time: testTime()}
	if err := s.Validate(); err == nil {
		t.Fatal("Validate() = nil, want an error for a nil descriptor")
	}
}

func TestValidateAcceptsAWellFormedSample(t *testing.T) {
	s := Sample{Desc: BandCondition, Labels: []string{"30m-20m", "day"}, Value: 2, Time: testTime()}
	if err := s.Validate(); err != nil {
		t.Errorf("Validate() = %v, want nil", err)
	}
}

func TestValidateAcceptsAGaugeWithNoLabels(t *testing.T) {
	s := Sample{Desc: FluxSFU, Value: 110, Time: testTime()}
	if err := s.Validate(); err != nil {
		t.Errorf("Validate() = %v, want nil", err)
	}
}

func TestLabelForFindsALabelByName(t *testing.T) {
	s := Sample{Desc: BandCondition, Labels: []string{"30m-20m", "night"}, Time: testTime()}

	got, ok := s.LabelFor("period")
	if !ok {
		t.Fatal("LabelFor(period) reported the label missing")
	}
	if got != "night" {
		t.Errorf("LabelFor(period) = %q, want %q", got, "night")
	}
}

func TestLabelForReportsAnUnknownLabelMissing(t *testing.T) {
	s := Sample{Desc: BandCondition, Labels: []string{"30m-20m", "night"}, Time: testTime()}
	if _, ok := s.LabelFor("station"); ok {
		t.Error("LabelFor(station) reported a label the descriptor does not declare")
	}
}

// Two samples of the same metric with different labels are different series and
// must never share a key, or one would silently overwrite the other in the
// store.
func TestKeyDistinguishesLabelSets(t *testing.T) {
	day := Sample{Desc: BandCondition, Labels: []string{"30m-20m", "day"}, Time: testTime()}
	night := Sample{Desc: BandCondition, Labels: []string{"30m-20m", "night"}, Time: testTime()}

	if day.Key() == night.Key() {
		t.Errorf("Key() collided for different label sets: %q", day.Key())
	}
}

// A naive key built by concatenating label values would make {"a","bc"} and
// {"ab","c"} collide. The separator is what prevents that.
func TestKeyDoesNotCollideAcrossLabelBoundaries(t *testing.T) {
	first := Sample{Desc: BandCondition, Labels: []string{"a", "bc"}, Time: testTime()}
	second := Sample{Desc: BandCondition, Labels: []string{"ab", "c"}, Time: testTime()}

	if first.Key() == second.Key() {
		t.Errorf("Key() collided across a label boundary: %q", first.Key())
	}
}

func TestKeyIsStableForTheSameSeries(t *testing.T) {
	a := Sample{Desc: FluxSFU, Value: 110, Time: testTime()}
	b := Sample{Desc: FluxSFU, Value: 999, Time: testTime().Add(time.Hour)}

	if a.Key() != b.Key() {
		t.Errorf("Key() differed for the same series: %q vs %q", a.Key(), b.Key())
	}
}

func TestBatchLenCountsSamples(t *testing.T) {
	b := Batch{Samples: []Sample{{Desc: FluxSFU}, {Desc: KIndexEstimated}}}
	if got := b.Len(); got != 2 {
		t.Errorf("Len() = %d, want 2", got)
	}
}

func TestKindStringNamesTheKind(t *testing.T) {
	if got := KindGauge.String(); got != "gauge" {
		t.Errorf("KindGauge.String() = %q, want %q", got, "gauge")
	}
	if got := KindInfo.String(); got != "info" {
		t.Errorf("KindInfo.String() = %q, want %q", got, "info")
	}
}
