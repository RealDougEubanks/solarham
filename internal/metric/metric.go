// Package metric defines the vocabulary this exporter publishes: the static
// descriptor for every metric, the sample type that carries one observation of
// one descriptor, and the batch a source produces from one poll.
//
// Descriptors are declared once, here, and every sink derives its naming, help
// text and units from this table. That is what keeps a Prometheus series, an
// InfluxDB field and an MQTT topic describing the same quantity by the same
// name.
package metric

import (
	"fmt"
	"time"
)

// Prefix is prepended to every metric name.
//
// The domain is space weather rather than the Sun alone, but "solar" is what
// operators call this data and what the project is named for, so the shorter
// prefix wins over a more literal one.
const Prefix = "solar_"

// Kind decides how a sample is represented in each sink.
type Kind uint8

const (
	// KindGauge is a numeric instantaneous value.
	KindGauge Kind = iota

	// KindInfo is a categorical value. It publishes as the constant 1 with the
	// category carried in a label, which is the only way Prometheus can
	// represent a string. Sinks that can store text directly, such as InfluxDB
	// and MQTT, publish the label value instead.
	KindInfo
)

// String names the kind for logs and error messages.
func (k Kind) String() string {
	switch k {
	case KindGauge:
		return "gauge"
	case KindInfo:
		return "info"
	default:
		return fmt.Sprintf("Kind(%d)", uint8(k))
	}
}

// Descriptor is the static definition of one metric.
//
// Name excludes Prefix; FullName applies it. Unit is UCUM, which OTLP requires
// and the other sinks ignore.
type Descriptor struct {
	Name   string
	Help   string
	Unit   string
	Kind   Kind
	Labels []string
}

// FullName is the metric name as published, including Prefix.
func (d *Descriptor) FullName() string { return Prefix + d.Name }

// Sample is one observation of one descriptor.
//
// Time is the upstream's own observation time, never time.Now(). The exporter
// this replaces stamped every point at write time, so a three-hourly K-index
// was rewritten hundreds of times per interval under hundreds of distinct
// timestamps. A sample whose source publishes no timestamp uses the time the
// response was received, which is the closest honest approximation available.
type Sample struct {
	Desc   *Descriptor
	Labels []string
	Value  float64
	Time   time.Time
}

// Batch is everything one source produced in one poll.
//
// A poll that legitimately produced nothing — an unchanged document answered
// with 304, or a feed whose optional fields were all empty — yields a batch
// with no samples. That is a success, not a failure, and sinks must treat it as
// one.
type Batch struct {
	Source  string
	Fetched time.Time
	Samples []Sample

	// Authority breaks ties when two sources publish the same series.
	//
	// Several quantities here are available from more than one upstream, and
	// they are not equally good. The 10.7 cm flux comes both from NOAA, which
	// republishes a single daily value, and from DRAO Penticton, the
	// observatory whose instrument actually makes the measurement three times
	// a day. Kp comes from NOAA as a US-subnetwork estimate and from GFZ
	// Potsdam as the definitive index. In each case one answer is better.
	//
	// Without an explicit ordering the winner would be whichever source
	// happened to poll last with a newer upstream timestamp, so the series
	// would flap between two subtly different numbers for no visible reason.
	// A higher Authority wins outright; equal authorities fall back to the
	// newer upstream observation.
	//
	// The scheduler stamps this from a table declared in main, so the ordering
	// lives in one place next to its reasoning rather than being implied by
	// whichever source was written first.
	Authority int
}

// Len reports how many samples the batch carries.
func (b Batch) Len() int { return len(b.Samples) }

// LabelFor returns the value of the named label, and whether it was present.
//
// Sinks need this to find a specific label — the category on a KindInfo sample,
// say — without assuming a position in the descriptor's label list.
func (s Sample) LabelFor(name string) (string, bool) {
	if s.Desc == nil {
		return "", false
	}
	for i, l := range s.Desc.Labels {
		if l != name {
			continue
		}
		if i >= len(s.Labels) {
			return "", false
		}
		return s.Labels[i], true
	}
	return "", false
}

// Validate reports whether the sample is internally consistent.
//
// A sample whose label values do not match its descriptor's label names would
// be rejected by Prometheus at collection time and would silently corrupt tag
// sets in InfluxDB. Catching it at construction turns a confusing runtime
// failure into an obvious one, so sources are checked as they emit.
func (s Sample) Validate() error {
	if s.Desc == nil {
		return fmt.Errorf("sample has no descriptor")
	}
	if len(s.Labels) != len(s.Desc.Labels) {
		return fmt.Errorf("metric %s expects %d label values %v, got %d %v",
			s.Desc.FullName(), len(s.Desc.Labels), s.Desc.Labels, len(s.Labels), s.Labels)
	}
	for i, v := range s.Labels {
		if v == "" {
			return fmt.Errorf("metric %s has an empty value for label %q",
				s.Desc.FullName(), s.Desc.Labels[i])
		}
	}
	if s.Time.IsZero() {
		return fmt.Errorf("metric %s has no timestamp", s.Desc.FullName())
	}
	return nil
}

// Key identifies the series a sample belongs to: its metric name plus its label
// values. Two samples with the same key are two observations of the same
// series, and the later one supersedes the earlier.
func (s Sample) Key() string {
	if s.Desc == nil {
		return ""
	}
	key := s.Desc.FullName()
	for _, v := range s.Labels {
		key += "\x00" + v
	}
	return key
}
