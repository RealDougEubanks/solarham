package mqtt

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/RealDougEubanks/solarham/internal/metric"
)

// manufacturer and model populate the Home Assistant device block. There is no
// hardware here — the data comes from NOAA SWPC, hamqsl.com and kc2g — so the
// "device" Home Assistant shows is this exporter itself.
const (
	manufacturer = "solarham"
	model        = "Space Weather Exporter"
)

// stateClassMeasurement puts a sensor into Home Assistant's long-term
// statistics engine. Every numeric metric in this exporter's vocabulary is an
// instantaneous reading rather than a running total, so measurement is right
// for all of them and total/total_increasing is right for none.
const stateClassMeasurement = "measurement"

// Home Assistant device classes used here. Only quantities whose Home Assistant
// device class genuinely matches get one. A wrong device class is not a
// cosmetic error: Home Assistant validates the unit against it and either
// rejects the entity or silently converts values into a different quantity.
const (
	deviceClassFrequency  = "frequency"
	deviceClassDistance   = "distance"
	deviceClassPower      = "power"
	deviceClassIrradiance = "irradiance"
)

// Value templates. A gauge's state is the number; a KindInfo sensor's state is
// the category string, because publishing the constant 1 to Home Assistant
// would show every band as "1" forever.
const (
	templateValue    = "{{ value_json.value }}"
	templateCategory = "{{ value_json.category }}"
)

// haDevice is the device block. It is byte-for-byte identical in every sensor's
// config, which is what makes Home Assistant group the entities under one
// device instead of creating one device per sensor.
type haDevice struct {
	Identifiers  []string `json:"identifiers"`
	Name         string   `json:"name"`
	Manufacturer string   `json:"manufacturer"`
	Model        string   `json:"model"`
	SWVersion    string   `json:"sw_version,omitempty"`
}

// haSensor is one MQTT sensor discovery document.
//
// The availability fields are what stop a dead exporter from looking healthy:
// Home Assistant marks the entity unavailable the moment the broker delivers
// the retained will, instead of continuing to display the last K-index as
// though it were current.
type haSensor struct {
	Name                string   `json:"name"`
	UniqueID            string   `json:"unique_id"`
	StateTopic          string   `json:"state_topic"`
	ValueTemplate       string   `json:"value_template"`
	AvailabilityTopic   string   `json:"availability_topic"`
	PayloadAvailable    string   `json:"payload_available"`
	PayloadNotAvailable string   `json:"payload_not_available"`
	UnitOfMeasurement   string   `json:"unit_of_measurement,omitempty"`
	DeviceClass         string   `json:"device_class,omitempty"`
	StateClass          string   `json:"state_class,omitempty"`
	Icon                string   `json:"icon,omitempty"`
	Device              haDevice `json:"device"`
}

// isAnnounced reports whether this series' discovery document has already been
// published on the current connection.
func (s *Sink) isAnnounced(sample metric.Sample) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.announced[seriesKey(sample)]
}

// announce publishes one series' discovery document and records it.
//
// The record is kept only on success, so a broker that rejected the config is
// retried on the next batch rather than leaving an entity that Home Assistant
// never heard of publishing to a topic nobody reads.
func (s *Sink) announce(ctx context.Context, sample metric.Sample, stateTopic string) error {
	doc, err := s.discoveryDocument(sample, stateTopic)
	if err != nil {
		return err
	}

	// Retained, so Home Assistant picks the entity up whenever it next starts
	// rather than only if it happened to be listening at this moment.
	if err := s.publish(ctx, s.discoveryTopic(sample), string(doc), true); err != nil {
		return err
	}

	s.mu.Lock()
	s.announced[seriesKey(sample)] = true
	s.mu.Unlock()
	return nil
}

// discoveryTopic is where Home Assistant looks for one sensor's config.
func (s *Sink) discoveryTopic(sample metric.Sample) string {
	return fmt.Sprintf("%s/sensor/%s/%s/config",
		s.cfg.DiscoveryPrefix, s.deviceID, objectID(sample))
}

// objectID is the last topic segment of the discovery topic and the suffix of
// the unique_id.
//
// For a series with labels the sanitised form is followed by a short hash of
// the raw series key. Sanitising is lossy — "Austin, TX, USA" and
// "Austin/TX/USA" reduce to the same string — and two entities sharing a
// unique_id is the one discovery mistake Home Assistant does not recover from:
// the second config silently replaces the first. The hash makes that
// impossible without making the common case unreadable.
func objectID(sample metric.Sample) string {
	parts := make([]string, 0, len(sample.Labels)+2)
	parts = append(parts, sample.Desc.Name)
	parts = append(parts, sample.Labels...)
	if len(sample.Labels) > 0 {
		parts = append(parts, seriesHash(sample))
	}
	return sanitizeID(strings.Join(parts, "_"), emptySegment)
}

// uniqueID identifies the entity across restarts.
//
// It is derived from the device id, the metric name and the label values, all
// of which are stable properties of the series, so a restarted exporter adopts
// the entities it created last time instead of creating duplicates.
func (s *Sink) uniqueID(sample metric.Sample) string {
	return s.deviceID + "_" + objectID(sample)
}

// entityName is what Home Assistant shows, after prefixing it with the device
// name. Label values are appended as they were written upstream, since
// "Band Condition 30m-20m day" is what an operator recognises.
func entityName(sample metric.Sample) string {
	name := titleize(sample.Desc.Name)
	if len(sample.Labels) == 0 {
		return name
	}
	return name + " " + strings.Join(sample.Labels, " ")
}

// titleize turns a metric name into a human label: "k_index" to "K Index".
func titleize(name string) string {
	words := strings.Split(name, "_")
	for i, w := range words {
		if w == "" {
			continue
		}
		words[i] = strings.ToUpper(w[:1]) + w[1:]
	}
	return strings.Join(words, " ")
}

// discoveryDocument renders one sensor's config.
func (s *Sink) discoveryDocument(sample metric.Sample, stateTopic string) ([]byte, error) {
	sensor := haSensor{
		Name:                entityName(sample),
		UniqueID:            s.uniqueID(sample),
		StateTopic:          stateTopic,
		ValueTemplate:       templateValue,
		AvailabilityTopic:   s.availTopic,
		PayloadAvailable:    payloadOnline,
		PayloadNotAvailable: payloadOffline,
		Device:              s.deviceBlock(),
	}

	if sample.Desc.Kind == metric.KindInfo {
		// The state is a word, not a number. A state_class on a non-numeric
		// sensor makes Home Assistant try to record statistics for it and log
		// an error on every single message.
		sensor.ValueTemplate = templateCategory
	} else {
		sensor.StateClass = stateClassMeasurement
		sensor.UnitOfMeasurement, sensor.DeviceClass = haUnit(sample.Desc.Unit)
	}

	doc, err := json.Marshal(sensor)
	if err != nil {
		return nil, fmt.Errorf("mqtt: marshal discovery for %s: %w", sample.Desc.FullName(), err)
	}
	return doc, nil
}

// deviceBlock builds the shared device block.
func (s *Sink) deviceBlock() haDevice {
	return haDevice{
		Identifiers:  []string{s.deviceID},
		Name:         s.cfg.DeviceName,
		Manufacturer: manufacturer,
		Model:        model,
		SWVersion:    Version,
	}
}

// haUnit maps a descriptor's UCUM unit to what Home Assistant expects, and to a
// device class where one genuinely applies.
//
// Home Assistant is not a general unit system. A typed sensor accepts only the
// units its device class knows, and there is no device class at all for a solar
// flux unit, a K-index, an ordinal or a particle flux. UCUM's annotation
// syntax — anything in braces, plus the dimensionless "1" — covers exactly
// those cases, so they return no unit rather than a string Home Assistant would
// display as if it meant something. An entity with no unit still graphs and
// still records statistics; an entity with an invalid unit for its device class
// is rejected outright.
func haUnit(unit string) (unitOfMeasurement, deviceClass string) {
	switch unit {
	case "nT":
		// Nanotesla. No Home Assistant device class covers magnetic flux
		// density, so the unit stands alone.
		return "nT", ""
	case "km/s":
		// Home Assistant's speed device class knows m/s, km/h, mph and knots,
		// but not km/s, so this carries the unit without a class.
		return "km/s", ""
	case "MHz":
		return "MHz", deviceClassFrequency
	case "km":
		return "km", deviceClassDistance
	case "GW":
		return "GW", deviceClassPower
	case "W/m2":
		// Irradiance wants the typographic squared sign.
		return "W/m²", deviceClassIrradiance
	case "deg":
		// Degrees of magnetic latitude. Not an angle Home Assistant has a class
		// for, and emphatically not a temperature.
		return "°", ""
	case "/cm3":
		return "cm⁻³", ""
	}

	// Everything else — "{sfu}", "{index}", "{ordinal}", "{count}", "{scale}",
	// "{s_unit}", "{particles}/(cm2.s.sr)", "1" and the empty unit — is
	// dimensionless or an annotation Home Assistant cannot use.
	return "", ""
}
