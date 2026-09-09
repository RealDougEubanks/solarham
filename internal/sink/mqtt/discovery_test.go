package mqtt

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/RealDougEubanks/solarham/internal/metric"
)

// errBrokerRejected stands in for a broker refusing a message.
var errBrokerRejected = errors.New("broker rejected the message")

// discoveryConfigs collects every retained discovery document the sink
// published, keyed by topic and decoded as generic JSON so the assertions are
// about what Home Assistant actually receives rather than about this package's
// structs.
func discoveryConfigs(t *testing.T, client *fakeClient, prefix string) map[string]map[string]any {
	t.Helper()

	out := make(map[string]map[string]any)
	for _, m := range client.sent() {
		if !strings.HasPrefix(m.topic, prefix+"/") {
			continue
		}
		if !m.retained {
			// A discovery document that is not retained is lost the moment
			// Home Assistant restarts.
			t.Errorf("discovery config on %s was published without the retain flag", m.topic)
		}
		out[m.topic] = decodeJSON(t, m.payload)
	}
	return out
}

func TestDiscoveryDocumentMatchesHomeAssistantConventions(t *testing.T) {
	s, client := newTestSink(t, testConfig())

	sample := metric.Sample{
		Desc:   metric.MUF,
		Labels: []string{"AU930", "Austin, TX, USA"},
		Value:  18.4,
		Time:   time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC),
	}
	if err := s.Publish(context.Background(), batchOf(sample)); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	configs := discoveryConfigs(t, client, "homeassistant")
	if len(configs) != 1 {
		t.Fatalf("published %d discovery configs, want 1: %v", len(configs), client.topics())
	}

	var topic string
	var got map[string]any
	for k, v := range configs {
		topic, got = k, v
	}

	wantTopic := "homeassistant/sensor/solarham/" + objectID(sample) + "/config"
	if topic != wantTopic {
		t.Errorf("discovery topic = %q, want %q", topic, wantTopic)
	}

	want := map[string]any{
		"name":                  "Muf Megahertz AU930 Austin, TX, USA",
		"unique_id":             "solarham_" + objectID(sample),
		"state_topic":           "solarham/muf_megahertz/AU930/Austin_TX_USA",
		"value_template":        "{{ value_json.value }}",
		"availability_topic":    "solarham/availability",
		"payload_available":     "online",
		"payload_not_available": "offline",
		"unit_of_measurement":   "MHz",
		"device_class":          "frequency",
		"state_class":           "measurement",
		"device": map[string]any{
			"identifiers":  []any{"solarham"},
			"name":         "Space Weather",
			"manufacturer": "solarham",
			"model":        "Space Weather Exporter",
			"sw_version":   Version,
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("discovery document:\ngot  %#v\nwant %#v", got, want)
	}
}

func TestDiscoveryIsPublishedOnceAndNotRepublishedForTheSameSeries(t *testing.T) {
	s, client := newTestSink(t, testConfig())
	batch := batchOf(gaugeSample("30m-20m", "day", 2))

	if err := s.Publish(context.Background(), batch); err != nil {
		t.Fatalf("first Publish: %v", err)
	}
	if len(discoveryConfigs(t, client, "homeassistant")) != 1 {
		t.Fatalf("first publish sent %d discovery configs, want 1", len(discoveryConfigs(t, client, "homeassistant")))
	}

	client.reset()
	if err := s.Publish(context.Background(), batch); err != nil {
		t.Fatalf("second Publish: %v", err)
	}

	// Republishing thousands of identical retained configs every minute is
	// abusive to the broker and buys nothing: the configs are retained.
	if n := len(discoveryConfigs(t, client, "homeassistant")); n != 0 {
		t.Errorf("second publish sent %d discovery configs, want 0", n)
	}
	if _, ok := client.messageFor("solarham/band_condition/30m-20m/day"); !ok {
		t.Error("second publish sent no state message")
	}
}

func TestDiscoveryIsRepublishedAfterAReconnect(t *testing.T) {
	s, client := newTestSink(t, testConfig())
	batch := batchOf(gaugeSample("30m-20m", "day", 2))

	if err := s.Publish(context.Background(), batch); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// A broker that restarted lost every retained message it held, so a fresh
	// connection has to be treated as a fresh broker.
	s.onConnected()
	client.reset()

	if err := s.Publish(context.Background(), batch); err != nil {
		t.Fatalf("Publish after reconnect: %v", err)
	}
	if n := len(discoveryConfigs(t, client, "homeassistant")); n != 1 {
		t.Errorf("published %d discovery configs after a reconnect, want 1", n)
	}
}

func TestDiscoveryIsNotPublishedWhenDisabled(t *testing.T) {
	cfg := testConfig()
	cfg.DiscoveryEnabled = false

	s, client := newTestSink(t, cfg)
	if err := s.Publish(context.Background(), batchOf(gaugeSample("30m-20m", "day", 2))); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	for _, topic := range client.topics() {
		if strings.HasPrefix(topic, "homeassistant/") {
			t.Errorf("published %s with discovery disabled", topic)
		}
	}
}

func TestDifferentLabelSetsGetDifferentTopicsAndUniqueIDs(t *testing.T) {
	s, client := newTestSink(t, testConfig())

	day := gaugeSample("30m-20m", "day", 2)
	night := gaugeSample("30m-20m", "night", 1)
	if err := s.Publish(context.Background(), batchOf(day, night)); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	if s.stateTopic(day) == s.stateTopic(night) {
		t.Errorf("both label sets published to %s", s.stateTopic(day))
	}
	if s.uniqueID(day) == s.uniqueID(night) {
		// Two entities sharing a unique_id is the one discovery mistake Home
		// Assistant does not recover from: the second config silently replaces
		// the first.
		t.Errorf("both label sets produced unique_id %s", s.uniqueID(day))
	}
	if n := len(discoveryConfigs(t, client, "homeassistant")); n != 2 {
		t.Errorf("published %d discovery configs for two series, want 2", n)
	}
}

func TestLabelSetsThatSanitiseAlikeStillGetDistinctUniqueIDs(t *testing.T) {
	s, _ := newTestSink(t, testConfig())

	// Sanitising is lossy, so these two share a topic. Identity must not
	// depend on it.
	a := metric.Sample{Desc: metric.FoF2, Labels: []string{"AU930", "Austin, TX"}, Value: 8, Time: time.Now()}
	b := metric.Sample{Desc: metric.FoF2, Labels: []string{"AU930", "Austin/TX"}, Value: 8, Time: time.Now()}

	if s.stateTopic(a) != s.stateTopic(b) {
		t.Skip("these label values no longer collide under sanitising; the guard below is what matters")
	}
	if s.uniqueID(a) == s.uniqueID(b) {
		t.Errorf("colliding topics produced the same unique_id %s", s.uniqueID(a))
	}
}

func TestUniqueIDIsStableAcrossSinkInstances(t *testing.T) {
	first, _ := newTestSink(t, testConfig())
	second, _ := newTestSink(t, testConfig())

	sample := gaugeSample("30m-20m", "day", 2)
	// An unstable unique_id makes a restarted exporter create duplicate
	// entities rather than adopting its own.
	if first.uniqueID(sample) != second.uniqueID(sample) {
		t.Errorf("unique_id changed across instances: %q then %q",
			first.uniqueID(sample), second.uniqueID(sample))
	}
}

func TestKindInfoDiscoveryReadsTheCategoryAndCarriesNoStateClass(t *testing.T) {
	s, client := newTestSink(t, testConfig())

	sample := metric.Sample{
		Desc:   metric.GeomagneticFieldInfo,
		Labels: []string{"QUIET"},
		Value:  1,
		Time:   time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC),
	}
	if err := s.Publish(context.Background(), batchOf(sample)); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	configs := discoveryConfigs(t, client, "homeassistant")
	doc, ok := configs["homeassistant/sensor/solarham/"+objectID(sample)+"/config"]
	if !ok {
		t.Fatalf("no discovery config for the info sample, got %v", client.topics())
	}

	if doc["value_template"] != "{{ value_json.category }}" {
		t.Errorf("value_template = %v, want the category template", doc["value_template"])
	}
	if _, present := doc["state_class"]; present {
		// A state_class on a non-numeric sensor makes Home Assistant try to
		// record statistics for it and log an error on every message.
		t.Errorf("info sensor carries state_class %v, want it omitted", doc["state_class"])
	}
	if _, present := doc["unit_of_measurement"]; present {
		t.Errorf("info sensor carries unit_of_measurement %v, want it omitted", doc["unit_of_measurement"])
	}
}

func TestDiscoveryOmitsUnitsHomeAssistantWouldReject(t *testing.T) {
	s, client := newTestSink(t, testConfig())

	samples := []metric.Sample{
		{Desc: metric.FluxSFU, Value: 142, Time: time.Now()},                                         // {sfu}
		{Desc: metric.KIndexEstimated, Value: 3, Time: time.Now()},                                   // {index}
		{Desc: metric.SunspotNumber, Value: 61, Time: time.Now()},                                    // {count}
		{Desc: metric.GeomagneticStormScale, Value: 1, Time: time.Now()},                             // {scale}
		{Desc: metric.SignalNoise, Labels: []string{"min"}, Value: 2, Time: time.Now()},              // {s_unit}
		{Desc: metric.ProtonFlux, Labels: []string{">=10 MeV"}, Value: 1, Time: time.Now()},          // particles
		{Desc: metric.BandCondition, Labels: []string{"30m-20m", "day"}, Value: 2, Time: time.Now()}, // {ordinal}
	}
	if err := s.Publish(context.Background(), batchOf(samples...)); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	configs := discoveryConfigs(t, client, "homeassistant")
	for _, sample := range samples {
		topic := "homeassistant/sensor/solarham/" + objectID(sample) + "/config"
		doc, ok := configs[topic]
		if !ok {
			t.Errorf("no discovery config for %s", sample.Desc.FullName())
			continue
		}
		// Home Assistant has no unit for a solar flux unit, a K-index or an
		// ordinal. Emitting one anyway would either be rejected or displayed as
		// though it meant something.
		if _, present := doc["unit_of_measurement"]; present {
			t.Errorf("%s carries unit_of_measurement %v, want it omitted",
				sample.Desc.FullName(), doc["unit_of_measurement"])
		}
		if _, present := doc["device_class"]; present {
			t.Errorf("%s carries device_class %v, want it omitted",
				sample.Desc.FullName(), doc["device_class"])
		}
		if doc["state_class"] != "measurement" {
			t.Errorf("%s state_class = %v, want measurement", sample.Desc.FullName(), doc["state_class"])
		}
	}
}

func TestHAUnitMapsOnlyTheUnitsHomeAssistantUnderstands(t *testing.T) {
	tests := []struct {
		ucum        string
		wantUnit    string
		wantDevices string
	}{
		{"nT", "nT", ""},
		{"km/s", "km/s", ""},
		{"MHz", "MHz", "frequency"},
		{"km", "km", "distance"},
		{"GW", "GW", "power"},
		{"W/m2", "W/m²", "irradiance"},
		{"deg", "°", ""},
		{"/cm3", "cm⁻³", ""},
		{"{sfu}", "", ""},
		{"{index}", "", ""},
		{"{ordinal}", "", ""},
		{"{count}", "", ""},
		{"{scale}", "", ""},
		{"{s_unit}", "", ""},
		{"{particles}/(cm2.s.sr)", "", ""},
		{"1", "", ""},
		{"", "", ""},
	}
	for _, tt := range tests {
		unit, class := haUnit(tt.ucum)
		if unit != tt.wantUnit || class != tt.wantDevices {
			t.Errorf("haUnit(%q) = (%q, %q), want (%q, %q)",
				tt.ucum, unit, class, tt.wantUnit, tt.wantDevices)
		}
	}
}

func TestEveryDescriptorUnitIsAccountedFor(t *testing.T) {
	// A new descriptor with an unmapped unit should be a deliberate decision,
	// not a silently dropped unit. Every unit in the table must either map to a
	// Home Assistant unit or be a UCUM annotation this package knowingly drops.
	for _, unit := range descriptorUnits() {
		mapped, _ := haUnit(unit)
		annotation := unit == "" || unit == "1" || strings.HasPrefix(unit, "{")
		if mapped == "" && !annotation {
			t.Errorf("descriptor unit %q maps to no Home Assistant unit and is not an annotation", unit)
		}
	}
}

// descriptorUnits lists every unit used in the descriptor table, so the test
// above fails when a new one appears rather than when someone notices.
func descriptorUnits() []string {
	descs := []*metric.Descriptor{
		metric.FluxSFU, metric.FluxNinetyDayMeanSFU, metric.SunspotNumber,
		metric.XRayFlux, metric.XRayClassInfo, metric.ProtonFlux, metric.ElectronFlux,
		metric.AIndex, metric.KIndex, metric.KIndexEstimated, metric.DstNanotesla,
		metric.GeomagneticFieldInfo, metric.WindSpeed, metric.WindDensity,
		metric.WindMagneticField, metric.AuroraHemisphericPower, metric.AuroraBoundary,
		metric.GeomagneticStormScale, metric.RadioBlackoutScale, metric.RadiationStormScale,
		metric.AlertActive, metric.DRAPMaxFrequency, metric.BandCondition,
		metric.BandConditionInfo, metric.VHFCondition, metric.VHFConditionInfo,
		metric.SignalNoise, metric.FoF2, metric.MUF,
	}
	seen := make(map[string]bool)
	out := make([]string, 0, len(descs))
	for _, d := range descs {
		if d == nil || seen[d.Unit] {
			continue
		}
		seen[d.Unit] = true
		out = append(out, d.Unit)
	}
	return out
}

func TestObjectIDIsSafeForADiscoveryTopic(t *testing.T) {
	sample := metric.Sample{
		Desc:   metric.FoF2,
		Labels: []string{"AU930", "Austin, TX, USA"},
		Value:  8,
		Time:   time.Now(),
	}
	id := objectID(sample)
	for _, r := range id {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_', r == '-':
		default:
			t.Fatalf("object id %q contains %q, which Home Assistant does not accept", id, r)
		}
	}
	if strings.Contains(id, "//") || strings.HasPrefix(id, "_") || strings.HasSuffix(id, "_") {
		t.Errorf("object id %q is not well formed", id)
	}
}

func TestDiscoveryFailureDoesNotStopTheStateMessage(t *testing.T) {
	s, client := newTestSink(t, testConfig())

	sample := gaugeSample("30m-20m", "day", 2)
	client.failWith = errBrokerRejected
	client.failTopic = "homeassistant/sensor/solarham/" + objectID(sample) + "/config"

	// Discovery failing costs auto-configuration, not data.
	if err := s.Publish(context.Background(), batchOf(sample)); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if _, ok := client.messageFor("solarham/band_condition/30m-20m/day"); !ok {
		t.Error("state message was not published after a discovery failure")
	}
	if s.isAnnounced(sample) {
		t.Error("series recorded as announced after the discovery publish failed")
	}
}
