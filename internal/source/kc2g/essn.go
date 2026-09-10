package kc2g

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/RealDougEubanks/solarham/internal/metric"
	"github.com/RealDougEubanks/solarham/internal/redact"
)

// essnPreferredSeries is the averaging window this source publishes.
//
// The endpoint returns several windows keyed by their length. The 24 hour one is
// the least noisy and is what the site's own map uses, so it is the one that
// gets published; the shorter windows swing on individual station outages.
const essnPreferredSeries = "24h"

// essnPoint is one entry in one series. Time is a Unix epoch in seconds, not a
// formatted timestamp like the one in stations.json.
type essnPoint struct {
	Time int64    `json:"time"`
	SSN  *float64 `json:"ssn"`
	SFI  *float64 `json:"sfi"`
}

// pollESSN fetches and converts essn.json.
func (s *Source) pollESSN(ctx context.Context) ([]metric.Sample, error) {
	endpoint := s.baseURL + pathESSN + "?days=" + essnDays

	body, err := s.fetch(ctx, endpoint)
	if err != nil {
		return nil, err
	}

	var series map[string]json.RawMessage
	if err := json.Unmarshal(body, &series); err != nil {
		return nil, fmt.Errorf("kc2g: parsing %s: %w", redact.URL(endpoint), err)
	}
	if len(series) == 0 {
		return nil, fmt.Errorf("kc2g: %s carried no series", redact.URL(endpoint))
	}

	key, point, ok := newestPoint(series)
	if !ok {
		return nil, fmt.Errorf("kc2g: %s carried no usable data point", redact.URL(endpoint))
	}
	if key != essnPreferredSeries {
		// Worth saying out loud: the published numbers are then an average over
		// a different window than usual, which changes how twitchy they look.
		s.log.Info("kc2g essn has no "+essnPreferredSeries+" series; using another window",
			"series", key)
	}

	observed := time.Unix(point.Time, 0).UTC()
	samples := make([]metric.Sample, 0, 2)
	if point.SSN != nil && *point.SSN != 0 {
		samples = s.emit(samples, metric.Sample{
			Desc: metric.EffectiveSunspotNumber, Value: *point.SSN, Time: observed,
		})
	}
	if point.SFI != nil && *point.SFI != 0 {
		samples = s.emit(samples, metric.Sample{
			Desc: metric.EffectiveFluxSFU, Value: *point.SFI, Time: observed,
		})
	}

	s.log.Debug("kc2g effective indices processed",
		"series", key, "observed", observed, "samples", len(samples))

	return samples, nil
}

// newestPoint returns the most recent point from the preferred series, falling
// back to the other windows in a stable order so that a missing key degrades
// rather than failing.
func newestPoint(series map[string]json.RawMessage) (string, essnPoint, bool) {
	keys := make([]string, 0, len(series))
	for k := range series {
		if k != essnPreferredSeries {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	if _, ok := series[essnPreferredSeries]; ok {
		keys = append([]string{essnPreferredSeries}, keys...)
	}

	for _, key := range keys {
		var points []essnPoint
		if err := json.Unmarshal(series[key], &points); err != nil {
			continue
		}

		// The series arrives in ascending time order, but the newest entry is
		// picked by comparison rather than by position: an out-of-order feed
		// would otherwise publish an older value as the current one.
		var newest essnPoint
		found := false
		for _, p := range points {
			if p.Time <= 0 {
				continue
			}
			if !found || p.Time > newest.Time {
				newest, found = p, true
			}
		}
		if found {
			return key, newest, true
		}
	}
	return "", essnPoint{}, false
}
