<!--
doc: DESIGN
last-refreshed: 2026-09-09
generated-by: doc-refresh skill
-->

# solarham-exporter — design

A space-weather and HF propagation exporter. It polls several upstream sources,
normalises what they publish into one metric vocabulary, and fans the result out
to Prometheus, InfluxDB 1.x, InfluxDB 2.x, OTLP and MQTT.

It replaces the Python `solarham.py` that ran in the `dougeubanks/solarham`
container. See [baseline-findings.md](baseline-findings.md) for what was wrong
with that script and why a rewrite rather than a patch.

## Why Go

The sibling project `gogmc320` (`dougeubanks/gmc-exporter`) already established
this exact shape: a Go exporter, distroless image, `sink.Sink` fan-out,
`GOGMC_`-prefixed config with `_FILE` secrets, `/healthz` + `/readyz` +
`/health`, and a GitHub Actions CI/release pipeline publishing multi-arch images
to Docker Hub on `v*` tags. Writing the second exporter in the same language
against the same conventions means one set of patterns to maintain rather than
two. This document deviates from that sibling only where the problem genuinely
differs, and each deviation is called out.

The concrete wins over the Python original are the ones that matter for a
process meant to run unattended for months: a static binary in a ~20 MB
distroless image with no interpreter or package manager, compile-time proof that
every sink satisfies the interface, and a race detector in CI.

## Sources

The single largest defect in the original was polling `hamqsl.com` every 30
seconds. N0NBH's FAQ asks for hourly polling and states he has already been shut
down by his ISP once over exactly this load. His own stated cadences are 3 hours
for flux/A/K/sunspots, 1 hour for X-ray and particle flux, and 30 minutes for
VHF conditions, so 30-second polling was oversampling by up to 360x. The feed
sends no `ETag` and no `Last-Modified`, so conditional GETs are not available
and self-throttling is the only control.

That makes hamqsl the wrong primary source for anything with an authoritative
equivalent. NOAA SWPC publishes most of it first-hand, in the public domain,
with no API key, at far better cadence, and — unlike hamqsl — with `ETag` and
`Last-Modified` so repeat polls cost a 304.

hamqsl is therefore kept for exactly the four things nobody else produces: the
HF band ratings from `calculatedconditions`, the VHF phenomena from
`calculatedvhfconditions`, the `signalnoise` estimate, and the `geomagfield`
wording. Those are N0NBH's own model output. Everything else comes from NOAA.

| Source | Interval | Provides |
|---|---|---|
| `hamqsl` | 1 h | HF band conditions, VHF conditions, signal/noise, geomagnetic field wording |
| `swpc-fast` | 1 min | solar wind speed and density, Bt/Bz, 1-min Kp, X-ray flare class, NOAA G/S/R scales, alerts |
| `swpc-medium` | 5 min | GOES integral protons and electrons, auroral hemispheric power, D-RAP absorption (opt-in) |
| `swpc-slow` | 1 h | F10.7 and its 90-day mean, 3-hourly planetary K and A, sunspot number, Dst |
| `kc2g` | 15 min | foF2, MUF, M(D) factor and hmF2 per ionosonde station; effective SSN/SFI |

Intervals are defaults and are individually configurable, but each is clamped to
a floor that encodes the upstream's stated policy. `hamqsl` cannot be configured
below 900 s and `kc2g` cannot go below 300 s. A courtesy limit that an operator
can casually override is not a limit.

### Sources deliberately not used

- `services.swpc.noaa.gov/products/solar-wind/*` — the entire directory now
  returns 404. It is widely cited in older code and blog posts.
- `json/rtsw/rtsw_wind_1m.json` — the modern replacement for the above, and not
  used for solar wind density. It returns roughly 2.5 MB of 3,478 records
  covering a full day, every minute, to yield one number: about 3.6 GB of daily
  transfer against a public service for a single gauge. `text/ace-swepam.txt`
  carries the same quantity at the same cadence in about 9.5 KB. The trade is
  that it is ACE only, where RTSW fails over between ACE, DSCOVR and IMAP, so
  density becomes absent rather than wrong if ACE alone stops reporting.
- `text/ovation_latest_aurora_n.txt` — fetched and inspected, then dropped. Its
  only usable scalar is hemispheric power, which
  `text/aurora-nowcast-hemi-power.txt` already gives for both hemispheres in a
  fraction of the 560 KB.
- GIRO `lgdc.uml.edu/common/DIDBGetValues` — returns 404; the servlet is gone.
  The working replacement is `fastchar/getbest`, but GIRO rate-limits hard (HTTP
  429 after a handful of requests) and its licence requires registration and
  contacting data providers. `prop.kc2g.com` aggregates the same GIRO and INGV
  data into one quality-scored request, so we use that instead.
- `wsprnet.org` bulk endpoints — the CSV export times out and the public archive
  is months stale.

### Attribution and licensing

Recorded here because two of these have real conditions attached:

- **NOAA SWPC** — public domain, no key, no published rate limit. Must not imply
  NOAA endorsement.
- **hamqsl.com (N0NBH)** — credit requested; poll no faster than hourly.
- **prop.kc2g.com** — CC BY-NC-SA 4.0, **non-commercial only**. Underlying GIRO
  data carries the same terms. Attribution: data from ionosonde operators
  distributed through GIRO and INGV, funded by WWROF.

The `kc2g` source is disabled by default for that reason: a non-commercial
licence should be an opt-in an operator makes knowingly, not something they
inherit from a default.

## Data model

The sibling exporter has a single fixed `reading.Reading` struct because a
Geiger counter reports the same five numbers every poll. This exporter pulls
heterogeneous data from five sources on five schedules, so the model is a
descriptor table plus samples that reference it.

```go
// Kind decides how a sample is represented in each sink.
type Kind uint8

const (
    // KindGauge is a numeric instantaneous value.
    KindGauge Kind = iota
    // KindInfo is a categorical value. It publishes as the constant 1 with the
    // category carried in a label, which is the only way Prometheus can
    // represent a string.
    KindInfo
)

// Descriptor is the static definition of one metric. Descriptors are declared
// once in package metric so that every sink derives its naming, help text and
// units from the same table.
type Descriptor struct {
    Name   string   // without prefix, e.g. "flux_sfu"
    Help   string
    Unit   string   // UCUM, for OTLP
    Kind   Kind
    Labels []string // label names, in order
}

// Sample is one observation of one Descriptor.
type Sample struct {
    Desc   *Descriptor
    Labels []string  // values, positionally matching Desc.Labels
    Value  float64   // KindGauge: the value. KindInfo: always 1.
    Time   time.Time // the upstream's own observation time, never time.Now()
}

// Batch is everything one source produced in one poll.
type Batch struct {
    Source  string
    Fetched time.Time
    Samples []Sample
}
```

Two rules this model exists to enforce:

**Absent is not zero.** A field the upstream did not supply produces no sample.
It must not appear as 0, and it must not retain the previous cycle's value. A
stale solar wind speed reported as current is worse than no reading. The
Prometheus sink is a custom collector over a sample store rather than a set of
registered gauges, precisely so a series can genuinely disappear.

**Timestamps come from upstream.** The original stamped every point with
`time.Now()`, so a 3-hourly K-index was rewritten 360 times per interval under
360 different timestamps. Every sample carries the source's own observation time
— hamqsl's `<updated>`, SWPC's `time_tag`, kc2g's per-station `time`.

## Metric vocabulary

Prefix `solar_`, snake_case, unit-suffixed, matching the sibling's `gmc_`
convention and ordinary Prometheus practice.

| Metric | Kind | Labels | Source |
|---|---|---|---|
| `solar_flux_sfu` | gauge | | swpc |
| `solar_flux_ninety_day_mean_sfu` | gauge | | swpc |
| `solar_sunspot_number` | gauge | | swpc |
| `solar_a_index` | gauge | `station` | swpc |
| `solar_k_index` | gauge | `station` | swpc |
| `solar_k_index_estimated` | gauge | | swpc |
| `solar_xray_flux_watts_per_square_meter` | gauge | `band` | swpc |
| `solar_xray_class_info` | info | `class` | swpc |
| `solar_proton_flux_particles` | gauge | `energy` | swpc |
| `solar_electron_flux_particles` | gauge | `energy` | swpc |
| `solar_wind_speed_kilometers_per_second` | gauge | | swpc |
| `solar_wind_density_protons_per_cubic_centimeter` | gauge | | swpc |
| `solar_wind_magnetic_field_nanotesla` | gauge | `component` (`bt`,`bz`) | swpc |
| `solar_aurora_hemispheric_power_gigawatts` | gauge | `hemisphere` | swpc |
| `solar_geomagnetic_storm_scale` | gauge | | swpc |
| `solar_radio_blackout_scale` | gauge | | swpc |
| `solar_radiation_storm_scale` | gauge | | swpc |
| `solar_dst_nanotesla` | gauge | | swpc |
| `solar_drap_max_absorbed_frequency_megahertz` | gauge | `latitude`,`longitude` | swpc |
| `solar_alert_active` | info | `product_id`,`serial` | swpc |
| `solar_band_condition` | gauge | `band`,`period` | hamqsl |
| `solar_band_condition_info` | info | `band`,`period`,`condition` | hamqsl |
| `solar_vhf_condition` | gauge | `phenomenon`,`location` | hamqsl |
| `solar_vhf_condition_info` | info | `phenomenon`,`location`,`condition` | hamqsl |
| `solar_signal_noise_s_units` | gauge | `bound` (`min`,`max`) | hamqsl |
| `solar_geomagnetic_field_info` | info | `state` | hamqsl |
| `solar_fof2_megahertz` | gauge | `station`,`station_name` | kc2g |
| `solar_muf_megahertz` | gauge | `station`,`station_name` | kc2g |
| `solar_muf_factor` | gauge | `station`,`station_name` | kc2g |
| `solar_hmf2_kilometers` | gauge | `station`,`station_name` | kc2g |
| `solar_station_confidence_score` | gauge | `station` | kc2g |
| `solar_effective_sunspot_number` | gauge | | kc2g |
| `solar_effective_flux_sfu` | gauge | | kc2g |

### Declared but not published

`solar_aurora_equatorward_boundary_degrees` is in the descriptor table and is
deliberately emitted by nothing.

SWPC's OVATION product states no boundary. It is a grid of energy flux by
magnetic local time and magnetic latitude, and deriving a boundary from it means
choosing a flux threshold that the data does not supply. Plausible thresholds
move the answer by several degrees of latitude, so any number published under
this name would be an invention dressed as a measurement.

The descriptor stays so the name and unit are settled if a defensible source
appears. Hemispheric power, which OVATION does state, is published instead.

Self-instrumentation, mirroring the sibling:

| Metric | Kind | Labels |
|---|---|---|
| `solar_source_poll_total` | counter | `source`,`result` |
| `solar_source_poll_duration_seconds` | histogram | `source` |
| `solar_source_last_success_timestamp_seconds` | gauge | `source` |
| `solar_sink_publish_total` | counter | `sink`,`result` |
| `solar_sink_publish_duration_seconds` | histogram | `sink` |
| `solar_build_info` | gauge (always 1) | `version`,`commit`,`goversion` |

### Categorical decoding

The original wrote every value as a string, so `"110"` and `" 29"` landed in
InfluxDB as text with leading whitespace. Everything numeric is parsed to
`float64` here. The genuinely non-numeric fields get deliberate treatment rather
than being passed through:

- **`xray`** — `B3.8` is flare-class notation. It decodes to W/m² as
  `3.8 × 10^-7` (A=1e-8, B=1e-7, C=1e-6, M=1e-5, X=1e-4) and publishes as a
  gauge, with the original class also published as `solar_xray_class_info`.
- **band and VHF conditions** — ordinal so they can be graphed and alerted on:
  `Poor`=0, `Fair`=1, `Good`=2; `Band Closed`=0, `Band Open`=1. The original
  string is preserved in the paired `_info` metric.
- **`signalnoise`** — `S2-S3` publishes as two gauges, `bound="min"` and
  `bound="max"`.
- **`geomagfield`** — genuinely categorical (`UNSETTLD`, `QUIET`, `STORM`), so
  info only. The numeric equivalent is SWPC's G-scale.
- **empty elements** — `fof2`, `muffactor` and `muf` are frequently empty, and
  `muf` carries the literal `NoRpt`. These produce no sample at all.

## Sinks

Ported from the sibling, which already implements all five. The interface is
unchanged except that it publishes a `Batch` rather than a `Reading`:

```go
type Sink interface {
    Name() string
    Publish(ctx context.Context, b metric.Batch) error
    Close() error
}
```

Fan-out keeps the sibling's three-layer isolation: each sink publishes in its
own goroutine, under its own `context.WithTimeout`, behind a `recover()`.
`Set.Publish` returns nothing, because the correct response to a failed publish
is always to carry on to the next poll. All sinks are disabled by default and
startup fails if none is enabled.

| Sink | Notes |
|---|---|
| `prometheus` | Custom collector over a sample store. Serves `/metrics`. Also the `Observer` for the fan-out. |
| `influxv2` | Raw line protocol over `net/http`, no client library, matching the sibling. Supports `org` **or** `orgID`. |
| `influxv1` | Raw line protocol. Database, retention policy, optional auth. |
| `otlp` | gRPC and HTTP, reusing the same metric names with UCUM units. |
| `mqtt` | Publishes per-metric topics plus Home Assistant discovery. |

### InfluxDB and the org bug

The original hardcoded `org='Home'`, which does not exist, and wrote to bucket
`solarham` when the real bucket is `SolarHAM`. Every write 404'd for the life of
the container. The bucket held no solar data at all when audited.

The supplied token is bucket-scoped and cannot read `/api/v2/orgs`, so org-name
resolution fails even for a correct name. The sink therefore accepts `orgID`
directly and prefers it when set. `orgID=8ba47ba1e329d213` with bucket
`SolarHAM` is the combination verified working.

Because the bucket is empty there is no historical schema to preserve, so the
Influx line protocol follows the metric vocabulary above rather than
reproducing the old `measurement=solardata, tag=<name>, field=value` shape.

## Configuration

Env-first with prefix `SOLARHAM_`, precedence flags → env (incl. `_FILE`) →
config file → defaults, exactly as the sibling. Every credential is a
`redact.Secret` sourced from either the plain variable or its `_FILE` variant
but never both. All configuration errors accumulate and report together, so an
operator fixing a container's environment does not restart it six times to find
six mistakes.

### Drop-in compatibility

The point of "drop-in" here is that swapping the image and changing nothing else
keeps working. The old container passes two lowercase variables, `url` and
`token`. Both are accepted as deprecated aliases for
`SOLARHAM_INFLUX2_URL` and `SOLARHAM_INFLUX2_TOKEN`, and their presence
implies `SOLARHAM_INFLUX2_ENABLED=true`. Using either logs a warning naming the
modern variable. Setting both an alias and its modern equivalent is a
configuration error rather than a silent precedence rule.

The bucket and org bug is fixed in the process: with only the legacy variables
set, the sink defaults to bucket `SolarHAM` and requires an org or orgID to be
supplied, failing at startup with a message naming the setting rather than
404ing silently forever.

## HTTP surface

Same triad and same reasoning as the sibling:

| Path | Checks | Codes |
|---|---|---|
| `/metrics` | — | 200 |
| `/healthz` | process liveness only | always 200 |
| `/readyz` | at least one source has succeeded and is not stale | 200 / 503 |
| `/health` | JSON: build info, per-source state, configured sinks, dependencies | 200 / 503 |
| `/` | index of the above | 200 |

`/healthz` deliberately checks nothing else. Liveness that depends on a backend
turns an upstream outage into a restart loop, which is strictly worse than the
outage. `/health` reports which backends are configured but carries no URLs,
hostnames or tokens: it is unauthenticated so external monitors can reach it,
which means it must not become a reconnaissance tool.

Readiness is per-source. With five sources on five schedules, "ready" means
every *enabled* source has a fresh-enough success, where fresh-enough defaults
to three times that source's own interval.

## Resilience

- Every HTTP call has a client timeout, a bounded retry with exponential
  backoff, and permanent-vs-transient classification via sentinel errors. A 401,
  403 or 404 is not retried.
- HTTP 429 is honoured: `Retry-After` where present, otherwise a doubling
  cooldown held on the source. GIRO and PSK Reporter both rate-limit in
  practice, and kc2g publishes no limit but sends no cache headers either.
- Conditional GETs against SWPC. Every response's `ETag` and `Last-Modified` are
  retained and replayed as `If-None-Match` / `If-Modified-Since`; a 304 is a
  success that produces no samples.
- A panic is recovered at two levels, per sink publish and per source poll.
- `signal.NotifyContext` on SIGTERM and SIGINT before anything is constructed;
  graceful HTTP shutdown; `defer set.Close()`. A clean SIGTERM exits 0, because
  exiting non-zero after a deliberate stop makes a supervisor restart a
  container that was stopped on purpose.
- One source failing never stops another. Sources are independent goroutines
  with independent tickers.

## Repository layout

```
cmd/solarham-exporter/     main, wiring, version stamping
internal/
  config/                  env, file, validation, provenance
  redact/                  Secret type, error scrubbing
  metric/                  Descriptor table, Sample, Batch, Store
  source/                  Source interface + scheduler
    hamqsl/                XML fetch and parse, testdata fixtures
    swpc/                  NOAA endpoint clients, conditional GET
    kc2g/                  stations.json and essn.json
  sink/                    Sink interface + fan-out Set
    prommetrics/ influxv1/ influxv2/ otlpmetrics/ mqtt/
  httpserver/              /metrics, /healthz, /readyz, /health
docs/
```

Everything is `internal/`; there is no `pkg/`. Tests sit beside the code in the
same package, as in the sibling.

## CI and release

Mirrors the sibling's two workflows.

`ci.yml` on every push and PR: `go mod tidy` diff check, `gofmt -l` gate,
`go vet`, `go build`, `go test -race -covermode=atomic`, `golangci-lint`, and a
multi-arch image build that is not pushed, with a size gate.

`release.yml` on `v*` tags: re-runs vet and race tests first, because a tag must
never publish something that would have failed CI, then builds
`linux/amd64,linux/arm64`, pushes to `dougeubanks/solarham` with semver tags plus
`latest`, attaches an SBOM and provenance attestation, and generates GitHub
release notes. Docker Hub credentials come from the repository secrets
`DOCKERHUB_USERNAME` and `DOCKERHUB_TOKEN`.
