# solarham

Exports space weather and HF propagation data to Prometheus, InfluxDB, MQTT with Home Assistant discovery, and OTLP.

It polls NOAA SWPC, NASA CCMC, GFZ Potsdam, DRAO Penticton, LASP, USGS and others — each on its own schedule, matched to how often that upstream actually publishes. Around 530 series from 16 sources by default.

**Source, issues and full documentation:** https://github.com/RealDougEubanks/solarham

---

## Upgrading from a pre-1.0 tag? Read this first

If you are running `latest` from before September 2026, or any of tags `25`–`28`, you are running a Python script that **has been silently writing nothing**. It addressed a nonexistent InfluxDB organisation and a wrongly-cased bucket, and because the client batched writes asynchronously and nothing checked the result, the process looked healthy while dropping every point.

Version 1.0.0 is a complete rewrite in Go. Two things change and will stop the container starting if you miss them:

- **The HTTP port is now `9102`.** The old image exposed nothing.
- **InfluxDB 2.x needs an organisation.** Set `SOLARHAM_INFLUX2_ORG_ID` (preferred) or `SOLARHAM_INFLUX2_ORG`.

The old lowercase `url` and `token` variables still work as deprecated aliases, but neither supplies an organisation, so a drop-in upgrade still needs the setting above. Startup now fails with a message saying exactly that, rather than returning 404 in silence indefinitely.

---

## Quick start

```bash
docker run -d --name solarham \
  -p 9102:9102 \
  -e SOLARHAM_PROMETHEUS_ENABLED=true \
  dougeubanks/solarham:1.0.0
```

Then `curl localhost:9102/metrics`.

Nothing is written anywhere by default except Prometheus, which is pull-based. Enable the sinks you want; startup fails if none is enabled.

### Writing to InfluxDB 2.x

```bash
docker run -d --name solarham \
  --read-only --cap-drop ALL --security-opt no-new-privileges \
  -p 9102:9102 \
  -e SOLARHAM_INFLUX2_ENABLED=true \
  -e SOLARHAM_INFLUX2_URL=http://influxdb:8086 \
  -e SOLARHAM_INFLUX2_ORG_ID=your_org_id \
  -e SOLARHAM_INFLUX2_BUCKET=SolarHAM \
  -e SOLARHAM_INFLUX2_TOKEN_FILE=/run/secrets/influx_token \
  dougeubanks/solarham:1.0.0
```

The container needs no root, no writable filesystem and no capabilities.

**Find your org ID** — a bucket-scoped token cannot resolve an organisation by name, so use the ID:

```bash
curl -s 'http://influxdb:8086/api/v2/buckets' -H "Authorization: Token $TOKEN" \
  | jq -r '.buckets[] | "\(.name)\t\(.orgID)"'
```

Bucket names are case-sensitive.

---

## Sources

Cadence is matched to publication, not to a convenient number. A document rewritten once a day is fetched once a day.

| Source | Default | Cadence | Provides |
|---|---|---|---|
| `swpc` | on | 1m / 5m / 1h | X-ray class, GOES particle flux, NOAA G/S/R scales, alerts, sunspots, auroral power |
| `swpc-forecast` | on | 4×/day | flare probability by class at 1/2/3 days, proton-event probability, active regions |
| `glotec` | on | 10m | total electron content, hmF2, NmF2, quiet-time TEC anomaly |
| `iswa` | on | 1m | real-time solar wind and IMF from L1, plus Dst |
| `donki` | on | 30m | CME speed, half-angle and source location; flare event counts |
| `gfz` | on | 10m | the definitive Kp, plus Hp30 at half-hourly resolution |
| `drao` | on | 3×/day | the 10.7 cm flux from the observatory that measures it |
| `lasp` | on | 1m / 3h | EUV irradiance, the heliographic centroid of EUV emission, TSI, Mg II, Lyman-α |
| `usgs-geomag` | on | 5m | ground magnetometer vectors and field rate-of-change |
| `fmi` | on | 5m | regional auroral activity index, Finland |
| `hamqsl` | on | 1h | HF and VHF band conditions, signal/noise, geomagnetic field wording |
| `lotw` | on | 1×/day | how many operators are actually on the air |
| `pota` | on | 60s | current portable activations by band and mode |
| `celestrak` | on | 2×/day | orbital element-set freshness |
| `kiwisdr` | off | 30m | your own measured HF noise floor in dBm |
| `kc2g` | off | 15m | foF2, MUF, measured sporadic-E, ionosonde TEC |
| `wspr-live` | off | 5m | observed propagation per band |
| `pskreporter` | off | 5m | band activity score by grid |
| `nmdb` | off | 10m | neutron monitor counts — galactic cosmic rays |
| `intermagnet` | off | 5m | per-observatory magnetometer vectors |
| `silso` | off | 6h | the International Sunspot Number and its smoothed series |

**Sources that are off by default carry non-commercial or otherwise restricted licences.** Enabling one accepts its terms, and the exporter logs those terms at startup. `kiwisdr` is off simply because it needs a receiver address.

Request spacing is enforced per host, shared across every source, so enabling another NOAA source costs nothing extra in request rate. Where an upstream publishes a stated polling limit, it is enforced in code and cannot be configured below.

---

## Health endpoints

| Path | Reports |
|---|---|
| `/metrics` | Prometheus exposition |
| `/healthz` | liveness — always 200, checks nothing else |
| `/readyz` | 200 while any enabled source has a fresh success, else 503 |
| `/health` | JSON with build info, per-source status and configured sinks |

`/healthz` deliberately depends on nothing. Liveness that checks a backend turns an upstream outage into a restart loop, which is worse than the outage.

`/readyz` applies the same reasoning one level up. One upstream going down does not stop this instance serving, and neither a restart nor a traffic shift brings it back, so readiness does not fail on it — `/health` does. Point an orchestrator at `/readyz` and an uptime monitor at `/health`. Set `SOLARHAM_READY_REQUIRE_ALL=true` if you want the strict rule on both.

`/health` names which backends are configured but carries no URLs, hostnames or credentials — it is unauthenticated so external monitors can reach it, so it must not become a reconnaissance tool.

---

## Configuration

Environment variables prefixed `SOLARHAM_`. Every credential also accepts a `_FILE` variant naming a file to read, which is preferable — a plain environment variable is readable by anyone who can run `docker inspect`.

There are around 100 settings. The complete annotated reference is
[`.env.example`](https://github.com/RealDougEubanks/solarham/blob/main/.env.example),
and [`docs/ENV_VARS.md`](https://github.com/RealDougEubanks/solarham/blob/main/docs/ENV_VARS.md)
documents each one with its default and reasoning.

Every configuration problem is reported at once, not one per restart.

---

## Unraid

Repository: `dougeubanks/solarham` · Network: `bridge` · Port: `9102` → `9102`

If you are updating an existing install, clear **Post Arguments** — the old image had no entrypoint and Unraid supplied the Python command there. Leaving it set will stop the container starting.

---

## Notes on what this does not do

**Satellite pass predictions.** A countdown to the next pass is a computed function of time rather than an observation: wrong between scrapes, quantised to the scrape interval, and it reconstructs badly in any range query. Element-set *age* is exported instead, because stale elements are a real operational problem worth alerting on.

**Hosted propagation predictions.** P.533 and VOACAP are monthly-median models driven by a smoothed sunspot number, so they step only at month boundaries. Ship the inputs and compute predictions at query time.

---

## Image

`linux/amd64` and `linux/arm64`. Built from `gcr.io/distroless/static-debian12:nonroot` — no shell, no package manager, no libc. Around 19 MB. Every release carries an SBOM and a signed build-provenance attestation:

```bash
gh attestation verify oci://dougeubanks/solarham:1.0.0 --repo RealDougEubanks/solarham
```

---

## Licence and attribution

MIT. Data sources carry their own terms:

- **NOAA SWPC, NASA, USGS** — public domain. Not endorsed by any of them.
- **GFZ Potsdam** — CC BY 4.0.
- **DRAO Penticton** — Open Government Licence, Canada.
- **hamqsl.com** — solar data courtesy of N0NBH. Please respect his hourly polling request.
- Sources disabled by default are non-commercial or otherwise restricted; each logs its terms when enabled.
