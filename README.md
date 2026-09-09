<!--
doc: README
last-refreshed: 2026-09-09
generated-by: doc-refresh skill
-->

# solarham-exporter

Exports space weather and HF propagation data to Prometheus, InfluxDB 1.x and
2.x, MQTT with Home Assistant discovery, and OTLP.

It polls NOAA SWPC for solar and geomagnetic conditions, hamqsl.com for N0NBH's
band-condition model, and optionally prop.kc2g.com for ionosonde foF2 and MUF —
each on its own schedule, because their update cadences range from one minute to
one day.

This replaces a Python script that ran in the same image name. If you are
upgrading from it, read [Migrating](#migrating-from-the-python-image) first: the
old container was silently writing nothing, and the fix needs one setting you
probably do not have set.

## Quick start

```bash
docker run -d --name solarham \
  -p 9102:9102 \
  -e SOLARHAM_PROMETHEUS_ENABLED=true \
  dougeubanks/solarham:latest
```

Then `curl localhost:9102/metrics`.

Nothing is written anywhere by default except Prometheus, which is pull-based.
Enable the sinks you want; startup fails if none is enabled.

## Docker Compose

```yaml
services:
  solarham:
    image: dougeubanks/solarham:latest
    container_name: solarham
    restart: unless-stopped
    read_only: true
    cap_drop: ["ALL"]
    ports:
      - "9102:9102"
    environment:
      SOLARHAM_PROMETHEUS_ENABLED: "true"
      SOLARHAM_INFLUX2_ENABLED: "true"
      SOLARHAM_INFLUX2_URL: "http://influxdb:8086"
      SOLARHAM_INFLUX2_ORG_ID: "8ba47ba1e329d213"
      SOLARHAM_INFLUX2_BUCKET: "SolarHAM"
      SOLARHAM_INFLUX2_TOKEN_FILE: "/run/secrets/influx_token"
    secrets:
      - influx_token

secrets:
  influx_token:
    file: ./influx_token
```

The container needs no root, no writable filesystem and no capabilities. It
makes outbound HTTPS requests and serves one port.

## Sources

| Source | Default | Interval | Provides |
|---|---|---|---|
| `swpc` | on | 1 m / 5 m / 1 h | solar wind speed and density, Bt/Bz, Kp, A, F10.7, sunspots, X-ray class, particle flux, auroral power, NOAA G/S/R scales, Dst, alerts, D-RAP (opt-in) |
| `hamqsl` | on | 1 h | HF band conditions, VHF conditions, signal/noise, geomagnetic field wording |
| `kc2g` | **off** | 15 m | foF2, MUF, M(3000)F2, effective SSN and flux |

### Why hamqsl is polled hourly

The Python original polled `hamqsl.com/solarxml.php` every 30 seconds. N0NBH's
FAQ asks for hourly polling and says, in as many words, that he has already been
shut down by his ISP once over this exact load. His own stated cadences are 3
hours for flux, A, K and sunspots, 1 hour for X-ray and particle flux, and 30
minutes for VHF conditions — so 30-second polling was oversampling by up to 360
times, against a hobbyist's server, for data that had not changed.

The feed's `<updated>` timestamp advances about once a minute because Sucuri's
cache TTL is roughly 60 seconds and the PHP restamps on each origin miss. **That
timestamp moving does not mean the data moved.** There is no `ETag` and no
`Last-Modified`, so conditional requests are impossible and self-throttling is
the only control available.

The interval is therefore clamped to a 15-minute floor. Configure it lower and
it is raised, with a warning.

This exporter also takes only four things from that feed — the HF and VHF
conditions, signal/noise, and the geomagnetic field wording — because those are
N0NBH's own model output and nothing else produces them. Everything else it
carries is available first-hand from NOAA, in the public domain, at better
cadence, with cache headers.

### Why kc2g is off by default

Its data is **CC BY-NC-SA 4.0, non-commercial only**, aggregated from GIRO and
INGV and funded by WWROF. Enabling it means accepting that restriction, which
should be a decision rather than a default.

Be aware that ionosonde coverage is thin and thinning: Russia and China stopped
sharing data in 2021, Japan in 2023, the US Space Force withdrew the NEXION
network covering most of CONUS in 2023, and NOAA shut down its ionosonde
distribution in 2024. This is why hamqsl's `fof2` and `muffactor` fields are so
often empty — it depends on the same upstream.

Stations are filtered on their own timestamps, not the document's. The feed is
served fresh even when a station inside it has been silent for months.

## Sinks

| Sink | Enable with | Needs |
|---|---|---|
| Prometheus | `SOLARHAM_PROMETHEUS_ENABLED=true` | a scrape pointed at `/metrics` |
| InfluxDB 2.x | `SOLARHAM_INFLUX2_ENABLED=true` | URL, token, bucket, **and org or org ID** |
| InfluxDB 1.x | `SOLARHAM_INFLUX1_ENABLED=true` | URL, database |
| MQTT | `SOLARHAM_MQTT_ENABLED=true` | broker |
| OTLP | `SOLARHAM_OTLP_ENABLED=true` | endpoint |

All are off except Prometheus. Each publishes in its own goroutine under its own
timeout, so one slow or broken backend cannot delay the others or stall a poll.

### InfluxDB 2.x and the org problem

**Exactly one of `SOLARHAM_INFLUX2_ORG` or `SOLARHAM_INFLUX2_ORG_ID` is
required.** If writes fail with `organization name "..." not found` while the
name looks correct, you need the ID: a bucket-scoped token cannot read
`/api/v2/orgs`, so it cannot resolve an org by name at all.

Find it from a bucket listing:

```bash
curl -s 'http://influxdb:8086/api/v2/buckets' -H "Authorization: Token $TOKEN" \
  | jq -r '.buckets[] | "\(.name)\t\(.orgID)"'
```

Bucket names are case-sensitive.

### Home Assistant

With `SOLARHAM_MQTT_DISCOVERY_ENABLED=true` (the default when MQTT is on), each
series publishes a retained discovery document once, on first sight, and the
entities appear without manual configuration. An availability topic backed by a
last will means Home Assistant marks them unavailable when the exporter stops,
rather than showing stale numbers forever.

## Metrics

All metrics are prefixed `solar_`. `/metrics` is the authoritative list; the
[design document](docs/design.md) has the full table with sources.

A field an upstream did not supply produces **no series at all** — not a zero,
and not the previous cycle's value. Series expire out of `/metrics` when their
source stops reporting them, because a six-hour-old solar wind speed presented as
current is worse than no reading.

Every sample carries its upstream's own observation time, not the time it was
collected.

### Health endpoints

| Path | Reports |
|---|---|
| `/metrics` | Prometheus exposition |
| `/healthz` | liveness — always 200, checks nothing else |
| `/readyz` | 200 when every enabled source has a fresh success, else 503 |
| `/health` | JSON with build info, per-source status and configured sinks |

`/healthz` deliberately depends on nothing. Liveness that checks a backend turns
an upstream outage into a restart loop, which is strictly worse than the outage.

`/health` names which backends are configured but carries no URLs, hostnames or
credentials. It is unauthenticated so external monitors can reach it, which
means it must not become a reconnaissance tool.

## Documentation

| Document | Read it when |
|---|---|
| [docs/RUNBOOK.md](docs/RUNBOOK.md) | Something is broken and you need it working now |
| [docs/ENV_VARS.md](docs/ENV_VARS.md) | You need the complete list of all 70 settings |
| [docs/design.md](docs/design.md) | You want to know why it is built this way |
| [docs/assumptions.md](docs/assumptions.md) | You disagree with a decision and want the reasoning |
| [docs/baseline-findings.md](docs/baseline-findings.md) | You want the audit of what the Python version got wrong |
| [SECURITY.md](SECURITY.md) | You are handling credentials or exposing the port |
| [CONTRIBUTING.md](CONTRIBUTING.md) | You are about to change the code |

## Configuration

Environment variables prefixed `SOLARHAM_`. Credentials also accept a `_FILE`
variant naming a file to read — set one or the other, never both.

Precedence: flags, then environment, then a config file, then defaults.

**[`.env.example`](.env.example) documents every setting** with its default and
the reasoning behind it. An optional INI file is read from
`/etc/solarham-exporter/config.ini` or `/config.ini`, or from `--config`.

Every configuration problem is reported at once, not one per restart.

## Migrating from the Python image

If you are running the previous `dougeubanks/solarham` image, **it has not been
writing any data.** Its logs are a continuous stream of:

```
{"code":"not found","message":"organization name \"Home\" not found"}
```

It hardcoded an organisation that does not exist and a bucket whose case did not
match the real one, and because the client batched writes asynchronously and
nothing checked the result, the process looked healthy while dropping every
point. Four more fields never matched the feed's element names and were silently
skipped, and the entire VHF section was never parsed. See
[docs/baseline-findings.md](docs/baseline-findings.md).

The old container passed two lowercase variables, `url` and `token`. Both are
still accepted as deprecated aliases for `SOLARHAM_INFLUX2_URL` and
`SOLARHAM_INFLUX2_TOKEN`, and either implies `SOLARHAM_INFLUX2_ENABLED=true`.
Both log a deprecation warning. Setting a legacy variable together with its
modern equivalent is an error, not a silent precedence rule.

**You must add one setting**: neither legacy variable supplies an organisation,
so add `SOLARHAM_INFLUX2_ORG_ID` (or `_ORG`). Startup now fails with a message
saying exactly that — which is the intended improvement on returning 404 in
silence indefinitely.

Also change the port mapping: this image serves HTTP on 9102.

## Unraid

Repository: `dougeubanks/solarham`
Network: `bridge`
Port: `9102` → `9102`

Variables, at minimum:

| Key | Value |
|---|---|
| `SOLARHAM_PROMETHEUS_ENABLED` | `true` |
| `SOLARHAM_INFLUX2_ENABLED` | `true` |
| `SOLARHAM_INFLUX2_URL` | `http://192.168.60.50:8086` |
| `SOLARHAM_INFLUX2_ORG_ID` | your org ID |
| `SOLARHAM_INFLUX2_BUCKET` | `SolarHAM` |
| `SOLARHAM_INFLUX2_TOKEN` | your token |

Point an external monitor at `/readyz`.

## Building

```bash
go build ./cmd/solarham-exporter
go test -race ./...
docker build -t solarham:dev .
```

The image is distroless static and non-root; a release build is around 20 MB.

## Licence

MIT. See [LICENSE](LICENSE).

Data sources and their terms:

- **NOAA SWPC** — public domain. This project is not endorsed by NOAA.
- **hamqsl.com** — solar data courtesy of N0NBH. Please respect the hourly
  polling request.
- **prop.kc2g.com** — CC BY-NC-SA 4.0, non-commercial. Data from ionosonde
  operators worldwide, distributed through GIRO and INGV, funded by WWROF.
