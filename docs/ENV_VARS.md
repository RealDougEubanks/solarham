<!--
doc: ENV_VARS
last-refreshed: 2026-09-09
generated-by: doc-refresh skill
-->

# Environment Variables

Complete reference for all 70 settings. Every variable is prefixed
`SOLARHAM_`.

> **SECURITY:** Never log, share, screenshot, or commit credential values.
> Four variables are credentials: `SOLARHAM_INFLUX2_TOKEN`,
> `SOLARHAM_INFLUX1_PASSWORD`, `SOLARHAM_MQTT_PASSWORD`, and
> `SOLARHAM_OTLP_HEADERS`. If one is exposed, rotate it in the upstream system
> immediately. Changing it here does not invalidate the old value.

## Nothing is required to start

The service runs with no configuration at all. It defaults to reading NOAA and
hamqsl and serving `/metrics`. You only need to set variables to send data
somewhere else.

Startup fails only if you turn on a sink and omit what it needs.

## How values are resolved

Highest priority first:

1. Command-line flags (`--config` and `--version` only)
2. Environment variables, including their `_FILE` forms
3. A config file (INI format)
4. Built-in defaults

Every problem is reported at once, not one per restart. Each message names the
setting responsible.

## Credentials and the `_FILE` suffix

> **SECURITY:** Prefer the `_FILE` form in production. A plain environment
> variable is readable by anyone who can run `docker inspect` on the host. A
> file can be mounted as a Docker secret with restricted permissions.

Any credential variable also accepts a `_FILE` variant naming a file to read
the value from:

```
SOLARHAM_INFLUX2_TOKEN_FILE=/run/secrets/influx_token
SOLARHAM_INFLUX1_PASSWORD_FILE=/run/secrets/influx1_password
SOLARHAM_MQTT_PASSWORD_FILE=/run/secrets/mqtt_password
```

Trailing newlines are stripped. Setting **both** a variable and its `_FILE`
form is a configuration error — the service refuses to start rather than
silently picking one.

If the file is missing, the error names the **path**, never the contents.

## Logging

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `SOLARHAM_LOG_LEVEL` | No | `info` | One of `debug`, `info`, `warn`, `error` |
| `SOLARHAM_LOG_FORMAT` | No | `json` | `json` or `text`. Use `text` when reading by eye. |

Consumed in `cmd/solarham-exporter/main.go` (`newLogger`).

## HTTP server

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `SOLARHAM_HTTP_ADDR` | No | `0.0.0.0:9102` | Listen address for all endpoints |
| `SOLARHAM_HTTP_READ_TIMEOUT` | No | `10s` | Request read timeout. Range 1s–5m. |
| `SOLARHAM_HTTP_SHUTDOWN_TIMEOUT` | No | `10s` | Grace period on SIGTERM. Range 1s–5m. |
| `SOLARHAM_HTTP_STALE_AFTER` | No | `0` | Overrides how long a source may go without success before it counts as stale. `0` derives it per source from that source's schedule: three intervals for a fixed cadence, or the longest gap between publication slots plus lag and grace for a daily one. |
| `SOLARHAM_READY_REQUIRE_ALL` | No | `false` | Makes `/readyz` fail when any single source is stale. The default fails only when no source is fresh. See the note below. |

> **SECURITY:** `0.0.0.0` listens on every interface. The endpoints are
> unauthenticated by design so external monitors can reach `/readyz`. Do not
> expose port 9102 to the internet. `/health` deliberately carries no
> credentials, URLs or hostnames, but `/metrics` reveals your monitoring
> posture and station identifiers.

Consumed in `internal/httpserver/server.go`.

## Source: NOAA SWPC

Public domain. No API key. Polled in three tiers because NOAA's own update
rates differ by orders of magnitude.

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `SOLARHAM_SWPC_ENABLED` | No | `true` | Turn the whole source on or off |
| `SOLARHAM_SWPC_BASE_URL` | No | `https://services.swpc.noaa.gov` | Override only for testing |
| `SOLARHAM_SWPC_FAST_INTERVAL` | No | `1m` | Solar wind, Bz, 1-min Kp, X-ray class, scales, alerts. Range 1m–1h. |
| `SOLARHAM_SWPC_MEDIUM_INTERVAL` | No | `5m` | GOES particles, aurora power. Range 1m–6h. |
| `SOLARHAM_SWPC_SLOW_INTERVAL` | No | `1h` | F10.7, 3-hourly Kp/A, sunspots, Dst. Range 5m–24h. |
| `SOLARHAM_SWPC_TIMEOUT` | No | `20s` | Per-request timeout. Must be less than the fast interval. |
| `SOLARHAM_SWPC_RETRIES` | No | `2` | Retries per request. Range 0–10. |
| `SOLARHAM_SWPC_DRAP_ENABLED` | No | `false` | Publish the D-RAP absorption grid |
| `SOLARHAM_SWPC_DRAP_GRID_STEP` | No | `10` | Publish every Nth grid cell per axis. Range 1–90. |

**Watch the D-RAP grid step.** D-RAP is a 90×90 global grid. At
`GRID_STEP=10` you get a few hundred series. At `GRID_STEP=1` you get tens of
thousands, which will strain a small Prometheus or InfluxDB instance. That is
why it is off by default.

Consumed in `internal/source/swpc/`.

## Source: hamqsl.com

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `SOLARHAM_HAMQSL_ENABLED` | No | `true` | Turn the source on or off |
| `SOLARHAM_HAMQSL_URL` | No | `https://www.hamqsl.com/solarxml.php` | Override only for testing |
| `SOLARHAM_HAMQSL_INTERVAL` | No | `1h` | **Minimum enforced: 15m.** Range 15m–24h. |
| `SOLARHAM_HAMQSL_TIMEOUT` | No | `15s` | Per-request timeout |
| `SOLARHAM_HAMQSL_RETRIES` | No | `2` | Retries per request. Range 0–10. |

> **Do not lower the interval.** The operator of hamqsl.com asks publicly for
> hourly polling and has stated he was shut down by his internet provider once
> because of clients polling too often. Values below 15 minutes are silently
> raised to the floor with a warning. This is a courtesy limit, and it is the
> one setting in this file you should leave alone.

Consumed in `internal/source/hamqsl/`.

## Source: prop.kc2g.com

> **LICENCE:** This data is CC BY-NC-SA 4.0 and **non-commercial only**. It
> comes from ionosonde operators worldwide via GIRO and INGV, funded by WWROF.
> Enabling this source means accepting that restriction. That is why it is off
> by default.

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `SOLARHAM_KC2G_ENABLED` | No | `false` | Turn the source on. See the licence note above. |
| `SOLARHAM_KC2G_BASE_URL` | No | `https://prop.kc2g.com` | Override only for testing |
| `SOLARHAM_KC2G_INTERVAL` | No | `15m` | **Minimum enforced: 5m.** Maps regenerate every 15m, so faster gains nothing. |
| `SOLARHAM_KC2G_TIMEOUT` | No | `20s` | Per-request timeout |
| `SOLARHAM_KC2G_RETRIES` | No | `2` | Retries per request. Range 0–10. |
| `SOLARHAM_KC2G_MIN_CONFIDENCE` | No | `25` | Drop stations scoring below this. Range 0–100. |
| `SOLARHAM_KC2G_MAX_AGE` | No | `3h` | Drop stations whose own timestamp is older than this. Range 5m–72h. |
| `SOLARHAM_KC2G_STATIONS` | No | *(empty)* | Comma-separated URSI codes. Empty publishes every station worldwide. |
| `SOLARHAM_KC2G_EFFECTIVE_INDICES` | No | `true` | Publish effective sunspot number and flux |

**`MAX_AGE` matters more than it looks.** The document is served fresh even
when individual stations inside it have been silent for months. Raising this
value risks putting a months-old measurement on a dashboard as current.

**Set `STATIONS`** unless you want every ionosonde on Earth. A propagation
dashboard usually cares about a handful nearby.

Consumed in `internal/source/kc2g/`.

## Sink: Prometheus

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `SOLARHAM_PROMETHEUS_ENABLED` | No | `true` | Serve the metrics endpoint |
| `SOLARHAM_PROMETHEUS_PATH` | No | `/metrics` | Must begin with `/` |
| `SOLARHAM_PROMETHEUS_RETENTION` | No | `0` | How long a series survives after its last observation. `0` derives it from the slowest enabled source. Range 0–24h. |

Prometheus is pull-based. Point a scrape job or Grafana Alloy at
`http://<host>:9102/metrics`.

Consumed in `internal/sink/prommetrics/`.

## Sink: InfluxDB 2.x

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `SOLARHAM_INFLUX2_ENABLED` | No | `false` | Turn the sink on |
| `SOLARHAM_INFLUX2_URL` | **Yes**, if enabled | *(none)* | Base URL, e.g. `http://influxdb:8086` |
| `SOLARHAM_INFLUX2_ORG` | See note | *(none)* | Organisation **name** |
| `SOLARHAM_INFLUX2_ORG_ID` | See note | *(none)* | Organisation **ID** |
| `SOLARHAM_INFLUX2_BUCKET` | **Yes**, if enabled | `SolarHAM` | Bucket name. **Case-sensitive.** |
| `SOLARHAM_INFLUX2_TOKEN` | **Yes**, if enabled | *(none)* | Write token. Prefer `_FILE`. |
| `SOLARHAM_INFLUX2_MEASUREMENT` | No | `solar` | Measurement name for every point |
| `SOLARHAM_INFLUX2_TIMEOUT` | No | `10s` | Per-write timeout |
| `SOLARHAM_INFLUX2_RETRIES` | No | `2` | Retries per write. Range 0–10. |

**Exactly one of `ORG` or `ORG_ID` is required.** Setting neither is an error.
Setting both is also an error.

**Use `ORG_ID` if in doubt.** A bucket-scoped token cannot read
`/api/v2/orgs`, so it cannot resolve an organisation by name, and every write
addressed by name returns 404 even when spelled correctly. This exact mistake
made the predecessor of this service write nothing for years.

Find both values with:

```bash
curl -s 'http://<influx-host>:8086/api/v2/buckets' \
  -H "Authorization: Token $TOKEN" | jq -r '.buckets[] | "\(.name)\t\(.orgID)"'
```

Consumed in `internal/sink/influxv2/`.

## Sink: InfluxDB 1.x

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `SOLARHAM_INFLUX1_ENABLED` | No | `false` | Turn the sink on |
| `SOLARHAM_INFLUX1_URL` | **Yes**, if enabled | *(none)* | Base URL |
| `SOLARHAM_INFLUX1_DATABASE` | **Yes**, if enabled | *(none)* | Database name |
| `SOLARHAM_INFLUX1_RETENTION_POLICY` | No | *(empty)* | Sent only when set |
| `SOLARHAM_INFLUX1_USERNAME` | No | *(empty)* | Omit for an unauthenticated server |
| `SOLARHAM_INFLUX1_PASSWORD` | No | *(empty)* | Prefer `_FILE` |
| `SOLARHAM_INFLUX1_MEASUREMENT` | No | `solar` | Measurement name |
| `SOLARHAM_INFLUX1_TIMEOUT` | No | `10s` | Per-write timeout |
| `SOLARHAM_INFLUX1_RETRIES` | No | `2` | Retries per write. Range 0–10. |

> **SECURITY:** Credentials are sent as an HTTP Basic auth header, never as
> `u=`/`p=` query parameters. Query parameters land in server access logs and
> in Go's own transport error strings.

Consumed in `internal/sink/influxv1/`.

## Sink: MQTT and Home Assistant

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `SOLARHAM_MQTT_ENABLED` | No | `false` | Turn the sink on |
| `SOLARHAM_MQTT_BROKER` | **Yes**, if enabled | *(none)* | e.g. `tcp://192.168.52.2:1883` |
| `SOLARHAM_MQTT_CLIENT_ID` | No | `solarham-exporter` | Must be unique on the broker |
| `SOLARHAM_MQTT_USERNAME` | No | *(empty)* | Omit for an anonymous broker |
| `SOLARHAM_MQTT_PASSWORD` | No | *(empty)* | Prefer `_FILE` |
| `SOLARHAM_MQTT_TOPIC` | No | `solarham` | Prefix for every published topic |
| `SOLARHAM_MQTT_QOS` | No | `0` | 0, 1 or 2 |
| `SOLARHAM_MQTT_RETAIN` | No | `true` | Retain messages so a restarting subscriber sees current values |
| `SOLARHAM_MQTT_TIMEOUT` | No | `10s` | Connect and publish timeout |
| `SOLARHAM_MQTT_DISCOVERY_ENABLED` | No | `true` | Publish Home Assistant discovery documents |
| `SOLARHAM_MQTT_DISCOVERY_PREFIX` | No | `homeassistant` | Change only if your HA uses a custom prefix |
| `SOLARHAM_MQTT_DEVICE_ID` | No | `solarham` | Identifier for the HA device |
| `SOLARHAM_MQTT_DEVICE_NAME` | No | `SolarHAM` | Display name in HA |

> **SECURITY:** Use `ssl://` rather than `tcp://` if your broker supports TLS.
> A `tcp://` connection sends the username and password in clear text across
> your network.

With discovery on, entities appear in Home Assistant automatically. An
availability topic backed by a last will means HA marks them unavailable when
this service stops, rather than showing stale numbers indefinitely.

Consumed in `internal/sink/mqtt/`.

## Sink: OTLP

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `SOLARHAM_OTLP_ENABLED` | No | `false` | Turn the sink on |
| `SOLARHAM_OTLP_PROTOCOL` | No | `grpc` | `grpc` or `http` |
| `SOLARHAM_OTLP_ENDPOINT` | **Yes**, if enabled | *(none)* | Collector address |
| `SOLARHAM_OTLP_INSECURE` | No | `false` | Disable TLS |
| `SOLARHAM_OTLP_HEADERS` | No | *(empty)* | Comma-separated `key=value` pairs |
| `SOLARHAM_OTLP_INTERVAL` | No | `60s` | Export interval. Range 10s–1h. |
| `SOLARHAM_OTLP_TIMEOUT` | No | `10s` | Export timeout |

> **SECURITY:** `SOLARHAM_OTLP_HEADERS` commonly carries an authentication
> token for a hosted collector. It is treated as a credential and never
> logged. Setting `SOLARHAM_OTLP_INSECURE=true` sends your metrics, and any
> token in those headers, unencrypted. Only do that on a trusted local network.

Consumed in `internal/sink/otlpmetrics/`.

## Deprecated variables

These are the predecessor Python container's variables. They are **lowercase**,
unlike everything else, and are accepted so the image can be swapped without
editing the container.

| Deprecated | Replaced by |
|------------|-------------|
| `url` | `SOLARHAM_INFLUX2_URL` |
| `token` | `SOLARHAM_INFLUX2_TOKEN` |

Setting either one turns on the InfluxDB 2.x sink automatically and logs a
deprecation warning naming the modern variable.

Setting a deprecated variable **and** its modern equivalent is a configuration
error, not a silent precedence rule.

**Neither supplies an organisation.** A drop-in upgrade must also add
`SOLARHAM_INFLUX2_ORG_ID`. The service fails at startup with a message saying
exactly that.

## Config file

Instead of environment variables, settings can come from an INI file. Searched
in order:

1. The path given to `--config`
2. `/etc/solarham-exporter/config.ini`
3. `/config.ini`

Section names become setting prefixes:

```ini
[log]
level = debug

[influx2]
enabled = true
bucket = SolarHAM
```

That is equivalent to `SOLARHAM_LOG_LEVEL=debug`,
`SOLARHAM_INFLUX2_ENABLED=true`, `SOLARHAM_INFLUX2_BUCKET=SolarHAM`.

A file named explicitly with `--config` must exist. A discovered one is
optional.

> **SECURITY:** A config file containing credentials must not be committed. The
> repository's `.gitignore` already excludes `config.ini` and `*.local.ini`.
> Prefer `_FILE` secrets over putting credentials in a config file at all.

## Getting secret values

This is a single-maintainer homelab project with no shared vault.

| Credential | Where to get it |
|------------|----------------|
| `SOLARHAM_INFLUX2_TOKEN` | InfluxDB web UI → Load Data → API Tokens → Generate. Needs write access to the bucket. |
| `SOLARHAM_INFLUX1_PASSWORD` | Whatever the InfluxDB 1.x instance was configured with |
| `SOLARHAM_MQTT_PASSWORD` | Your broker's password file, e.g. Mosquitto's `passwd` |
| `SOLARHAM_OTLP_HEADERS` | Your collector vendor's dashboard |

> **SECURITY:** Generate a token scoped to **write on one bucket**. Do not use
> an all-access or operator token. This service only ever writes; it never
> needs to read, create buckets, or manage organisations.

## Why `/readyz` ignores a single stale source

`/readyz` answers one question: should an orchestrator keep routing traffic to
this instance, or restart it?

A single upstream being down does not answer that question in the affirmative.
Every replica polls the same public endpoints, so draining to another instance
reaches one missing exactly the same source, and restarting does not bring
`celestrak.org` back. Under the strict rule a third-party outage pinned
`/readyz` at 503 for as long as the outage lasted, with no action available to
whoever it woke up.

Every source stale is a different signal. That points at something local and
fixable — no egress, broken DNS, a clock so far off that every window looks
expired — so that is the condition the default policy fails on.

The strict signal is not lost, only moved to the endpoint whose job it is:

| Endpoint | Fails when | Point it at |
|---|---|---|
| `/healthz` | never (process is alive) | container liveness probe |
| `/readyz` | no source is fresh | orchestrator readiness, load balancer |
| `/health` | any source is stale | external uptime monitor, on-call alerting |

Set `SOLARHAM_READY_REQUIRE_ALL=true` to restore the old behaviour if you would
rather a partial dataset be treated as no dataset.

### Alerting on one source

`/health` names the failing source in its body, and Prometheus carries the same
verdict without you restating each source's schedule in the alert rule:

```promql
time() - solar_source_last_success_timestamp_seconds
  > solar_source_stale_window_seconds
```

`solar_source_stale_window_seconds` is the window the exporter derived from that
source's own schedule — three minutes for the one-minute SWPC feeds, better than
twenty hours for an observatory that publishes three times a day. A single
hand-written threshold cannot serve both.

A source that has never succeeded publishes no timestamp at all, so pair the
rule above with `absent()` if a cold start matters to you.
