<!--
doc: RUNBOOK
last-refreshed: 2026-09-09
generated-by: doc-refresh skill
-->

# Runbook — solarham-exporter

> **You were just paged. Start here.**

This service reads space weather data from three public websites. It then
publishes that data to Prometheus, InfluxDB, MQTT and OTLP.

It is a **read-only data collector**. It stores nothing itself. If it is down,
you lose new data points. You do not lose existing data, and nothing else
breaks.

## Is the service alive?

Run these three commands. Replace `<host>` with the machine running the
container, usually `192.168.60.50`.

```bash
# 1. Is the process up? Always returns 200 if it is running at all.
curl -f http://<host>:9102/healthz || echo "PROCESS IS DOWN"

# 2. Can it serve real data? Returns 503 if any source has gone stale.
curl -s -o /dev/null -w '%{http_code}\n' http://<host>:9102/readyz

# 3. What exactly is wrong? Returns JSON naming each source.
curl -s http://<host>:9102/health | jq
```

Then read the logs:

```bash
docker logs --tail 50 solarham
```

Healthy `/health` looks like this. Every source shows `"status": "ok"`:

```json
{
  "status": "ok",
  "build": { "version": "v0.1.0", "commit": "70ac9c7", "buildDate": "..." },
  "sources": [
    { "name": "hamqsl", "interval": "1h0m0s", "status": "ok", "samples": 29,
      "successes": 12, "failures": 0 }
  ],
  "sinks": ["prometheus", "influxv2"]
}
```

## Service overview

| Property | Value |
|----------|-------|
| Port | `9102` (HTTP) |
| Liveness endpoint | `/healthz` — always 200 while the process runs |
| Readiness endpoint | `/readyz` — 200 or 503 |
| Detailed health | `/health` — JSON, 200 or 503 |
| Metrics endpoint | `/metrics` — Prometheus format |
| Log location | `docker logs solarham` (JSON to stdout) |
| Restart command | `docker restart solarham` |
| Deployed via | Unraid Docker, image `dougeubanks/solarham` |
| Runs as | user `nonroot` (uid 65532), read-only filesystem, no capabilities |
| Source repo | `github.com/RealDougEubanks/solarham` |

## What the three health endpoints mean

Read this before acting. They answer different questions.

| Endpoint | Question it answers | When it fails |
|----------|--------------------|---------------|
| `/healthz` | Is the process running? | Only if the process is dead or hung |
| `/readyz` | Is the data current? | When any source has missed 3 polls |
| `/health` | Which source is broken? | Same as `/readyz`, but names the source |

> `/healthz` deliberately checks nothing except itself. This is on purpose. If
> liveness depended on NOAA being reachable, a NOAA outage would make your
> container restart in a loop. That is worse than the outage.

## Start / stop / restart

```bash
# Restart (the usual first action)
docker restart solarham

# Stop gracefully. The process traps SIGTERM and exits 0.
docker stop solarham

# Start
docker start solarham

# Follow logs live
docker logs -f solarham
```

> **SECURITY:** If you are restarting because you suspect a security incident,
> do NOT restart in place. Stop the container, leave it stopped, and preserve
> the logs with `docker logs solarham > /tmp/solarham-incident.log` before
> anything else. A restart destroys the in-memory state and the log history you
> would need to investigate.

## Known failure modes

Every message below is a real log line from this codebase. Search the logs for
the message text.

### The service is up but publishing nothing

| Log message | Root cause | Immediate fix |
|-------------|-----------|---------------|
| `influxdb 2.x write target does not exist; every write will fail until this is corrected` | Wrong org, org ID, or bucket name. Bucket names are case-sensitive. | See [InfluxDB 404](#influxdb-returns-404-on-every-write) below |
| `influxdb 2.x rejected the token` | Token is wrong, expired, or lacks write permission on the bucket | Generate a new token in the InfluxDB UI with write access to the bucket |
| `influxdb 1.x rejected the credentials` | Wrong username or password | Check `SOLARHAM_INFLUX1_USERNAME` and `SOLARHAM_INFLUX1_PASSWORD` |
| `no sinks are enabled, so nothing would be published` | Startup refused. No sink was turned on. | Set `SOLARHAM_PROMETHEUS_ENABLED=true` and restart |

### A data source is failing

| Log message | Root cause | Immediate fix |
|-------------|-----------|---------------|
| `poll failed` | An upstream website is unreachable or returned an error | Usually self-healing. The source retries with backoff. Check `/health` to see which one. |
| `swpc endpoint failed, continuing with the rest` | One NOAA endpoint is down; the others worked | No action. This is designed behaviour — partial data is published. |
| `SWPC answered 429; pausing all SWPC polling` | Polling NOAA too fast | No action. It resumes automatically. If it repeats, raise `SOLARHAM_SWPC_FAST_INTERVAL`. |
| `hamqsl refused the request as too frequent; pausing this source` | Polling hamqsl.com too fast | **Act on this.** See [hamqsl rate limiting](#hamqslcom-is-rate-limiting-us) below. |
| `prop.kc2g.com refused a request as too frequent; pausing this source` | Polling kc2g too fast | Raise `SOLARHAM_KC2G_INTERVAL` to 15m or more |
| `hamqsl <updated> could not be parsed; stamping samples with the receipt time` | The feed changed its timestamp format | Not urgent. Data still publishes with a slightly less accurate time. Open an issue. |

### Configuration problems

| Log message | Root cause | Immediate fix |
|-------------|-----------|---------------|
| `invalid configuration:` on stderr, process exits | One or more settings are wrong | Read the list. Every problem is reported at once and each names its setting. |
| `hamqsl poll interval raised to the courtesy floor` | Interval set below 15 minutes | Not an error. Set `SOLARHAM_HAMQSL_INTERVAL=1h` to silence it. |
| `source has a non-positive interval and will not be polled` | An interval was set to zero | Set a positive duration such as `1m` |
| `mqtt connection lost` | The MQTT broker went away | Reconnects automatically. Check the broker is up. |

### Metrics look wrong

| Symptom | Root cause | What to do |
|---------|-----------|------------|
| A series disappeared from `/metrics` | Its source stopped supplying that field | **This is correct behaviour.** A field the upstream no longer sends must vanish rather than show a stale value. Check `/health`. |
| `solar_aurora_equatorward_boundary_degrees` is absent | It is deliberately never published | Not a bug. NOAA publishes no such boundary; see `docs/design.md`. |
| `solar_wind_density...` is absent | ACE stopped reporting usable data | Absent is intended when the data is flagged bad. It returns on its own. |
| Thousands of `solar_drap_*` series | D-RAP is enabled with a small grid step | Set `SOLARHAM_SWPC_DRAP_GRID_STEP=10`, or `SOLARHAM_SWPC_DRAP_ENABLED=false` |

## InfluxDB returns 404 on every write

This is the failure that broke the predecessor of this service for years, so it
gets its own section.

**Symptom:** logs repeat `organization name "..." not found`, or
`influxdb 2.x write target does not exist`.

**Cause:** the token cannot resolve the organisation by name.

Follow these steps in order.

1. Confirm what the token can actually see:

   ```bash
   curl -s 'http://<influx-host>:8086/api/v2/buckets' \
     -H "Authorization: Token $TOKEN" | jq -r '.buckets[] | "\(.name)\t\(.orgID)"'
   ```

2. Note the exact bucket name. **It is case-sensitive.** `SolarHAM` and
   `solarham` are different buckets.

3. Note the `orgID` value next to it.

4. Set the org **by ID, not by name**:

   ```
   SOLARHAM_INFLUX2_ORG_ID=<the orgID from step 3>
   SOLARHAM_INFLUX2_BUCKET=<the exact name from step 2>
   ```

5. Remove `SOLARHAM_INFLUX2_ORG` if it is set. Setting both is a configuration
   error and the service will refuse to start.

6. Restart and confirm:

   ```bash
   docker restart solarham
   sleep 20
   curl -s http://<host>:9102/health | jq '.sinks'
   docker logs --tail 20 solarham | grep -i influx
   ```

**Why the ID and not the name:** a bucket-scoped token cannot read
`/api/v2/orgs`. It therefore cannot look up an organisation by name, and every
write addressed by name returns 404 even when the name is spelled correctly.

## hamqsl.com is rate limiting us

Treat this as a courtesy problem, not just a technical one.

The operator of hamqsl.com asks publicly for **hourly** polling. He has stated
he was shut down by his internet provider once because of automated clients
polling too often.

1. Check the configured interval:

   ```bash
   docker exec solarham env | grep HAMQSL_INTERVAL || echo "using the 1h default"
   ```

2. Set it to one hour or longer:

   ```
   SOLARHAM_HAMQSL_INTERVAL=1h
   ```

3. If you do not need HF band condition ratings at all, turn the source off
   entirely. NOAA supplies everything else:

   ```
   SOLARHAM_HAMQSL_ENABLED=false
   ```

The service enforces a 15-minute minimum regardless of configuration. You
cannot set it lower, only higher.

## Environment variables

> **SECURITY:** `SOLARHAM_INFLUX2_TOKEN`, `SOLARHAM_INFLUX1_PASSWORD` and
> `SOLARHAM_MQTT_PASSWORD` are credentials. Never paste them into a ticket,
> chat message, or screenshot. If one is exposed, rotate it in the upstream
> system immediately — changing it here is not enough.

The most common variables are below. **[docs/ENV_VARS.md](ENV_VARS.md) is the
complete reference.**

| Variable | Required | Description | Where to find it |
|----------|----------|-------------|-----------------|
| `SOLARHAM_HTTP_ADDR` | No | Listen address. Default `0.0.0.0:9102`. | — |
| `SOLARHAM_PROMETHEUS_ENABLED` | No | Serve `/metrics`. Default `true`. | — |
| `SOLARHAM_INFLUX2_URL` | If InfluxDB 2 on | InfluxDB base URL | Your InfluxDB host and port |
| `SOLARHAM_INFLUX2_ORG_ID` | If InfluxDB 2 on | Organisation ID | The buckets API call above |
| `SOLARHAM_INFLUX2_BUCKET` | If InfluxDB 2 on | Bucket name, case-sensitive | The buckets API call above |
| `SOLARHAM_INFLUX2_TOKEN` | If InfluxDB 2 on | Write token | InfluxDB UI, API Tokens |
| `SOLARHAM_LOG_LEVEL` | No | `debug`, `info`, `warn`, `error`. Default `info`. | — |

> **SECURITY:** Every credential variable also accepts a `_FILE` variant, for
> example `SOLARHAM_INFLUX2_TOKEN_FILE=/run/secrets/influx_token`. Prefer it.
> A plain environment variable is visible to anyone who can run
> `docker inspect`. Set the variable or its `_FILE` form, never both — the
> service treats that as a configuration error rather than guessing.

## Turning up the logging

If the tables above did not explain it, get more detail:

```bash
docker run --rm -e SOLARHAM_LOG_LEVEL=debug -e SOLARHAM_LOG_FORMAT=text \
  dougeubanks/solarham:latest
```

Debug level logs every individual poll, every sink publish, and the resolved
value of every configuration setting. Credentials appear as `REDACTED`.

Run it in the foreground like this rather than changing the live container, so
you can stop it with Ctrl-C.

## Rollback

```bash
# See what tags are available
docker image ls dougeubanks/solarham

# Roll back to a specific version
docker pull dougeubanks/solarham:0.1.0
# Then change the container's image tag in the Unraid UI and apply.
```

For a code rollback:

```bash
git log --oneline -10
git revert HEAD        # safe: creates a new commit rather than rewriting
```

> Rolling back past `v0.1.0` returns you to the Python image, which does not
> work. It writes nothing to InfluxDB. Do not roll back that far.

## Escalation path

This is a single-maintainer project.

1. Check `/health` and the failure-mode tables above.
2. Check whether the upstream is simply down:
   - NOAA: <https://services.swpc.noaa.gov/products/alerts.json> should return JSON
   - hamqsl: <https://www.hamqsl.com/solarxml.php> should return XML
3. If the upstream is down, there is nothing to fix. The service recovers on
   its own.
4. Otherwise open an issue at
   <https://github.com/RealDougEubanks/solarham/issues> with the output of
   `/health` and the last 50 log lines.

> **SECURITY:** Redact tokens and passwords from logs before attaching them to
> an issue. The service redacts credentials in its own output, but a
> configuration file or shell history you paste alongside will not be.
