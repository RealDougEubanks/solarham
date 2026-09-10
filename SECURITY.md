<!--
doc: SECURITY
last-refreshed: 2026-09-09
generated-by: doc-refresh skill
-->

# Security Policy

## Reporting a vulnerability

> **SECURITY: Do NOT open a public GitHub issue for security vulnerabilities.**

Report privately through **GitHub Security Advisories**:
<https://github.com/RealDougEubanks/solarham/security/advisories/new>

This is a single-maintainer hobby project. Expect acknowledgement within
**7 days**, not 48 hours. If the issue is being actively exploited, say so in
the subject line.

## What this project is, in security terms

Understanding the shape of the service tells you where the risk is.

| Property | Value |
|----------|-------|
| Inbound network | One HTTP port (9102), unauthenticated, no TLS |
| Outbound network | HTTPS to three public websites |
| Data written | Numeric space-weather measurements only |
| Data read from users | **None.** No user input is accepted at all. |
| Personal data handled | **None** |
| Authentication implemented | **None.** There are no accounts and no login. |
| Runs as | `nonroot`, uid 65532 |
| Filesystem | Read-only; nothing is written to disk |
| Capabilities needed | None |

The service has **no request-handling attack surface in the usual sense**. It
parses no user-supplied input. Every byte it processes comes from a public
website it chose to contact.

## Sensitive data this project handles

It handles **no personal data**, no payment data, and no user accounts.

It does handle **four credentials**, all of which are write credentials for
systems you control:

| Credential | What it grants | Blast radius if leaked |
|------------|---------------|----------------------|
| `SOLARHAM_INFLUX2_TOKEN` | Write to one InfluxDB 2.x bucket | Someone can write junk into that bucket |
| `SOLARHAM_INFLUX1_PASSWORD` | Write to an InfluxDB 1.x database | Same, for 1.x |
| `SOLARHAM_MQTT_PASSWORD` | Publish to your MQTT broker | Someone can publish to your broker, which may drive Home Assistant automations |
| `SOLARHAM_OTLP_HEADERS` | Usually an auth token for a hosted collector | Depends on your vendor; may allow metric injection or incur cost |

> **SECURITY:** Scope the InfluxDB token to **write on one bucket**. Do not use
> an all-access or operator token. This service only ever writes. It never
> needs to read data, create buckets, or manage organisations. A leaked
> write-only bucket token is an annoyance; a leaked operator token is an
> incident.

## Credential and secret rules

> **SECURITY:** Never commit a secret. Git history is effectively permanent,
> and a secret pushed to a public repository must be treated as compromised
> the moment it lands, not when someone notices.

1. **Prefer the `_FILE` form of every credential.** A plain environment
   variable is readable by anyone who can run `docker inspect` on the host.

   ```
   SOLARHAM_INFLUX2_TOKEN_FILE=/run/secrets/influx_token
   ```

2. **Never set both a variable and its `_FILE` form.** The service treats that
   as a configuration error and refuses to start rather than guessing which
   one you meant.

3. **Do not commit config files.** `.gitignore` already excludes `.env`,
   `config.ini` and `*.local.ini`.

4. **If a credential is exposed, rotate it upstream.** Changing the value here
   does nothing. Revoke the InfluxDB token in the InfluxDB UI, change the MQTT
   password on the broker.

## How credentials are protected in this codebase

These are implemented controls, not intentions.

| Control | Where | What it prevents |
|---------|-------|-----------------|
| Secrets held in a closure, not a string field | `internal/redact/redact.go` | `fmt.Sprintf("%v", someStruct)` printing a token in clear text. Reflection cannot read a func field. |
| `String`, `GoString`, `Format`, `LogValue`, `MarshalJSON`, `MarshalText` all return `REDACTED` | `internal/redact/redact.go` | A credential leaking through any formatting or serialisation path |
| URL query strings scrubbed from transport errors | `redact.Error`, `redact.URL` | Go's `*url.Error` embedding a full URL, including credentials, into a logged error |
| InfluxDB 1.x uses HTTP Basic auth, never `u=`/`p=` query params | `internal/sink/influxv1/` | Credentials appearing in server access logs |
| `/health` carries no URLs, hostnames, tokens or connection strings | `internal/httpserver/server.go` | An unauthenticated endpoint becoming a reconnaissance tool |
| Configuration provenance logging shows secrets as `REDACTED` | `internal/config/resolved.go` | The startup config dump leaking a token |
| Tests assert a canary credential never appears in logs or errors | throughout `*_test.go` | Regression of any of the above |

## Network exposure

> **SECURITY:** Do not expose port 9102 to the internet. The endpoints are
> deliberately unauthenticated so that external uptime monitors can reach
> `/readyz`, which means anyone who can reach the port can read them.

What an attacker learns from each endpoint:

| Endpoint | Discloses |
|----------|-----------|
| `/healthz` | That the process is running |
| `/readyz` | Which data sources are stale |
| `/health` | Build version, commit, source status, **names** of configured sinks. No addresses or credentials. |
| `/metrics` | All space-weather values, plus which ionosonde stations you monitor |

None of that is secret, but `/metrics` reveals your monitoring setup and
`/health` reveals your exact build. Bind to a LAN interface or put it behind
your reverse proxy.

There is **no TLS** on the listener. Terminate TLS at a proxy if you need it.

## Outbound connections

The service contacts only these hosts, all over HTTPS:

| Host | Purpose |
|------|---------|
| `services.swpc.noaa.gov` | NOAA space weather data |
| `www.hamqsl.com` | HF band condition model |
| `prop.kc2g.com` | Ionosonde data (only when explicitly enabled) |

All three are configurable via `*_BASE_URL` / `*_URL` settings, which exist for
testing. Changing them points the service at a host of your choosing, so treat
those settings as trusted configuration.

Every response body is bounded with `io.LimitReader` so a misbehaving or
hostile origin cannot exhaust memory. Every request has a timeout, a bounded
retry, and permanent-versus-transient error classification.

## Dependency security

Direct dependencies are deliberately few:

| Dependency | Purpose |
|------------|---------|
| `github.com/prometheus/client_golang` | Prometheus exposition |
| `github.com/eclipse/paho.mqtt.golang` | MQTT client |
| `go.opentelemetry.io/otel` and its OTLP exporters | OTLP export |

Both InfluxDB sinks write line protocol over the standard library's
`net/http`. No InfluxDB client library is used, which removes a dependency and
its transitive tree.

Three checks run automatically, and they cover different things:

| Check | Runs | Finds |
|---|---|---|
| `govulncheck` | every push and pull request | known CVEs that are **reachable** from this code, so a finding is real exposure rather than a CVE somewhere in the tree we never call |
| Dependency review | every pull request | a dependency being *introduced* with a known vulnerability, blocking at moderate severity |
| CodeQL | pushes, pull requests, and weekly | security-relevant data flows — a value reaching a dangerous sink by a path no single-file linter follows |

CodeQL also runs on a schedule because its query pack is updated continuously:
code that was clean when it merged can be found vulnerable months later without
a line of it changing.

To run the dependency scan yourself:

```bash
go install golang.org/x/vuln/cmd/govulncheck@latest
govulncheck ./...
```

Dependabot is configured in `.github/dependabot.yml` and opens grouped weekly
pull requests for Go modules, GitHub Actions, and the base image. Dependabot
**alerts** are a separate switch from that file and are enabled on the
repository: the file keeps dependencies current, the alerts say one has a known
CVE.

Patch and minor updates merge themselves once every required check has passed.
That is not a weakening of review — the same five checks gate them as gate any
human's pull request, and auto-merge waits for all of them. It removes the step
where somebody clicks merge on a green patch bump they were always going to
accept, which is the step most likely to be skipped for weeks.

**Major updates are excluded and wait for a human.** A major version is where
the upstream is saying it broke something, and a passing test suite is weaker
evidence than usual there: the tests exercise the API we currently call, not
the parts whose behaviour changed.

## Supply chain

Released images carry provenance you can verify:

- Built by GitHub Actions from a tagged commit, never from a laptop
- An SBOM is attached to each image
- Build provenance is attested with `actions/attest-build-provenance` and
  pushed to the registry
- The release workflow re-runs `go vet` and the race-detector test suite before
  publishing, so a tag cannot ship something that would have failed CI

Every GitHub Action is pinned to a full commit SHA rather than a version tag,
with the human-readable version kept in a trailing comment:

```yaml
uses: actions/checkout@11d5960a326750d5838078e36cf38b85af677262 # v4
```

A version tag is mutable. If an action's maintainer account is compromised, or
a tag is simply re-pointed, `@v4` silently becomes different code — inside a
workflow that holds `contents: write`, `id-token: write` and a Docker Hub
token. That is not hypothetical: it is how the tj-actions/changed-files
compromise reached thousands of repositories in March 2025. A SHA cannot be
re-pointed.

Dependabot reads the trailing comment and opens a pull request when a pinned
action has a new release, so pinning does not mean going stale. Review those
pull requests as you would any other dependency bump: check what changed
between the two SHAs, not just that the version number went up.

Verify a published image:

```bash
gh attestation verify oci://dougeubanks/solarham:v0.1.0 \
  --repo RealDougEubanks/solarham
```

## Container hardening

The image is `gcr.io/distroless/static-debian12:nonroot`. It contains no
shell, no package manager and no libc — only the statically linked binary.
There is nothing for an attacker who achieves code execution to pivot to.

Run it locked down:

```bash
docker run -d --name solarham \
  --read-only \
  --cap-drop ALL \
  --security-opt no-new-privileges \
  -p 9102:9102 \
  dougeubanks/solarham:latest
```

The service needs no writable filesystem, no capabilities, and no root. It has
been verified to run with all three restrictions applied.

## Out of scope

- **Accuracy of upstream data.** If NOAA publishes a wrong number, this service
  faithfully republishes it.
- **Denial of service against the listener.** It is a LAN metrics endpoint with
  no authentication by design. Do not expose it publicly.
- **The `*_BASE_URL` settings.** Pointing the service at a hostile host is a
  configuration decision, not a vulnerability.
