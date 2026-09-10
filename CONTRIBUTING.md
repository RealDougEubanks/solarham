<!--
doc: CONTRIBUTING
last-refreshed: 2026-09-09
generated-by: doc-refresh skill
-->

# Contributing to solarham-exporter

## Before you start

1. Read [README.md](README.md) to see what the service does.
2. Read [docs/design.md](docs/design.md) before proposing anything structural.
   Most design questions are answered there, including several decisions that
   look wrong until you know why.
3. Check open issues to avoid duplicate work.
4. For anything large, open an issue first.

> **SECURITY:** Never commit secrets, API keys, tokens, or credentials. They
> are effectively impossible to revoke once pushed. See [SECURITY.md](SECURITY.md).

## Prerequisites

| Tool | Version | Why |
|------|---------|-----|
| Go | 1.26 or newer | The module targets 1.26 |
| Docker | any recent | Only for building the image |
| golangci-lint | v2 | Matches what CI runs |

Install the linter:

```bash
go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest
```

## Workflow

1. Branch from `main`. **Never commit to `main` directly.**

   ```bash
   git checkout main && git pull
   git checkout -b feature/short-description
   ```

   Use the prefixes `feature/`, `fix/`, `hotfix/` or `docs/`.

2. Make your change.

3. Run everything CI runs, in this order:

   ```bash
   go mod tidy                      # must produce no diff
   gofmt -l .                       # must print nothing
   go vet ./...
   go test -race ./...
   golangci-lint run --timeout=5m
   ```

4. Open a pull request against `main`.

## Pull request checklist

- [ ] `go test -race ./...` passes
- [ ] `gofmt -l .` prints nothing
- [ ] `golangci-lint run` reports no issues
- [ ] `go mod tidy` produces no diff
- [ ] No secrets, tokens or hardcoded credentials added
- [ ] Docs updated if behaviour changed — including `.env.example` if you added
      a setting
- [ ] New logic has tests
- [ ] Any non-obvious decision recorded in [docs/assumptions.md](docs/assumptions.md)

Self-merge is permitted for the maintainer, but CI must pass and you must read
your own diff first.

## Code style

Run the linter; it is the arbiter. Configuration is in `.golangci.yml`.

Beyond what the linter checks, this codebase has conventions worth knowing:

### Absent is never zero

A value the upstream did not supply must produce **no sample at all**. Not a
zero, and never the previous cycle's value.

```go
// Right: no data means no sample.
speed, ok := rec.numberField("proton_speed")
if !ok {
    return nil, nil
}

// Wrong: publishes 0 km/s as though it were measured.
speed := rec.numberFieldOrZero("proton_speed")
```

A stale solar wind speed presented as current is worse than no reading. This is
the single rule most likely to be broken by a well-meaning change.

### Timestamps come from upstream

Every sample carries the upstream's own observation time, never `time.Now()`.
The predecessor of this service stamped everything at write time, so a
three-hourly index was rewritten hundreds of times per interval under hundreds
of distinct timestamps.

### Nothing recoverable may stop the process

A failing upstream, a malformed response, a broker that went away — all are
ordinary conditions for a process meant to run unattended for months. Return an
error; do not exit. Panics are recovered at two levels, per source poll and per
sink publish.

Construction errors **are** fatal, because they mean a setting is wrong and
retrying will not fix it.

### Credentials go through `redact.Secret`

Never store a credential in a plain `string` field. `redact.Secret` holds the
value in a closure so that reflection-based formatting cannot print it.

```go
// Right
Password redact.Secret

// Wrong: fmt.Sprintf("%v", cfg) prints this in clear text
Password string
```

Pass every HTTP error through `redact.Error()` before returning or logging it.

### Comments explain the decision, not the mechanics

Explain why, and what breaks otherwise. Assume the reader can see what the code
does.

```go
// Right:
// A non-zero status flag means the row is unusable. Reading its density anyway
// is exactly how a -9999.9 reaches a dashboard as a real number.

// Wrong:
// Check the status flag.
```

### No panicking constructors

No `MustRegister`, no `MustNewConstMetric`, no `panic()` in production paths.
Log the failure and skip the metric.

## Testing conventions

- Plain `testing` with hand-written `t.Errorf`. **No testify.** The codebase
  has no assertion library and should not gain one.
- Test names are full sentences describing the behaviour:
  `TestParseWindPlasmaSkipsFlaggedAndSentinelRows`, not `TestParse2`.
- HTTP is tested against `httptest.NewServer`. **Tests never touch the
  network.** Capture a real response as a fixture in `testdata/` instead.
- Fixtures are real captured responses, truncated in place. Invented data hides
  the shapes upstreams actually send — several bugs in the predecessor existed
  because nobody had looked at the real bytes.
- Every sink package asserts `var _ sink.Sink = (*Sink)(nil)` as compile-time
  proof it satisfies the interface. Same for sources.
- Security-relevant tests use a distinctive canary credential and then grep the
  logs and errors for it.

## Adding a metric

1. Add a `Descriptor` to `internal/metric/descriptors.go` and append it to
   `All`. Help text is written for someone reading a Grafana tooltip who does
   not already know the field.
2. Emit it from a source.
3. Add its row to the metric table in `docs/design.md`.

The descriptor table is the single source of truth. Every sink derives naming,
help text and units from it, which is what keeps a Prometheus series, an
InfluxDB field and an MQTT topic describing the same quantity by the same name.

> A descriptor that no source emits will be caught in review. If you have a
> deliberate reason for one — see `AuroraBoundary` — document it in the
> descriptor comment and in `docs/design.md`.

## Adding a data source

Read `internal/source/source.go` first, then copy the shape of
`internal/source/hamqsl/`.

Requirements:

- A timeout on every request
- A bounded retry with backoff, honouring `context`
- Permanent-versus-transient classification. Never retry a 401, 403 or 404.
- `io.LimitReader` on every response body
- A descriptive `User-Agent` naming the project and repository
- Conditional GET (`ETag` / `Last-Modified`) if the upstream supports it
- A real captured fixture and tests

> **Respect the upstream.** Check for a stated polling policy before choosing a
> default interval. If one exists, encode it as a floor in `Interval()` that
> configuration cannot go below — see `hamqsl`, whose operator asks for hourly
> polling and was once shut down by his ISP over automated clients. Record the
> licence and attribution in `docs/design.md`. If the licence is restrictive,
> default the source to disabled.

## Adding a sink

Implement `sink.Sink` in `internal/sink/<name>/`, add its config struct to
`internal/config/config.go`, load it in `internal/config/load.go`, wire it in
`buildSinks` in `cmd/solarham-exporter/main.go`, and document it in
`.env.example` and `docs/ENV_VARS.md`.

New sinks default to **disabled**.

## Releasing

Maintainer only.

1. Merge to `main` and confirm CI is green.
2. Go to **Actions → Release → Run workflow**, choose `patch`, `minor` or
   `major`, and run it.

That is the whole process. The workflow reads the latest tag, computes the next
version, creates and pushes the annotated tag, re-runs vet and the race suite,
builds `linux/amd64` and `linux/arm64`, pushes to Docker Hub with the full
version plus the `major.minor`, `major` and `latest` tags, attaches an SBOM and
a provenance attestation, syncs the Docker Hub description, and generates
release notes.

Tick **dry run** to see which version it would produce without tagging or
publishing anything.

Pushing a tag by hand still works and does the same thing, so nothing here is
load-bearing on the Actions tab:

```bash
git tag -a v1.1.0 -m "v1.1.0 — short summary"
git push origin v1.1.0
```

### Why the bump is chosen rather than inferred

Tools like release-please derive the version from Conventional Commit prefixes
(`feat:`, `fix:`). This project writes sentence-case imperative subjects with no
prefix, so nothing in the history distinguishes a patch from a minor. Rather
than change how every commit is written to satisfy a tool, the one decision the
history cannot express is made at release time — which for a public image with
hundreds of pulls is a decision worth making deliberately anyway.

Release tags are protected: `v*` cannot be deleted or moved. A published
version therefore always refers to the same commit.

> **SECURITY:** Publishing requires the repository secrets
> `DOCKERHUB_USERNAME` and `DOCKERHUB_TOKEN`. Use a Docker Hub **access token**
> scoped to read/write, never your account password. Rotate it if a workflow
> log is ever made public.

Tagging moves `latest` on a public image. Consider who else pulls it before you
push a tag.
