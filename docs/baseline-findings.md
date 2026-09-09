# Baseline findings — pre-rewrite audit

Audit of `solarham.py` as captured from the running `SolarHam` container on
Unraid (192.168.60.50) on 2026-09-09. Recorded before the rewrite so the
defects being fixed are documented.

## The container is not writing any data

Every write batch fails. Container logs show a continuous stream of:

```
The batch item wasn't processed successfully because: (404)
HTTP response body: {"code":"not found","message":"organization name \"Home\" not found"}
```

Two independent causes:

1. **Org `Home` does not exist.** The script hardcodes `org='Home'`. The
   InfluxDB 2.9.1 instance has no org by that name resolvable with this token.
2. **Bucket name case mismatch.** The script writes to `solarham`. The actual
   bucket is `SolarHAM` (orgID `8ba47ba1e329d213`).

The supplied token is bucket-scoped — it cannot read `/api/v2/orgs`, so
org-name lookup fails even for a valid name. A write to
`?orgID=8ba47ba1e329d213&bucket=SolarHAM` returns HTTP 204.

Because the write API batches asynchronously and the loop never inspects the
result, the process stays "healthy" forever while dropping every point. There
is no health check and no non-zero exit, so nothing external noticed.

## Four fields never matched the feed and were silently skipped

The script's XPath names disagree with the actual element names in
`solarxml.php`. `findall` returns an empty list, the `for` body never runs, and
no error is raised:

| Script looks for    | Feed actually has | Result             |
|---------------------|-------------------|--------------------|
| `electronflux`      | `electonflux`     | never written      |
| `geomagneticfield`  | `geomagfield`     | never written      |
| `mufffactor` (3 f)  | `muffactor` (2 f) | never written      |
| `muff`              | `muf`             | never written      |

`electonflux` is misspelled in the upstream feed itself; the parser must match
the feed's spelling while publishing a corrected metric name.

## Fields present in the feed but never parsed

- `kindexnt` — non-tracking K-index (`No Report` when absent)
- `updated` — the feed's own GMT generation timestamp
- `source` — attribution (`N0NBH`, with URL attribute)
- `calculatedvhfconditions` — the entire VHF section: `vhf-aurora` for the
  northern hemisphere, plus E-skip for europe, north_america, europe_6m,
  europe_4m

## Values are written as strings, not numbers

Every field is passed through as raw element text, so InfluxDB stores
`"110"` rather than `110`. Numeric fields also carry leading whitespace
(`' 29'`, `'  0.0'`). This makes arithmetic, thresholds, and alerting in
Grafana awkward.

Genuinely non-numeric fields need deliberate handling rather than being
lumped in with the rest:

- `xray` — flare class notation, e.g. `B3.8` (decodes to W/m²: B3.8 = 3.8e-7)
- `signalnoise` — S-meter range, e.g. `S2-S3`
- `geomagfield` — categorical, e.g. `UNSETTLD`
- `muf` — `NoRpt` when unavailable
- `fof2`, `muffactor` — frequently empty elements (both empty at audit time)

## Timestamps are wall-clock, not source time

Each point is stamped `int(time.time() * 1e9)` at write time. The feed carries
its own `updated` field and regenerates far less often than the 30-second poll,
so the same observation is rewritten repeatedly under new timestamps. This
inflates series cardinality over time and misrepresents when a measurement was
actually taken.

## Poll rate

The loop runs every 30 seconds. At audit time the feed's `updated` value was
`09 Sep 2026 0322 GMT` and it is served through Sucuri with `x-sucuri-cache:
HIT`, so most polls return a cached, byte-identical document. This is a
courtesy problem against a free third-party service as much as a correctness
one.

## Smaller defects

- `influx.close` — missing call parens, so the client is never closed. Moot in
  an infinite loop, but wrong.
- Unused imports and dead variables: `json`, `WriteApi`, `utcTime`,
  `localTime`. `utcTime`/`localTime` are computed once at import and would be
  stale anyway.
- `getSolarXML` writes to `/tmp/solar.xml` and the two parse functions each
  re-read and re-parse that file from disk — three file operations and two
  full XML parses per cycle, for a 1.6 KB document already in memory.
- No timeout on `requests.get`. A hung connection stalls the loop indefinitely.
- No error handling anywhere: a fetch failure, a malformed document, or a DNS
  blip raises and kills the process.
- The container bind mount is `/mnt/nvme/solarham/solarham.py` →
  `/user/src/solarham/solarham.py`. The destination is `/user/`, not `/usr/`,
  so the mount never shadowed the real script — which is why updates had to be
  applied by hand inside the running container.
- The InfluxDB token is passed as a plain `env` var visible in
  `docker inspect`, and is committed to no secret store.
