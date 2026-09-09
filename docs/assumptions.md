<!--
doc: ASSUMPTIONS
last-refreshed: 2026-09-09
generated-by: doc-refresh skill
-->

# Assumptions

Non-obvious decisions, recorded per the project's golden rules in
[CLAUDE.md](../CLAUDE.md).

Format for each entry:

- **Assumption:** one clear sentence
- **Why:** rationale
- **Recorded by:** name
- **Date:** YYYY-MM-DD

The entries below are backfilled from the September 2026 rewrite. They were
decided during that work and written down afterwards.

---

## Go rather than Python

- **Assumption:** Rewriting in Go is worth the cost of abandoning the working
  Python script.
- **Why:** The sibling project `gogmc320` (`dougeubanks/gmc-exporter`) already
  established the shape — sink fan-out, distroless image, `_FILE` secrets,
  health endpoint triad, CI and release pipeline. A second exporter in a
  different language means maintaining two of everything. The concrete gains
  are a static binary in an 18 MB image with no interpreter, compile-time proof
  every sink satisfies its interface, and a race detector in CI.
- **Recorded by:** Claude (with Doug Eubanks)
- **Date:** 2026-09-09

## hamqsl.com is polled hourly with a hard 15-minute floor

- **Assumption:** A courtesy limit that configuration can override is not a
  limit, so the floor is enforced in code.
- **Why:** N0NBH's FAQ asks for hourly polling and states he has already been
  shut down by his ISP once because of clients polling too often. The
  predecessor polled every 30 seconds — up to 360 times his stated update rate.
  The feed sends no `ETag` and no `Last-Modified`, so conditional requests are
  impossible and self-throttling is the only available control. A configured
  value below the floor is raised with a warning rather than rejected, because
  refusing to start would let one bad number take down an exporter serving four
  other sources.
- **Recorded by:** Claude (with Doug Eubanks)
- **Date:** 2026-09-09

## hamqsl supplies only four quantities

- **Assumption:** Everything hamqsl carries that NOAA also carries is taken
  from NOAA instead.
- **Why:** Emitting both would create duplicate series that disagree — they are
  different instruments sampled at different instants, and two writers to one
  series makes it flap for no physical reason. hamqsl is kept only for the HF
  band ratings, the VHF phenomena, the signal-to-noise estimate and the
  geomagnetic field wording, which are N0NBH's own model output with no
  authoritative equivalent.
- **Recorded by:** Claude (with Doug Eubanks)
- **Date:** 2026-09-09

## InfluxDB 2.x accepts an organisation ID as well as a name

- **Assumption:** Addressing the organisation by ID must be possible, not just
  by name.
- **Why:** A bucket-scoped token cannot read `/api/v2/orgs`, so it cannot
  resolve an organisation by name and every write addressed by name returns 404
  even when spelled correctly. This, combined with a bucket name whose case did
  not match, is why the predecessor container wrote nothing for its entire
  lifetime while appearing healthy. Requiring exactly one of the two — neither
  is an error, both is an error — makes the failure a startup message instead
  of a silent 404 loop.
- **Recorded by:** Claude (with Doug Eubanks)
- **Date:** 2026-09-09

## prop.kc2g.com is disabled by default

- **Assumption:** A restrictive licence should be an opt-in the operator makes
  knowingly, not something inherited from a default.
- **Why:** The data is CC BY-NC-SA 4.0 and non-commercial only, sourced from
  ionosonde operators via GIRO and INGV. Shipping it enabled would silently
  impose that restriction on every deployment.
- **Recorded by:** Claude (with Doug Eubanks)
- **Date:** 2026-09-09

## Ionosonde stations are filtered on their own timestamps

- **Assumption:** Per-station freshness must be checked, not just the
  document's.
- **Why:** `stations.json` is served fresh even when individual stations inside
  it have been silent for months — a September document was observed carrying a
  March reading. Publishing without a per-station age check would put a
  six-month-old foF2 on a dashboard as current. Coverage is thinning
  independently of this project: Russia and China stopped sharing in 2021,
  Japan in 2023, the US Space Force withdrew the NEXION network covering most
  of CONUS in 2023, and NOAA shut down its ionosonde distribution in 2024.
- **Recorded by:** Claude (with Doug Eubanks)
- **Date:** 2026-09-09

## No aurora equatorward boundary is published

- **Assumption:** The descriptor is kept but deliberately emitted by nothing.
- **Why:** SWPC's OVATION product states no boundary. It is a grid of energy
  flux by magnetic local time and magnetic latitude, and deriving a boundary
  requires choosing a flux threshold the data does not supply. Plausible
  thresholds move the answer by several degrees of latitude with nothing to
  arbitrate between them, so any published number would be an invention dressed
  as a measurement. Hemispheric power, which OVATION does state, is published
  instead. The descriptor remains so the name and unit are settled if a
  defensible source appears.
- **Recorded by:** Claude (with Doug Eubanks)
- **Date:** 2026-09-09

## Solar wind density comes from a legacy text product

- **Assumption:** `/text/ace-swepam.txt` is preferred over
  `/json/rtsw/rtsw_wind_1m.json` despite being the older, ACE-only feed.
- **Why:** Density is not in `/products/summary/`, which carries only speed and
  the magnetic field. The modern RTSW endpoint returns roughly 2.5 MB of 3,478
  records covering a full day, every minute, to yield one number — about 3.6 GB
  of daily transfer against a public service for a single gauge. The text
  product carries the same quantity at the same cadence in about 9.5 KB, 265
  times smaller. The trade is that RTSW fails over between ACE, DSCOVR and
  IMAP, so density becomes absent if ACE alone stops reporting. Sentinel and
  status-flag handling makes that an absent series rather than a wrong one,
  which is the outcome that matters.
- **Recorded by:** Claude (with Doug Eubanks)
- **Date:** 2026-09-09

## The D-RAP grid is opt-in

- **Assumption:** The most propagation-relevant product SWPC publishes ships
  disabled.
- **Why:** It is a 90×90 global grid. At full resolution it produces tens of
  thousands of series, more than everything else in the exporter combined, which
  would strain a small Prometheus or InfluxDB instance. `GRID_STEP` subsamples
  it, defaulting to every tenth cell per axis.
- **Recorded by:** Claude (with Doug Eubanks)
- **Date:** 2026-09-09

## A partial poll publishes what it fetched

- **Assumption:** One failing endpoint within a tier does not discard the
  samples its siblings returned.
- **Why:** Partial space-weather data is far more useful than none. A tier
  returns an error only when every endpoint in it failed.
- **Recorded by:** Claude (with Doug Eubanks)
- **Date:** 2026-09-09

## An HTTP 304 counts as a successful poll

- **Assumption:** Not-modified is success, not failure.
- **Why:** The data is current and we already have it. Counting it as a failure
  would make a source that behaves well — using conditional requests and
  costing the upstream almost nothing — look broken in the metrics and flap
  `/readyz`.
- **Recorded by:** Claude (with Doug Eubanks)
- **Date:** 2026-09-09

## Store retention and sink timeouts are derived, not configured

- **Assumption:** These are computed from the source intervals rather than
  exposed as settings.
- **Why:** Retention is three times the slowest source's interval; each sink's
  publish timeout is half the fastest source's interval, clamped to 1–30
  seconds. Both could be settings, but doing so would ask an operator to reason
  about five different cadences to answer a question the process can answer
  itself. `SOLARHAM_PROMETHEUS_RETENTION` overrides the former for anyone who
  disagrees.
- **Recorded by:** Claude (with Doug Eubanks)
- **Date:** 2026-09-09

## Prometheus reading gauges are a custom collector, not registered gauges

- **Assumption:** Metrics are collected from a sample store at scrape time.
- **Why:** A field the upstream stopped supplying must genuinely disappear from
  `/metrics`. A registered `prometheus.Gauge` can only hold a value; it cannot
  become absent. A stale solar wind speed presented to a dashboard as current is
  worse than no reading at all.
- **Recorded by:** Claude (with Doug Eubanks)
- **Date:** 2026-09-09

## Prometheus scrape errors do not fail the whole endpoint

- **Assumption:** `promhttp.ContinueOnError` rather than the sibling's
  `HTTPErrorOnError`.
- **Why:** One malformed series should cost its own series, not every metric on
  the endpoint. This exporter publishes many more, and far more heterogeneous,
  series than the sibling does, so the blast radius of the stricter setting is
  correspondingly larger.
- **Recorded by:** Claude (with Doug Eubanks)
- **Date:** 2026-09-09

## Go runtime and process collectors are not registered

- **Assumption:** No `go_*` or `process_*` metrics are exposed.
- **Why:** This process is a data path, not a service whose heap is
  interesting. Around forty runtime series would outnumber the space-weather
  ones on a quiet scrape. `Registry()` is exported so anyone who disagrees can
  add them.
- **Recorded by:** Claude (with Doug Eubanks)
- **Date:** 2026-09-09

## MQTT connects lazily and Home Assistant discovery is published once per series

- **Assumption:** The broker is dialled on first publish, not at construction,
  and discovery documents are sent on first sight of a series rather than every
  batch.
- **Why:** The exporter's primary job is serving Prometheus, and a broker that
  is not up thirty seconds after a host reboot must not delay or fail that.
  Re-publishing thousands of retained configuration documents every minute
  would be abusive to the broker; the announced set is cleared on reconnect so a
  restarted broker is re-announced.
- **Recorded by:** Claude (with Doug Eubanks)
- **Date:** 2026-09-09

## The legacy lowercase `url` and `token` variables are still accepted

- **Assumption:** Deprecated aliases are kept rather than requiring a clean
  cutover.
- **Why:** "Drop-in replacement" means swapping the image and changing nothing
  else keeps working. Both log a deprecation warning naming the modern
  variable, and either implies `SOLARHAM_INFLUX2_ENABLED=true`. Setting an alias
  together with its modern equivalent is an error rather than a silent
  precedence rule, because a silent rule is the kind of thing that wastes an
  hour at 3am.
- **Recorded by:** Claude (with Doug Eubanks)
- **Date:** 2026-09-09

## A clean SIGTERM exits zero

- **Assumption:** Deliberate shutdown is not an error condition.
- **Why:** Exiting non-zero after a clean stop makes a supervisor restart a
  container that was stopped on purpose.
- **Recorded by:** Claude (with Doug Eubanks)
- **Date:** 2026-09-09

## `/healthz` checks nothing but the process

- **Assumption:** Liveness deliberately does not verify any upstream or sink.
- **Why:** Liveness that depends on a backend turns an upstream outage into a
  restart loop, which is strictly worse than the outage. Readiness is where
  dependency state belongs, and it is evaluated per source against that
  source's own interval.
- **Recorded by:** Claude (with Doug Eubanks)
- **Date:** 2026-09-09

## No InfluxDB client library

- **Assumption:** Both InfluxDB sinks write line protocol over `net/http`.
- **Why:** The project's golden rules require justifying each dependency
  against its size, security surface and maintenance cost. Line protocol is a
  simple, stable text format, and hand-writing the encoder — with explicit
  escaping tests for commas, spaces, equals signs, quotes and backslashes —
  costs less than carrying a client library and its transitive tree. The
  predecessor's use of the official async client is also what hid its 404s.
- **Recorded by:** Claude (with Doug Eubanks)
- **Date:** 2026-09-09

## Series collisions are resolved by declared authority, not by arrival

- **Assumption:** Where two sources publish the same series, an explicit
  per-source ranking decides the winner, and only timestamps break ties within
  a rank.
- **Why:** Eighteen additional sources brought eight genuine same-key
  collisions. Without an ordering the winner would be whichever source last
  polled with a newer upstream timestamp, so a dashboard would show a number
  flapping between two subtly different values for no visible reason — not
  obviously broken, just quietly wrong, which is the worst available outcome.
  The rule is that the more authoritative and current source wins: DRAO
  Penticton over NOAA for the 10.7 cm flux because it operates the instrument,
  GFZ Potsdam over NOAA for Kp because it defines the index, USGS over
  INTERMAGNET for BOU and FRD because it operates them. Losing a contest costs
  a source that one series and nothing else.
- **Recorded by:** Claude (with Doug Eubanks)
- **Date:** 2026-09-09

## Polling cadence is matched to publication, not chosen for convenience

- **Assumption:** A source whose upstream publishes on a known clock declares
  that clock and is polled shortly after it, rather than on a fixed interval.
- **Why:** Most of these upstreams are not continuous. DRAO measures the flux
  three times per UT day; NOAA regenerates its daily indices once, around 02:25
  UT; the ARRL activity file is rebuilt about weekly. Polling any of them every
  few minutes fetches an identical document hundreds of times to learn nothing,
  which is discourteous to the publisher and useless to us. The schedule
  carries a lag because a stated time is when a publisher starts writing, not
  when the file becomes readable — polling at exactly the stated minute fetches
  yesterday's copy and then waits a full period to notice.
- **Recorded by:** Claude (with Doug Eubanks)
- **Date:** 2026-09-09

## Request spacing is enforced per host, shared across sources

- **Assumption:** Rate limiting lives in one shared HTTP client keyed by
  hostname, not in each source.
- **Why:** A per-source limiter lets ten sources each politely make one request
  per second to the same host and collectively make ten. Several of these
  publishers have asked in writing to be polled gently and one has been shut
  down by his ISP over exactly this load, so the limit has to be a property of
  the host rather than of the caller. Spacing also cannot be disabled by
  omission — an unconfigured policy is floored to a safe value, and only an
  explicit zero disables it, which is the case for polling your own receiver on
  localhost.
- **Recorded by:** Claude (with Doug Eubanks)
- **Date:** 2026-09-09

## Accept-Encoding is left to Go's transport

- **Assumption:** The shared HTTP client never sets `Accept-Encoding` itself.
- **Why:** Go's transport adds the header and transparently decompresses the
  response, but only while the caller has not set it. Setting it manually
  silently transfers decoding responsibility to the caller, so every body
  arrives still compressed. This was a real bug: four sources failed on the
  gzip magic byte, and it was invisible to the test suite because `httptest`
  servers do not compress. Requests are still compressed; the transport simply
  owns both halves.
- **Recorded by:** Claude (with Doug Eubanks)
- **Date:** 2026-09-09

## Satellite pass predictions are not published

- **Assumption:** Only element-set freshness is exported, never a countdown to
  the next pass.
- **Why:** A countdown is a computed function of time rather than an
  observation. It is wrong between scrapes, quantised to the scrape interval,
  reconstructs badly in any range query, and cannot be meaningfully averaged or
  rate-limited. Element-set age is a genuine fact about the outside world, and
  stale elements are a real operational problem worth alerting on. The same
  reasoning excludes hosted propagation predictions, which are monthly-median
  models driven by a smoothed sunspot number and change only at month
  boundaries.
- **Recorded by:** Claude (with Doug Eubanks)
- **Date:** 2026-09-09
