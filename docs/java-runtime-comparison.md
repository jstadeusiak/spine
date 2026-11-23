# Java and dependency comparison

Historical measurements; see [release readiness](release-readiness.md) for current counts and validation.
Spine baseline commits below predate the public history reset. Raw reports under `build/` are local artifacts,
not files published in this repository; the recorded measurements and dependency release links remain unchanged.

2026-10-08. **Default: Temurin 25.0.4.1+1, Aeron 1.53.3, Agrona 2.6.1, SBE 1.40.2 and Gradle 9.8.1.**
Compatibility, deterministic bytes and existing shared/dedicated StageOne budgets pass locally.
The low-tail operating envelope and production startup qualification remain **OPEN**. No production warmup or AOT is enabled.

Baseline `2ffa24f`: Temurin **21.0.5+11**, Gradle **8.10**, Aeron **1.53.0**, Agrona **2.6.0**, SBE **1.38.1**.
Comparison JVMs: Temurin **25.0.4.1+1** and **27+35**. Handwritten production Java: **2,985 → 2,985 NCLOC**;
no runtime Java or schema change. JUnit **5.10.2** and AssertJ **3.25.3** remain fixed.
Java 25 is selected for upstream-tested compatibility, [LTS support](https://adoptium.net/support/), modern low-level APIs
including stable [Foreign Function & Memory](https://openjdk.org/jeps/454), and unchanged-budget passes.
Lower whole-process RSS/CPU in the frozen sample supports the choice; **Java 25 is not established as a tail winner**.

Completed architecture, now using the selected runtime:

```mermaid
flowchart LR
    F[Feed collectors] --> I[Captured proposals]
    I --> C[One Cluster input commit]
    C --> B[Deterministic in-memory book core]
    B --> O[Direct output publication and Archive]
    O --> U[Independent recorded consumers]
```

Proposed next increment; the startup prototype below is test-only:

```mermaid
flowchart LR
    T[Representative discarded training] --> W[Isolated core and transport warmup]
    W --> G[Readiness / first-window / allocation gates]
    G --> R[Restore authoritative state and admit real feeds]
```

## Completed compatibility gates

| Gate | Retained result |
|---|---|
| Old tuple on Java 21 / 25 / 27 | Each full suite: 150 tests, zero failures/errors/skips; strict warmed zero-allocation and snapshot/retry checks |
| New tuple on Java 25; default full build | Each full suite: 150 tests, zero failures/errors/skips; resolved dependency/codegen checks pass |
| Regenerated SBE sources | All 53 sources byte-identical across tooling versions; no added/removed sources |
| Deterministic corpus | 1,441 commands, 1,432 observations, 12 snapshot cuts; exact state and canonical suffix bytes pass |
| Canonical byte export | Old tuple on 21/25/27 and new tuple on 25: actual proposal/time, canonical, result and snapshot files byte-identical |
| Shared / dedicated StageOne | PASSED with unchanged budgets; run IDs `086aeb2d-0068-40ad-bbab-44d140d8be9c` / `f0cc838a-fbd3-4432-9086-afa97348285a` |

Java 27 remains a comparison option: its full-suite pass applies to the **old tuple**; no complete new-tuple Java 27 gate ran.
The final default build (local artifact: `build/reports/version-comparison-20261008/final-default-build/summary.json`) also passes all 150 tests
after correcting the collector diagnostic manifest to report the selected library versions.
StageOne proves the existing synthetic single-node page-cache contract; its CPU gate is disabled, and it is not an HFT or power-loss qualification.
The initial Java 21 suite failed during fixture teardown (`failed to send service termination position: NOT_CONNECTED`).
Closing consensus before the service container fixes the test lifecycle race; no warning filtering or production behavior changed.
The failed XML, focused rerun and subsequent passing full suites remain retained. Sandbox `/usr/bin/time -l` failures for frozen/startup runs
also remain under `failures/frozen-jvms-time-sandbox` and `startup-timing-sandbox-failure`; these are not JVM performance results.

## Dependency compatibility

[Aeron's published POM](https://repo.maven.apache.org/maven2/io/aeron/aeron-client/1.53.3/aeron-client-1.53.3.pom)
and [SBE's POM](https://repo.maven.apache.org/maven2/uk/co/real-logic/sbe-tool/1.40.2/sbe-tool-1.40.2.pom) both declare Agrona 2.6.1.
All Aeron modules are aligned. Reviewed immutable releases:

| Component | Baseline → selected | Release commit |
|---|---|---|
| Aeron | 1.53.0 → 1.53.3 | `377943bcb8256fa5279fc4e5c6d9f2a076294222` |
| Agrona | 2.6.0 → 2.6.1 | `6528b9e83975126b134185a6326ffb17ec44673a` |
| SBE | 1.38.1 → 1.40.2 | `05b076c0c64d7164b56db2f9b5e6ca476ac46072` |

Tagged CI explicitly tests **17/21/25** for [Aeron](https://github.com/aeron-io/aeron/blob/377943bcb8256fa5279fc4e5c6d9f2a076294222/.github/workflows/ci.yml),
[Agrona](https://github.com/aeron-io/agrona/blob/6528b9e83975126b134185a6326ffb17ec44673a/.github/workflows/ci.yml) and
[SBE](https://github.com/aeron-io/simple-binary-encoding/blob/05b076c0c64d7164b56db2f9b5e6ca476ac46072/.github/workflows/ci.yml); none explicitly lists 27.
Aeron fixes active-log joining retries, snapshot ordering and failed mark-file creation recovery; its egress `tryClaim` change concerns
control/session/admin replies, while application `ClientSession.offer` is unchanged. Agrona fixes an unused `ExpandableRingBuffer` failure;
SBE adds bit-set/raw APIs and validation/infinity fixes outside current schemas. See [Aeron](https://github.com/aeron-io/aeron/compare/1.53.0...1.53.3),
[Agrona](https://github.com/aeron-io/agrona/compare/2.6.0...2.6.1) and [SBE](https://github.com/aeron-io/simple-binary-encoding/compare/1.38.1...1.40.2) diffs.
Gradle **9.8.1** distribution SHA-256: `dce76f55f8e251a3a1f130eb120f30b3d271de2b76c9b0729d316b5a1b6dc01f`.
Official [compatibility](https://docs.gradle.org/current/userguide/compatibility.html) requires ≥9.1.0 for Java 25 and ≥9.8.0 for 27.
Explicit JUnit Platform launcher **1.10.2** matches Jupiter; `javaVersion` selects compilation/test/execution toolchains. Existing opens remain.

## Frozen JVM and selected-tuple replay

Nine frozen runs (local artifact: `build/reports/version-comparison-20261008/frozen-jvms/summary.json`) use identical Java 21 bytecode/classpath and the public Tokyo JSON/SBE corpus.
Three sequential rounds rotate **21/25/27**, **25/27/21**, **27/21/25**. G1, `-Xms512m -Xmx512m`, opens and GC/safepoint logging are fixed;
40 warmup and 400 measured full-corpus replays yield **572,800 timed observations/run**. Host: Apple M4 Max, 16 logical CPUs, macOS 26.5.1 arm64, no affinity.
Three selected-tuple runs (local artifact: `build/reports/version-comparison-20261008/candidate-runtime/summary.json`) use the same corpus/method but recompile on Java 25 with new libraries;
they do not isolate library changes from bytecode changes. Values below are independent-run minimum–maximum, not pooled percentiles or confidence intervals.

| JVM / tuple | Core p50 µs | Core p99 µs | Core p99.9 µs | Maximum µs | Peak process RSS MiB | Process CPU seconds |
|---|---:|---:|---:|---:|---:|---:|
| 21 / old | 0.458–0.459 | 4.542–5.125 | 18.875–21.375 | 64.292–121.584 | 583.50–592.50 | 4.44–4.59 |
| 25 / old | 0.375–0.458 | 4.667–5.000 | 19.417–21.917 | 102.833–136.250 | 562.97–563.20 | 4.14–4.23 |
| 27 / old | 0.375–0.458 | 4.875–5.208 | 20.083–22.208 | 90.250–154.958 | 576.00–579.22 | 4.01–4.26 |
| 25 / new | 0.375–0.500 | 4.792–5.167 | 21.125–24.292 | 84.875–112.959 | 564.28–565.30 | 4.22–4.38 |

Frozen p99/p99.9 ranges overlap; the new tuple also has higher p99.9 samples. No steady-tail or peak-speed win is claimed.
Timed boundary: preencoded proposal → `SequencerCore.onCommand` → borrowed result, including timer overhead; transport, bootstrap,
restore and cold oracle hashing are excluded. Restore allocates between passes and may affect later GC. CPU/RSS covers the **whole process**,
including setup, correctness, recovery, warmup and measurement; logs do not establish individual outlier causes.

## Full local IPC samples

Each configuration ran **once**. Closed-loop: 10,000 samples, 1,024 warmup, 5,000 levels/side, one outstanding proposal.
Boundary: ready before complete capture fence → Cluster book mutation → output Archive → recorded-consumer callback, same-process `nanoTime`.
Dedicated transport agents, busy application polling, no affinity, sync0 and test observer overhead apply. This is not venue latency or sustained capacity.

| JVM / tuple / transport idle | p50 µs | p99 µs | p99.9 µs | Maximum µs | Whole-process CPU / elapsed (cores) |
|---|---:|---:|---:|---:|---:|
| 21 / old / busy | 16.792 | 90.500 | 721.292 | 3,485.667 | 12.146 |
| 25 / old / busy | 17.792 | 218.333 | 1,142.875 | 5,218.334 | 11.749 |
| 27 / old / busy | 18.333 | 423.667 | 2,180.792 | 3,729.250 | 11.033 |
| 25 / new / busy | 16.750 | 109.334 | 1,502.208 | 6,581.417 | 11.452 |
| 25 / new / backoff | 13.750 | 123.375 | 350.917 | 3,054.625 | 8.659 |

Scheduled: 3,000 native heads at fixed, never-rebased 1,000 heads/s; 5,000 JSON/SBE observations across six 500-head phases.
SBE normally arrives at +50 µs, JSON +200 µs; degraded SBE +2 ms. A paused source is omitted, not actually disconnected.
Native scheduled time → first qualified recorded result includes source delay and schedule lateness. Each cell is **p99 / p99.9=max, milliseconds**;
500 qualified heads/phase cannot resolve a finer p99.9. Warmup is SBE-only; delayed proposals still serialize through one outstanding producer.

| Phase | 21 old busy | 25 old busy | 27 old busy | 25 new busy | 25 new backoff |
|---|---:|---:|---:|---:|---:|
| Baseline | 0.159 / 1.305 | 9.827 / 14.404 | 1.503 / 3.198 | 1.337 / 2.548 | 0.203 / 1.812 |
| SBE delayed | 5.407 / 10.151 | 5.510 / 9.458 | 5.355 / 10.115 | 5.404 / 10.176 | 5.697 / 10.216 |
| SBE paused | 0.280 / 0.608 | 1.650 / 2.880 | 0.474 / 1.064 | 0.348 / 1.015 | 2.013 / 3.989 |
| SBE restored | 0.251 / 0.817 | 0.354 / 0.725 | 0.877 / 1.905 | 0.325 / 1.077 | 0.153 / 0.194 |
| JSON paused | 0.462 / 1.446 | 0.219 / 1.698 | 13.434 / 17.954 | 2.989 / 4.918 | 0.200 / 0.249 |
| JSON restored | 4.526 / 9.284 | 4.497 / 9.218 | 25.420 / 30.035 | 14.273 / 19.023 | 4.567 / 9.328 |

| Scheduled configuration | Worst phase ready→recorded p99 / p99.9 / max ms | Whole-process CPU / elapsed (cores) |
|---|---:|---:|
| 21 old busy | 0.342 / 0.565 / 0.766 | 9.269 |
| 25 old busy | 1.068 / 6.484 / 14.353 | 8.835 |
| 27 old busy | 0.515 / 17.903 / 30.688 | 8.849 |
| 25 new busy | 0.429 / 5.628 / 7.438 | 9.172 |
| 25 new backoff | 0.208 / 3.789 / 3.789 | 3.366 |

These regressions remain visible: new busy closed p99.9/max exceed old 21; new busy JSON-restored p99 is 14.273 ms versus 4.526 ms;
backoff SBE-paused p99 is 2.013 ms. No repeated-run, causal, saturation or low-tail-envelope claim follows.
Closed sampled capture lag ≤256 bytes and acknowledged→follower lag ≤1 event. Output recorder lag is **unavailable/null**, not zero.
CPU and lag deltas are reported aggregate observations; the cold audit checks arithmetic, not independent counter measurement.

## Startup replay and HotSpot AOT prototype

Coinbase's [USENIX SREcon23 talk](https://www.usenix.org/conference/srecon23americas/presentation/sun) by Yucong Sun and Jonathan Ting has a
[Java warmup slide](https://www.usenix.org/sites/default/files/conference/protected-files/sre23amer_slides_sun.pdf) describing Azul Prime ReadyNow and repeated production-log replay.
This supports the mechanism in the user's recollection; the exact unnamed article remains unidentified. [ReadyNow](https://docs.azul.com/prime/Use-ReadyNow.html)
persists execution/compiler profiles in Azul Prime. Temurin 25 [JEP 515](https://openjdk.org/jeps/515) supplies saved method profiles for earlier JIT;
[JEP 514](https://openjdk.org/jeps/514) simplifies AOT class loading/linking caches. Native compiled-code AOT [JEP 544](https://openjdk.org/jeps/544) targets Java 28 as reviewed.

Startup evidence (local artifact: `build/reports/version-comparison-20261008/startup/summary.json`): old tuple, discarded core after binding/bootstrap, one cold pass,
40 replay warmups, 400 measured passes, equal JIT/GC/safepoint logging. Nine rotated JVM runs, one cache-training run and six alternating packed-classpath
Java 25 cache/off runs all pass. No transport or process-start→readiness timing; first post-warm pass is included in steady samples. Three-run ranges:

| JVM / mode | Cold p99 / p99.9 µs | First post-warm p99 / p99.9 µs | Steady maximum µs | 40-replay warmup ms |
|---|---|---|---:|---:|
| 21 normal | 53.291–56.875 / 99.250–103.750 | 4.333–4.375 / 19.041–19.375 | 56.041–87.250 | 86.60–89.81 |
| 25 normal | 57.084–78.209 / 103.750–142.500 | 4.917–6.083 / 20.041–23.875 | 65.500–149.250 | 91.68–98.09 |
| 27 normal | 58.166–64.167 / 89.458–105.833 | 4.208–6.334 / 19.917–49.875 | 58.167–83.625 | 89.44–95.57 |
| 25 packed AOT off | 56.084–61.125 / 89.958–111.208 | 4.209–5.084 / 19.875–29.667 | 68.083–101.250 | 88.87–92.48 |
| 25 packed AOT on | 41.792–55.167 / 279.208–318.250 | 4.042–4.333 / 14.041–21.000 | 46.750–1,273.292 | 67.94–74.79 |

Cache-on cold p50 is 1.459–1.583 µs versus off 3.041–3.417, but cold maxima worsen to **460.333–570.500 µs** versus **105.042–134.959**.
A 1.273 ms steady outlier also remains. Earlier compilation/shorter replay warmup does not establish a tail win or peak readiness.
Spine compiler log records continue during steady windows: normal 60–66, packed cache-on 38–45; records are not unique methods or proof of completed JIT.
The 15,925,248-byte cache SHA-256 is `e26c7ae274f82640204fbd1f998b3d9c650b116ddc7ee460e570d37ced0f554b`; flags apply only to test experiments.
A production gate must measure process-start→readiness and first-window tails/allocations/compiler activity, train representative JSON/SBE/books/retries/fragmentation,
discard synthetic state before authoritative restore, and isolate transport channels/recordings. Fake inputs must never enter the Cluster log, real books,
canonical output or consumer checkpoints. Production warmup/AOT and AWS Tokyo qualification remain **OPEN**.

## Reproduction and independent audit

Evidence index (local artifact: `build/reports/version-comparison-20261008/evidence-index.json`) retains gate counts and hashes; cold audit (local artifact: `build/reports/version-comparison-20261008/independent-audit/summary.json`)
verifies five closed sample distributions, five scheduled CSV/plan audits with **25,000 captured and 25,000 canonical frames**, and all 16 startup CSVs
(**10,104,192 rows**, order/identities/nearest-rank bounds, including 640 warmup passes). Four exported byte-stream sets and all 18 indexed artifact hashes match.
CPU/lag observations, startup state fields and JIT logs are not independently remeasured; closed sorted samples retain no event order/wire lineage.
Baseline bundle manifest: `10ef50c2c6a6bcac19c00197551a3df2ffb25f3faa8be21252def50f97f640bc`; new bundle: `ce5263363e3b5467806139f43ccf1323fc18257db20490340b3b773c469392b3`.
All core runs share actual JSONL SHA `fd7ccc860188305a019a99798f44bc61b750890dba7cfcff48ee7fbd9286fddf` and 170,630-byte snapshot SHA
`e45d128c553886c67b0533c54f997678759be6e732cf97bcc076149a205e0f07`. Historical invocation paths remain verbatim after evidence relocation to `build/reports/`.
