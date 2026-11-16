# First in-core book increment — 2026-10-08

Historical measurements from before the optional Binance extraction; see [release readiness](release-readiness.md)
for initial release validation and [composability](composable-applications-result.md) for subsequent result/snapshot
changes. The snapshot term sizes below describe these historical measurements. Raw reports under `build/` directories are local artifacts, not files published in
this repository. Historical implementation commits predate the public history reset.

One committed input now decodes and reconciles a bounded Binance Spot BTCUSDT L2 book, updates its integer BBO,
and emits one canonical `BookResult` containing the entire original observation. Producer retries reproduce the
original result while redundant sources remain separate observations. Full market state and retry results survive
core snapshots. See [the application contract](market-state-core.md).

## Real Tokyo corpus

A closed 30.274-second capture from the existing AWS Tokyo host retained 300 JSON and 1,130 SBE depth frames, their
subscription acknowledgements, and a 320,050-byte REST snapshot with 5,000 levels per side. The independent cold
exact-decimal JSON and SBE books have zero continuity gaps and agree at all 299 common native heads. The streams
end at different native IDs, so terminal states at different heads are not compared as equivalent.

Core replay admits 1,441 proposals: three typed bindings, six bootstrap chunks and 1,432 application frames. It
checks exact covered level state, frontier and BBO after every input against the independent Decimal oracle. The
retained ordered merge applies 1,127 advancing depth observations and retains 303 stale depth observations; it ends
at native frontier 101221733133 with 5,006 covered levels per side. Source-2 bytes use the existing collector framing:
tag 1 before unchanged binary SBE and tag 0 before UTF-8 acknowledgement text. The framing transformation is
declared in the retained fixture; core output preserves the entire captured representation.

Twelve real-corpus checkpoint cuts, including partial bootstrap, reproduce identical canonical suffix bytes and
complete final snapshots. Small fault fixtures additionally compare every possible cut. Tests cover JSON/SBE batch
boundaries, same-frontier contradictions, older stale arrivals, bounded pending gaps/repair, malformed or unsupported
frames, exact source/clock binding, changed epochs, capacity, stale prices and corruption rejected before restore.

The primary capture SHA-256 is `d0a443cb0e93cd3f58424125e6f06bffc31500dcaa214538595107f541c1c976`. The primary corpus
is under `feeds/build/reports/binance-book-20261008/binance-book-tokyo-20261008-f6e4869f/`; source schema
`stream_1_0.xml` pins Binance documentation commit `bed5d358dbe98bfa4722b6c00c27be1e8f0ba40b` and SHA-256
`6ea328467e144311b1f1efff38e9fe613829997f041dd02a3b7077885d10a1f7`. Other capture attempts, including the
conservatively rejected bootstrap attempt, remain retained.

## Allocation and recovery

Strict ThreadMXBean gates require zero bytes in both warmed windows for actual JSON/SBE core mutation, native
redundancy, producer retries after another producer mutates state, pending gap repair, malformed admitted input,
source scope rejection, maximum fragment/result encoding, maximum 8,192-level-per-side snapshot and staged-bootstrap
snapshot encoding. Escaping allocations verify the accounting counter. Client gates separately verify zero bytes
for maximum wrapped receipt validation and canonical consumer dispatch. Setup/restore and JVM warm-up are excluded.

An early staged-snapshot measurement with 2,048 warm calls reported a fixed 168-byte second-window allocation and
failed. It was not accepted or given a noise allowance. The gate now declares 30,000 warm calls and still requires
zero bytes in each measured window. Snapshot sorting uses bounded in-place heapsort rather than JDK merge workspace.

The actual Cluster integration captures complete exact proposals before offers, snapshots mid-bootstrap and with
a pending gap, restarts twice, repairs from committed suffix, checks a held receipt retry after another producer
changes state, and compares replayed output and capture bytes. Each installed book has 5,000 levels per side; its
snapshot exceeds the old 128 KiB publication limit. Snapshot channels now use 32 MiB terms (4 MiB message limit).
Ordinary data channels retain 1 MiB terms. There is no second Cluster append.

The previous fixed shared and dedicated Stage One acceptance gates pass with their numerical budgets unchanged.
The retained runs are `run-9dac9a99-b7c8-4435-ad0d-c215c5b7867a` (shared) and
`run-52aeb82e-ca3f-4099-90b1-a294febf0098` (dedicated), under `sequencer/build/reports/stage-one/`. Their
`termBytes` field describes ordinary data channels; snapshot terms for this increment are 32 MiB.

## Measured processing and runtime size

On this macOS arm64 host with Java 21.0.5, Aeron 1.53.0, Agrona 2.6.0 and SBE 1.38.1, 40 complete corpus
replays warm the isolated core, followed by 400 measured replays (572,800 calls). Timer overhead is included.

| Core processing | p50 | p99 | p99.9 | Maximum |
|---|---:|---:|---:|---:|
| All application frames | 0.542 µs | 4.458 µs | 17.792 µs | 72.792 µs |
| JSON source, including its ACK | 1.209 µs | 7.750 µs | 9.500 µs | 24.541 µs |
| Tagged SBE source, including its ACK | 0.459 µs | 1.750 µs | 18.291 µs | 72.792 µs |

The first diagnostic rescanned every current level for BBO and measured 26.958 µs p50 / 35.584 µs p99. The final
implementation keeps cached best prices, updates quantities directly and scans an affected side only when its best
level disappears. The earlier diagnostic used untagged replay input and is retained as qualified pre-change evidence;
the final table uses the strict collector framing and a frozen executable. This is core processing, not publish RTT.

Actual recorded-path profiles each retain 1,024 warmups and 10,000 one-level SBE updates against a 5,000-level-per-side
book. Ready is stamped before producer copying and the complete capture fence, and the end is the first recorded
follower callback. All timestamp differences use the same process `System.nanoTime` domain. Both profiles verify the
same two snapshot/restart/retry scenarios before measurement.

| Ready → recorded follower | p50 | p99 | p99.9 | Maximum | Sampled process CPU / wall time |
|---|---:|---:|---:|---:|---:|
| Backoff transport agents | 14.583 µs | 150.916 µs | 543.333 µs | 4.840 ms | 8.70 logical CPUs |
| BusySpin transport agents | 17.000 µs | 50.625 µs | 267.417 µs | 1.914 ms | 12.76 logical CPUs |

Both run on an Apple M4 Max with 16 logical CPUs and 64 GiB memory, using a dedicated driver, separate Cluster/Archive agents, busy application polling, no affinity and
`fileSyncLevel=0`. CPU includes test assertions, copies and JVM work; it is not a service-only CPU measurement.
Both report a sampled maximum capture lag of 256 bytes and acknowledged-to-recorded-follower lag of one event.
Exact output-recorder byte lag was not instrumented. These runs do not change production idle defaults or imply
that a two-CPU Tokyo host will achieve the same tails. No scheduled-rate, multi-subscriber saturation or live-feed
delay improvement claim follows from one outstanding proposal.

Reports and sorted nanosecond samples are under `sequencer/build/reports/market-book-integration/`:
`Backoff-1791500028327-329739898974291` and `BusySpin-1791500063202-329774773991208`. Test-fixture failures are
retained under `failures/`: the first restart fixture omitted explicit cold catalog refresh, and the first profile
stamped incorrect SBE timestamp offsets. Both were corrected in test code; production remained unchanged.
`./gradlew --offline check` passes all 150 component/integration tests, and both profile tasks pass.

## Scheduled paired-feed faults

The same runtime also passes a fixed 1,000-native-heads/second fixture with 3,000 heads and 5,000 admitted
observations. Six 500-head phases use baseline SBE delay 50 µs and JSON delay 200 µs, then SBE delay 2 ms,
omitted SBE observations, restored SBE, omitted JSON observations and restored JSON. All 3,000 heads apply once;
the other 2,000 observations are stale. Every result remains valid and matches the independently planned frontier
and BBO, with exact original observation bytes/source tuple and contiguous canonical sequence. JSON wins every head
in the delayed/paused-SBE phases; SBE wins every head in the other phases.

The schedule starts once and is never rebased. One exact proposal is captured, committed and followed before the
next; lateness therefore remains visible rather than disappearing into a closed loop. Both runs complete in about
2.999 seconds, or approximately 1,000 heads/second and 1,667 observations/second. Warmup is 1,024 SBE observations;
JSON is exercised by preceding correctness cases but does not receive an equivalent warmup. Phase comparisons may
include JSON/JIT warmup effects. This is a serial synthetic arrival plan with omitted observations, not an actual
WebSocket disconnect, independently racing collectors, saturation test or measured single-source comparison.

All values below are p99 in microseconds. Native time is the planned head origin before the deliberate feed delay;
ready is actual arrival before copying/capture. Each phase has 500 qualified heads and 500 or 1,000 observations.

| Phase | Backoff ready → recorded | Backoff native → first recorded | BusySpin ready → recorded | BusySpin native → first recorded | BusySpin arrival lateness | BusySpin ready → service exit upper bound |
|---|---:|---:|---:|---:|---:|---:|
| Baseline | 144.500 | 211.042 | 150.584 | 273.167 | 78.792 | 103.625 |
| SBE delayed | 362.000 | 6,213.542 | 69.583 | 5,338.625 | 5,126.000 | 51.750 |
| SBE paused | 6,303.667 | 82,141.875 | 67.750 | 3,855.208 | 2,662.583 | 66.709 |
| SBE restored | 350.209 | 400.292 | 45.375 | 96.792 | 1.958 | 33.792 |
| JSON paused | 110.291 | 160.333 | 45.458 | 96.375 | 1.875 | 35.250 |
| JSON restored | 115.792 | 5,146.333 | 54.291 | 4,472.042 | 4,408.875 | 42.500 |

The worst native-to-first-recorded samples are 83.431 ms for Backoff and 10.122 ms for BusySpin. Backoff's
SBE-paused phase has 80.951 ms p99 schedule lateness; these are observed scheduling/backlog tails in this fixture,
not an isolated core-processing measurement. No causal attribution to GC, JIT or operating-system scheduling was
instrumented. The recorded-path low-tail operating envelope is still open; neither run demonstrates a redundancy
tail improvement against a measured single-source comparator.

Service callback exit follows the core application/output call, so it is a combined upper bound for state application
from ready, including capture, Cluster commitment and output publication. It does not isolate commitment or book
mutation. The recorded callback can race ahead of the exit timestamp; the two are not assumed to be ordered. The
isolated core table above measures the decode/reconcile/apply/encode call separately. Raw samples retain all five
timing origins and every observation; with 500 qualified heads per phase, p99.9 is the sample maximum.

Sampled process CPU/wall averages are 3.36 logical CPUs for Backoff and 9.29 for BusySpin. CPU includes idle polling,
test assertions/copies and JVM work, attributed by each arrival's native-head phase. Both report sampled maximum
capture lag 288 bytes and acknowledged-to-recorded-follower lag one event. Exact output-recorder byte lag remains
uninstrumented. Page-cache recording, no affinity and the 16-CPU local host remain the declared envelope.

Each report retains `summary.json`, `samples.csv`, exact length-prefixed captured/canonical frames and its successful
`task-test.xml` under `sequencer/build/reports/market-book-integration/`:
`scheduled-Backoff-1791500758784-330470350894208` and `scheduled-BusySpin-1791500783167-330494734026166`.
The final repository check after this test-only addition again passes all 150 tests.

The independent cold audit re-decodes every JSON/tagged-SBE input and joins all 5,000 exact captured/canonical pairs
per run, verifying producer sequence on captured proposals, canonical sequence/source/times, outcomes, native frontier,
BBO/coverage/scales and first-winner counts. It recomputes every retained percentile and rate; CPU deltas and lag maxima
remain reported observations rather than independently sampled counters. Canonical schema 101 does not carry producer
identity/sequence fields; capture lineage is joined through exact original observation bytes and the source tuple.
The separate `feeds/build/reports/binance-book-20261008/scheduled-proof-20261008.tgz` retains both runs, cold audits,
172-file manifest, final test XMLs and unchanged runtime/source hashes. SHA-256 is
`fce10c4c7f4eac83b975dbd36d23075c4506943c61a7e471329f454f78cd3e4e` (1,049,215 bytes). Its copied-evidence auditor
also passes. Like the compact IPC proof, this is evidence rather than a self-contained Gradle/JUnit runner.

## Runtime size and retained executable

Handwritten Java NCLOC before this increment (`d9c22bea37b9e4694c3c23dd1e2d5de45700bf5f`) and after:

| Module | Before | After |
|---|---:|---:|
| Protocol | 262 | 267 |
| Client | 546 | 561 |
| Sequencer | 341 | 1,370 |
| Feeds | 787 | 787 |
| **Runtime total** | **1,936** | **2,985** |

**Delta: +1,049 lines.** This counts nonblank Java `src/main/java` lines after lexically removing comments while
preserving literals, including imports/braces. Generated sources, tests, fixtures, schema XML and build scripts are
excluded; the 60-line example is unchanged. New production owners are the bounded depth decoder, book and market
state; no framework or generic registry was added.

Exact reports are in `feeds/build/reports/binance-book-20261008/core-replay-f6e4869f/`: `verification.json`,
`profile.json`, `production-ncloc.json`, `frozen-invocation.json` and frozen source/classpath manifests. The executable
archive SHA-256 is `808159e35bc52b0ebfbfd35a6e686af98f2d903e45bb31cb73a593610c536ab3`. The self-contained
`reproduction-bundle.tgz` also retains the public corpus, independent oracle, source/dependency hashes and offline
`reproduce.py`; its SHA-256 is `8d0ea3c91e3ac5bb52ed6524d53365b798e78e2e52052ce84cdaf120fb8c1141`.

The compact `feeds/build/reports/binance-book-20261008/ipc-proof-20261008.tgz` retains the closed-loop profiles,
raw samples, test XMLs, failed attempts and source/class hashes. SHA-256 is
`45f05410f662c36ef5f12846c3dae2f025627d6b638c29789fdff3e5817ba959`. Its manifest verifies 18 runtime sources and
83 runtime classes against the frozen core executable and independently recomputes both 10,000-sample distributions.
This is an evidence bundle; it omits Gradle/JUnit worker libraries and is not a standalone pipeline runner.

## Reproduce

Use the retained reproduction bundle above for the exact Java 21 baseline. The commands below create a new run
with the current build; select Java 25 as documented in the [version comparison](java-runtime-comparison.md),
then prepare a retained corpus fixture:

```sh
python3 examples-binance/src/test/python/binance_book_replay.py prepare /absolute/path/to/corpus /absolute/path/to/fixture
./gradlew --offline :examples-binance:binanceBookReplay --args="/absolute/path/to/fixture 400"
python3 examples-binance/src/test/python/binance_book_replay.py verify /absolute/path/to/fixture
./gradlew --offline :examples-binance:marketBookProfile
./gradlew --offline :examples-binance:marketBookProfile -PmarketIdle=busy
./gradlew --offline :examples-binance:marketBookProfile -PmarketWorkload=scheduled
./gradlew --offline :examples-binance:marketBookProfile -PmarketWorkload=scheduled -PmarketIdle=busy
./gradlew --offline check
```

The application extraction moved these tools into optional `examples-binance`; new IPC profiles are written under
`examples-binance/build/reports/market-book-integration/`. The dated evidence locations above remain unchanged.

The standalone core profile measures preencoded proposal through `onCommand` and borrowed result bytes. It excludes
transport, cold oracle hashing and bootstrap/restore. Cold resets allocate between replay windows and can influence
process GC tails. The actual IPC profile uses one outstanding proposal, complete capture recording, one Cluster
commit, book mutation, output recording and a recorded-follower callback. It is a synthetic closed-loop diagnostic,
with test observer overhead, no affinity and page-cache recording. The scheduled paired-feed fixture above adds
fixed-deadline delay/pause measurements; it does not establish live-venue, independent subscriber saturation or a
redundancy tail improvement. Those need a subsequent measured operating envelope.

Coverage remains the bootstrap price domain. Unchanged deeper levels are unknown. This increment does not claim a
complete venue order-level book, run a continuous live depth repair operator, expire prices during idle periods or
combine a second venue.
