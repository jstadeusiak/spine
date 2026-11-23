# Stage-one measured result

Historical replacement measurements; see [release readiness](release-readiness.md) for current release results.
The implementation commits below predate the public history reset. Raw reports under `sequencer/build/` are local
artifacts, not files published in this repository.

**PASSED in both shared and dedicated profiles.** The clean deterministic runtime completes the unchanged local
operating envelope and all five separate-process recovery proofs. The full build, 55 component tests and runnable
recovery demonstration also pass. Strict warmed maximum-size transport windows allocate zero bytes on the producer,
service and consumer owners; see [measurement boundaries](performance.md).

Measured implementation: `6958da3`. Runtime: Java 21.0.5+11-LTS, macOS/aarch64, 16 available processors. Scope is
synthetic, single host/single-node Cluster, IPC and page-cache recording. Schema IDs are 101/201. That runtime contains 1,138 handwritten NCLOC against
2,657 in the baseline, excluding generated code/tests/demo: a 57% reduction.
Later application-contract changes have their own component/allocation checks; these retained runs establish no
new feed-collector or source-comparison latency claim.

## Fixed operating envelope

| Measurement | Shared | Dedicated | Limit |
|---|---:|---:|---:|
| 1,000-observation worker elapsed | 9.994 seconds | 9.991 seconds | 10-second schedule |
| Maximum scheduling lateness | 8.28 ms | 4.58 ms | 100 ms |
| Admission p99 / p99.9 | 14.00 / 16.31 ms | 11.38 / 11.89 ms | 500 / 1,000 ms |
| Complete recording p99 / p99.9 | 7.07 / 11.01 ms | 1.42 / 4.63 ms | 1,000 / 2,000 ms |
| Fast application p99 / p99.9 | 7.24 / 11.15 ms | 1.51 / 4.73 ms | 2,000 / 3,000 ms |
| Idle / ongoing-input recovery | 86.08 / 9.36 ms | 84.46 / 9.26 ms | Each at most 20 seconds |
| Ongoing-input catch-up | 42,850/second | 43,209/second | At least 200/second |
| Parent observed heap | 464.86 MiB | 457.61 MiB | 768 MiB |
| Largest child observed heap | 34.10 MiB | 34.17 MiB | 192 MiB |
| Observed Archive-delivery cursor lag | 1 event | 1 event | Diagnostic |
| Parent average observed CPU cores | 0.340 | 2.135 | Diagnostic |

Each latency distribution contains the 256 maximum-size burst plus 1,000 sustained observations. Admission uses
producer-local elapsed time through a completely recorded correlated receipt. Recording/application use
parent-observed Archive/stdout delivery, including scheduling/control I/O. No cross-JVM monotonic timestamps are
subtracted. Cursor lag is admitted minus Archive-delivered cursor and includes replay scheduling; it is not a direct
recorder-position measurement. Heap values are 100 ms observed samples, not physical allocation maxima. CPU excludes
child JVMs and is diagnostic. These two local runs establish this envelope, not an HFT latency or multicast result.

## Recovery proofs completed in each profile

1. Kill a collector after its actual ACCEPTED reply is discarded. Restore its exact complete captured proposal and
   original producer incarnation; DUPLICATE returns the byte-identical original event with no extra output.
2. Pause independent consumer data at 22, observe the parent's recorded cursor over test control I/O and recover the
   idle final fence 280 without another input. The burst contains 256 complete 64 KiB capture payloads.
3. Drain a 400-event Archive backlog while actual fixed-rate input continues. A later BEGIN/GO observed by the parent
   proves ongoing input after recovery; reach 1,280 with equal projection and ordered full-frame digest.
4. Snapshot at 280, ABORT at 1,280 and restart. One authoritative input suffix regenerates 1,000 output frames into
   a new physical generation. Cold qualification verifies every overlapping frame's SHA-256.
5. Fresh and cursor-22 checkpoint consumers reconstruct identical state/digest; original captured producer progress
   continues the same logical history through 1,282.

Each final history contains two definitions and 1,280 observations representing 640 paired synthetic native events.
Both provider observations remain distinct. Output recordings 0/6 are physical coordinates, not ordering identities.
A repeated initialized command does not produce canonical output.

## Retained runs

Shared local report: `sequencer/build/reports/stage-one/run-603e147d-0e76-4b8a-b5a6-1e8fe09fb803/report.json`.
Profile SHA-256: `16a35d014358001baefdfecf39c2b40ab63a766213dc70fd617919c9de13bc0b`.
Replay / continued full-frame SHA-256: `cf73cd3ab9f43dd3f1e1dc50b8799fdf4169c4da1a0403040cb586c5f0870253` / `1e2bfa7f818db246fb78f61e6a273faa72304dabd0f0cb704d9885276be9dadc`.

Dedicated local report: `sequencer/build/reports/stage-one/run-98a42551-0506-45f2-b3aa-12eaf304d190/report.json`.
Profile SHA-256: `4cd55f4c6b8414add4ebb0795dce1c5492a8e2410a9e798636b7b8b8a05aff3e`.
Replay / continued full-frame SHA-256: `a062a591cddc298acd0229641fe81630486c7c7f55a5abea9925a1c532453d98` / `9e148fa320a4b5717febe34b6ffd11d2f697798a5b3e0c80e1cbd8955f7704f6`.

Every manifest field except transport configuration is identical between these passing runs. The workload and
numerical limits remain stage-one-local-v1; manifest v3 describes the replacement transport/capture boundary.
Reports preserve their original execution paths; retained directories are also copied to the main checkout.

Two earlier replacement shared runs remain FAILED: `dceeab1b-1c4e-44ec-b922-2f1beb151169` (246 ms maximum lateness)
and `87c0e564-1224-4943-93c0-c71bdee69df5` (167 ms). A 250 ms retry interval was shortened to 10 ms. Further journal
inspection found stale replies being synchronously fenced before the next pending proposal; those replies now follow
the producer's normal ignore rule. A cached parent heap sampler replaces repeated management-object lookup every
pump. The manifest declares its 100 ms cadence. All five recovery stages completed in both failed runs.

Historical allocating two-commit baseline reports remain retained: shared `c90294aa-9a2e-4b7b-8d1a-0d655f31836a`
and dedicated `4aae3f08-9a36-453a-8134-99a11185f5b1`. They failed at 34.17/38.56 admissions per second and
19.22/15.89 seconds scheduling lateness. Historical implementation/results remain in local backup history and run data.
No failed result was relabeled as passing.

## Boundary at this measurement

Stage one closed for this declared local envelope. Collector capture and redundant-source comparison followed in
later increments. The current release also qualifies Archive protocol authority, retained-suffix recovery and native
three-member local failover. Distinct-host qualification, multicast, host/power-loss durability and external-effect
reconciliation retain separate acceptance gates; see [release readiness](release-readiness.md).
