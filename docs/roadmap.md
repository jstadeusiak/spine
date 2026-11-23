# Build sequence

Release 1.0.0 provides the generic command-driven application boundary, identity-bound complete snapshots,
retention-relative consumer recovery and Archive protocol authority. Tests cover separate-driver UDP replay and
native three-member local failover. The [composability increment](composable-applications-result.md) adds declared
capacities, ordered application batches and segmented snapshots. Native timers, multicast output, distinct-host qualification, host/power-loss
durability and external-effect reconciliation remain open; see [release evidence](release-readiness.md).

The sequence below describes the optional feed and market-application work built on that runtime.

1. Establish a minimal canonical observation spine: compact SBE provenance, one deterministic input commit,
   recorded output, exact retry, independent catch-up and measured allocation/recovery boundaries.
2. Close the fixed Stage One operating envelope with retained manifests and measurements. Keep any unmet condition
   explicit; do not move the budgets to accommodate a result.
3. Connect real feed collectors using the complete captured-input boundary. Compare redundant sources using preserved
   native identity/raw bytes and timestamp/clock definitions. Build deterministic in-core venue books and native
   reconciliation, then instrument BBO/cross-venue evaluation. Measure redundant-feed tails and declared coverage.
4. Build deterministic hypothesis/replay tooling on recorded windows: interpreter/configuration versions, controlled
   experiments and reproducible results. Add ML only when the data/evaluation harness demonstrates a useful task.
5. Extend deployment delivery where required: multicast fan-out, independent idle-lag heads, distinct-host networks
   and host/power-loss recovery, each with a specific acceptance test.
6. Add execution with replay-safe effect reconciliation only after observation/research correctness is established.

The trading-profit objective motivates this backbone; infrastructure capacity or a backtest alone establishes no
profitability. Optional market applications own market state and deterministic price evaluation inside the core's
single writer; external order effects remain outside. Avoid generic registries or deployment frameworks.

Steps 1–2 are complete for the declared single-host IPC/page-cache envelope: both profiles pass. Step 3 is underway;
distinct-host delivery and production fault domains still require their separate acceptance gates.

The first Step 3 increment standardizes [collector/consumer composition](application-contract.md): existing Agent
owners, cold source profile v1, capture-controlled producer offers and chained handlers. The [raw adapters](../feeds/README.md)
connect a retained Hyperliquid order-level node segment and public L2 WebSocket. Exact capture/restart and raw-byte
replay are tested, including live public capture. Actual node access and retained snapshot bootstrap/framing remain
outstanding for the original order-level book comparison.

While node access is unavailable, [Binance testing](binance-testing.md) exercises anonymous Spot JSON trades through
the existing capture path. The same collector now supports tagged binary capture, and a test-only paired runner
compares JSON and SBE trades by native trade ID. The live paired Tokyo gate passed on 2026-10-08: a 60-second capture
plus 30-second process/Cluster restart, 846 matched trade IDs, no within-session unmatched trades or payload mismatches,
exact retained-prefix recovery and byte-identical repeated audits. The key is stored as an encrypted Tokyo SecureString.
The planned reconnect leaves eight trade IDs unobserved; this is not continuous upstream coverage. Public Binance
books are L2, so this does not close the original order-level gate. That trade gate predates the in-core depth increment described below.

The user clarified on 2026-10-08 that venue books, redundant-feed reconciliation and instrument pricing state belong
inside the pure single-writer core. This supersedes the proposed downstream-only Hyperliquid book consumer. The
[market-state core contract](market-state-core.md) diagrams the architecture boundaries and defines the gate.
The optional `examples-binance` module implements one Binance BTCUSDT L2 book with recorded bootstrap, JSON/SBE depth-range reconciliation,
validity, BBO and full core snapshot recovery. Retain both raw observations while applying a native transition once.
Its gate includes exact oracle state, all-cut replay/restore, gap/conflict/capacity behavior, zero warmed allocation,
measured full-path tails under redundant-feed faults and runtime size before/after. The retained Tokyo depth corpus now verifies exact covered book state and byte-identical snapshot recovery.
Bootstrap staging and coverage are declared; this is not an entire-venue-book guarantee. The
[retained fixed-schedule paired-feed run](market-state-result.md#scheduled-paired-feed-faults) keeps all 3,000 native
transitions valid through synthetic feed delays/pauses and preserves observed long scheduling tails. Independent
collector/subscriber saturation and a measured redundancy tail improvement still need their operating-envelope gate.

Then add a second qualified venue and instrument mappings for coherent composite BBO/cross-venue evaluation, followed
by pinned-window pricing experiments. The 167 retained Hyperliquid five-level snapshots remain useful partial-book
fixtures. Actual order-level node access, complete bootstrap and framing remain outstanding for full-book work.

Key-independent paired acceptance now passes with two actual loopback WebSockets, schema-valid JSON/SBE trades,
separate process/Cluster restart and exact cold audit against the complete corpus. A [fixed ingestion diagnostic](collector-ingest-profile.md)
also reconciles all 2,056 planned trades through capture and canonical output. These additions are test-only. They
neither rank live providers nor change the Step One budgets. The test-only boundary observer now separates that
interval under the same fixed workload and records its instrumentation effects. Capture-fence to canonical-publication
observation is the largest measured subinterval; producer scheduling, Cluster admission/commitment and service/output
work remain combined. A subsequent same-executable [idle-strategy comparison](collector-ingest-profile.md#idle-strategy-comparison-2026-10-06)
keeps all recording fences and selects existing BusySpin infrastructure strategies through five JVM properties.
Baseline ready-to-recorded-follower p99 falls to 65.75–114.125 µs, at approximately 6.26 sampled logical CPUs;
full scheduled-send-to-follower p99 remains 315.209–458.126 µs. WebSocket callback delivery and collector-owner
availability account for the largest remaining baseline intervals. High-rate/long-run and live measurements still need
their declared acceptance; default launch settings and Step One budgets remain unchanged. The keyed live trade
comparison now passes; the original order-level source/bootstrap gate remains open. Multicast, distinct-host delivery
and host/power-loss acceptance remain Step 5 work.
