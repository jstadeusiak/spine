# Stage one: capture, order, record and reconstruct

The exit gate is a usable single-host observation backbone: capture a declared window, interrupt its components,
then reconstruct identical ordered bytes and deterministic consumer state through a fixed recorded cursor in fresh
processes, within the declared operating envelope. The canonical order is Spine's acceptance order, not a claim
about the physical ordering of events across venues.

The [deterministic-core contract](deterministic-core.md) governs the replacement runtime: one committed input,
pure in-memory single-writer logic, direct recorded output and measured zero allocation in steady-state Spine
processing. The workload, numerical budgets, exact-retry, idle catch-up and byte/state reconstruction outcomes below
remain required. Direct-output recording, snapshot-boundary and repeated-output tests replace the historical
bridge/pending-self-message fixtures. Regeneration uses complete retained input or an adequate earlier baseline;
consumers apply identical replay/member overlap only once. Retain current run results alongside the historical baseline.

## Capture contract

Record source definitions before their observations. The outer `(sourceId, sourceConfigVersion, clockId)` identifies
an immutable definition containing provider, venue-qualified product/subscription scope, source session, collector, configuration,
clock meaning and synchronization uncertainty. A session or interpretation change requires a new configuration
version. An uncertainty of `-1` is unknown; a clock ID alone does not establish synchronization.
The definition's product scope never changes per event. The [application profile v1](application-contract.md)
captures a whole raw frame at subevent zero, including multiplexed product scopes; downstream normalized children
carry actual product identities. An explicitly declared per-product/subevent capture layout uses distinct source
keys for its products and can reference the same physical raw frame/session. This interpretation clarification
does not change the fixed Stage One workload or numerical budgets.

Each capture payload retains native event identity when supplied, raw-frame identity scoped to its source session,
subevent index and exact original raw bytes. Empty native identity explicitly means unavailable. Matching across
providers is a downstream decision. Both provider observations remain in the canonical history even when their
native identities and bytes match. Receive, complete-message and decoded-ready times retain their original clock
meaning through retries and replay.

The complete capture payload must fit the existing 64 KiB admission limit. Oversize input rejects explicitly;
there is no truncation. The one-slot producer refuses another observation while pending. Collectors must respond
to that backpressure explicitly; this gate adds no hidden queue or automatic discard policy.

## Recovery gate

One runnable acceptance command exercises collector-owned recorded capture, the one-slot producer codec, real
single-node Cluster/direct recorded output, Archive and independent consumer JVMs. It must establish all of the following:

- A producer killed after real admission but before recorded acknowledgement reopens its external complete capture
  with its original incarnation and exact pending bytes. Retry produces the original receipt and no extra event.
- A paused consumer observes its lag independently of data polling, recovers through Archive and reaches a final
  idle fence without needing another canonical event.
- Snapshotting behind the final head and restarting Cluster regenerates a suffix into a new output recording.
  Cold qualification verifies overlapping bytes. Fresh replay and checkpoint-restored consumers reconstruct the
  same ordered full-frame SHA-256, projection and cursor. The next observation continues that history.
- Missing/corrupt history, conflicting identity and differing repeated output fail explicitly. Bounded replay has
  monotonic progress/startup deadlines; fragment callback failures reach the owner and stop application.

Compare only consumers using the same interpreter and configuration at the same cursor. Checkpointed prefix
state is restored together with its cursor. Recovery never authorizes external effects.

## Declared development load profile

These budgets are fixed before measurement. They describe a local synthetic acceptance envelope, not an exchange
feed capacity or HFT latency promise. Measure the actual feed/hardware envelope before deploying collectors.

| Quantity | Required value |
|---|---|
| Sustained observations | 100 per second for 10 seconds |
| Sustained scheduler lateness | At most 100 ms; all 1,000 scheduled observations must be submitted |
| Unpaced burst | 256 observations, each complete capture payload 64 KiB |
| Admission observed latency | p99 at most 500 ms; p99.9 at most 1 second |
| Complete recording observed latency | p99 at most 1 second; p99.9 at most 2 seconds |
| Fast consumer application observed latency | p99 at most 2 seconds; p99.9 at most 3 seconds |
| Consumer recovery | Within 20 seconds; catch-up at least 200 observations per second while live input continues |
| Transport startup | Within 30 seconds |
| Parent observed heap | At most 768 MiB |
| Each consumer/producer child observed heap | At most 192 MiB, with a 256 MiB heap limit |

Report recorder lag, observed processing/CPU time, elapsed time and memory measurement method alongside the gate
results. Latency comparisons across processes must document clock calibration/resolution or use parent-observed
acknowledgements that include the communication overhead. `System.nanoTime()` values from different JVMs are not
assumed directly comparable. CPU time is diagnostic rather than an unmeasured capacity guarantee.

Write the profile manifest before publishing test traffic, and retain a machine-readable result containing source
definitions, counts, cursors, ordered hashes, failure stages, latency quantiles, catch-up rate and resource samples.
A failed gate stays failed; any changed profile requires a separate declared run.

The harness accepts `-PaeronProfile=shared` (default) or `-PaeronProfile=dedicated`. Manifest version 1 records
threading, idle strategies, direct output, external capture and Archive-follow delivery. Both profiles use the same
local IPC canonical path, load profile `stage-one-local-v1` and every numerical budget above. See
[performance notes](performance.md) for the settings and measured comparison.

## Stage boundary

Passing this gate permits feed comparison and causal research tooling to build on Spine. Multi-node failover,
host-loss/power-loss durability, cold retention, filtered proxies and external-effect reconciliation have separate
acceptance contracts. Prior complete output and an adequate input/snapshot baseline must remain retained. Generation requalification is
an explicit cold operation; production head-beacon/multicast fan-out and external capture connectors remain future
work. The idle-lag signal in the acceptance fixture comes from its parent Archive follower over test control I/O.
