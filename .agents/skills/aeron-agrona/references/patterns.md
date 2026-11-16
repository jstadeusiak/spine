# Performance, allocations and ownership

Read the relevant section rather than applying every optimization. These notes combine inspected source patterns
with explicit Spine decisions. They do not certify entire examples as allocation free.

## Reviewed versions

The source inventory below reviewed Spine's baseline Aeron 1.53.0, Agrona 2.6.0 and SBE 1.38.1. Their immutable commits are respectively
`af20315a6b6323783ae717b075ba4a70c9abbf0c`, `879ff419c620c30e88bde9e86a38220de76cccf2` and
`95f4e5c2e932c6befa2da58d19564ecf17670851`.

Current pins are authoritative in [the version catalog](../../../../gradle/libs.versions.toml).
The [2026-10-08 upgrade](../../../../docs/java-runtime-comparison.md) selects Aeron 1.53.3, Agrona 2.6.1 and
SBE 1.40.2 on Java 25. Historical example/source checks below remain tied to their recorded revisions;
check the current release API before applying them.

The requested cookbook was reviewed at `6fc95e9b07a9a25d9ed7cda10508d5ecde924fa0`; its catalog pins Aeron 1.52.1,
Agrona 2.5.0 and SBE 1.39.0. The requested Aeron master samples were reviewed at
`093fd4c49527feda9b99606fd165d59626fabfdc`. All 97 master main Java sample files were verified byte-identical to the pinned Aeron 1.53.0 source jar. Native
samples/tests and cookbook generated schemas are not promises of source compatibility with Spine's versions. The per-file catalogs describe those trees; the API decisions below
use pinned release sources where specified.

## Buffers and SBE

Own reusable headers, encoders, decoders, buffers and scratch regions once per agent or caller context.
`wrap` changes a flyweight's view; it does not turn borrowed storage into owned immutable state. Read fields while
the callback buffer is valid. A field's length, units, byte order and schema version remain part of its contract.
Variable-field/group access advances decoder limits, so reuse requires wrapping/resetting for each message.

The pinned [MarketDataBenchmark](https://github.com/aeron-io/simple-binary-encoding/blob/95f4e5c2e932c6befa2da58d19564ecf17670851/sbe-benchmarks/src/main/java/uk/co/real_logic/sbe/MarketDataBenchmark.java)
reuses buffers/codecs and reads primitives/groups. The
[CarBenchmark](https://github.com/aeron-io/simple-binary-encoding/blob/95f4e5c2e932c6befa2da58d19564ecf17670851/sbe-benchmarks/src/main/java/uk/co/real_logic/sbe/CarBenchmark.java)
reuses scratch bytes and preencoded labels. These are better allocation patterns than logging-oriented examples.

Generated String getters construct bytes and Strings; UTF-8 String setters encode new bytes. Borrowed wrappers or
caller-supplied byte regions can avoid those objects. Generated variable-data getters copy at most the requested
length and advance past the full field: validate the field length against destination capacity to prevent silent
truncation. Strict malformed-length/UTF-8 validation still belongs at the trust boundary. See the pinned
[Java generator](https://github.com/aeron-io/simple-binary-encoding/blob/95f4e5c2e932c6befa2da58d19564ecf17670851/sbe-tool/src/main/java/uk/co/real_logic/sbe/generation/java/JavaGenerator.java).

Allocate maximum known backing capacity once when justified. `ExpandableArrayBuffer`/`ExpandableDirectByteBuffer`
can allocate on growth; direct storage itself does not imply no Java allocations. `UnsafeBuffer.wrap` reuses a
wrapper without copying, but wrapper mutation is not thread safe. See
[UnsafeBuffer](https://github.com/aeron-io/agrona/blob/879ff419c620c30e88bde9e86a38220de76cccf2/agrona/src/main/java/org/agrona/concurrent/UnsafeBuffer.java).

Fragment assembly is another owner: first fragmented traffic/session and growth may allocate backing buffers.
Warm up realistic sessions and maximum message sizes; do not only warm up tiny unfragmented traffic.
[FragmentAssembler](https://github.com/aeron-io/aeron/blob/af20315a6b6323783ae717b075ba4a70c9abbf0c/aeron-client/src/main/java/io/aeron/FragmentAssembler.java)
reuses per-session assembly storage. An image-specific assembler can remove the session map when the design pins
exactly one image. The completed message remains borrowed.

## Agrona collections and concurrency

Primitive-key maps remove boxed keys and entry nodes within capacity. Use primitive overloads; a generic
`Map<Long,V>` or boxed iterator path can restore boxing. Pre-size the load-factor capacity, not just the desired
entry count. Growth creates new arrays. `Long2ObjectHashMap` is generated from the checked-in
[Int2ObjectHashMap template](https://github.com/aeron-io/agrona/blob/879ff419c620c30e88bde9e86a38220de76cccf2/agrona/src/main/java/org/agrona/collections/Int2ObjectHashMap.java)
by [SpecialisationGenerator](https://github.com/aeron-io/agrona/blob/879ff419c620c30e88bde9e86a38220de76cccf2/agrona/src/main/java/org/agrona/generation/SpecialisationGenerator.java).

Default iterators/entries may be cached and reused. Calling `iterator()` again can reset the active iterator;
retaining a map entry can retain the iterator's mutable view. Avoid nested/retained iteration or request independent
allocating iterators explicitly when required. Hash-table iteration order is not deterministic wire order.
Spine snapshots sort provisioned uint64 producer IDs with unsigned comparison. A hash is not an identity.
Agrona 2.6 has Long2LongHashMap, not Long2IntHashMap; bounded dense int indices can use long map values
without introducing a custom container.

Ordinary Agrona maps/sets are not automatically concurrent. Spine's target gives each hot mutable object one agent
owner and requires no application locks/monitors on its event path. Spine now has no application monitors or per-event producer state files. Collector capture and application
checkpoints have explicit external ownership; process-memory pending state alone is not restart durability. Agrona 2.6 lacks a matching general ordered immutable object list; keep the
recording catalog's JDK list rather than inventing indexed maps.

Choose queues by producer/consumer topology and required bounds. Bounded
[array queues](https://github.com/aeron-io/agrona/blob/879ff419c620c30e88bde9e86a38220de76cccf2/agrona/src/main/java/org/agrona/concurrent/ManyToOneConcurrentArrayQueue.java)
preallocate slots, but queued objects can still allocate.
[ManyToOneConcurrentLinkedQueue](https://github.com/aeron-io/agrona/blob/879ff419c620c30e88bde9e86a38220de76cccf2/agrona/src/main/java/org/agrona/concurrent/ManyToOneConcurrentLinkedQueue.java)
allocates a node per offer. Agrona's name alone does not establish an allocation guarantee.

A byte ring buffer can let producers encode directly into a successful claim and publish it by commit; abort a
failed write. Capacity includes its trailer and record alignment/maximum-message constraints. Fail/full results
need an explicit policy. See the pinned
[claim/commit concurrency tests](https://github.com/aeron-io/agrona/blob/879ff419c620c30e88bde9e86a38220de76cccf2/agrona-concurrency-tests/src/main/java/org/agrona/concurrent/ringbuffer/ManyToOneRingBufferTests.java).
An in-memory ring is neither a durable producer spool nor an Archive recovery contract. Broadcasting and queueing
also have different loss/slow-reader semantics; do not substitute them just to reduce allocations.

Primitive [Hashing](https://github.com/aeron-io/agrona/blob/879ff419c620c30e88bde9e86a38220de76cccf2/agrona/src/main/java/org/agrona/collections/Hashing.java)
is useful for table indexes, not a replacement for exact-byte integrity/deduplication hashes.
[CachedNanoClock](https://github.com/aeron-io/agrona/blob/879ff419c620c30e88bde9e86a38220de76cccf2/agrona/src/main/java/org/agrona/concurrent/CachedNanoClock.java)
returns an updater-controlled volatile value: account for its freshness. `SystemEpochNanoClock` uses
[HighResolutionClock](https://github.com/aeron-io/agrona/blob/879ff419c620c30e88bde9e86a38220de76cccf2/agrona/src/main/java/org/agrona/concurrent/HighResolutionClock.java),
whose epoch-nanosecond path calls `Instant.now()`; a singleton is not proof of allocation-free reads.

## Publication and controlled polling

An exclusive publication matches one publishing owner. A concurrent publication has different synchronization
costs and is appropriate when concurrent calls are required. Share neither an exclusive publication nor mutable
claim/codec scratch state across owners without a design change.

The pinned [reusable IPC publisher](https://github.com/aeron-io/aeron/blob/af20315a6b6323783ae717b075ba4a70c9abbf0c/aeron-samples/src/main/java/io/aeron/samples/EmbeddedExclusiveIpcThroughput.java)
offers one aligned direct buffer repeatedly. The
[claim publisher](https://github.com/aeron-io/aeron/blob/af20315a6b6323783ae717b075ba4a70c9abbf0c/aeron-samples/src/main/java/io/aeron/samples/EmbeddedExclusiveBufferClaimIpcThroughput.java)
reuses `BufferClaim` and writes into the term buffer. `offer` still copies into the publication log, even if an
application avoids an intermediate copy.

`tryClaim` cannot fragment. Check `maxPayloadLength`, with Cluster's session header subtracted for its APIs; begin
the application encoding after that header. Commit or abort promptly, without disk/network waits inside the claim.
The 64 KiB fragmented Spine path must retain `offer`. See pinned
[Publication](https://github.com/aeron-io/aeron/blob/af20315a6b6323783ae717b075ba4a70c9abbf0c/aeron-client/src/main/java/io/aeron/Publication.java)
and [Cluster](https://github.com/aeron-io/aeron/blob/af20315a6b6323783ae717b075ba4a70c9abbf0c/aeron-cluster/src/main/java/io/aeron/cluster/service/Cluster.java).

Distinguish publication results. Backpressure/admin changes need polling/idle/retry; loss of a required source,
closed publication or impossible terminal position must not loop indefinitely. Spine retains deadlines and fatal
lifecycle checks that throughput demonstrations may omit. An offer succeeds at publication, before an independent
Archive necessarily records it or an application applies it.

Scope timeout decisions to their owner. Spine's service first mutates admitted state, then publishes deterministic
canonical bytes directly to its ordinary recorded publication before attempting the receipt. Transport failure after
mutation is fatal; retained Cluster input drives recovery. A timeout or input `ABORT` cannot roll back admitted state.
Never fabricate acknowledgement or advance past missing output.

[EchoPair](https://github.com/aeron-io/aeron/blob/af20315a6b6323783ae717b075ba4a70c9abbf0c/aeron-samples/src/main/java/io/aeron/samples/echo/EchoPair.java)
shows controlled polling with transient `ABORT` and progress after successful forwarding. Its terminal/drop choices
are demonstration policy. The pinned
[ControlledFragmentAssembler](https://github.com/aeron-io/aeron/blob/af20315a6b6323783ae717b075ba4a70c9abbf0c/aeron-client/src/main/java/io/aeron/ControlledFragmentAssembler.java)
restores the assembly limit when a final-fragment callback aborts, enabling identical retry. It cannot roll back
application state already mutated in that callback.

## Agents, threading and profiles

Return actual work counts to the runner; zero after real work causes inappropriate idling. Statefully back off when
idle, reset on progress and keep each idle strategy owned by one agent. `INVOKER` reduces threads only if the owner
regularly invokes all required conductor/driver/archive work. Composite agents share an execution budget; a blocking
task stalls its peers. IPC can avoid sender/receiver network work but still needs conductor/client progress.

Pinned [LowLatencyMediaDriver](https://github.com/aeron-io/aeron/blob/af20315a6b6323783ae717b075ba4a70c9abbf0c/aeron-samples/src/main/java/io/aeron/samples/LowLatencyMediaDriver.java)
uses dedicated threads and aggressive idling. Its
[properties profile](https://github.com/aeron-io/aeron/blob/af20315a6b6323783ae717b075ba4a70c9abbf0c/aeron-driver/src/main/resources/low-latency.properties)
includes pre-touch/non-sparse mappings and bounds-check flags; do not copy all flags into correctness code.
Dedicated sender/receiver/conductor agents can consume several cores. Affinity, NUMA, GC, pre-touch and socket
buffers are measured deployment choices, not magic defaults. Disabling checks changes failure behavior; oversized
network MTUs need a supporting path. See [official tuning guidance](https://aeron.io/docs/cookbook-content/aeron-increase-performance/).

Spine's acceptance profiles vary only declared transport/idle settings. Shared is default; dedicated uses fresh
`BackoffIdleStrategy(100,100,1000,10000)` for each driver/Cluster/Archive agent. Neither pins cores. Keep fixed rate,
burst, latency, memory and recovery limits while comparing them. Persistence can dominate even a tuned transport.

## Cluster and complete-path measurement

Pinned Aeron 1.53.0 source jars distinguish background work from valid service sends: `doBackgroundWork` must not
mutate deterministic state or use Cluster log/timer operations. Its invocation guard also rejects
`ClientSession.offer`, because `ContainerClientSession` delegates through `ClusteredServiceAgent.offer` and
`checkForValidInvocation`. Release deferred session egress from an allowed service callback; ordinary external
transport maintenance is a separate operation. `CollectorAdmissionTest` verifies the permitted callback path.

Pinned `AeronArchive.Context.messageTimeoutNs()` calls `CommonContext.checkDebugTimeout` and reads a system
property. It is not a plain field read. Cache the effective timeout during an owner-scoped replay's setup rather
than calling that getter on every successful poll. Archive-follow polls need no progress deadline after the
image joins; bounded historical replay still resets its cached timeout on progress. Allocation traces must
separate the actual polling owner from fixture assertions and observer bookkeeping.

Spine's governing target is the [deterministic-core contract](../../../../docs/deterministic-core.md), informed by
the user's `divine-agora` core. One committed input drives pure in-memory logic and a synchronous borrowed-buffer
event sink. Cluster input replay regenerates the outputs; a second canonical append and a second continuously
replaying reducer are unnecessary. Archive output storage and consumer catch-up remain outside the pure core.
Older self-offer notes apply to other reducers that use that mechanism; Spine has removed it.

Use logged callback time/configuration for deterministic state. Snapshot everything needed for subsequent commands,
including duplicate/retry progress; stage validation before replacing live state. Avoid synchronous external I/O
in the reducer. Client egress and service-generated self-offers have distinct role/replay behavior. The pinned
[ClusteredServiceAgent](https://github.com/aeron-io/aeron/blob/af20315a6b6323783ae717b075ba4a70c9abbf0c/aeron-cluster/src/main/java/io/aeron/cluster/service/ClusteredServiceAgent.java)
implements those rules and callback send restrictions. Spine ordinary recorded output is emitted on every member and during replay; consumers qualify repeated output.
Cluster.timeUnit() can be null in onStart before the first leadership event: obtain it in the committed callback.
Image.poll can send callback exceptions to a nonthrowing error handler and advance its physical cursor. An owned
consumer callback must latch failures and surface them after polling, preserving the application cursor. Cluster
enforces a RethrowingErrorHandler for its own Aeron subscriptions; check the pinned ownership contract before
adding redundant error machinery.

The official benchmarks were additionally inspected at `1b11bfc293691c883c8b43e0ca914fe04644a966`:

- [LoadTestRig](https://github.com/aeron-io/benchmarks/blob/1b11bfc293691c883c8b43e0ca914fe04644a966/benchmarks-api/src/main/java/io/aeron/benchmarks/LoadTestRig.java)
  schedules from intended timestamps, warms up/reset histograms and bounds send grace/drain. A synchronous loop
  that schedules the next message only after completion hides inability to sustain an intended arrival rate.
- [EchoMessageTransceiver](https://github.com/aeron-io/benchmarks/blob/1b11bfc293691c883c8b43e0ca914fe04644a966/benchmarks-aeron/src/main/java/io/aeron/benchmarks/aeron/EchoMessageTransceiver.java)
  separates setup/connection checks, reusable send policy and receive polling. Match receiver topology and count.
- [ManyToOneRingBufferBenchmark](https://github.com/aeron-io/benchmarks/blob/1b11bfc293691c883c8b43e0ca914fe04644a966/benchmarks-aeron/src/main/java/io/aeron/benchmarks/aeron/jmh/ManyToOneRingBufferBenchmark.java)
  preallocates per-thread scratch/ring storage and compares producer counts. Its boxed loop/counters mean it is
  evidence for a specific benchmark, not proof that all surrounding code allocates nothing.

Measure full admission/record/application paths and allocation rate after representative warm-up. Include scheduled
lateness, loss/retry, fragmentation, bursts, term rollover, concurrent catch-up and recorder lag. RTT divided by two
is not a measured one-way latency. Clock-domain uncertainty and synchronization must be explicit for one-way timing.
Use appropriate profiles/JFR/allocation tools rather than inferring runtime elimination from source code alone.

## Historical allocation ledger (7597951)

These historical sites were removed by the clean replacement. Escape analysis could eliminate some temporary
objects; this source ledger is not a measurement. Current measured boundaries are in docs/performance.md.

| Path | Historical allocations / ownership |
|---|---|
| Envelope/proposal codecs | Generated header/codec instances; UUID, cursor, metadata, result/view objects; decoded payload bytes remain borrowed |
| Service admission/retry | Full ingress byte array for hashing and SHA-256 result; accepted proposal retains a receipt |
| Event result encoding | Codecs/domain objects, including a validation-only decoded event; rejected results use empty arrays |
| Consumer duplicate verification | SHA-256 result array per checked frame; primitive map avoids boxed keys/nodes within capacity |
| Capture observation | UTF-8 identity bytes/String and view objects; raw observation bytes remain borrowed |
| Durable producer | Defensive pending copies, scoped/proposal/state arrays and persistence objects required by that baseline ownership/disk contract |
| Cold paths | Snapshot sorting/restoration, source definitions, paths/locks/catalog setup |

This ledger describes unfinished baseline code, not an acceptable allocation budget. Replace per-call codecs/domain
objects with owner-scoped flyweights and primitive fields, full-input temporary arrays with bounded reusable hashing
storage, and receipt/digest replacement with preallocated state. Prove zero per-observation allocation after warm-up
with supported/enabled per-thread accounting and an escaping-allocation negative control. Preserve exact retry
identity, strict validation, callback lifetime, deterministic snapshots and the declared producer crash contract.
Per-event atomic producer files were that baseline durability implementation, not a required architecture. Source
capture persistence, if required before admission, belongs outside the core and needs its own recording fence.
Container substitution and transport tuning alone did not remove that baseline's measured synchronous prepare/ACK storage cost.
