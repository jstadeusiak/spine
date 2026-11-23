---
name: aeron-agrona
description: Design, implement, review and tune Java Aeron, Aeron Archive/Cluster, Agrona and SBE paths using source-grounded patterns, allocation ownership and explicit recovery contracts. Use for transport, agents, codecs, collections, recording, replay or performance work in this stack.
---

# Aeron, Agrona and SBE

Prefer the smallest implementation that preserves the requested semantics. Use the library's existing transport,
recovery and buffer capabilities before introducing another wrapper, queue, catalog or merge engine.

For Spine, minimal functional code is a hard design constraint. The first implementation overbuilt sequencing;
the user authorizes clean replacement. Prefer deleting obsolete machinery over adding layers to preserve it.
Existing tests protect required behavior and may be replaced when they encode the architecture being removed.

## Route the work

- For allocations, collections, agents, codecs or benchmarking, read [patterns](references/patterns.md).
- For Spine's deterministic core or zero-allocation acceptance, also read [the core contract](../../../docs/deterministic-core.md).
- For IPC, UDP multicast, MDC, flow control, remote Archive access or replay/live handover, read
  [transport and recovery](references/transport.md).
- For a concrete cookbook example, read the relevant module in [cookbook transport](references/cookbook-transport.md)
  or [cookbook applications](references/cookbook-applications.md).
- For a concrete Aeron sample, tool or test, read its entry in [Aeron samples](references/aeron-samples.md).
- For launchers, JVM/driver profiles or C/C++ counterparts, read [sample launchers](references/samples-launchers.md).
- For C/C++ sample implementation details, read [native samples](references/native-samples.md).
- For coverage or refreshing upstream research, use [source inventory](references/source-inventory.json).
  Do not load the whole inventory or every catalog for an ordinary code change.

The references distinguish source observations, recommended patterns and Spine-specific contracts. The source
inventory preserves historical review notes; current Spine pins and contracts take precedence over those notes. Example code
is evidence about an API or tradeoff; an entire example is not automatically production safe or allocation free.
Check the project's pinned dependency APIs before adapting examples from another revision. Updating references
does not authorize upgrading dependencies or changing durability, admission, threading or effect contracts.

## Make these decisions explicit

1. Establish the owner of each agent, publication, mutable codec, buffer and container. Use `ExclusivePublication`
   for a single publishing owner; use a concurrent publication when concurrent publishing is actually required.
   Give each agent its own stateful idle strategy. Account for invoker progress if choosing `INVOKER` mode.
2. Use Agrona buffers and matching mutable containers. Prefer primitive APIs and pre-size known bounds.
   Preserve required synchronization and deterministic wire order. Cached iterators/entries are borrowed and
   may reset on another iteration. Keep a JDK ordered object list when Agrona has no matching structure.
3. Reuse buffers, SBE headers/flyweights and claims within their owner. Decode primitive fields or borrowed byte
   regions in the callback; use bounded owned storage for data that survives it. Validate lengths before copies.
   String conveniences, expandable storage, linked queues and domain objects can still allocate.
4. Treat `offer` success as transport publication. Handle transient backpressure/admin changes with bounded
   polling/retry; stop explicitly on terminal lifecycle failures. Do not advance application progress, discard a
   pending durable observation or manufacture success after a failed publication.
   In a deterministic reducer, preserve an already-admitted pending self-offer and the framework's retry machinery;
   do not introduce replica-local timeout decisions or `ABORT` after state mutation. External transport deadlines
   and reducer recovery are different contracts.
5. Use `tryClaim` only for a complete unfragmented message within the pinned API's payload bound, including
   Cluster's session header. Commit or abort each successful claim promptly. Preserve `offer` for larger frames.
   Controlled polling can `ABORT` transient backpressure before effects; it does not undo a mutated application.
6. Prefer IPC for same-host communication. Prefer reliable Aeron UDP multicast for cross-host canonical fan-out
   on a supporting network; measure it against unicast/MDC for the actual topology. Separate required recording
   from optional lagging consumers. Use bounded replay/repair budgets and retain logical cursor continuity.
7. Keep Cluster reducers deterministic: logged time, commands, timers and configuration drive state. Keep network,
   disk and external effects outside them. Snapshot complete state and restore it fully before installing it.
   Preserve required service-generated calls across roles/replay; client egress and self-offers have different roles.
8. Separate admission, commitment, publication, complete recording, application and checkpoint progress. Aeron
   connection, a local counter or a position alone does not establish business readiness or power-loss durability.
   Local recording counters are not remotely readable; choose a protocol authority for remote consumers.
9. Tune against a fixed workload and recovery envelope. Record threading, idle strategies, terms, MTU, flow control,
   storage sync, hardware and runtime. Measure scheduled rate/lateness, tails, allocations, CPU, recorder lag and
   catch-up under ongoing input. Keep failed runs. Do not import aggressive sample flags as universal defaults.

## Spine application

For Spine work, also read repository `AGENTS.md` and the relevant architecture/acceptance contract. Preserve exact
identity, exact retry bytes, original observation times, separate redundant-provider observations and
contiguous consumer application. The user's target is a zero-allocation, fully in-memory, single-writer core:
one committed input produces deterministic output directly through a reused buffer/sink. The clean runtime
replaces the second canonical Cluster commit and continuous committed-input bridge. Do not add
a second continuously replaying reducer to compensate for a deterministic core. Use integer identities or
primitive identity words; preserve uniqueness and exact application/wire identities rather than requiring UUID objects.
Current output is IPC; consumer recording authority uses Archive protocol positions and supports a separate media
driver. Distinct-host qualification and the multicast design in the references remain proposed, not implemented.

Before a Spine implementation increment, show completed/proposed component diagrams and its acceptance gate.
Verify the changed invariants with meaningful tests, then measure the complete path when making a performance
claim. Source inspection can identify allocations; it cannot prove warmed runtime allocation counts or latency.

When refreshing references, resolve immutable commits, compare the complete file inventory and review changed
examples and dependency versions. Update each changed file's note and the relevant decision guidance. Never
report unchanged inventory coverage as proof that new upstream code was reviewed.
