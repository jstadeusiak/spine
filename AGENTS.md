# Spine implementation rule

Minimize functional code. Prefer the smallest clear implementation that preserves the required invariants. Fewer lines make this system easier to audit, maintain and reason about.

Correction recorded 2026-10-05: the first implementation violated this instruction by overbuilding sequencing and
recovery. The user explicitly authorizes a clean replacement where it yields a simpler and faster result. Existing
code is not a constraint on the next design; preserve the baseline in Git, not in compatibility layers.

- Use existing Aeron, Agrona and JDK capabilities before adding abstractions or custom machinery.
- Remove unused interfaces, wrappers, options and duplicated logic.
- Prefer deletion and direct data flow. Rebuild a component from a clean implementation when that is simpler than adapting it. Do not add a framework, service, registry or compatibility path for a hypothetical future requirement.
- Tests protect required behavior, not an obsolete architecture. Replace mechanism-specific tests instead of retaining machinery merely to keep them green.
- Show the handwritten production-code size before and after each runtime increment, alongside behavior and performance evidence. Reduced code does not justify lost correctness; performance claims require measurement.
- Keep correctness checks explicit; do not trade safety or readability for compact formatting.
- Tests and fixtures may be extensive. Put failure scenarios there rather than adding production complexity solely for tests.
- Add a feature only when its contract and acceptance test justify the additional functional code.
- The Cluster input log is authoritative. The pure, fully in-memory, deterministic single-writer core regenerates the same state and output bytes from the same ordered inputs and logged time. Follow [the deterministic-core contract](docs/deterministic-core.md); preserve the direct output path with one Cluster commit and no continuous committed-input bridge.
- Zero heap allocation is required in Spine-owned steady-state producer, core, output and consumer processing/polling, including ordinary retries/rejections. Preallocate declared bounds before readiness, reuse SBE flyweights and buffer views, and prove allocation accounting works. Setup/restore and JVM warm-up are separately budgeted and disclosed.
- Use Agrona for production buffers and mutable containers where it supplies the matching data structure. Prefer primitive-key collections and pre-size known bounds. Each hot mutable object has one agent owner; no application locks or monitors belong on the event path. Preserve explicit deterministic snapshot order; Agrona containers are not automatically thread safe or insertion ordered.
- The clean replacement uses SBE uint64 log/producer-incarnation/sequences, uint32 source/configuration/clock IDs, and int64 timestamps with explicit missing values. Provision non-reused IDs; keep native identities lossless and borrowed. Reuse SBE tooling, not the old UUID/string layout; see the core contract. Keep descriptive text in infrequent definition records.
- Keep JDK ordered/immutable object lists when Agrona has no equivalent, and retain standard file, cryptographic and domain-value APIs. Do not add custom containers merely to avoid those exceptions. Tests and fixtures may use JDK collections.
- Read [the Aeron/Agrona skill](.agents/skills/aeron-agrona/SKILL.md) for relevant transport, codec, agent, collection, Archive/Cluster or performance work. Load only the reference sections relevant to the change; check example APIs against the project's pinned versions.
- Collector/consumer work follows [the application contract](docs/application-contract.md): Agrona Agent owners, immutable cold source profiles, exact captured proposals, borrowed EventHandler transitions and application-owned grouped checkpoints. Prefer these boundaries to a collector/consumer framework.
- Prefer IPC locally and reliable UDP multicast for cross-host canonical fan-out on supporting networks. Keep required recording and independent consumer recovery explicit; use unicast/IPC for targeted replay. Consumer Archive protocol authority and separate-driver UDP replay are implemented; multicast output and distinct-host qualification remain proposed work, as documented in the skill.

## Working procedure

Before each implementation increment, show component diagrams of the completed architecture and the proposed addition so the user can steer the boundaries. Keep increments small and state their acceptance criteria.

The primary agent coordinates scope, research, implementation and review through subagents. Delegate bounded tasks with clear ownership; keep integration decisions and verification with the primary agent.
