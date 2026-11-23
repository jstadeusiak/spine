# Plan review

This records the earlier market-application plan. The generic `1.0.0` release and its tested deployment envelope
are described in [release readiness](release-readiness.md); Binance is now an optional application module.

The goal is a reproducible observation backbone and in-memory market state for pricing, quoting and cross-venue
evaluation, as clarified by the user on 2026-10-08. The first runtime
overbuilt sequencing/recovery and violated the minimal-functional-code requirement. Its failed capacity measurements
are summarized in the historical reports; the baseline is not part of the reset public Git history.

The replacement pairs a pure in-memory core with a thin Cluster adapter. The adapter records the core's output
directly. Replaying committed inputs reproduces the same bytes. Numeric SBE identities remove UUID objects and
extra wire words. The replacement also removed the second canonical commit, continuous output bridge and per-event
producer state files. [Deterministic core contract](deterministic-core.md)

Recording, application and restart boundaries still require explicit checks. Snapshot acceptance waits for complete
output recording; collectors own pre-admission capture; applications own state/cursor checkpoints; consumers verify
repeated output and apply contiguous events once. These requirements live at their actual boundaries.

Both fixed local acceptance profiles pass; [the release results](release-readiness.md) define the tested boundary.
Real paired Tokyo trade capture/source comparison now passes. The first [market-state core](market-state-core.md) increment now implements a bounded Binance BTCUSDT L2 book,
JSON/SBE native reconciliation and qualified BBO. Next add a second qualified venue for multi-venue evaluation and
reproducible pricing experiments. This supersedes the proposed downstream-only book consumer. Native three-member
local failover and separate-driver UDP Archive recovery are tested. Multicast, distinct-host recovery and host-loss
behavior need separate deployment gates. ML enters after the experiment harness has a useful task.
