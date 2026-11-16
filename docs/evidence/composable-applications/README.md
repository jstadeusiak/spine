# Composable application verification

Validated on 16 November 2026 against the [source SHA-256 manifest](source-sha256.json).
The 111-file source manifest covers handwritten/generated-input source trees, tests, fixtures and build configuration;
it excludes generated outputs and documentation. Full test XML, logs and original recordings remain local under
`build/reports/capture-recording-identity-20261116` and `sequencer/build/reports/stage-one`.

The first local attempt was interrupted by verified system sleep; its logs and XML remain in the report directory's
`failed-java21` subdirectory. The unchanged awake rerun passed without relaxing timeouts or allocation assertions.

- [Java 21](tests-java21.json) and [Java 25](tests-java25.json): 196 tests each, zero failures, errors or skips.
- Independent staged-library consumers: [Java 21](consumer-java21.log), [Java 25](consumer-java25.log).
  Generic and optional Binance resolution both pass. [Java 25 artifact hashes](artifacts-java25-sha256.json)
  identify the 27 staged artifacts for version 1.0.0, not a registry publication or reproducible-binary claim.
- Unchanged Stage One gates: [shared report](stage-one-shared-report.json), [shared profile](stage-one-shared-profile.json),
  [dedicated report](stage-one-dedicated-report.json), [dedicated profile](stage-one-dedicated-profile.json).
- The independent conformance oracle matches on both JDKs; 42 Python audit tests pass.
- Strict warmed allocation gates, including maximum 2 MiB batches and exact retries, retain escaping-allocation
  controls. Setup, restore, catalog qualification and arbitrary application code are separate scopes.
- [Linux CI](ci-linux.json) at `d77a22a` passed 195 tests on each JDK, both Stage One profiles and both external
  publication consumers; its exact inputs are in the [CI source manifest](ci-source-sha256.json).
  This baseline includes output recording readiness and precedes the two-line capture readiness fix and its regression described below.
  The native file probe's fixed warm-up and earlier 64-byte observation are disclosed in
  [performance](../../performance.md); zero-byte assertions are unchanged.

The sequencer test task now uses `-Xbatch` after a local JFR trace attributed a 256-byte service callback allocation
to constant-string resolution before C2 compilation. The hosted failure had the same byte count, but lacked a trace;
its attribution remains an inference. See the [diagnosis](transport-compilation-allocation.md) and
[event evidence](transport-compilation-allocation.json). Fixed warm-up and strict zero-byte assertions are unchanged.
Production and Stage One JVM settings are unchanged.

The three-member fixture now waits for every native election to close before opening its first client. A hosted
run exposed a quorum accepting a session while the third follower still expected log position zero; its image
joined at 256 instead. The barrier follows [Aeron's pinned test-cluster pattern](https://github.com/aeron-io/aeron/blob/1.53.3/aeron-test-support/src/main/java/io/aeron/test/cluster/TestCluster.java#L1367)
and retains the existing deadline, fatal warning checks and recovery comparisons.

Recording discovery also waits for the native registration ID before retaining counter identity. Aeron publishes
the allocated state before writing that ID; caching its initial zero can later fail a fence while the recording
is still active. `RecordingIdentityTest` reproduces this window and the former teardown failure, then verifies
that startup waits and admission, snapshot and termination succeed. The stopped/reused-recording checks remain
unchanged. `CaptureRecordingIdentityTest` applies the same delayed-registration boundary to captured ingress,
then verifies exact admission, byte-for-byte recovery and duplicate retry with the original canonical sequence.
The synchronous fixture exposes two zero reads before the actual identity; the former constructor caches zero
and fails its later identity check. Capture and output startup retain their existing deadlines and progress,
and all stopped/reused-counter checks remain. The current suites include all 74 sequencer tests on both JDKs.

Commands, with `JAVA_HOME` set to the selected JDK:

```sh
./gradlew -PjavaVersion=21 test stagePublications :testkit:conformance :examples:run
python3 testkit/src/test/publication-consumer/check.py --java-version 21
./gradlew test :sequencer:stageOneAcceptance -PaeronProfile=shared stagePublications :testkit:conformance :examples:run
python3 testkit/src/test/publication-consumer/check.py --java-version 25
./gradlew :sequencer:stageOneAcceptance -PaeronProfile=dedicated
python3 -m unittest discover -s sequencer/src/test/python -p 'test_*.py'
```

These are single-host, single-node IPC/page-cache measurements. Farm development tests also ran on this machine;
these samples establish acceptance against unchanged budgets, not an isolated speed comparison, HFT latency
qualification, cross-host performance or power-loss durability. Absolute checkout paths in consumer logs and
report artifact locations are replaced with placeholders; measurements are unchanged.
