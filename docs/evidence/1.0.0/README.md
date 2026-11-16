# Initial release 1.0.0 baseline evidence

These reports record the 2026-11-16 verification of the source tree published as
[the initialization commit](https://github.com/jstadeusiak/spine/commit/232071f6312cfbb3dd1171216277f27c1386f32d),
tree `79d39f1104bb637f10ad5cb1d3700cf2dc968b36`. Resetting Git history preserved that tested tree exactly.
The subsequent [composability increment](../../composable-applications-result.md) changes runtime and snapshot
receipt encoding. These preserved reports describe the initial baseline, not that later tree or its regenerated
snapshot oracle. They have not been rerun or rewritten to imply new performance or publication evidence.

Reports are copied from the local runs; test counts were summarized from their XML. Only trailing whitespace was
removed from the two consumer logs. Original machine paths describe the measurement environment, not portable download locations. Full XML, process logs,
recordings and staged JARs are not included. Artifact hashes identify the locally inspected Java 25 build, not a
remote Maven publication. Pre-release commit labels in the code-size report are historical and are not public refs.

| Check | Retained evidence |
|---|---|
| 179 tests, zero failures/errors/skips on each JVM | [Java 21](tests-java21.json), [Java 25](tests-java25.json) |
| Exact independent output and snapshot oracle | [Java 21](conformance-java21.json), [Java 25](conformance-java25.json), [oracle source and fixtures](../../../testkit/README.md) |
| Outside-checkout artifact consumers | [Java 21](consumer-java21.log), [Java 25](consumer-java25.log) |
| Staged binary/source/Javadoc checksums and metadata | [Artifact inspection](artifact-inspection.json) |
| Shared Stage One acceptance | [Report](stage-one-shared-report.json), [pre-traffic profile](stage-one-shared-profile.json) |
| Dedicated Stage One acceptance | [Report](stage-one-dedicated-report.json), [pre-traffic profile](stage-one-dedicated-profile.json) |
| 42 Python audit tests | [Audit output](python-audits.log) |
| Handwritten production code: 3,123 → 3,115 lines | [Count method and totals](production-ncloc.json) |

From the repository root, set `JAVA_HOME` to the selected JDK and run:

```sh
./gradlew build :testkit:conformance :examples:run stagePublications --no-daemon -PjavaVersion=25
python3 testkit/src/test/publication-consumer/check.py --java-version 25
./gradlew :sequencer:stageOneAcceptance --no-daemon -PjavaVersion=25 -PaeronProfile=shared
./gradlew :sequencer:stageOneAcceptance --no-daemon -PjavaVersion=25 -PaeronProfile=dedicated
python3 -m unittest discover -s sequencer/src/test/python -p 'test_*audit.py'
```

Repeat the full build and publication-consumer check with Java 21 and `javaVersion`/`java-version` set to `21`.
The measured runtimes were Temurin 21.0.11+10 and 25.0.4.1+1 on macOS aarch64. Stage One ran on Java 25;
both JVMs ran the native Cluster recovery and allocation tests in the full build. See the
[release report](../../release-readiness.md) for measurement boundaries and remaining deployment qualifications.

Personal checkout prefixes in the retained report and consumer-log location fields are
normalized to `<repository-root>` for publication. Measurements, runtime versions,
artifact digests and other provenance fields are unchanged.
