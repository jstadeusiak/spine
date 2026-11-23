# Contributing to Spine

Use the smallest clear implementation that preserves Spine's contracts. Start with [AGENTS.md](AGENTS.md),
the [deterministic-core contract](docs/deterministic-core.md), and the
[application contract](docs/application-contract.md). Keep application behavior in its application module.

Use the checked-in Gradle wrapper and set `JAVA_HOME` to Java 25. Java 21 is also tested through the explicit
`-PjavaVersion=21` override. Dependencies are pinned in [gradle/libs.versions.toml](gradle/libs.versions.toml).

```sh
./gradlew build :testkit:conformance :examples:run --no-daemon
./gradlew :sequencer:stageOneAcceptance -PaeronProfile=shared --no-daemon
./gradlew :sequencer:stageOneAcceptance -PaeronProfile=dedicated --no-daemon
```

Before a runtime increment, explain the completed and proposed component boundaries and state its acceptance
criteria. Include handwritten production-code size before and after, excluding generated code and tests. Moving
code to an optional module does not reduce the total maintained code.

Tests must preserve exact retry results, deterministic output and complete snapshot recovery. Measure warmed
allocation with working accounting and an escaping-allocation negative control. Report setup and restore separately.
Performance claims need fixed-workload measurements and retained failures; a component benchmark does not establish
full-path latency or durability. Keep the existing Stage One budgets unchanged.

Keep changes focused and document any wire, snapshot, application, or dependency compatibility change. Generated
SBE sources belong in build directories. Do not add a runtime abstraction without a required contract and test.

Use four spaces for Java and one statement per line. Give each state change, retry and failure path its own
line so a reviewer can follow the order of effects. The repository [EditorConfig](.editorconfig) sets whitespace
defaults for source files, build scripts and documentation.

CI runs the full build and conformance checks on Java 21 and 25, with the two Stage One profiles in separate jobs.
Reports are uploaded even when a job fails. Reproduce the affected checks locally before submitting changes.

Changes to feed capture or its audit tools also use the Python standard-library tests. Fetch the pinned upstream
schema once before running them. Subsequent runs use the verified local copy.

```sh
python3 sequencer/src/test/python/binance_pair_audit.py --fetch-schema
python3 -m unittest discover -s sequencer/src/test/python
```

Spine uses the [MIT License](LICENSE). The [release procedure](docs/releasing.md) describes local artifact staging
and verification before a public release.
