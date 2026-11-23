# Preparing a Spine release

Spine is licensed under the [MIT License](../LICENSE). The build stages consumable Maven artifacts locally;
publishing to an external repository and creating a release tag are separate release actions.

## Source archive

From the reviewed release commit, export the tracked source tree. Git preserves the executable wrapper and
includes the license, test fixtures and build scripts. Local recordings, downloaded schemas and Git history
stay outside the archive.

```sh
mkdir -p build/distributions
git archive --format=tar.gz --prefix=spine-1.0.0/ --output=build/distributions/spine-1.0.0-source.tar.gz HEAD
```

## Artifacts

Spine stages its libraries under the Maven group `spine` at version `1.0.0`. Java packages also start with `spine`.
Historical verification reports record the coordinates and class names of the binaries they tested.

Applications built against the earlier namespace need updated imports and dependencies. The package rename
preserves the SBE wire layout and snapshot format. For historical feed captures, use the collector entry point
and bundle digest recorded in each source definition.

| Artifact | Contents |
|---|---|
| `protocol` | SBE schemas, generated codecs and reusable protocol helpers |
| `client` | Producer and independent Archive consumer APIs |
| `sequencer` | Deterministic application boundary, admission and Cluster service |
| `testkit` | Application conformance and test fixtures |
| `examples-binance` | Optional Binance application and its schema/codecs |

The generic libraries do not depend on the Binance example. `feeds` and the runnable `examples` application are
not published as libraries. Each published module includes its binary, sources, Javadocs, Gradle module metadata
and Maven POM. The POM identifies the MIT license and the repository at
[github.com/jstadeusiak/spine](https://github.com/jstadeusiak/spine). Jars include `META-INF/LICENSE`.
Gradle test-fixture variants are published with their owning libraries and consumed using `testFixtures(...)`.
Their source and API documentation are included in the owning library's sources and Javadoc jars.

### Optional research bundles

The staged Maven libraries declare their dependencies in metadata. The optional `:feeds:bundle` task embeds
the runtime classes and their license texts together. The JAR carries Spine's MIT license in `META-INF/LICENSE`
and the Apache 2.0 licenses in `META-INF/licenses/aeron/LICENSE` and `META-INF/licenses/agrona/LICENSE`.
Those copies come from [Aeron 1.53.3](https://github.com/aeron-io/aeron/blob/1.53.3/LICENSE) and
[Agrona 2.6.1](https://github.com/aeron-io/agrona/blob/2.6.1/LICENSE).

Aeron Samples brings in HdrHistogram 2.2.2. The bundle preserves its `META-INF/LICENSE.txt` and adds the
accompanying `META-INF/COPYING.txt`. [HdrHistogram license and attribution](https://github.com/HdrHistogram/HdrHistogram/blob/HdrHistogram-2.2.2/LICENSE.txt)
and [CC0 terms](https://github.com/HdrHistogram/HdrHistogram/blob/HdrHistogram-2.2.2/COPYING.txt).

The local Binance smoke bundle includes the feed bundle and a separately obtained Binance stream schema for
offline research. The pinned upstream schema revision includes no license file. The source checkout and staged
Maven libraries contain Spine's own schemas. The research setup identifies the Binance schema by its revision
and digest. [Smoke bundle instructions](binance-testing.md#reproduce-capture-and-recovery).

## Local staging

Use Java 25 for the default artifacts. A Java 21 verification build must explicitly pass `-PjavaVersion=21`;
artifacts compiled for Java 25 require Java 25 or newer. Preserve the selected Java version in release evidence.

```sh
./gradlew build :testkit:conformance :examples:run stagePublications --no-daemon
python3 testkit/src/test/publication-consumer/check.py
./gradlew :sequencer:stageOneAcceptance -PaeronProfile=shared --no-daemon
./gradlew :sequencer:stageOneAcceptance -PaeronProfile=dedicated --no-daemon
```

`stagePublications` writes only to `build/repository`. It does not upload artifacts, create a Git tag, or require
repository credentials. Select the release version in the root build before staging a release candidate.

Verify the staged artifacts from a separate Gradle application outside this checkout. Use the staged repository
and published coordinates, with no project dependencies or composite-build substitution:

```kotlin
repositories {
    maven { url = uri("/absolute/path/to/spine/build/repository") }
    mavenCentral()
}
dependencies {
    implementation("spine:sequencer:1.0.0")
    implementation("spine:client:1.0.0")
    testImplementation(testFixtures("spine:testkit:1.0.0"))
}
```

Give the consumer JVM the same module access as the repository's runnable applications. For Gradle's
`application` plugin, add the following to the consuming project's build.

```kotlin
application {
    applicationDefaultJvmArgs = listOf(
        "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED",
        "--add-opens=java.base/java.util.zip=ALL-UNNAMED",
    )
}
```

Run a neutral application's conformance and snapshot/replay checks through those artifacts. Confirm its dependency
graph contains no `examples-binance` artifact or market codec classes. Also resolve the optional Binance artifact
separately, and inspect binary/source/Javadoc jars plus POM/module metadata before release.
The retained publication-consumer gate above creates this separate application in a temporary directory, compiles
against the staged coordinates, and checks both the neutral oracle and the optional Binance snapshot round trip.
Pass `--java-version 21` after staging Java 21 artifacts; pass `--spine-version VERSION` for a different release
version. Logs remain in `build/reports/publication-consumer` on success or failure.

Retain test XML, conformance output, Stage One reports, artifact checksums, the Git revision, Java version and exact
commands. Check that the documented supported deployment envelope matches this evidence. A successful localhost
IPC/page-cache gate does not establish cross-host or power-loss durability.

The initial release's compact [baseline evidence set](evidence/1.0.0/README.md) is checked in so its documentation links work
on GitHub. It predates the [composability increment](composable-applications-result.md), whose
[current verification evidence](evidence/composable-applications/README.md) records the rerun gates for the changed runtime. Keep future release evidence in a tracked release directory or attached release artifacts; generated
`build/` paths alone are not available to repository readers.

## CI gates

The build workflow tests Java 21 and 25 explicitly, runs deterministic conformance, stages publications and runs
the external publication-consumer gate. The Java 25 job also verifies the pinned Binance schema and runs the
Python capture audits.
Separate Java 25 jobs run the unchanged shared and dedicated Stage One profiles. Every job uploads reports and
test results on success or failure. Public release should use the chosen artifact Java version consistently.
