# Transport allocation diagnosis

A hosted Java 21 run reported 256 bytes in the 1,024-message service measurement. A local diagnostic captured the same size during compilation warm-up. The hosted cause remains an inference because that job did not collect per-callback traces.

On Temurin 21.0.11+10, macOS aarch64, a diagnostic copy of the test recorded a 256-byte service callback at proposal 6,402. Outside-TLAB JFR attributed four objects to `ContainerClientSession.offer`: two 24-byte Strings and their 72-byte and 136-byte backing arrays. At the same instant, HotSpot queued that method for C2 compilation (compile ID 3809, invocation count 6,404). No native conductor timeout or driver-event processing occurred inside the captured callback.

| Constant-pool string in Aeron 1.53.3 | Characters | String + backing array |
| --- | ---: | ---: |
| Failed-connect concatenation recipe | 49 | 96 bytes |
| Session `toString` concatenation recipe | 116 | 160 bytes |
| Total | | 256 bytes |

The pinned class has exactly these two string constants. [OpenJDK 21.0.11+10 `CompileBroker::compile_method`](https://raw.githubusercontent.com/openjdk/jdk21u/jdk-21.0.11%2B10/src/hotspot/share/compiler/compileBroker.cpp) resolves the method holder's constant-pool strings on the requesting thread before C2 compilation (lines 1287–1291). The recipes correspond to the error and diagnostic text in [Aeron 1.53.3 `ContainerClientSession`](https://raw.githubusercontent.com/aeron-io/aeron/1.53.3/aeron-cluster/src/main/java/io/aeron/cluster/service/ContainerClientSession.java).

An external runner passed the unchanged strict test once on Temurin 21.0.11+10 and once on Temurin 25.0.4.1+1 with `-Xbatch`. These validation runs used normal TLAB and no diagnostic instrumentation. They retained the existing fixed warm-up, escaping allocation controls, native transport and zero-byte assertions; existing Java 21 class files ran on each VM. Full Gradle suites and hosted CI are separate evidence.

Synchronous compilation is a test measurement setting. It does not change production code, retry behavior, native idle work or latency benchmarks. The diagnostic did not establish a recurring allocation in Spine's steady path. Allocation failures remain fatal; no callbacks or nonzero samples were discarded.

The accompanying JSON contains object sizes, compilation identity, constant-pool identities, class/source hashes and exact source URLs. Large raw recordings remain private diagnostic artifacts.
