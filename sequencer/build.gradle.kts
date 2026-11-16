plugins { `java-library`; `java-test-fixtures` }

dependencies {
    api(project(":protocol"))
    api(rootProject.libs.aeron.cluster)
    api(rootProject.libs.aeron.archive)
    testFixturesImplementation(rootProject.libs.aeron.samples)
    testImplementation(project(":client"))
    testImplementation(project(":feeds"))
    testImplementation(rootProject.libs.aeron.samples)
    testImplementation(rootProject.libs.junit.jupiter)
    testImplementation(rootProject.libs.assertj)
}

tasks.withType<Test>().configureEach {
    systemProperty("spine.acceptance.classpath", sourceSets["test"].runtimeClasspath.asPath)
}

tasks.named<Test>("test") {
    exclude("**/StageOneAcceptanceTest.class")
    jvmArgs("-Xbatch")
}

tasks.register<Test>("stageOneAcceptance") {
    val aeronProfile = providers.gradleProperty("aeronProfile").getOrElse("shared")
    require(aeronProfile == "shared" || aeronProfile == "dedicated") { "aeronProfile must be shared or dedicated" }
    description = "Run the fixed local capture/crash/replay envelope in separate producer and consumer JVMs."
    group = "verification"
    testClassesDirs = sourceSets["test"].output.classesDirs
    classpath = sourceSets["test"].runtimeClasspath
    include("**/StageOneAcceptanceTest.class")
    dependsOn(tasks.named("test"), ":protocol:test", ":client:test", ":testkit:test")
    maxHeapSize = "768m"
    systemProperty("spine.acceptance.aeron.profile", aeronProfile)
    systemProperty("spine.acceptance.output", layout.buildDirectory.dir("reports/stage-one").get().asFile.absolutePath)
    outputs.upToDateWhen { false }
}

val binanceSmokeBundle = tasks.register<Jar>("binanceSmokeBundle") {
    dependsOn(tasks.named("testClasses"), tasks.named("testFixturesClasses"), ":feeds:bundle")
    archiveClassifier.set("binance-smoke")
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    manifest { attributes["Main-Class"] = "io.github.jstadeusiak.spine.sequencer.BinanceJsonSmoke" }
    from({ zipTree(project(":feeds").layout.buildDirectory.file("libs/feeds-${project.version}-bundle.jar").get().asFile) }) {
        exclude("META-INF/MANIFEST.MF")
    }
    from(sourceSets["test"].output) {
        include("io/github/jstadeusiak/spine/sequencer/BinanceJsonSmoke*.class",
            "io/github/jstadeusiak/spine/sequencer/BinancePairSmoke*.class",
            "io/github/jstadeusiak/spine/sequencer/BinancePairLocalSmoke*.class",
            "io/github/jstadeusiak/spine/sequencer/CollectorIngestProfile*.class")
    }
    from(sourceSets["testFixtures"].output)
    from("src/test/python") {
        include("binance_pair_audit.py", "binance_json_audit.py", "collector_ingest_audit.py", "fixtures/*.xml")
        into("research")
    }
}

tasks.register<JavaExec>("binanceJsonSmoke") {
    description = "Capture anonymous Binance Spot JSON trades and audit recorded output from retained local state."
    group = "verification"
    dependsOn(binanceSmokeBundle)
    classpath = files(binanceSmokeBundle)
    mainClass.set("io.github.jstadeusiak.spine.sequencer.BinanceJsonSmoke")
}

tasks.register<JavaExec>("binancePairSmoke") {
    description = "Capture Spot JSON/SBE trades on a shared clock and export recorded application messages."
    group = "verification"
    dependsOn(binanceSmokeBundle)
    classpath = files(binanceSmokeBundle)
    mainClass.set("io.github.jstadeusiak.spine.sequencer.BinancePairSmoke")
}

tasks.register<JavaExec>("binancePairLocalSmoke") {
    description = "Verify paired scripted JSON/SBE capture, process restart and strict local audit without credentials."
    group = "verification"
    dependsOn(binanceSmokeBundle)
    classpath = files(binanceSmokeBundle)
    mainClass.set("io.github.jstadeusiak.spine.sequencer.BinancePairLocalSmoke")
}

tasks.register<JavaExec>("collectorIngestProfile") {
    description = "Measure fixed local collector arrival, capture/admission and recorded-consumer timing."
    group = "verification"
    dependsOn(binanceSmokeBundle)
    classpath = files(binanceSmokeBundle)
    mainClass.set("io.github.jstadeusiak.spine.sequencer.CollectorIngestProfile")
    minHeapSize = "512m"
    maxHeapSize = "512m"
}
