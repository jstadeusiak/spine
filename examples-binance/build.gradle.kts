plugins { `java-library` }

val generatedDir = layout.buildDirectory.dir("generated/sbe")
val sbeCodegen = configurations.create("sbeCodegen")

dependencies {
    api(project(":sequencer"))
    sbeCodegen(rootProject.libs.sbe.tool)
    testImplementation(project(":client"))
    testImplementation(testFixtures(project(":sequencer")))
    testImplementation(rootProject.libs.junit.jupiter)
    testImplementation(rootProject.libs.assertj)
}

val generateSbe = tasks.register<JavaExec>("generateSbe") {
    classpath = sbeCodegen
    mainClass.set("uk.co.real_logic.sbe.SbeTool")
    args("src/main/resources/market-schema.xml")
    systemProperty("sbe.output.dir", generatedDir.get().asFile.absolutePath)
    systemProperty("sbe.validation.stop.on.error", "true")
    inputs.file("src/main/resources/market-schema.xml")
    outputs.dir(generatedDir)
    doFirst { project.delete(generatedDir) }
}

sourceSets.main { java.srcDir(generateSbe) }

tasks.register<JavaExec>("binanceBookReplay") {
    description = "Verify real retained JSON/SBE depth through the in-memory core and measure its processing cost."
    group = "verification"
    dependsOn(tasks.named("testClasses"))
    classpath = sourceSets["test"].runtimeClasspath
    mainClass.set("io.github.jstadeusiak.spine.examples.binance.BinanceBookReplay")
    minHeapSize = "512m"
    maxHeapSize = "512m"
}

tasks.register<Test>("marketBookProfile") {
    val idle = providers.gradleProperty("marketIdle").getOrElse("backoff")
    val workload = providers.gradleProperty("marketWorkload").getOrElse("closed")
    require(idle == "backoff" || idle == "busy") { "marketIdle must be backoff or busy" }
    require(workload == "closed" || workload == "scheduled") { "marketWorkload must be closed or scheduled" }
    description = "Verify Cluster book recovery and measure the local complete-capture to recorded-follower path."
    group = "verification"
    testClassesDirs = sourceSets["test"].output.classesDirs
    classpath = sourceSets["test"].runtimeClasspath
    include("**/MarketBookIntegrationTest.class")
    dependsOn(tasks.named("testClasses"))
    systemProperty("spine.market.profile", true)
    systemProperty("spine.market.busySpin", idle == "busy")
    systemProperty("spine.market.scheduled", workload == "scheduled")
    systemProperty("spine.market.profile.output", layout.buildDirectory.dir("reports/market-book-integration").get().asFile.absolutePath)
    outputs.upToDateWhen { false }
}
