plugins { `java-test-fixtures` }

dependencies {
    testFixturesApi(project(":protocol"))
    testFixturesApi(project(":client"))
    testFixturesApi(project(":sequencer"))
    testFixturesApi(rootProject.libs.aeron.archive)
    testFixturesImplementation(rootProject.libs.aeron.driver)
    testImplementation(rootProject.libs.junit.jupiter)
    testImplementation(rootProject.libs.assertj)
}

val counterSbe = configurations.create("counterSbe")
dependencies { counterSbe(rootProject.libs.sbe.tool) }
val counterGenerated = layout.buildDirectory.dir("generated/counter-sbe")
val generateCounterSbe = tasks.register<JavaExec>("generateCounterSbe") {
    classpath = counterSbe
    mainClass.set("uk.co.real_logic.sbe.SbeTool")
    args("src/testFixtures/resources/counter-schema.xml")
    systemProperty("sbe.output.dir", counterGenerated.get().asFile.absolutePath)
    systemProperty("sbe.validation.stop.on.error", "true")
    inputs.file("src/testFixtures/resources/counter-schema.xml")
    outputs.dir(counterGenerated)
    doFirst { project.delete(counterGenerated) }
}
sourceSets["testFixtures"].java.srcDir(generateCounterSbe)

tasks.register<JavaExec>("conformance") {
    group = "verification"
    description = "Verify a neutral application through exact retry and every snapshot/replay cut."
    dependsOn(tasks.named("testFixturesClasses"))
    classpath = sourceSets["testFixtures"].runtimeClasspath
    mainClass.set("spine.testkit.CounterExample")
    args(layout.buildDirectory.file("reports/conformance/counter.json").get().asFile.absolutePath)
}
