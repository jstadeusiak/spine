plugins { `java-library` }

val generatedDir = layout.buildDirectory.dir("generated/sbe")
val sbeCodegen = configurations.create("sbeCodegen")

dependencies {
    sbeCodegen(rootProject.libs.sbe.tool)
    api(rootProject.libs.agrona)
    testImplementation(rootProject.libs.junit.jupiter)
    testImplementation(rootProject.libs.assertj)
}

val generateSbe = tasks.register<JavaExec>("generateSbe") {
    classpath = sbeCodegen
    mainClass.set("uk.co.real_logic.sbe.SbeTool")
    args("src/main/resources/spine-schema.xml", "src/main/resources/capture-schema.xml")
    systemProperty("sbe.output.dir", generatedDir.get().asFile.absolutePath)
    systemProperty("sbe.validation.stop.on.error", "true")
    inputs.files("src/main/resources/spine-schema.xml", "src/main/resources/capture-schema.xml")
    outputs.dir(generatedDir)
    doFirst { project.delete(generatedDir) }
}

sourceSets.main { java.srcDir(generateSbe) }
