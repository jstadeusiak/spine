plugins { application }

val spineVersion = providers.gradleProperty("spineVersion").get()
repositories {
    maven {
        url = uri(providers.gradleProperty("spineRepository").get())
        content { includeGroup("spine") }
    }
    mavenCentral { content { excludeGroup("spine") } }
}
java { toolchain { languageVersion.set(JavaLanguageVersion.of(providers.gradleProperty("javaVersion").get().toInt())) } }
dependencies {
    implementation("spine:sequencer:$spineVersion")
    implementation("spine:client:$spineVersion")
    implementation(testFixtures("spine:testkit:$spineVersion"))
}
application {
    mainClass.set("PublicationConsumer")
    applicationDefaultJvmArgs = listOf("--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED")
}
val binance = sourceSets.create("binance")
dependencies { add(binance.implementationConfigurationName, "spine:examples-binance:$spineVersion") }
tasks.register<JavaExec>("optionalBinance") {
    dependsOn(binance.classesTaskName)
    classpath = binance.runtimeClasspath
    mainClass.set("OptionalConsumer")
    jvmArgs("--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED")
}
