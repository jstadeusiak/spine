plugins { application }

dependencies {
    api(project(":client"))
    implementation(project(":sequencer"))
    implementation(rootProject.libs.aeron.samples)
    testImplementation(rootProject.libs.junit.jupiter)
    testImplementation(rootProject.libs.assertj)
}

application { mainClass.set("io.github.jstadeusiak.spine.feeds.FeedMain") }

tasks.named<Test>("test") {
    jvmArgs("-Xbatch")
}

tasks.register<Jar>("bundle") {
    dependsOn(configurations.runtimeClasspath)
    archiveClassifier.set("bundle")
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    manifest { attributes["Main-Class"] = application.mainClass.get() }
    from(sourceSets.main.get().output)
    from(configurations.runtimeClasspath.get().map { if (it.isDirectory) it else zipTree(it) })
    exclude("META-INF/*.SF", "META-INF/*.RSA", "META-INF/*.DSA")
}
