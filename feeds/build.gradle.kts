plugins { application }

dependencies {
    api(project(":client"))
    implementation(project(":sequencer"))
    implementation(rootProject.libs.aeron.samples)
    testImplementation(rootProject.libs.junit.jupiter)
    testImplementation(rootProject.libs.assertj)
}

application { mainClass.set("spine.feeds.FeedMain") }

tasks.named<Test>("test") {
    jvmArgs("-Xbatch")
}

tasks.register<Jar>("bundle") {
    dependsOn(configurations.runtimeClasspath)
    archiveClassifier.set("bundle")
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    manifest { attributes["Main-Class"] = application.mainClass.get() }
    from(sourceSets.main.get().output)
    from(rootProject.file("LICENSE")) { into("META-INF") }
    from("src/bundle") { into("META-INF") }
    from(configurations.runtimeClasspath.get().map { if (it.isDirectory) it else zipTree(it) })
    exclude("META-INF/*.SF", "META-INF/*.RSA", "META-INF/*.DSA")
}
