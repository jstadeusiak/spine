import org.gradle.api.publish.PublishingExtension
import org.gradle.api.publish.maven.MavenPublication

plugins { base }

allprojects {
    group = "spine"
    version = "1.0.0"
    repositories { mavenCentral() }
}

val aeronOpens = listOf(
    "--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED",
    "--add-opens", "java.base/java.util.zip=ALL-UNNAMED",
)
val spineJavaVersion = providers.gradleProperty("javaVersion").getOrElse("25").toInt()
val publishedLibraries = mapOf(
    "protocol" to "Spine SBE protocol and reusable buffer codecs",
    "client" to "Spine producers and independent recorded-history consumers",
    "sequencer" to "Spine deterministic application core and Aeron Cluster service",
    "testkit" to "Spine application conformance and Archive test fixtures",
    "examples-binance" to "Optional Binance state-machine example for Spine",
)

subprojects {
    apply(plugin = "java-library")
    extensions.configure<JavaPluginExtension> {
        toolchain { languageVersion.set(JavaLanguageVersion.of(spineJavaVersion)) }
    }
    dependencies { add("testRuntimeOnly", rootProject.libs.junit.platform.launcher) }
    tasks.withType<Test>().configureEach {
        useJUnitPlatform()
        jvmArgs(aeronOpens)
        maxHeapSize = "1g"
        testLogging.exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
    }
    tasks.withType<JavaExec>().configureEach { jvmArgs(aeronOpens) }
    plugins.withId("application") {
        extensions.configure<JavaApplication> { applicationDefaultJvmArgs = aeronOpens }
    }
    if (name in publishedLibraries) {
        val libraryName = name
        apply(plugin = "maven-publish")
        extensions.configure<JavaPluginExtension> {
            withSourcesJar()
            withJavadocJar()
        }
        extensions.configure<PublishingExtension> {
            publications.create<MavenPublication>("mavenJava") {
                from(components["java"])
                pom {
                    name.set("Spine $libraryName")
                    description.set(publishedLibraries.getValue(libraryName))
                    url.set("https://github.com/jstadeusiak/spine")
                    licenses {
                        license {
                            name.set("MIT License")
                            url.set("https://opensource.org/license/mit")
                            distribution.set("repo")
                        }
                    }
                    scm {
                        connection.set("scm:git:https://github.com/jstadeusiak/spine.git")
                        developerConnection.set("scm:git:ssh://git@github.com/jstadeusiak/spine.git")
                        url.set("https://github.com/jstadeusiak/spine")
                    }
                }
            }
            repositories.maven {
                name = "staging"
                url = uri(rootProject.layout.buildDirectory.dir("repository"))
            }
        }
        tasks.withType<Jar>().configureEach {
            from(rootProject.file("LICENSE")) { into("META-INF") }
        }
        plugins.withId("java-test-fixtures") {
            val fixtures = extensions.getByType<SourceSetContainer>()["testFixtures"]
            tasks.named<Jar>("sourcesJar") { from(fixtures.allSource) }
            tasks.named<Javadoc>("javadoc") {
                source(fixtures.allJava)
                classpath += fixtures.compileClasspath
            }
        }
    }
}

tasks.named("build") { dependsOn(subprojects.map { "${it.path}:build" }) }
tasks.named("check") { dependsOn(subprojects.map { "${it.path}:check" }) }

tasks.register("stagePublications") {
    group = "publishing"
    description = "Stage library binaries, sources, Javadocs and metadata in build/repository for independent consumption."
    dependsOn(publishedLibraries.keys.map { ":$it:publishMavenJavaPublicationToStagingRepository" })
}
