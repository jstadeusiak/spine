plugins { `java-library` }

dependencies {
    api(project(":protocol"))
    api(rootProject.libs.aeron.archive)
    api(rootProject.libs.aeron.cluster)
    testImplementation(rootProject.libs.junit.jupiter)
    testImplementation(rootProject.libs.assertj)
    testImplementation(rootProject.libs.aeron.driver)
}
