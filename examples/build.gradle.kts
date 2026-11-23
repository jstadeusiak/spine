plugins { application }

dependencies {
    implementation(project(":protocol"))
    implementation(project(":client"))
    implementation(testFixtures(project(":testkit")))
    testImplementation(rootProject.libs.junit.jupiter)
}

application { mainClass.set("spine.examples.RecoveryDemo") }
