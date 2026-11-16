plugins { application }

dependencies {
    implementation(project(":protocol"))
    implementation(project(":client"))
    implementation(testFixtures(project(":testkit")))
}

application { mainClass.set("io.github.jstadeusiak.spine.examples.RecoveryDemo") }
