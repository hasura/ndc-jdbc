plugins {
    alias(libs.plugins.kotlin.jvm)
}

repositories {
    mavenCentral()
    google()
}

dependencies {
    // Hasura NDC Kotlin SDK
    implementation(project(":ndc-sdk-kotlin"))
}
