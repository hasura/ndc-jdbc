plugins {
    alias(libs.plugins.kotlin.jvm)
}

dependencies {
    // Hasura NDC Kotlin SDK
    implementation(libs.ndc.sdk.kotlin)
    implementation(project(":common"))
}
