plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.serialization)
}

dependencies {
    // Hasura NDC Kotlin SDK
    implementation(libs.ndc.sdk.kotlin)
    implementation(libs.kotlinx.serialization.json)
    implementation(project(":common"))

    // Jooq
    implementation("org.jooq.pro:jooq:3.20.1")
}
