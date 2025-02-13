plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.serialization)
}

dependencies {
    implementation(libs.ndc.sdk.kotlin)
    implementation(project(":common"))

    implementation(libs.kotlinx.serialization.json)
}
