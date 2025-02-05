plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.serialization)
}

repositories {
    mavenCentral()
    google()
    maven {
        url = uri("https://repo.jooq.org/repo")
        credentials {
            username = System.getenv("JOOQ_PRO_EMAIL")
            password = System.getenv("JOOQ_PRO_LICENSE")
        }
    }
}

dependencies {
    // Hasura NDC Kotlin SDK
    implementation(project(":ndc-sdk-kotlin"))

    implementation(libs.hikari)
    implementation(libs.kotlinx.cli)
    implementation(libs.kotlinx.serialization.json)
    implementation(libs.logback.classic)
    implementation(libs.semver4j)
    
    // Micrometer
    implementation("io.micrometer:micrometer-core:1.11.3")
    implementation("io.micrometer:micrometer-registry-prometheus:1.11.3")
    
    // OpenTelemetry
    implementation(libs.opentelemetry.api)
    implementation(libs.opentelemetry.sdk)
    implementation(libs.opentelemetry.kotlin)
    implementation(libs.opentelemetry.semconv)
    implementation(libs.opentelemetry.otlp)

    // Jooq
    implementation("org.jooq.pro:jooq:3.19.8")
}
