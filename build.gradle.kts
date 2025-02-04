plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.serialization)
    id("com.github.johnrengelman.shadow") version "8.1.1"
    application
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
    implementation(libs.vertx.core)
    implementation(libs.vertx.kotlin)
    implementation(libs.vertx.kotlin.coroutines)
    implementation(libs.vertx.web)
    
    // Micrometer
    implementation("io.micrometer:micrometer-core:1.11.3")
    implementation("io.micrometer:micrometer-registry-prometheus:1.11.3")
    
    // OpenTelemetry
    implementation(libs.opentelemetry.api)
    implementation(libs.opentelemetry.sdk)
    implementation(libs.opentelemetry.kotlin)
    implementation(libs.opentelemetry.semconv)
    implementation(libs.opentelemetry.otlp)

    /* 
    * Database drivers
    */
    // Databricks
    implementation(libs.databricks.jdbc)
    // MySQL
    implementation(libs.mysql.jdbc)
    // Oracle
    implementation(libs.oracle.jdbc)
    // Snowflake
    implementation(libs.snowflake.jdbc)
    // Redshift
    implementation(libs.redshift.jdbc)
    // BigQuery
    implementation(files("libs/bigquery/GoogleBigQueryJDBC42.jar"))
    implementation("com.google.cloud:google-cloud-bigquery:2.35.0")
    implementation("com.google.cloud:google-cloud-bigquerystorage:3.9.3")
    implementation("com.google.api-client:google-api-client:2.7.0")
    implementation("com.google.auth:google-auth-library-oauth2-http:1.28.0")
    implementation("com.google.auth:google-auth-library-credentials:1.28.0")
    implementation("com.google.oauth-client:google-oauth-client:1.36.0")
    implementation("com.google.api:api-common:2.38.0")
    implementation("com.google.cloud:google-cloud-core:2.45.0")
    implementation("com.google.api:gax:2.55.0")
    implementation("com.google.api:gax-grpc:2.55.0")
    implementation("io.grpc:grpc-api:1.67.1")
    implementation("io.grpc:grpc-core:1.67.1")
    implementation("io.grpc:grpc-stub:1.67.1")
    implementation("io.grpc:grpc-auth:1.67.1")
    implementation("com.google.protobuf:protobuf-java:3.25.5")
    implementation("com.google.protobuf:protobuf-java-util:3.25.5")

    // Jooq
    implementation("org.jooq.pro:jooq:3.19.8")
}

tasks.shadowJar {
    manifest {
        attributes(mapOf("Main-Class" to "hasura.MainKt"))
    }
}

application {
    mainClass.set("hasura.MainKt")
}
