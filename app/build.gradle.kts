plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.serialization)
}

repositories {
    mavenCentral()
    google()
    maven {
        url = uri("https://maven.pkg.github.com/hasura/ndc-sdk-kotlin")
        credentials {
            username = System.getenv("GITHUB_USERNAME")
            password = System.getenv("GITHUB_TOKEN")
        }
    }
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
    implementation(libs.ndc.sdk.kotlin)

    implementation(libs.arvo)
    implementation(libs.hikari)
    implementation(libs.joda.time)
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

    // Jooq
    implementation("org.jooq.pro:jooq:3.19.8")
}