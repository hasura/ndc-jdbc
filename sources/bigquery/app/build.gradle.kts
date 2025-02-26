plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.serialization)
    id("com.github.johnrengelman.shadow") version "8.1.1"
    application
}

dependencies {
    // Hasura NDC Kotlin SDK
    implementation(libs.ndc.sdk.kotlin)
    implementation(project(":app"))
    implementation(project(":common"))
    implementation(project(":sources:bigquery"))

    // BigQuery JDBC driver
    implementation(files("../../../libs/bigquery/GoogleBigQueryJDBC42.jar"))

    // BigQuery client libraries
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
    implementation("org.jooq.pro:jooq:3.20.1")
}

tasks.shadowJar {
    // Enable "zip64" for large JARs
    // Prevents following build error:
    //      To build this archive, please enable the zip64 extension.
    //      See: https://docs.gradle.org/8.12.1/dsl/org.gradle.api.tasks.bundling.Zip.html#org.gradle.api.tasks.bundling.Zip:zip64
    setProperty("zip64", true)
    manifest {
        attributes(mapOf("Main-Class" to "io.hasura.bigquery.app.MainKt"))
    }
}

tasks.withType<Tar>().configureEach {
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}
tasks.withType<Zip>().configureEach {
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}

application {
    mainClass.set("io.hasura.bigquery.app.MainKt")
}
