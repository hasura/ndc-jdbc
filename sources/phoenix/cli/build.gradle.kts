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
    implementation(project(":sources:phoenix"))

    // Phoenix JDBC driver
    implementation("org.apache.phoenix:phoenix-client-hbase-2.4:5.1.1")
    implementation("org.apache.phoenix:phoenix-queryserver-client:5.0.0-HBase-2.0")

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
        attributes(mapOf("Main-Class" to "io.hasura.phoenix.cli.MainKt"))
    }
}

tasks.withType<Tar>().configureEach {
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}
tasks.withType<Zip>().configureEach {
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}

application {
    mainClass.set("io.hasura.phoenix.cli.MainKt")
}
