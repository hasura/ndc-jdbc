plugins {
    alias(libs.plugins.kotlin.jvm) apply false
}

allprojects {
    repositories {
        mavenCentral()
        google()
        maven { url = uri("https://raw.githubusercontent.com/hasura/ndc-sdk-kotlin/m2repo/") }
        maven {
            url = uri("https://repo.jooq.org/repo")
            credentials {
                username = System.getenv("JOOQ_PRO_EMAIL")
                password = System.getenv("JOOQ_PRO_LICENSE")
            }
        }
    }
}

subprojects {
    apply(plugin = "org.jetbrains.kotlin.jvm")

    afterEvaluate {
        configurations {
            all {
                resolutionStrategy {
                    // Optional: Add any resolution strategy if needed
                }
            }
        }
        
        dependencies {
            "implementation"(libs.arvo)
            "implementation"(libs.hikari)
            "implementation"(libs.joda.time)
            "implementation"(libs.kotlinx.cli)
            "implementation"(libs.kotlinx.serialization.json)
            "implementation"(libs.logback.classic)
            "implementation"(libs.semver4j)
            "implementation"(libs.vertx.core)
            "implementation"(libs.vertx.kotlin)
            "implementation"(libs.vertx.kotlin.coroutines)
            "implementation"(libs.vertx.web)
            
            // Micrometer
            "implementation"("io.micrometer:micrometer-core:1.11.3")
            "implementation"("io.micrometer:micrometer-registry-prometheus:1.11.3")
            
            // OpenTelemetry
            "implementation"(libs.opentelemetry.api)
            "implementation"(libs.opentelemetry.sdk)
            "implementation"(libs.opentelemetry.kotlin)
            "implementation"(libs.opentelemetry.semconv)
            "implementation"(libs.opentelemetry.otlp)
        }
    }
}
