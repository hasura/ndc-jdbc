package io.hasura.common.configuration

import io.hasura.common.configuration.version1.ConfigurationV1
import io.hasura.common.configuration.version1.ConnectionUri
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json

@Serializable
enum class ParsedConfiguration {
    @SerialName("v1")
    V1;

    companion object {
        private val json = Json { ignoreUnknownKeys = false }

        // Parse configuration from JSON string
        fun parse(jsonString: String): ConfigurationV1 {
            // First determine the version (could be extended for future versions)
            return when (determineVersion(jsonString)) {
                V1 -> parseV1(jsonString)
            }
        }

        // Helper to determine version from JSON
        private fun determineVersion(jsonString: String): ParsedConfiguration {
            // For now we only have V1, but this method allows for future extension
            // In future versions, we could parse a version field from the JSON
            return V1
        }

        // V1-specific parsing logic
        private fun parseV1(jsonString: String): ConfigurationV1 {
            return try {
                json.decodeFromString<ConfigurationV1>(jsonString)
            } catch (e: Exception) {
                throw IllegalArgumentException("Failed to parse V1 configuration: ${e.message}")
            }
        }

        // Create default configuration for a specific version
        fun defaultFor(version: ParsedConfiguration): ConfigurationV1 {
            return when (version) {
                V1 -> ConfigurationV1(
                    connectionUri = ConnectionUri(),
                    schemas = emptyList(),
                    tables = emptyList(),
                    functions = emptyList(),
                    nativeOperations = emptyMap()
                )
            }
        }
    }
}
