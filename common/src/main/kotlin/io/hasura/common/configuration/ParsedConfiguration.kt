import kotlinx.serialization.*
import kotlinx.serialization.json.*

import io.hasura.common.configuration.ColumnType
import io.hasura.common.configuration.ConnectionUri
import io.hasura.common.configuration.version1.ConfigurationV1
import io.hasura.common.configuration.version1.TableInfoV1
import io.hasura.common.configuration.version1.FunctionInfoV1
import io.hasura.common.configuration.version1.NativeOperationV1
import io.hasura.common.configuration.DefaultConfiguration
import kotlinx.serialization.builtins.serializer




/**
 * Enum representing configuration versions
 */
@Serializable
enum class ConfigVersion {
    @SerialName("v1")
    V1,

    // More versions can be added here in the future
}

/**
 * Base interface for all configuration versions
 */
interface Configuration<T : ColumnType> {
    val version: ConfigVersion

    /**
     * Convert this specific configuration to the standard DefaultConfiguration
     */
    fun toDefaultConfiguration(): DefaultConfiguration<T>
}


/**
 * Main configuration parser responsible for handling different versions
 */
object ConfigurationParser {
    // Make json public so it can be accessed from inline functions
    val json = Json {
        ignoreUnknownKeys = false
        isLenient = true
    }

    /**
     * Parse configuration from JSON string
     * This public method can be called directly from your DefaultConnector
     */
    inline fun <reified T : ColumnType> parse(jsonString: String, serializer: KSerializer<T>): DefaultConfiguration<T> {
        return parseInternal(jsonString, serializer)
    }

    /**
     * Internal implementation of the parse function
     * This separates the public API from the implementation details
     */
    @PublishedApi
    internal inline fun <reified T : ColumnType> parseInternal(jsonString: String, serializer: KSerializer<T>): DefaultConfiguration<T> {
        val version = determineVersion(jsonString)

        return when (version) {
            ConfigVersion.V1 -> {
                val configV1 = json.decodeFromString(ConfigurationV1.serializer(serializer), jsonString)
                configV1.toDefaultConfiguration()
            }
            // Additional versions can be handled here in the future
        }
    }

    /**
     * Determine the version from the JSON string
     * Made public to be accessible from inline functions
     */
    @PublishedApi
    internal fun determineVersion(jsonString: String): ConfigVersion {
        @Serializable
        data class VersionWrapper(val version: String)

        return try {
            val versionInfo = json.decodeFromString<VersionWrapper>(jsonString)
            when (versionInfo.version) {
                "v1" -> ConfigVersion.V1
                else -> throw IllegalArgumentException("Unsupported configuration version: ${versionInfo.version}")
            }
        } catch (e: Exception) {
            throw IllegalArgumentException("Failed to determine configuration version: ${e.message}")
        }
    }
}
