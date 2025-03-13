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
import java.nio.file.Path




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
     * Parse configuration from a directory containing configuration.json
     * This public method can be called directly from your DefaultConnector
     */
    fun <T : ColumnType> parse(configurationDir: Path, serializer: KSerializer<T>): DefaultConfiguration<T> {
        return parseInternal(configurationDir, serializer)
    }

    /**
     * Internal implementation of the parse function
     * This separates the public API from the implementation details
     */
    internal fun <T : ColumnType> parseInternal(configurationDir: Path, serializer: KSerializer<T>): DefaultConfiguration<T> {
        val configFile = configurationDir.resolve("configuration.json")
        val jsonString = configFile.toFile().readText()

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
     */
    internal fun determineVersion(jsonString: String): ConfigVersion {
        return try {
            // Create a JSON element from the string
            val jsonElement = json.parseToJsonElement(jsonString)

            // Extract the version field
            val versionString = jsonElement.jsonObject["version"]?.jsonPrimitive?.contentOrNull
                ?: throw IllegalArgumentException("Missing or invalid 'version' field in configuration")

            // Parse directly to the enum
            try {
                json.decodeFromString<ConfigVersion>("\"$versionString\"")
            } catch (e: SerializationException) {
                throw IllegalArgumentException("Unsupported configuration version: $versionString")
            }
        } catch (e: Exception) {
            when (e) {
                is IllegalArgumentException -> throw e
                else -> throw IllegalArgumentException("Failed to determine configuration version: ${e.message}")
            }
        }
    }

}
