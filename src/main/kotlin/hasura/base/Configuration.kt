package hasura.base

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

@Serializable
data class ConnectionUri(
    val value: String? = null,
    val variable: String? = null
) {
    fun resolve(): String = when {
        value != null -> value
        variable != null -> System.getenv(variable) 
            ?: throw IllegalStateException("Environment variable $variable not found")
        else -> throw IllegalStateException("Either value or variable must be set")
    }
}

@Serializable
sealed class Configuration {
    @SerialName("connection_uri")
    abstract val connectionUri: ConnectionUri
}