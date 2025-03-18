package io.hasura.common

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
data class ConnectionPoolSettings(
    @SerialName("max_connections")
    val maxConnections: Int,
    @SerialName("min_idle")
    val minIdle: Int,
    @SerialName("connection_timeout")
    val connectionTimeout: Long,
    @SerialName("initialization_fail_timeout")
    val initializationFailTimeout: Long
)

interface Configuration {
    @SerialName("connection_uri")
    val connectionUri: ConnectionUri
    @SerialName("connection_pool_settings")
    val connectionPoolSettings: ConnectionPoolSettings?
        get() = ConnectionPoolSettings(10, 1, 30000L, 30000L)
}
