package io.hasura.common.configuration

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
enum class Version {
    @SerialName("v1")
    V1
}

interface Configuration<T: ColumnType> {
    @SerialName("version") val version: Version

    @SerialName("connection_uri") val connectionUri: ConnectionUri

    fun toDefaultConfiguration(): DefaultConfiguration<T>
}
