package io.hasura.snowflake.common

import io.hasura.common.ColumnType
import kotlinx.serialization.Serializable
import kotlinx.serialization.SerialName
import kotlinx.serialization.json.JsonClassDiscriminator

@Serializable
@OptIn(kotlinx.serialization.ExperimentalSerializationApi::class)
@JsonClassDiscriminator("scalar_type")
sealed class SnowflakeDataType : ColumnType {
    @Serializable
    @SerialName("ARRAY")
    object ARRAY : SnowflakeDataType()

    @Serializable
    @SerialName("BINARY")
    object BINARY : SnowflakeDataType()

    @Serializable
    @SerialName("BOOLEAN")
    object BOOLEAN : SnowflakeDataType()

    @Serializable
    @SerialName("DATE")
    object DATE : SnowflakeDataType()

    @Serializable
    @SerialName("FLOAT")
    object FLOAT : SnowflakeDataType()

    @Serializable
    @SerialName("GEOGRAPHY")
    object GEOGRAPHY : SnowflakeDataType()

    @Serializable
    @SerialName("GEOMETRY")
    object GEOMETRY : SnowflakeDataType()

    @Serializable
    @SerialName("NUMBER")
    data class NUMBER(
        val precision: Int,
        val scale: Int
    ) : SnowflakeDataType()

    @Serializable
    @SerialName("OBJECT")
    object OBJECT : SnowflakeDataType()

    @Serializable
    @SerialName("TEXT")
    object TEXT : SnowflakeDataType()

    @Serializable
    @SerialName("TIME")
    object TIME : SnowflakeDataType()

    @Serializable
    @SerialName("TIMESTAMP_LTZ")
    object TIMESTAMP_LTZ : SnowflakeDataType()

    @Serializable
    @SerialName("TIMESTAMP_NTZ")
    object TIMESTAMP_NTZ : SnowflakeDataType()

    @Serializable
    @SerialName("TIMESTAMP_TZ")
    object TIMESTAMP_TZ : SnowflakeDataType()

    @Serializable
    @SerialName("VARIANT")
    object VARIANT : SnowflakeDataType()

    @Serializable
    @SerialName("VECTOR")
    object VECTOR : SnowflakeDataType()

    override val typeName: String
        get() = when (this) {
            is NUMBER -> {
                val (precision, scale) = this
                when {
                    scale == 0 ->
                        when {
                            precision <= 2 -> "Int8"
                            precision <= 4 -> "Int16"
                            precision <= 9 -> "Int32"
                            precision <= 18 -> "Int64"
                            else -> "Biginteger"
                        }
                    scale > 0 -> "Bigdecimal"
                    else -> "Bigdecimal"
                }
            }
            else -> this::class.simpleName?.uppercase() ?: "UNKNOWN"
        }
}
