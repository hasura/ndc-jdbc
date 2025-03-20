package io.hasura.athena.common

import io.hasura.common.ColumnType
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonClassDiscriminator


@Serializable
@OptIn(kotlinx.serialization.ExperimentalSerializationApi::class)
@SerialName("scalar_type")
sealed class AthenaDataType : ColumnType {
    @Serializable
    @SerialName("varchar")
    object VARCHAR : AthenaDataType()

    // @Serializable
    // @SerialName("char")
    // object Char(val length: Int? = null) : AthenaDataType()

    @Serializable
    @SerialName("string")
    object STRING : AthenaDataType()

    @Serializable
    @SerialName("tinyint")
    object TINYINT : AthenaDataType()

    @Serializable
    @SerialName("smallint")
    object SMALLINT : AthenaDataType()

    @Serializable
    @SerialName("integer")
    object INTEGER : AthenaDataType()

    @Serializable
    @SerialName("bigint")
    object BIGINT : AthenaDataType()

    @Serializable
    @SerialName("boolean")
    object BOOLEAN : AthenaDataType()

    @Serializable
    @SerialName("float")
    object FLOAT : AthenaDataType()

    @Serializable
    @SerialName("double")
    object DOUBLE : AthenaDataType()

    @Serializable
    @SerialName("decimal")
    data class DECIMAL(
        val precision: Int,
        val scale: Int
    ) : AthenaDataType()

    @Serializable
    @SerialName("date")
    object DATE : AthenaDataType()

    @Serializable
    @SerialName("timestamp")
    object TIMESTAMP : AthenaDataType()

    @Serializable
    @SerialName("binary")
    object BINARY : AthenaDataType()

    @Serializable
    @SerialName("json")
    object JSON : AthenaDataType()

    @Serializable
    @SerialName("unknown")
    data class UNKNOWN(
        val type: String
    ) : AthenaDataType()

override val typeName: String
    get() =
            when (this) {
                is DECIMAL -> {
                    val (precision, scale) = this
                    when {
                        scale == 0 ->
                                when {
                                    precision <= 2 -> "INT_8"
                                    precision <= 4 -> "INT_16"
                                    precision <= 9 -> "INT_32"
                                    precision <= 18 -> "INT_64"
                                    else -> "BIGINTEGER"
                                }
                        scale > 0 -> "BIGDECIMAL"
                        else -> "BIGDECIMAL"
                    }
                }
                is UNKNOWN -> type
                else -> this::class.simpleName?.uppercase() ?: "UNKNOWN"
            }


}
