package io.hasura.athena.common

import io.hasura.common.ColumnType
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonClassDiscriminator


@Serializable
@OptIn(kotlinx.serialization.ExperimentalSerializationApi::class)
@JsonClassDiscriminator("scalar_type")
sealed class AthenaDataType : ColumnType {
    @Serializable
    @SerialName("VARCHAR")
    object VARCHAR : AthenaDataType()

    @Serializable
    @SerialName("CHAR")
    object CHAR : AthenaDataType()

    @Serializable
    @SerialName("STRING")
    object STRING : AthenaDataType()

    @Serializable
    @SerialName("TINYINT")
    object TINYINT : AthenaDataType()

    @Serializable
    @SerialName("SMALLINT")
    object SMALLINT : AthenaDataType()

    @Serializable
    @SerialName("INTEGER")
    object INTEGER : AthenaDataType()

    @Serializable
    @SerialName("BIGINT")
    object BIGINT : AthenaDataType()

    @Serializable
    @SerialName("BOOLEAN")
    object BOOLEAN : AthenaDataType()

    @Serializable
    @SerialName("FLOAT")
    object FLOAT : AthenaDataType()

    @Serializable
    @SerialName("DOUBLE")
    object DOUBLE : AthenaDataType()

    @Serializable
    @SerialName("DECIMAL")
    data class DECIMAL(
        val precision: Int,
        val scale: Int
    ) : AthenaDataType()

    @Serializable
    @SerialName("DATE")
    object DATE : AthenaDataType()

    @Serializable
    @SerialName("TIMESTAMP")
    object TIMESTAMP : AthenaDataType()

    @Serializable
    @SerialName("TIMESTAMP_TZ")
    object TIMESTAMP_TZ : AthenaDataType()

    @Serializable
    @SerialName("BINARY")
    object BINARY : AthenaDataType()

    @Serializable
    @SerialName("JSON")
    object JSON : AthenaDataType()

    // TODO: Need to add support for ARRAY, MAP, STRUCT?

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

                else -> this::class.simpleName?.uppercase() ?: "UNKNOWN"
            }


}
