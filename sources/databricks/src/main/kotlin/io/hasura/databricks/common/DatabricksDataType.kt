package io.hasura.databricks.common

import io.hasura.common.ColumnType
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonClassDiscriminator

@Serializable
@OptIn(kotlinx.serialization.ExperimentalSerializationApi::class)
@JsonClassDiscriminator("scalar_type")
sealed class DatabricksDataType : ColumnType {
    @Serializable
    @SerialName("BOOLEAN")
    object BOOLEAN : DatabricksDataType()

    @Serializable
    @SerialName("TINYINT")
    object TINYINT : DatabricksDataType()

    @Serializable
    @SerialName("SMALLINT")
    object SMALLINT : DatabricksDataType()

    @Serializable
    @SerialName("INT")
    object INT : DatabricksDataType()

    @Serializable
    @SerialName("BIGINT")
    object BIGINT : DatabricksDataType()

    @Serializable
    @SerialName("FLOAT")
    object FLOAT : DatabricksDataType()

    @Serializable
    @SerialName("DOUBLE")
    object DOUBLE : DatabricksDataType()

    @Serializable
    @SerialName("DECIMAL")
    data class DECIMAL(
        val precision: Int?,
        val scale: Int?
    ) : DatabricksDataType()

    @Serializable
    @SerialName("STRING")
    object STRING : DatabricksDataType()

    @Serializable
    @SerialName("CHAR")
    object CHAR : DatabricksDataType()

    @Serializable
    @SerialName("VARCHAR")
    object VARCHAR : DatabricksDataType()

    @Serializable
    @SerialName("BINARY")
    object BINARY : DatabricksDataType()

    @Serializable
    @SerialName("DATE")
    object DATE : DatabricksDataType()

    @Serializable
    @SerialName("TIMESTAMP")
    object TIMESTAMP : DatabricksDataType()

    @Serializable
    @SerialName("TIMESTAMP_NTZ")
    object TIMESTAMP_NTZ : DatabricksDataType()

    @Serializable
    @SerialName("VARIANT")
    object VARIANT : DatabricksDataType()

    @Serializable
    @SerialName("ARRAY")
    object ARRAY : DatabricksDataType()

    @Serializable
    @SerialName("MAP")
    object MAP : DatabricksDataType()

    @Serializable
    @SerialName("STRUCT")
    object STRUCT : DatabricksDataType()

    override val typeName: String
        get() = when (this) {
            is DECIMAL -> {
                val (precision, scale) = this
                when {
                    scale == 0 ->
                        when {
                            precision!! <= 2 -> "TINYINT"
                            precision <= 4 -> "SMALLINT"
                            precision <= 9 -> "INT"
                            precision <= 18 -> "BIGINT"
                            else -> "BIGINT"
                        }
                    scale!! > 0 -> "DECIMAL"
                    else -> "DECIMAL"
                }
            }
            is ARRAY -> "ARRAY"
            is MAP -> "MAP"
            is STRUCT -> "STRUCT"
            else -> this::class.simpleName?.uppercase() ?: "UNKNOWN"
        }
}
