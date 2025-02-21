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
    data class ARRAY(val elementType: DatabricksDataType) : DatabricksDataType()

    @Serializable
    @SerialName("MAP")
    data class MAP(val keyType: DatabricksDataType, val valueType: DatabricksDataType) : DatabricksDataType()

    @Serializable
    @SerialName("STRUCT")
    data class STRUCT(val fields: List<StructField>) : DatabricksDataType()

    @Serializable
    @SerialName("STRUCT_FIELD")
    data class StructField(val name: String, val type: DatabricksDataType)

    override val typeName: String = when (this) {
        is DECIMAL -> "DECIMAL"
        is ARRAY -> "ARRAY"
        is MAP -> "MAP"
        is STRUCT -> "STRUCT"
        else -> this::class.simpleName?.uppercase() ?: "UNKNOWN"
    }
}
