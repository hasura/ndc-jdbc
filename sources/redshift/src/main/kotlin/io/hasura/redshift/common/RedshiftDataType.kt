package io.hasura.redshift.common

import io.hasura.common.ColumnType
import kotlinx.serialization.Serializable
import kotlinx.serialization.SerialName
import kotlinx.serialization.json.JsonClassDiscriminator

@Serializable
@OptIn(kotlinx.serialization.ExperimentalSerializationApi::class)
@JsonClassDiscriminator("scalar_type")
sealed class RedshiftDataType : ColumnType {
    @Serializable
    @SerialName("bigint")
    object BIGINT : RedshiftDataType()

    @Serializable
    @SerialName("boolean")
    object BOOLEAN : RedshiftDataType()

    @Serializable
    @SerialName("char")
    object CHAR : RedshiftDataType()

    @Serializable
    @SerialName("date")
    object DATE : RedshiftDataType()

    @Serializable
    @SerialName("decimal")
    data class DECIMAL(
        val precision: Int,
        val scale: Int
    ) : RedshiftDataType()

    @Serializable
    @SerialName("double precision")
    object DOUBLE_PRECISION : RedshiftDataType()

    @Serializable
    @SerialName("geometry")
    object GEOMETRY : RedshiftDataType()

    @Serializable
    @SerialName("geography")
    object GEOGRAPHY : RedshiftDataType()

    @Serializable
    @SerialName("hllsketch")
    object HLLSKETCH : RedshiftDataType()

    @Serializable
    @SerialName("integer")
    object INTEGER : RedshiftDataType()

    @Serializable
    @SerialName("interval year to month")
    object INTERVAL_YEAR_TO_MONTH : RedshiftDataType()

    @Serializable
    @SerialName("interval day to second")
    object INTERVAL_DAY_TO_SECOND : RedshiftDataType()

    @Serializable
    @SerialName("real")
    object REAL : RedshiftDataType()

    @Serializable
    @SerialName("smallint")
    object SMALLINT : RedshiftDataType()

    @Serializable
    @SerialName("super")
    object SUPER : RedshiftDataType()

    @Serializable
    @SerialName("text")
    object TEXT : RedshiftDataType()

    @Serializable
    @SerialName("time")
    object TIME : RedshiftDataType()

    @Serializable
    @SerialName("timetz")
    object TIMETZ : RedshiftDataType()

    @Serializable
    @SerialName("timestamp")
    object TIMESTAMP : RedshiftDataType()

    @Serializable
    @SerialName("timestamptz")
    object TIMESTAMPTZ : RedshiftDataType()

    @Serializable
    @SerialName("varbyte")
    object VARBYTE : RedshiftDataType()

    @Serializable
    @SerialName("varchar")
    object VARCHAR : RedshiftDataType()

    override val typeName: String
        get() = when (this) {
            is DECIMAL -> "DECIMAL"
            else -> this::class.simpleName?.uppercase() ?: "UNKNOWN"
        }
}
