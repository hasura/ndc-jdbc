package io.hasura.redshift.app

import io.hasura.app.base.*
import io.hasura.app.default.*
import io.hasura.ndc.ir.*
import kotlinx.serialization.Serializable
import kotlinx.serialization.SerialName

@Serializable
enum class RedshiftDataType : ColumnType {
    @SerialName("bigint") BIGINT,
    @SerialName("boolean") BOOLEAN,
    @SerialName("char") CHAR,
    @SerialName("character") CHARACTER,
    @SerialName("date") DATE,
    @SerialName("double precision") DOUBLE_PRECISION,
    @SerialName("integer") INTEGER,
    @SerialName("numeric") NUMERIC,
    @SerialName("real") REAL,
    @SerialName("smallint") SMALLINT,
    @SerialName("text") TEXT,
    @SerialName("time") TIME,
    @SerialName("timestamp") TIMESTAMP,
    @SerialName("timestamp with time zone") TIMESTAMP_WITH_TIME_ZONE,
    @SerialName("timestamp without time zone") TIMESTAMP_WITHOUT_TIME_ZONE,
    @SerialName("varchar") VARCHAR;

    override val typeName: String
        get() = toString()
}

class RedshiftSchemaGenerator : DefaultSchemaGenerator<RedshiftDataType>() {
    override fun mapToTypeRepresentation(
        columnType: RedshiftDataType,
        numericPrecision: Int?,
        numericScale: Int?
    ): ScalarType {
        val representationType = when (columnType) {
            RedshiftDataType.BIGINT -> RepresentationType.Int64
            RedshiftDataType.BOOLEAN -> RepresentationType.TypeBoolean
            RedshiftDataType.CHAR, RedshiftDataType.CHARACTER, RedshiftDataType.VARCHAR -> RepresentationType.TypeString
            RedshiftDataType.DATE -> RepresentationType.Date
            RedshiftDataType.DOUBLE_PRECISION -> RepresentationType.Float64
            RedshiftDataType.INTEGER -> RepresentationType.Int32
            RedshiftDataType.NUMERIC -> RepresentationType.Float64
            RedshiftDataType.REAL -> RepresentationType.Float32
            RedshiftDataType.SMALLINT -> RepresentationType.Int16
            RedshiftDataType.TEXT -> RepresentationType.TypeString
            RedshiftDataType.TIME -> RepresentationType.Timestamp
            RedshiftDataType.TIMESTAMP -> RepresentationType.Timestamp
            RedshiftDataType.TIMESTAMP_WITH_TIME_ZONE -> RepresentationType.Timestamptz
            RedshiftDataType.TIMESTAMP_WITHOUT_TIME_ZONE -> RepresentationType.Timestamp
        }
        
        return createScalarType(representationType, columnType.name)
    }
}
