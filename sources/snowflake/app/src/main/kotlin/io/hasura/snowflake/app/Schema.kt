package io.hasura.snowflake.app

import io.hasura.app.default.*
import io.hasura.ndc.ir.*
import io.hasura.snowflake.common.SnowflakeDataType
import org.jooq.DataType
import org.jooq.impl.DSL.*
import org.jooq.Field as JooqField
import org.jooq.impl.SQLDataType
import org.jooq.*

class SnowflakeSchemaGenerator : DefaultSchemaGenerator<SnowflakeDataType>() {
    override fun mapToTypeRepresentation(
        columnType: SnowflakeDataType
    ): ScalarType {
        val representationType = when (columnType) {
            is SnowflakeDataType.NUMBER -> {
                val (precision, scale) = columnType
                when {
                    scale == 0 -> when {
                        precision <= 2 -> RepresentationType.Int8
                        precision <= 4 -> RepresentationType.Int16
                        precision <= 9 -> RepresentationType.Int32
                        precision <= 18 -> RepresentationType.Int64
                        else -> RepresentationType.Biginteger
                    }
                    scale > 0 -> when {
                        precision <= 15 -> RepresentationType.Float64
                        else -> RepresentationType.Bigdecimal
                    }
                    else -> RepresentationType.Bigdecimal
                }
            }
            SnowflakeDataType.FLOAT -> RepresentationType.Float64
            SnowflakeDataType.TEXT -> RepresentationType.TypeString
            SnowflakeDataType.BINARY -> RepresentationType.Bytes
            SnowflakeDataType.DATE -> RepresentationType.Date
            SnowflakeDataType.TIME -> RepresentationType.Timestamp
            SnowflakeDataType.TIMESTAMP_NTZ -> RepresentationType.Timestamp
            SnowflakeDataType.TIMESTAMP_LTZ, 
            SnowflakeDataType.TIMESTAMP_TZ -> RepresentationType.Timestamptz
            SnowflakeDataType.VARIANT,
            SnowflakeDataType.OBJECT,
            SnowflakeDataType.ARRAY -> RepresentationType.JSON
            SnowflakeDataType.GEOGRAPHY -> RepresentationType.Geography
            SnowflakeDataType.GEOMETRY -> RepresentationType.Geometry
            SnowflakeDataType.BOOLEAN -> RepresentationType.TypeBoolean
            else -> null
        }
        
        return createScalarType(representationType, columnType.typeName)
    }

    override fun mapColumnDataTypeToSQLDataType(
        columnType: SnowflakeDataType,
    ): DataType<out Any> {
        return when (columnType) {
            SnowflakeDataType.NUMBER -> SQLDataType.NUMERIC
            SnowflakeDataType.FLOAT -> SQLDataType.DOUBLE
            SnowflakeDataType.TEXT -> SQLDataType.CLOB
            SnowflakeDataType.BINARY -> SQLDataType.BLOB
            SnowflakeDataType.DATE -> SQLDataType.DATE
            SnowflakeDataType.TIME -> SQLDataType.TIME
            SnowflakeDataType.TIMESTAMP_NTZ -> SQLDataType.TIMESTAMP
            SnowflakeDataType.TIMESTAMP_LTZ, 
            SnowflakeDataType.TIMESTAMP_TZ -> SQLDataType.TIMESTAMPWITHTIMEZONE
            SnowflakeDataType.VARIANT,
            SnowflakeDataType.OBJECT,
            SnowflakeDataType.ARRAY -> SQLDataType.JSON
            SnowflakeDataType.GEOGRAPHY -> SQLDataType.JSON
            SnowflakeDataType.GEOMETRY -> SQLDataType.JSON
            SnowflakeDataType.BOOLEAN -> SQLDataType.BOOLEAN
            else -> SQLDataType.CLOB
        }
    }

    override fun castToSQLDataType(field: JooqField<*>, columnType: SnowflakeDataType): JooqField<*> {
        return when (columnType) {
            is SnowflakeDataType.NUMBER -> {
                val (precision, scale) = columnType
                when {
                    scale == 0 && precision > 18 ->
                        cast(field, SQLDataType.VARCHAR)
                    scale > 0 && precision > 15 -> 
                        cast(field, SQLDataType.VARCHAR)
                    else -> field
                }
            }
            SnowflakeDataType.GEOGRAPHY, SnowflakeDataType.GEOMETRY -> 
                cast(field("ST_AsGeoJSON({0})", Any::class.java, field), SQLDataType.JSON)
            SnowflakeDataType.VECTOR -> 
                cast(field("TO_ARRAY({0})", Any::class.java, field), SQLDataType.JSON)
            else -> field
        }
    }
}
