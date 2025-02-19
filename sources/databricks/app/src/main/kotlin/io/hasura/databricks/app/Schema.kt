package io.hasura.databricks.app

import io.hasura.app.base.*
import io.hasura.app.default.*
import io.hasura.ndc.ir.*
import io.hasura.common.*
import io.hasura.databricks.common.DatabricksDataType
import org.jooq.impl.DSL.*
import org.jooq.Field as JooqField
import org.jooq.impl.SQLDataType
import org.jooq.*

class DatabricksSchemaGenerator : DefaultSchemaGenerator<DatabricksDataType>() {
    override fun mapToTypeRepresentation(
        columnType: DatabricksDataType,
    ): ScalarType {
        val representationType = when (columnType) {
            is DatabricksDataType.DECIMAL -> {
                val (precision, scale) = columnType
                when {
                    scale == 0 -> when {
                        precision <= 2 -> RepresentationType.Int8
                        precision <= 4 -> RepresentationType.Int16
                        precision <= 9 -> RepresentationType.Int32
                        precision <= 18 -> RepresentationType.Int64
                        else -> RepresentationType.Biginteger
                    }
                    scale > 0 -> RepresentationType.Bigdecimal
                    else -> RepresentationType.Bigdecimal
                }
            }
            is DatabricksDataType.ARRAY -> RepresentationType.JSON
            is DatabricksDataType.MAP -> RepresentationType.JSON
            is DatabricksDataType.STRUCT -> RepresentationType.JSON
            DatabricksDataType.BOOLEAN -> RepresentationType.TypeBoolean
            DatabricksDataType.BYTE -> RepresentationType.Int8
            DatabricksDataType.SHORT -> RepresentationType.Int16
            DatabricksDataType.INT -> RepresentationType.Int32
            DatabricksDataType.BIGINT -> RepresentationType.Int64
            DatabricksDataType.LONG -> RepresentationType.Int64
            DatabricksDataType.FLOAT -> RepresentationType.Float32
            DatabricksDataType.DOUBLE -> RepresentationType.Float64
            DatabricksDataType.STRING -> RepresentationType.TypeString
            DatabricksDataType.CHAR -> RepresentationType.TypeString
            DatabricksDataType.VARCHAR -> RepresentationType.TypeString
            DatabricksDataType.BINARY -> RepresentationType.Bytes
            DatabricksDataType.DATE -> RepresentationType.Date
            DatabricksDataType.TIMESTAMP -> RepresentationType.Timestamp
            DatabricksDataType.GEOMETRY -> RepresentationType.Geometry
            DatabricksDataType.GEOGRAPHY -> RepresentationType.Geography
            else -> null
        }
        
        return createScalarType(representationType, columnType)
    }

    override fun mapColumnDataTypeToSQLDataType(
        columnType: DatabricksDataType,
    ): DataType<out Any> {
        return when (columnType) {
            is DatabricksDataType.DECIMAL -> SQLDataType.NUMERIC
            is DatabricksDataType.ARRAY -> SQLDataType.JSON
            is DatabricksDataType.MAP -> SQLDataType.JSON
            is DatabricksDataType.STRUCT -> SQLDataType.JSON
            DatabricksDataType.BOOLEAN -> SQLDataType.BOOLEAN
            DatabricksDataType.BYTE -> SQLDataType.TINYINT
            DatabricksDataType.SHORT -> SQLDataType.SMALLINT
            DatabricksDataType.INT -> SQLDataType.INTEGER
            DatabricksDataType.BIGINT -> SQLDataType.BIGINT
            DatabricksDataType.LONG -> SQLDataType.BIGINT
            DatabricksDataType.FLOAT -> SQLDataType.REAL
            DatabricksDataType.DOUBLE -> SQLDataType.DOUBLE
            DatabricksDataType.STRING -> SQLDataType.CLOB
            DatabricksDataType.CHAR -> SQLDataType.CHAR
            DatabricksDataType.VARCHAR -> SQLDataType.VARCHAR
            DatabricksDataType.BINARY -> SQLDataType.BINARY
            DatabricksDataType.DATE -> SQLDataType.DATE
            DatabricksDataType.TIMESTAMP -> SQLDataType.TIMESTAMP
            DatabricksDataType.INTERVAL -> SQLDataType.INTERVAL
            DatabricksDataType.GEOMETRY -> SQLDataType.JSON
            DatabricksDataType.GEOGRAPHY -> SQLDataType.JSON
            else -> SQLDataType.CLOB
        }
    }

    override fun castToSQLDataType(
        field: JooqField<*>,
        columnType: DatabricksDataType?
    ): JooqField<*> {
        return when (columnType) {
            is DatabricksDataType.DECIMAL -> {
                val (precision, scale) = columnType
                when {
                    scale == 0 && precision > 18 ->
                        cast(field, SQLDataType.VARCHAR(255))
                    scale > 0 && precision > 15 -> 
                        cast(field, SQLDataType.VARCHAR(255))
                    else -> field
                }
            }
            DatabricksDataType.BIGINT -> cast(field, SQLDataType.VARCHAR(255))
            DatabricksDataType.GEOGRAPHY, DatabricksDataType.GEOMETRY -> 
                cast(field("ST_AsGeoJSON({0})", Any::class.java, field), SQLDataType.JSON)
            else -> field
        }
    }
}
