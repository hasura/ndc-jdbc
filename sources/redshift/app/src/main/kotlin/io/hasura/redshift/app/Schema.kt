package io.hasura.redshift.app

import io.hasura.app.base.*
import io.hasura.app.default.*
import io.hasura.ndc.ir.*
import io.hasura.common.configuration.*
import io.hasura.redshift.common.RedshiftDataType
import org.jooq.impl.DSL.*
import org.jooq.Field as JooqField
import org.jooq.impl.SQLDataType
import org.jooq.*

class RedshiftSchemaGenerator : DefaultSchemaGenerator<RedshiftDataType>() {
    override fun mapToTypeRepresentation(
        columnType: RedshiftDataType,
    ): ScalarType {
        val representationType = when (columnType) {
            is RedshiftDataType.DECIMAL -> {
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
            RedshiftDataType.BIGINT -> RepresentationType.Int64
            RedshiftDataType.BOOLEAN -> RepresentationType.TypeBoolean
            RedshiftDataType.CHAR, RedshiftDataType.VARCHAR -> RepresentationType.TypeString
            RedshiftDataType.DATE -> RepresentationType.Date
            RedshiftDataType.DOUBLE_PRECISION -> RepresentationType.Float64
            RedshiftDataType.GEOGRAPHY -> RepresentationType.Geography
            RedshiftDataType.GEOMETRY -> RepresentationType.Geometry
            RedshiftDataType.HLLSKETCH -> RepresentationType.JSON
            RedshiftDataType.INTEGER -> RepresentationType.Int32
            RedshiftDataType.REAL -> RepresentationType.Float32
            RedshiftDataType.SMALLINT -> RepresentationType.Int16
            RedshiftDataType.TEXT -> RepresentationType.TypeString
            RedshiftDataType.TIME -> RepresentationType.Timestamp
            RedshiftDataType.TIMETZ -> RepresentationType.Timestamptz
            RedshiftDataType.TIMESTAMP -> RepresentationType.Timestamp
            RedshiftDataType.TIMESTAMPTZ -> RepresentationType.Timestamptz
            RedshiftDataType.VARBYTE -> RepresentationType.Bytes
            RedshiftDataType.SUPER -> RepresentationType.JSON
            else -> null
        }

        return createScalarType(representationType, columnType)
    }

    override fun mapColumnDataTypeToSQLDataType(
        columnType: RedshiftDataType,
    ): DataType<out Any> {
        return when (columnType) {
            is RedshiftDataType.DECIMAL -> SQLDataType.NUMERIC
            RedshiftDataType.BIGINT -> SQLDataType.BIGINT
            RedshiftDataType.BOOLEAN -> SQLDataType.BOOLEAN
            RedshiftDataType.CHAR, RedshiftDataType.VARCHAR -> SQLDataType.CLOB
            RedshiftDataType.DATE -> SQLDataType.DATE
            RedshiftDataType.DOUBLE_PRECISION -> SQLDataType.DOUBLE
            RedshiftDataType.GEOGRAPHY -> SQLDataType.JSON
            RedshiftDataType.GEOMETRY -> SQLDataType.JSON
            RedshiftDataType.HLLSKETCH -> SQLDataType.JSON
            RedshiftDataType.INTEGER -> SQLDataType.INTEGER
            RedshiftDataType.REAL -> SQLDataType.REAL
            RedshiftDataType.SMALLINT -> SQLDataType.SMALLINT
            RedshiftDataType.TEXT -> SQLDataType.CLOB
            RedshiftDataType.TIME -> SQLDataType.TIME
            RedshiftDataType.TIMETZ -> SQLDataType.TIMESTAMPWITHTIMEZONE
            RedshiftDataType.TIMESTAMP -> SQLDataType.TIMESTAMP
            RedshiftDataType.TIMESTAMPTZ -> SQLDataType.TIMESTAMPWITHTIMEZONE
            RedshiftDataType.VARBYTE -> SQLDataType.BLOB
            RedshiftDataType.SUPER -> SQLDataType.JSON
            else -> SQLDataType.CLOB
        }
    }

    override fun castToSQLDataType(
        field: JooqField<*>,
        columnType: RedshiftDataType?
    ): JooqField<*> {
        return when (columnType) {
            is RedshiftDataType.DECIMAL -> {
                val (precision, scale) = columnType
                when {
                    scale == 0 && precision > 18 ->
                        cast(field, SQLDataType.VARCHAR)
                    scale > 0 ->
                        cast(field, SQLDataType.VARCHAR)
                    else -> field
                }
            }
            RedshiftDataType.GEOGRAPHY, RedshiftDataType.GEOMETRY ->
                cast(field("ST_AsGeoJSON({0})", Any::class.java, field), SQLDataType.JSON)
            else -> field
        }
    }
}
