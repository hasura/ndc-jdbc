package io.hasura.bigquery.app

import io.hasura.app.default.*
import io.hasura.bigquery.common.BigQueryScalarType
import io.hasura.bigquery.common.BigQueryType
import io.hasura.ndc.ir.*
import org.jooq.*
import org.jooq.DataType
import org.jooq.Field as JooqField
import org.jooq.impl.DSL.*
import org.jooq.impl.SQLDataType

class BigQuerySchemaGenerator : DefaultSchemaGenerator<BigQueryType>() {
    override fun mapToTypeRepresentation(
        columnType: BigQueryType
    ): ScalarType {
        val representationType = when (columnType) {
            is BigQueryType.ScalarType -> when (columnType.scalarType) {
                BigQueryScalarType.ANY -> null
                BigQueryScalarType.STRING -> RepresentationType.TypeString
                BigQueryScalarType.INT64 -> RepresentationType.Int64
                BigQueryScalarType.FLOAT64 -> RepresentationType.Float64
                BigQueryScalarType.NUMERIC, BigQueryScalarType.BIGNUMERIC -> RepresentationType.Bigdecimal
                BigQueryScalarType.BOOLEAN -> RepresentationType.TypeBoolean
                BigQueryScalarType.DATE -> RepresentationType.Date
                BigQueryScalarType.TIME -> RepresentationType.Timestamp
                BigQueryScalarType.DATETIME, BigQueryScalarType.TIMESTAMP -> RepresentationType.Timestamp
                BigQueryScalarType.BYTES -> RepresentationType.Bytes
                BigQueryScalarType.GEOGRAPHY -> RepresentationType.Geography
                BigQueryScalarType.JSON -> RepresentationType.JSON
                else -> null
            }
            is BigQueryType.ArrayType -> RepresentationType.JSON
            is BigQueryType.RangeType -> RepresentationType.JSON
            is BigQueryType.StructType -> RepresentationType.JSON
            else -> null
        }
        
        return createScalarType(representationType, columnType.typeName)
    }

    override fun mapColumnDataTypeToSQLDataType(
        columnType: BigQueryType,
    ): DataType<out Any> {
        return when (columnType) {
            is BigQueryType.ScalarType -> when (columnType.scalarType) {
                BigQueryScalarType.ANY -> SQLDataType.CLOB
                BigQueryScalarType.STRING -> SQLDataType.CLOB
                BigQueryScalarType.INT64 -> SQLDataType.BIGINT
                BigQueryScalarType.FLOAT64 -> SQLDataType.DOUBLE
                BigQueryScalarType.NUMERIC, BigQueryScalarType.BIGNUMERIC -> SQLDataType.NUMERIC
                BigQueryScalarType.BOOLEAN -> SQLDataType.BOOLEAN
                BigQueryScalarType.DATE -> SQLDataType.DATE
                BigQueryScalarType.TIME -> SQLDataType.TIME
                BigQueryScalarType.DATETIME, BigQueryScalarType.TIMESTAMP -> SQLDataType.TIMESTAMP
                BigQueryScalarType.BYTES -> SQLDataType.BLOB
                BigQueryScalarType.GEOGRAPHY -> SQLDataType.JSON
                BigQueryScalarType.JSON -> SQLDataType.JSON
            }
            is BigQueryType.ArrayType -> SQLDataType.JSON
            is BigQueryType.RangeType -> SQLDataType.JSON
            is BigQueryType.StructType -> SQLDataType.JSON
        }
    }

    override fun castToSQLDataType(
        field: JooqField<*>,
        columnType: BigQueryType
    ): JooqField<*> {
        return when (columnType) {
            is BigQueryType.ScalarType -> when (columnType.scalarType) {
                BigQueryScalarType.INT64,
                BigQueryScalarType.NUMERIC,
                BigQueryScalarType.BIGNUMERIC ->
                    cast(field, SQLDataType.VARCHAR)
                BigQueryScalarType.GEOGRAPHY ->
                    cast(field("ST_AsGeoJSON({0})", Any::class.java, field), SQLDataType.JSON)
                else -> field
            }
            else -> field
        }
    }

    override fun mapAggregateFunctions(
        columnTypeStr: String,
        representation: TypeRepresentation?
    ): Map<String, AggregateFunctionDefinition> {
        return emptyMap()
    }

    override fun mapComparisonOperators(
        columnTypeStr: String,
        representation: TypeRepresentation?
    ): Map<String, ComparisonOperatorDefinition> {
        return emptyMap()
    }
}
