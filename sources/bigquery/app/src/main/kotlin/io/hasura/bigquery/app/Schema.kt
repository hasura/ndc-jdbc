package io.hasura.bigquery.app

import io.hasura.app.default.*
import io.hasura.bigquery.common.BigQueryScalarType
import io.hasura.bigquery.common.BigQueryType
import io.hasura.ndc.ir.*

class BigQuerySchemaGenerator : DefaultSchemaGenerator<BigQueryType>() {
    override fun mapToTypeRepresentation(
        columnType: BigQueryType,
        numericPrecision: Int?,
        numericScale: Int?
    ): ScalarType {
        val representationType = when (columnType) {
            is BigQueryType.ScalarType -> when (columnType.scalarType) {
                BigQueryScalarType.ANY -> null
                BigQueryScalarType.STRING -> RepresentationType.TypeString
                BigQueryScalarType.INT64 -> RepresentationType.Int64
                BigQueryScalarType.FLOAT64 -> RepresentationType.Float64
                BigQueryScalarType.FLOAT -> RepresentationType.Float64
                BigQueryScalarType.BIGINT -> RepresentationType.Biginteger
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
