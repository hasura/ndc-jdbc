package io.hasura.bigquery.app

import io.hasura.app.default.*
import io.hasura.bigquery.common.BigQueryScalarType
import io.hasura.bigquery.common.BigQueryType
import io.hasura.ndc.ir.*
import io.hasura.ndc.ir.Type
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
            }
            is BigQueryType.ArrayType -> RepresentationType.JSON
            is BigQueryType.RangeType -> RepresentationType.JSON
            is BigQueryType.StructType -> RepresentationType.JSON
        }
        
        return createScalarType(representationType, columnType)
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
        columnType: BigQueryType?
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

    private fun getSupportedAggregateFunctions(columnType: BigQueryType): List<String> {
        val numericFunctions = listOf(
            "avg", "sum", "min", "max",
            "stddev_pop", "stddev_samp",
            "var_pop", "var_samp"
        )

        return when (columnType) {
            is BigQueryType.ScalarType -> when (columnType.scalarType) {
                BigQueryScalarType.INT64,
                BigQueryScalarType.FLOAT64,
                BigQueryScalarType.NUMERIC,
                BigQueryScalarType.BIGNUMERIC -> numericFunctions

                BigQueryScalarType.BOOLEAN,
                BigQueryScalarType.DATE,
                BigQueryScalarType.STRING,
                BigQueryScalarType.TIME,
                BigQueryScalarType.DATETIME,
                BigQueryScalarType.TIMESTAMP,
                BigQueryScalarType.BYTES,
                BigQueryScalarType.GEOGRAPHY,
                BigQueryScalarType.JSON,
                BigQueryScalarType.ANY -> listOf("min", "max")
            }
            is BigQueryType.ArrayType,
            is BigQueryType.RangeType,
            is BigQueryType.StructType -> emptyList()
        }
    }

    override fun mapAggregateFunctions(
        columnType: BigQueryType,
        representation: TypeRepresentation?
    ): Map<String, AggregateFunctionDefinition> {
        return getSupportedAggregateFunctions(columnType).associateWith { _ ->
            AggregateFunctionDefinition(
                resultType = Type.Named(name = columnType.typeName)
            )
        }
    }

    private fun getSupportedOperators(columnType: BigQueryType): List<String> {
        val baseOperators = listOf("_eq", "_neq", "_in")
        val comparisonOperators = listOf("_gt", "_lt", "_gte", "_lte")
        val textOperators = listOf(
            "_like", "_ilike", "_nlike", "_nilike",
            "_regex", "_iregex", "_nregex", "_niregex"
        )

        return when (columnType) {
            is BigQueryType.ScalarType -> when (columnType.scalarType) {
                BigQueryScalarType.STRING -> baseOperators + comparisonOperators + textOperators
                
                BigQueryScalarType.INT64,
                BigQueryScalarType.FLOAT64,
                BigQueryScalarType.NUMERIC,
                BigQueryScalarType.BIGNUMERIC -> baseOperators + comparisonOperators
                
                BigQueryScalarType.DATE,
                BigQueryScalarType.TIME,
                BigQueryScalarType.DATETIME,
                BigQueryScalarType.TIMESTAMP -> baseOperators + comparisonOperators
                
                BigQueryScalarType.BOOLEAN,
                BigQueryScalarType.BYTES,
                BigQueryScalarType.GEOGRAPHY,
                BigQueryScalarType.JSON,
                BigQueryScalarType.ANY -> baseOperators
            }
            
            is BigQueryType.ArrayType,
            is BigQueryType.RangeType,
            is BigQueryType.StructType -> baseOperators
        }
    }

    override fun mapComparisonOperators(
        columnType: BigQueryType,
        representation: TypeRepresentation?
    ): Map<String, ComparisonOperatorDefinition> {
        val operators = getSupportedOperators(columnType)

        return operators.associateWith { oper ->
            when (oper) {
                "_eq" -> ComparisonOperatorDefinition(
                    type = ComparisonOperatorDefinitionType.Equal,
                    argumentType = null
                )
                "_in" -> ComparisonOperatorDefinition(
                    type = ComparisonOperatorDefinitionType.In,
                    argumentType = null
                )
                else -> ComparisonOperatorDefinition(
                    type = ComparisonOperatorDefinitionType.Custom,
                    argumentType = Type.Named(name = columnType.typeName)
                )
            }
        }
    }

    override fun handleRegexComparison(
        field: JooqField<*>,
        compareWith: JooqField<*>,
        isCaseInsensitive: Boolean
    ): Condition {
        return if (isCaseInsensitive) {
            condition("REGEXP_CONTAINS(LOWER({0}), LOWER({1}))", field, compareWith)
        } else {
            condition("REGEXP_CONTAINS({0}, {1})", field, compareWith)
        }
    }

    override fun handleLikeComparison(
        field: JooqField<*>,
        compareWith: JooqField<*>,
        isCaseInsensitive: Boolean
    ): Condition {
        return if (isCaseInsensitive) {
            condition("LOWER({0}) LIKE LOWER({1})", field, compareWith)
        } else {
            condition("{0} LIKE {1}", field, compareWith)
        }
    }
}
