package io.hasura.databricks.app

import io.hasura.app.base.*
import io.hasura.app.default.*
import io.hasura.ndc.ir.*
import io.hasura.ndc.ir.Type
import io.hasura.common.configuration.*
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
                        precision!! <= 2 -> RepresentationType.Int8
                        precision <= 4 -> RepresentationType.Int16
                        precision <= 9 -> RepresentationType.Int32
                        precision <= 18 -> RepresentationType.Int64
                        else -> RepresentationType.Biginteger
                    }
                    scale!! > 0 -> RepresentationType.Bigdecimal
                    else -> RepresentationType.Bigdecimal
                }
            }
            is DatabricksDataType.ARRAY -> RepresentationType.JSON
            is DatabricksDataType.MAP -> RepresentationType.JSON
            is DatabricksDataType.STRUCT -> RepresentationType.JSON
            DatabricksDataType.BOOLEAN -> RepresentationType.TypeBoolean
            DatabricksDataType.TINYINT -> RepresentationType.Int8
            DatabricksDataType.SMALLINT -> RepresentationType.Int16
            DatabricksDataType.INT -> RepresentationType.Int32
            DatabricksDataType.BIGINT -> RepresentationType.Int64
            DatabricksDataType.FLOAT -> RepresentationType.Float32
            DatabricksDataType.DOUBLE -> RepresentationType.Float64
            DatabricksDataType.STRING -> RepresentationType.TypeString
            DatabricksDataType.CHAR -> RepresentationType.TypeString
            DatabricksDataType.VARCHAR -> RepresentationType.TypeString
            DatabricksDataType.BINARY -> RepresentationType.Bytes
            DatabricksDataType.DATE -> RepresentationType.Date
            DatabricksDataType.TIMESTAMP -> RepresentationType.Timestamp
            DatabricksDataType.TIMESTAMP_NTZ -> RepresentationType.Timestamptz
            DatabricksDataType.VARIANT -> RepresentationType.JSON
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
            DatabricksDataType.TINYINT -> SQLDataType.TINYINT
            DatabricksDataType.SMALLINT -> SQLDataType.SMALLINT
            DatabricksDataType.INT -> SQLDataType.INTEGER
            DatabricksDataType.BIGINT -> SQLDataType.BIGINT
            DatabricksDataType.FLOAT -> SQLDataType.REAL
            DatabricksDataType.DOUBLE -> SQLDataType.DOUBLE
            DatabricksDataType.STRING -> SQLDataType.CLOB
            DatabricksDataType.CHAR -> SQLDataType.CHAR
            DatabricksDataType.VARCHAR -> SQLDataType.VARCHAR
            DatabricksDataType.BINARY -> SQLDataType.BINARY
            DatabricksDataType.DATE -> SQLDataType.DATE
            DatabricksDataType.TIMESTAMP -> SQLDataType.TIMESTAMP
            DatabricksDataType.TIMESTAMP_NTZ -> SQLDataType.TIMEWITHTIMEZONE
            DatabricksDataType.VARIANT -> SQLDataType.JSON
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
                    scale == 0 && precision!! > 18 ->
                        cast(field, SQLDataType.VARCHAR(255))
                    scale!! > 0 && precision!! > 15 ->
                        cast(field, SQLDataType.VARCHAR(255))
                    else -> field
                }
            }
            DatabricksDataType.BIGINT -> cast(field, SQLDataType.VARCHAR(255))
            else -> field
        }
    }

    private fun getSupportedAggregateFunctions(columnType: DatabricksDataType): List<String> {
        val numericFunctions = listOf(
            "avg", "sum", "min", "max",
            "stddev_pop", "stddev_samp",
            "var_pop", "var_samp"
        )

        return when (columnType) {
            is DatabricksDataType.DECIMAL,
            DatabricksDataType.FLOAT,
            DatabricksDataType.DOUBLE,
            DatabricksDataType.TINYINT,
            DatabricksDataType.SMALLINT,
            DatabricksDataType.INT,
            DatabricksDataType.BIGINT -> numericFunctions

            DatabricksDataType.BOOLEAN,
            DatabricksDataType.DATE,
            DatabricksDataType.STRING,
            DatabricksDataType.CHAR,
            DatabricksDataType.VARCHAR,
            DatabricksDataType.TIMESTAMP,
            DatabricksDataType.TIMESTAMP_NTZ -> listOf("min", "max")

            else -> emptyList()
        }
    }

    override fun mapAggregateFunctions(
        columnType: DatabricksDataType,
        representation: TypeRepresentation?
    ): Map<String, AggregateFunctionDefinition> {
        return getSupportedAggregateFunctions(columnType).associateWith { _ ->
            AggregateFunctionDefinition(
                resultType = Type.Named(name = columnType.typeName)
            )
        }
    }

    private fun getSupportedOperators(columnType: DatabricksDataType): List<String> {
        val baseOperators = listOf("_eq", "_neq", "_in")
        val comparisonOperators = listOf("_gt", "_lt", "_gte", "_lte")
        val textOperators = listOf(
            "_like", "_ilike", "_nlike", "_nilike",
            "_regex", "_iregex", "_nregex", "_niregex"
        )

        return when (columnType) {
            DatabricksDataType.STRING,
            DatabricksDataType.CHAR,
            DatabricksDataType.VARCHAR -> baseOperators + comparisonOperators + textOperators

            is DatabricksDataType.DECIMAL,
            DatabricksDataType.BIGINT,
            DatabricksDataType.INT,
            DatabricksDataType.SMALLINT,
            DatabricksDataType.TINYINT,
            DatabricksDataType.FLOAT,
            DatabricksDataType.DOUBLE -> baseOperators + comparisonOperators

            DatabricksDataType.DATE,
            DatabricksDataType.TIMESTAMP,
            DatabricksDataType.TIMESTAMP_NTZ -> baseOperators + comparisonOperators

            DatabricksDataType.BOOLEAN,
            is DatabricksDataType.ARRAY,
            is DatabricksDataType.MAP,
            is DatabricksDataType.STRUCT,
            DatabricksDataType.VARIANT,
            DatabricksDataType.BINARY -> baseOperators
        }
    }

    override fun mapComparisonOperators(
        columnType: DatabricksDataType,
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
            condition("REGEXP_LIKE(LOWER({0}), LOWER({1}))", field, compareWith)
        } else {
            condition("REGEXP_LIKE({0}, {1})", field, compareWith)
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
