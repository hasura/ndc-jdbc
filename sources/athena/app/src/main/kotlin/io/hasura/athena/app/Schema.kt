package io.hasura.athena.app

import io.hasura.app.default.*
import io.hasura.athena.common.AthenaDataType
import io.hasura.ndc.ir.*
import io.hasura.ndc.ir.Type
import org.jooq.*
import org.jooq.DataType
import org.jooq.Field as JooqField
import org.jooq.impl.DSL.*
import org.jooq.impl.SQLDataType

class AthenaSchemaGenerator : DefaultSchemaGenerator<AthenaDataType>() {
    override fun mapToTypeRepresentation(
        columnType: AthenaDataType
    ): ScalarType {
        val representationType = when (columnType) {
            is AthenaDataType.DECIMAL -> {
              val (precision, scale) = columnType
              when {
                  scale == 0 ->
                  when {
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

            AthenaDataType.FLOAT -> RepresentationType.Float64
            AthenaDataType.VARCHAR -> RepresentationType.TypeString
            AthenaDataType.BINARY -> RepresentationType.Bytes
            AthenaDataType.DATE -> RepresentationType.Date
            AthenaDataType.TIMESTAMP -> RepresentationType.Timestamp
            AthenaDataType.TIMESTAMP_TZ -> RepresentationType.Timestamptz
            AthenaDataType.JSON -> RepresentationType.JSON
            AthenaDataType.BOOLEAN -> RepresentationType.TypeBoolean
            else -> null
        }

        return createScalarType(representationType, columnType)
    }

    override fun mapColumnDataTypeToSQLDataType(
        columnType: AthenaDataType,
    ): DataType<out Any> {
        return when (columnType) {
            is AthenaDataType.DECIMAL -> SQLDataType.NUMERIC
            AthenaDataType.FLOAT -> SQLDataType.DOUBLE
            AthenaDataType.VARCHAR -> SQLDataType.VARCHAR
            AthenaDataType.BINARY -> SQLDataType.BLOB
            AthenaDataType.DATE -> SQLDataType.DATE
            AthenaDataType.TIMESTAMP -> SQLDataType.TIMESTAMP
            AthenaDataType.TIMESTAMP_TZ -> SQLDataType.TIMESTAMPWITHTIMEZONE
            AthenaDataType.JSON -> SQLDataType.JSON
            AthenaDataType.BOOLEAN -> SQLDataType.BOOLEAN
            else -> SQLDataType.VARCHAR
        }
    }

    override fun generateScalarName(columnType: AthenaDataType): String {
        return when (columnType) {
            is AthenaDataType.DECIMAL -> {
                val (precision, scale) = columnType
                when {
                    scale == 0 ->
                    when {
                        precision <= 2 -> "INT_8"
                        precision <= 4 -> "INT_16"
                        precision <= 9 -> "INT_32"
                        precision <= 18 -> "INT_64"
                        else -> "BIGINTEGER"
                    }
                    scale > 0 -> "BIGDECIMAL"
                    else -> "BIGDECIMAL"
                }
            }
            else -> columnType.typeName

        }
    }

    override fun castToSQLDataType(
        field: JooqField<*>,
        columnType: AthenaDataType?
    ): JooqField<*> {
        return when (columnType) {
            is AthenaDataType.DECIMAL -> {
                val (precision, scale) = columnType
                when {
                    scale == 0 && precision > 18 ->
                        cast(field, SQLDataType.VARCHAR)
                    scale > 0 ->
                        cast(field, SQLDataType.VARCHAR)
                    else -> field
                }
            }

            else -> field
        }
    }

    private fun getSupportedAggregateFunctions(columnType: AthenaDataType): List<String> {
        val numericFunctions = listOf(
            "avg", "sum", "min", "max",
            "stddev_pop", "stddev_samp",
            "var_pop", "var_samp", "any_value", "arbitrary", "count"
        )

        return when (columnType) {
            is AthenaDataType.DECIMAL,
            AthenaDataType.FLOAT -> numericFunctions

            AthenaDataType.BOOLEAN,
            AthenaDataType.DATE,
            AthenaDataType.VARCHAR,
            AthenaDataType.TIMESTAMP,
            AthenaDataType.TIMESTAMP_TZ -> listOf("min", "max")

            else -> emptyList()
        }
    }

    override fun mapAggregateFunctions(
        columnType: AthenaDataType,
        representation: TypeRepresentation?
    ): Map<String, AggregateFunctionDefinition> {
        return getSupportedAggregateFunctions(columnType).associateWith { _ ->
            AggregateFunctionDefinition(
                resultType = Type.Named(name = columnType.typeName)
            )
        }
    }

    private fun getSupportedOperators(columnType: AthenaDataType): List<String> {
        val baseOperators = listOf("_eq", "_neq", "_in")
        val comparisonOperators = listOf("_gt", "_lt", "_gte", "_lte")
        val textOperators = listOf(
            "_like",
            "_regex"
        )

        return when (columnType) {
            AthenaDataType.VARCHAR, AthenaDataType.STRING ->
                baseOperators + comparisonOperators + textOperators

            is AthenaDataType.DECIMAL,
            AthenaDataType.FLOAT, AthenaDataType.BIGINT,
            AthenaDataType.INTEGER, AthenaDataType.SMALLINT,
            AthenaDataType.TINYINT, AthenaDataType.DOUBLE
                -> baseOperators + comparisonOperators

            AthenaDataType.DATE,
            AthenaDataType.TIMESTAMP,
            AthenaDataType.TIMESTAMP_TZ -> baseOperators + comparisonOperators

            AthenaDataType.BOOLEAN,
            AthenaDataType.BINARY, AthenaDataType.CHAR, AthenaDataType.JSON -> baseOperators
        }
    }

    override fun mapComparisonOperators(
        columnType: AthenaDataType,
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
            condition("REGEXP_LIKE({0}, {1}, 'i')", field, compareWith)
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
            field.likeIgnoreCase(compareWith.cast(SQLDataType.VARCHAR))
        } else {
            field.like(compareWith.cast(SQLDataType.VARCHAR))
        }
    }
}
