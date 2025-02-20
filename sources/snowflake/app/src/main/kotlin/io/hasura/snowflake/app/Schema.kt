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

        return createScalarType(representationType, columnType)
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

    override fun generateScalarName(columnType: SnowflakeDataType): String {
        return when (columnType) {
            is SnowflakeDataType.NUMBER -> {
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
        columnType: SnowflakeDataType?
    ): JooqField<*> {
        return when (columnType) {
            is SnowflakeDataType.NUMBER -> {
                val (precision, scale) = columnType
                when {
                    scale == 0 && precision > 18 ->
                        cast(field, SQLDataType.VARCHAR)
                    scale > 0 ->
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

    private fun getSupportedAggregateFunctions(columnType: SnowflakeDataType): List<String> {
        val numericFunctions = listOf(
            "avg", "sum", "min", "max",
            "stddev_pop", "stddev_samp",
            "var_pop", "var_samp"
        )

        return when (columnType) {
            is SnowflakeDataType.NUMBER,
            SnowflakeDataType.FLOAT -> numericFunctions

            SnowflakeDataType.BOOLEAN,
            SnowflakeDataType.DATE,
            SnowflakeDataType.TEXT,
            SnowflakeDataType.TIME,
            SnowflakeDataType.TIMESTAMP_NTZ,
            SnowflakeDataType.TIMESTAMP_LTZ,
            SnowflakeDataType.TIMESTAMP_TZ -> listOf("min", "max")

            else -> emptyList()
        }
    }

    override fun mapAggregateFunctions(
        columnType: SnowflakeDataType,
        representation: TypeRepresentation?
    ): Map<String, AggregateFunctionDefinition> {
        return getSupportedAggregateFunctions(columnType).associateWith { func ->
            AggregateFunctionDefinition(
                resultType = Type.Named(name = columnType.typeName)
            )
        }
    }

    private fun getSupportedOperators(columnType: SnowflakeDataType): List<String> {
        val baseOperators = listOf("_eq", "_neq", "_in")
        val comparisonOperators = listOf("_gt", "_lt", "_gte", "_lte")
        val textOperators = listOf(
            "_like", "_ilike", "_nlike", "_nilike",
            "_regex", "_iregex", "_nregex", "_niregex"
        )

        return when (columnType) {
            SnowflakeDataType.TEXT -> baseOperators + comparisonOperators + textOperators

            is SnowflakeDataType.NUMBER,
            SnowflakeDataType.FLOAT -> baseOperators + comparisonOperators

            SnowflakeDataType.DATE,
            SnowflakeDataType.TIME,
            SnowflakeDataType.TIMESTAMP_NTZ,
            SnowflakeDataType.TIMESTAMP_LTZ,
            SnowflakeDataType.TIMESTAMP_TZ -> baseOperators + comparisonOperators

            SnowflakeDataType.BOOLEAN,
            SnowflakeDataType.ARRAY,
            SnowflakeDataType.OBJECT,
            SnowflakeDataType.VARIANT,
            SnowflakeDataType.VECTOR,
            SnowflakeDataType.BINARY,
            SnowflakeDataType.GEOGRAPHY,
            SnowflakeDataType.GEOMETRY -> baseOperators
        }
    }

    override fun mapComparisonOperators(
        columnType: SnowflakeDataType,
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
}
