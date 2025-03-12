package io.hasura.redshift.app

import io.hasura.app.base.*
import io.hasura.app.default.*
import io.hasura.ndc.ir.*
import io.hasura.ndc.ir.Type
import io.hasura.common.*
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

    private fun getSupportedAggregateFunctions(columnType: RedshiftDataType): List<String> {
        val numericFunctions = listOf(
            "avg", "sum", "min", "max",
            "stddev_pop", "stddev_samp",
            "var_pop", "var_samp"
        )

        return when (columnType) {
            is RedshiftDataType.DECIMAL,
            RedshiftDataType.BIGINT,
            RedshiftDataType.DOUBLE_PRECISION,
            RedshiftDataType.INTEGER,
            RedshiftDataType.REAL,
            RedshiftDataType.SMALLINT -> numericFunctions

            RedshiftDataType.BOOLEAN,
            RedshiftDataType.DATE,
            RedshiftDataType.CHAR,
            RedshiftDataType.VARCHAR,
            RedshiftDataType.TEXT,
            RedshiftDataType.TIME,
            RedshiftDataType.TIMETZ,
            RedshiftDataType.TIMESTAMP,
            RedshiftDataType.TIMESTAMPTZ -> listOf("min", "max")

            else -> emptyList()
        }
    }

    override fun mapAggregateFunctions(
        columnType: RedshiftDataType,
        representation: TypeRepresentation?
    ): Map<String, AggregateFunctionDefinition> {
        return getSupportedAggregateFunctions(columnType).associateWith { _ ->
            AggregateFunctionDefinition(
                resultType = Type.Named(name = columnType.typeName)
            )
        }
    }

    private fun getSupportedOperators(columnType: RedshiftDataType): List<String> {
        val baseOperators = listOf("_eq", "_neq", "_in")
        val comparisonOperators = listOf("_gt", "_lt", "_gte", "_lte")
        val textOperators = listOf(
            "_like", "_ilike", "_nlike", "_nilike",
            "_regex", "_iregex", "_nregex", "_niregex"
        )

        return when (columnType) {
            RedshiftDataType.TEXT,
            RedshiftDataType.CHAR,
            RedshiftDataType.VARCHAR -> baseOperators + comparisonOperators + textOperators

            is RedshiftDataType.DECIMAL,
            RedshiftDataType.BIGINT,
            RedshiftDataType.DOUBLE_PRECISION,
            RedshiftDataType.INTEGER,
            RedshiftDataType.REAL,
            RedshiftDataType.SMALLINT -> baseOperators + comparisonOperators

            RedshiftDataType.DATE,
            RedshiftDataType.TIME,
            RedshiftDataType.TIMETZ,
            RedshiftDataType.TIMESTAMP,
            RedshiftDataType.TIMESTAMPTZ -> baseOperators + comparisonOperators

            RedshiftDataType.BOOLEAN,
            RedshiftDataType.HLLSKETCH,
            RedshiftDataType.INTERVAL_YEAR_TO_MONTH,
            RedshiftDataType.INTERVAL_DAY_TO_SECOND,
            RedshiftDataType.SUPER,
            RedshiftDataType.VARBYTE,
            RedshiftDataType.GEOGRAPHY,
            RedshiftDataType.GEOMETRY -> baseOperators
        }
    }

    override fun mapComparisonOperators(
        columnType: RedshiftDataType,
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
            condition("LOWER({0}) SIMILAR TO LOWER({1})", field, compareWith)
        } else {
            condition("{0} SIMILAR TO {1}", field, compareWith)
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
