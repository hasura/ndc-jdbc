package io.hasura.phoenix.app

import io.hasura.app.default.DefaultSchemaGenerator
import io.hasura.ndc.ir.*
import io.hasura.phoenix.common.PhoenixDataType
import org.jooq.DataType
import org.jooq.impl.SQLDataType
import org.jooq.Field as JooqField

object PhoenixSchemaGenerator : DefaultSchemaGenerator<PhoenixDataType>() {
    override fun mapToTypeRepresentation(
        columnType: PhoenixDataType,
    ): ScalarType {
        val representationType = RepresentationType.TypeString
        return createScalarType(representationType, columnType)
    }

    override fun mapColumnDataTypeToSQLDataType(
        columnType: PhoenixDataType,
    ): DataType<out Any> {
        return SQLDataType.VARCHAR
    }

    override fun castToSQLDataType(
        field: JooqField<*>,
        columnType: PhoenixDataType?
    ): JooqField<*> {
        return field.cast(SQLDataType.VARCHAR)
    }

    private fun getSupportedAggregateFunctions(columnType: PhoenixDataType): List<String> {
        val numericFunctions = listOf(
            "avg", "sum", "min", "max",
            "stddev_pop", "stddev_samp",
            "var_pop", "var_samp"
        )

        return numericFunctions
    }

    override fun mapAggregateFunctions(
        columnType: PhoenixDataType,
        representation: TypeRepresentation?
    ): Map<String, AggregateFunctionDefinition> {
        return getSupportedAggregateFunctions(columnType).associateWith { _ ->
            AggregateFunctionDefinition(
                resultType = Type.Named(name = columnType.typeName)
            )
        }
    }

    private fun getSupportedOperators(columnType: PhoenixDataType): List<String> {
        val baseOperators = listOf("_eq", "_neq", "_in")
        listOf("_gt", "_lt", "_gte", "_lte")
        listOf(
            "_like", "_ilike", "_nlike", "_nilike",
            "_regex", "_iregex", "_nregex", "_niregex"
        )

        return baseOperators
    }

    override fun mapComparisonOperators(
        columnType: PhoenixDataType,
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
