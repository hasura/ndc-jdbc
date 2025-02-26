package io.hasura.postgres.app

import io.hasura.app.default.DefaultSchemaGenerator
import io.hasura.ndc.ir.*
import io.hasura.postgres.PGColumnType
import org.jooq.DataType
import org.jooq.impl.SQLDataType
import org.jooq.Field as JooqField

object PostgresSchemaGenerator : DefaultSchemaGenerator<PGColumnType>() {
    override fun mapToTypeRepresentation(
        columnType: PGColumnType
    ): ScalarType {
        val t = columnType.typeName
        val rep = when {
            t == "bigint" -> RepresentationType.Biginteger
            t == "bigserial" -> RepresentationType.Biginteger
            t == "boolean" -> RepresentationType.TypeBoolean
            t == "date" -> RepresentationType.Date
            t == "double precision" -> RepresentationType.Float64
            t == "integer" -> RepresentationType.Int32
            t == "json" -> RepresentationType.JSON
            t == "jsonb" -> RepresentationType.JSON
            t.startsWith("numeric") -> RepresentationType.Number
            t == "real" -> RepresentationType.Float32
            t == "smallint" -> RepresentationType.Int16
            t == "serial" -> RepresentationType.Int32
            t == "time without time zone" -> RepresentationType.TypeString
            t == "time with time zone" -> RepresentationType.TypeString
            t == "timestamp without time zone" -> RepresentationType.Timestamp
            t == "timestamp with time zone" -> RepresentationType.Timestamptz
            t == "uuid" -> RepresentationType.UUID
            else -> RepresentationType.TypeString
        }
        return createScalarType(rep, columnType)
    }

    override fun mapColumnDataTypeToSQLDataType(
        columnType: PGColumnType,
    ): DataType<out Any> {
        val t = columnType.typeName
        return when {
            t == "bigint" -> SQLDataType.BIGINT
            t == "bigserial" -> SQLDataType.BIGINT
            t == "boolean" -> SQLDataType.BOOLEAN
            t == "date" -> SQLDataType.DATE
            t == "double precision" -> SQLDataType.DOUBLE
            t == "integer" -> SQLDataType.INTEGER
            t == "json" -> SQLDataType.JSON
            t == "jsonb" -> SQLDataType.JSONB
            t.startsWith("numeric") -> SQLDataType.NUMERIC
            t == "real" -> SQLDataType.REAL
            t == "smallint" -> SQLDataType.SMALLINT
            t == "serial" -> SQLDataType.INTEGER
            t == "time without time zone" -> SQLDataType.TIME
            t == "time with time zone" -> SQLDataType.TIMEWITHTIMEZONE
            t == "timestamp without time zone" -> SQLDataType.TIMESTAMP
            t == "timestamp with time zone" -> SQLDataType.TIMESTAMPWITHTIMEZONE
            t == "uuid" -> SQLDataType.UUID
            else -> SQLDataType.VARCHAR
        }
    }

    override fun castToSQLDataType(
        field: JooqField<*>,
        columnType: PGColumnType?
    ): JooqField<*> {
        return field.cast(mapColumnDataTypeToSQLDataType(columnType!!))
    }

    override fun mapAggregateFunctions(
        columnType: PGColumnType,
        representation: TypeRepresentation?
    ): Map<String, AggregateFunctionDefinition> {
        return emptyMap()
    }

    private fun getSupportedOperators(columnType: PGColumnType): List<String> {
        val baseOperators = listOf("_eq", "_neq", "_in")
        val comparisonOperators = listOf("_gt", "_lt", "_gte", "_lte")
        val textOperators = listOf(
            "_like", "_ilike", "_nlike", "_nilike",
            "_regex", "_iregex", "_nregex", "_niregex"
        )

        return when (columnType.typeName) {
            "bigint", "bigserial", "integer", "smallint" -> baseOperators + comparisonOperators
            "boolean" -> baseOperators
            "date", "time without time zone", "time with time zone", "timestamp without time zone", "timestamp with time zone" -> baseOperators + comparisonOperators
            "double precision", "numeric", "real" -> baseOperators + comparisonOperators
            "json", "jsonb" -> baseOperators
            "uuid" -> baseOperators
            else -> baseOperators + textOperators
        }
    }

    override fun mapComparisonOperators(
        columnType: PGColumnType,
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
