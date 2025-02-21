package io.hasura.postgres.app

import io.hasura.app.default.DefaultSchemaGenerator
import io.hasura.ndc.ir.*
import org.jooq.DataType
import org.jooq.impl.SQLDataType
import org.jooq.Field as JooqField

object PostgresSchemaGenerator : DefaultSchemaGenerator<PGColumnType>() {
    override fun mapToTypeRepresentation(
        columnType: PGColumnType
    ): ScalarType {
        return createScalarType(RepresentationType.TypeString, columnType)
    }

    override fun mapColumnDataTypeToSQLDataType(
        columnType: PGColumnType,
    ): DataType<out Any> {
        return SQLDataType.VARCHAR
    }

    override fun castToSQLDataType(
        field: JooqField<*>,
        columnType: PGColumnType?
    ): JooqField<*> {
        return field.cast(SQLDataType.VARCHAR)
    }

    override fun mapAggregateFunctions(
        columnType: PGColumnType,
        representation: TypeRepresentation?
    ): Map<String, AggregateFunctionDefinition> {
        return emptyMap()
    }

    override fun mapComparisonOperators(
        columnType: PGColumnType,
        representation: TypeRepresentation?
    ): Map<String, ComparisonOperatorDefinition> {
        return emptyMap()
    }
}
