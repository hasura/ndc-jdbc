package io.hasura.app.default

import io.hasura.app.base.*
import io.hasura.ndc.ir.*
import io.hasura.common.configuration.*
import org.jooq.DataType
import org.jooq.Field as JooqField
import org.jooq.impl.SQLDataType

abstract class DefaultSchemaGeneratorClass<T : ColumnType> : ISchemaGenerator<T, DefaultConfiguration<T>> {
    override fun getSchema(configuration: DefaultConfiguration<T>): SchemaResponse {
        return SchemaResponse(
            collections = getCollections(configuration),
            functions = emptyList(),
            objectTypes = getObjectTypes(configuration),
            procedures = emptyList(),
            scalarTypes = getScalars(configuration)
        )
    }

    abstract fun getScalars(configuration: DefaultConfiguration<T>): Map<String, ScalarType>

    abstract fun generateScalarName(columnType: T): String

    abstract fun mapToTypeRepresentation(
        columnType: T
    ): ScalarType

    abstract fun mapColumnDataTypeToSQLDataType(
        columnType: T,
    ): DataType<out Any>

    abstract fun mapColumnDataTypeToSQLDataTypeWDefault(
        columnType: T?,
    ): DataType<out Any>

    abstract fun mapAggregateFunctions(
        columnType: T,
        representation: TypeRepresentation?
    ): Map<String, AggregateFunctionDefinition>

    abstract fun mapComparisonOperators(
        columnType: T,
        representation: TypeRepresentation?
    ): Map<String, ComparisonOperatorDefinition>

    abstract fun castToSQLDataType(
        field: JooqField<*>,
        columnType: T?
    ): JooqField<*>

    open fun getCollections(configuration: DefaultConfiguration<T>): List<CollectionInfo> {
        return configuration.tables.map { table ->
            CollectionInfo(
                name = table.name,
                description = table.description,
                arguments = emptyMap(),
                foreignKeys = table.foreignKeys.mapValues { (_, fk) ->
                    ForeignKeyConstraint(
                        columnMapping = fk.columnMapping,
                        foreignCollection = fk.foreignCollection
                    )
                },
                type = table.name,
                uniquenessConstraints = table.primaryKeys.associate {
                    it to UniquenessConstraint(uniqueColumns = listOf(it))
                }
            )
        }
    }

    open fun getObjectTypes(configuration: DefaultConfiguration<T>): Map<String, ObjectType> {
        return configuration.tables.associate { table ->
            table.name to ObjectType(
                description = table.description,
                fields = table.columns.associate { column ->
                    column.name to ObjectField(
                        description = column.description,
                        arguments = emptyMap(),
                        type = if (column.nullable) {
                            Type.Nullable(underlyingType = Type.Named(name = column.type.typeName))
                        } else {
                            Type.Named(name = column.type.typeName)
                        }
                    )
                }
            )
        }
    }

    protected fun createScalarType(representationType: RepresentationType?, columnType: T): ScalarType {
        val representation = representationType?.let { TypeRepresentation(it) }
        return ScalarType(
            representation = representation,
            aggregateFunctions = mapAggregateFunctions(columnType, representation),
            comparisonOperators = mapComparisonOperators(columnType, representation)
        )
    }
}

abstract class DefaultSchemaGenerator<T : ColumnType> : DefaultSchemaGeneratorClass<T>() {
    override fun getScalars(configuration: DefaultConfiguration<T>): Map<String, ScalarType> {
        val scalarTypes = configuration.tables.flatMap { table ->
            table.columns.map { column ->
                generateScalarName(column.type) to mapToTypeRepresentation(column.type)
            }
        }.distinct()


        return scalarTypes.associate { (columnType, representation) ->
            columnType to representation
        }
    }

    override fun mapColumnDataTypeToSQLDataTypeWDefault(
        columnType: T?,
    ): DataType<out Any> {
      if (columnType == null) {
        return SQLDataType.CLOB
      }
      return mapColumnDataTypeToSQLDataType(columnType)
    }

    override fun generateScalarName(columnType: T): String {
        return columnType.typeName
    }

    override fun mapAggregateFunctions(
        columnType: T,
        representation: TypeRepresentation?
    ): Map<String, AggregateFunctionDefinition> {
        return emptyMap()
    }

    override fun mapComparisonOperators(
        columnType: T,
        representation: TypeRepresentation?
    ): Map<String, ComparisonOperatorDefinition> {
        return emptyMap()
    }
}
