package io.hasura.postgres.cli

import io.hasura.common.*
import io.hasura.postgres.PGColumnType
import org.jooq.impl.DSL

object PostgresConfigGenerator : IConfigGenerator<PostgresConfig, PGColumnType> {
    override fun generateConfig(config: PostgresConfig): DefaultConfiguration<PGColumnType> {
        val ctx = DSL.using(config.connectionUri.resolve())

        val tables = ctx.meta()
            .filterSchemas { it.name == "public" }
            .tables
            .map {
                TableInfo(
                    name = it.name,
                    description = it.comment,
                    category = Category.TABLE,
                    columns = it.fields().map { field ->
                        Column(
                            name = field.name,
                            description = field.comment,
                            type = PGColumnType(
                                typeName = field.dataType.typeName,
                            ),
                            nullable = field.dataType.nullable(),
                            autoIncrement = field.dataType.identity(),
                            isPrimaryKey = it.primaryKey?.fields?.any { key -> key.name == field.name } == true,
                        )
                    },
                    primaryKeys = it.primaryKey?.fields?.map { it.name } ?: emptyList(),
                    foreignKeys = it.references.associate { key ->
                        key.name to ForeignKeyInfo(
                            foreignCollection = key.inverseKey.table.name,
                            columnMapping = key.fields.mapIndexed { index, field ->
                                field.name to key.keyFields[index].name
                            }.toMap(),
                        )
                    }
                )
            }

        return DefaultConfiguration(
            connectionUri = config.connectionUri,
            tables = tables,
        )
    }
}