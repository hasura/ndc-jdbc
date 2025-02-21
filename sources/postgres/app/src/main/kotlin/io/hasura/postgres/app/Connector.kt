package io.hasura.postgres.app

import io.hasura.app.base.*
import io.hasura.app.default.*
import io.hasura.common.*
import io.hasura.ndc.connector.*
import kotlinx.serialization.Serializable
import kotlinx.serialization.builtins.serializer

@Serializable
data class PGColumnType(
    override val typeName: String
) : ColumnType

object PostgresConnector : ConnectorBuilder<DefaultConfiguration<PGColumnType>, DefaultState<PGColumnType>> {
    override fun createConnector() = ExperimentalSQLConnector<PGColumnType>(
        source = DatabaseSource.POSTGRES,
        connection = { config -> PostgresConnection(config) },
        schemaGenerator = PostgresSchemaGenerator,
        configSerializer = DefaultConfiguration.serializer(PGColumnType.serializer())
    )
}
