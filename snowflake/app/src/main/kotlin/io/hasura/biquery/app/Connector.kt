package io.hasura.bigquery.app

import io.hasura.app.base.*
import io.hasura.app.default.*
import hasura.ndc.connector.*
import kotlinx.serialization.builtins.serializer

object BigQueryConnector : ConnectorBuilder<DefaultConfiguration<BigQueryType>, DefaultState<BigQueryType>> {
    override fun createConnector(): Connector<DefaultConfiguration<BigQueryType>, DefaultState<BigQueryType>> {
        return DefaultConnector(
            source = DatabaseSource.BIGQUERY,
            connection = { config -> BigQueryConnection(config) },
            schemaGenerator = BigQuerySchemaGenerator(),
            configSerializer = DefaultConfiguration.serializer(BigQueryType.serializer())
        )
    }
}
