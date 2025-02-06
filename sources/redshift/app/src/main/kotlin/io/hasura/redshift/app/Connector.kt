package io.hasura.redshift.app

import io.hasura.app.base.*
import io.hasura.app.default.*
import io.hasura.ndc.connector.*

object RedshiftConnector : ConnectorBuilder<DefaultConfiguration<RedshiftDataType>, DefaultState<RedshiftDataType>> {
    override fun createConnector(): Connector<DefaultConfiguration<RedshiftDataType>, DefaultState<RedshiftDataType>> {
        return DefaultConnector(
            source = DatabaseSource.REDSHIFT,
            connection = { config -> RedshiftConnection(config) },
            schemaGenerator = RedshiftSchemaGenerator(),
            configSerializer = DefaultConfiguration.serializer(RedshiftDataType.serializer())
        )
    }
}
