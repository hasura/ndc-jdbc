package io.hasura.redshift.app

import io.hasura.app.base.*
import io.hasura.app.default.*
import io.hasura.ndc.connector.*
import io.hasura.common.*
import io.hasura.redshift.common.RedshiftDataType

object RedshiftConnector : ConnectorBuilder<DefaultConfiguration<RedshiftDataType>, DefaultState<RedshiftDataType>> {
    override fun createConnector() = SQLConnector(
        source = DatabaseSource.REDSHIFT,
        connection = { config -> RedshiftConnection(config) },
        schemaGenerator = RedshiftSchemaGenerator(),
        configSerializer = DefaultConfiguration.serializer(RedshiftDataType.serializer())
    )
}
