package io.hasura.databricks.app

import io.hasura.app.base.*
import io.hasura.app.default.*
import io.hasura.common.configuration.*
import io.hasura.ndc.connector.*
import io.hasura.databricks.common.DatabricksDataType

object DatabricksConnector : ConnectorBuilder<DefaultConfiguration<DatabricksDataType>, DefaultState<DatabricksDataType>> {
    override fun createConnector() = SQLConnector(
        source = DatabaseSource.DATABRICKS,
        connection = { config -> DatabricksConnection(config) },
        schemaGenerator = DatabricksSchemaGenerator(),
        configSerializer = DefaultConfiguration.serializer(DatabricksDataType.serializer())
    )
}
