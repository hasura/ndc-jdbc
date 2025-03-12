package io.hasura.snowflake.app

import io.hasura.app.base.*
import io.hasura.app.default.*
import io.hasura.common.configuration.*
import io.hasura.ndc.connector.*
import io.hasura.snowflake.common.SnowflakeDataType
import io.hasura.common.configuration.version1.ConfigurationV1

object SnowflakeConnector : ConnectorBuilder<DefaultConfiguration<SnowflakeDataType>, DefaultState<SnowflakeDataType>> {
    override fun createConnector() = SQLConnector(
        source = DatabaseSource.SNOWFLAKE,
        connection = { config -> SnowflakeConnection(config) },
        schemaGenerator = SnowflakeSchemaGenerator(),
        sourceColumnSerializer = SnowflakeDataType.serializer()

    )
}
