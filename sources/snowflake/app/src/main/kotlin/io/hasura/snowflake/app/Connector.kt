package io.hasura.snowflake.app

import io.hasura.app.base.*
import io.hasura.app.default.*
import io.hasura.ndc.connector.*
import io.hasura.common.*
import io.hasura.snowflake.common.SnowflakeDataType

object SnowflakeConnector : ConnectorBuilder<DefaultConfiguration<SnowflakeDataType>, DefaultState<SnowflakeDataType>> {
    override fun createConnector(): Connector<DefaultConfiguration<SnowflakeDataType>, DefaultState<SnowflakeDataType>> {
        return DefaultConnector(
            source = DatabaseSource.SNOWFLAKE,
            connection = { config -> SnowflakeConnection(config) },
            schemaGenerator = SnowflakeSchemaGenerator(),
            configSerializer = DefaultConfiguration.serializer(SnowflakeDataType.serializer())
        )
    }
}
