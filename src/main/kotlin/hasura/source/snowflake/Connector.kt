package hasura.source.snowflake

import hasura.base.*
import hasura.default.*
import hasura.ndc.connector.*

class SnowflakeConnector : ConnectorBuilder<DefaultConfiguration<SnowflakeDataType>, DefaultState<SnowflakeDataType>> {
    override fun createConnector(): Connector<DefaultConfiguration<SnowflakeDataType>, DefaultState<SnowflakeDataType>> {
        return DefaultConnector(
            source = DatabaseSource.SNOWFLAKE,
            connection = { config -> SnowflakeConnection(config) },
            schemaGenerator = SnowflakeSchemaGenerator(),
            configSerializer = DefaultConfiguration.serializer(SnowflakeDataType.serializer())
        )
    }
}
