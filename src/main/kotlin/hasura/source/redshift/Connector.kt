package hasura.source.redshift

import hasura.base.*
import hasura.default.*
import hasura.ndc.connector.*

class RedshiftConnector : ConnectorBuilder<DefaultConfiguration<RedshiftDataType>, DefaultState<RedshiftDataType>> {
    override fun createConnector(): Connector<DefaultConfiguration<RedshiftDataType>, DefaultState<RedshiftDataType>> {
        return DefaultConnector(
            source = DatabaseSource.REDSHIFT,
            connection = { config -> RedshiftConnection(config) },
            schemaGenerator = RedshiftSchemaGenerator(),
            configSerializer = DefaultConfiguration.serializer(RedshiftDataType.serializer())
        )
    }
}
