package hasura.source.bigquery

import hasura.base.*
import hasura.default.*
import hasura.ndc.connector.*

class BigQueryConnector : ConnectorBuilder<DefaultConfiguration<BigQueryType>, DefaultState<BigQueryType>> {
    override fun createConnector(): Connector<DefaultConfiguration<BigQueryType>, DefaultState<BigQueryType>> {
        return DefaultConnector(
            source = DatabaseSource.BIGQUERY,
            connection = { config -> BigQueryConnection(config) },
            schemaGenerator = BigQuerySchemaGenerator(),
            configSerializer = DefaultConfiguration.serializer(BigQueryType.serializer())
        )
    }
}
