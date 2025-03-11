package io.hasura.phoenix.app

import io.hasura.app.base.*
import io.hasura.app.default.*
import io.hasura.common.*
import io.hasura.ndc.connector.*
import io.hasura.phoenix.common.PhoenixDataType

object PhoenixConnector :
    ConnectorBuilder<DefaultConfiguration<PhoenixDataType>, DefaultState<PhoenixDataType>> {
    override fun createConnector(): Connector<DefaultConfiguration<PhoenixDataType>, DefaultState<PhoenixDataType>> {
        return DefaultConnector(
            source = DatabaseSource.DATABRICKS,
            connection = { config -> PhoenixConnection(config) },
            schemaGenerator = PhoenixSchemaGenerator,
            configSerializer = DefaultConfiguration.serializer(PhoenixDataType.serializer())
        )
    }
}
