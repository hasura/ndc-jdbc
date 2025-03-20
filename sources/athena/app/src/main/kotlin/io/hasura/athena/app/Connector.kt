package io.hasura.athena.app

import io.hasura.app.base.*
import io.hasura.app.default.*
import io.hasura.athena.common.AthenaDataType
import io.hasura.common.*
import io.hasura.ndc.connector.*
import kotlinx.serialization.builtins.serializer

object AthenaConnector :
        ConnectorBuilder<DefaultConfiguration<AthenaDataType>, DefaultState<AthenaDataType>> {
    override fun createConnector():
            Connector<DefaultConfiguration<AthenaDataType>, DefaultState<AthenaDataType>> {
        return DefaultConnector(
                source = DatabaseSource.ATHENA,
                connection = { config -> AthenaConnection(config) },
                schemaGenerator = AthenaSchemaGenerator(),
                configSerializer = DefaultConfiguration.serializer(AthenaDataType.serializer())
        )
    }
}
