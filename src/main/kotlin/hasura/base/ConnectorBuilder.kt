package hasura.base

import hasura.ndc.connector.Connector
import kotlin.reflect.KClass

interface ConnectorBuilder<Configuration : Any, State : Any> {
    fun createConnector(): Connector<Configuration, State>
}
