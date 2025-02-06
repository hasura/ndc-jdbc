package io.hasura.app.base

import io.hasura.ndc.connector.Connector

interface ConnectorBuilder<Configuration : Any, State : Any> {
    fun createConnector(): Connector<Configuration, State>
}
