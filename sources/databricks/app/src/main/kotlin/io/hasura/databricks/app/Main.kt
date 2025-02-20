package io.hasura.databricks.app

import io.hasura.ndc.connector.startServer

fun main(args: Array<String>) {
    val connector = DatabricksConnector.createConnector()
    startServer(connector, args)
}