package io.hasura.bigquery.app

import io.hasura.ndc.connector.startServer

fun main(args: Array<String>) {
    val connector = BigQueryConnector.createConnector()
    startServer(connector, args)
}