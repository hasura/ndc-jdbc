package io.hasura.biquery.app

import hasura.ndc.connector.startServer
import kotlinx.coroutines.runBlocking

fun main(args: Array<String>) {
    val connector = BigQueryConnector.createConnector()

    // Let SDK handle its options (including env vars)
    runBlocking<Unit> {
        startServer(connector, args)
    }
}