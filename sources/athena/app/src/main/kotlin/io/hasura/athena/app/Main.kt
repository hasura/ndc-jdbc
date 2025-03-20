package io.hasura.athena.app

import io.hasura.ndc.connector.startServer

fun main(args: Array<String>) {
    val connector = AthenaConnector.createConnector()
    startServer(connector, args)
}
