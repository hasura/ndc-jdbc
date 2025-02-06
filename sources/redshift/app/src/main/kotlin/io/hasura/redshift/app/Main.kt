package io.hasura.redshift.app

import io.hasura.ndc.connector.startServer

fun main(args: Array<String>) {
    val connector = RedshiftConnector.createConnector()
    startServer(connector, args)
}