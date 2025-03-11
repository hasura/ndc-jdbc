package io.hasura.phoenix.app

import io.hasura.ndc.connector.startServer

fun main(args: Array<String>) {
    val connector = PhoenixConnector.createConnector()
    startServer(connector, args)
}