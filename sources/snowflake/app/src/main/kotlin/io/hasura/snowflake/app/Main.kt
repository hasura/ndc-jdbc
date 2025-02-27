package io.hasura.snowflake.app

import io.hasura.ndc.connector.startServer

fun main(args: Array<String>) {
    val connector = SnowflakeConnector.createConnector()
    startServer(connector, args)
}
