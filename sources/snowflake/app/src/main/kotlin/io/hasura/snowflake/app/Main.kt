package io.hasura.snowflake.app


fun main(args: Array<String>) {
    val connector = SnowflakeConnector.createConnector()
    startServer(connector, args)
}
