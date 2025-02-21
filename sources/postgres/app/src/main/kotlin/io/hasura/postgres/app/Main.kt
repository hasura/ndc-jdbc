package io.hasura.postgres.app


fun main(args: Array<String>) {
    val connector = PostgresConnector.createConnector()
    startServer(connector, args)
}