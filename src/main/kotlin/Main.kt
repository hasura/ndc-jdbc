package hasura

import hasura.base.DatabaseSource
import hasura.ndc.connector.ServerOptions
import hasura.ndc.connector.startServer
import hasura.source.bigquery.BigQueryConnector
import hasura.source.snowflake.SnowflakeConnector
import hasura.source.redshift.RedshiftConnector
import kotlinx.cli.*
import kotlinx.coroutines.runBlocking

fun main(args: Array<String>) {
    // Check for NDC_JDBC_SOURCE environment variable
    val databaseSource = try {
        val source = System.getenv("NDC_JDBC_SOURCE") ?: throw IllegalArgumentException("NDC_JDBC_SOURCE environment variable is required")
        DatabaseSource.valueOf(source.uppercase())
    } catch (e: IllegalArgumentException) {
        println("Error: ${e.message}")
        println("Valid values are: ${DatabaseSource.values().joinToString(", ") { it.name.lowercase() }}")
        System.exit(1)
        return
    }

    // Create appropriate connector based on source
    val connector = when(databaseSource) {
        DatabaseSource.SNOWFLAKE -> SnowflakeConnector()
        DatabaseSource.BIGQUERY -> BigQueryConnector()
        DatabaseSource.REDSHIFT -> RedshiftConnector()
    }.createConnector()
    
    // Let SDK handle its options (including env vars)
    runBlocking {
        startServer(connector, args)
    }
}