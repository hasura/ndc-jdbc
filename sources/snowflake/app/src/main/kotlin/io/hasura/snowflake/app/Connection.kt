package io.hasura.snowflake.app

import io.hasura.app.base.*
import io.hasura.common.*

class SnowflakeConnection(config: Configuration) : BaseHikariConnection(config) {
    override fun getDriverConfig(): DatabaseDriver {
        return DatabaseDriver(
            className = "net.snowflake.client.jdbc.SnowflakeDriver"
        ) { config ->
            config.addDataSourceProperty("client_memory_limit", "0")
            config.addDataSourceProperty("JDBC_QUERY_RESULT_FORMAT", "JSON")
            config.addDataSourceProperty("ENABLE_ARROW_RESULTSET", "false")
        }
    }
}
