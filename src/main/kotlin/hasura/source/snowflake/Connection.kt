package hasura.source.snowflake

import hasura.base.*
import com.zaxxer.hikari.HikariConfig

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
