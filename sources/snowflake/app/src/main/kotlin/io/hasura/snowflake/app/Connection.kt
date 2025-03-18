package io.hasura.snowflake.app

import io.hasura.app.base.*
import io.hasura.common.*
import io.hasura.common.configuration.*
import io.hasura.snowflake.common.SnowflakeDataType
import io.hasura.common.configuration.Configuration


class SnowflakeConnection(config: Configuration<SnowflakeDataType>) : BaseHikariConnection<SnowflakeDataType>(config) {
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
