package io.hasura.databricks.app

import io.hasura.app.base.*
import com.zaxxer.hikari.HikariConfig
import io.hasura.common.configuration.*

class DatabricksConnection(config: Configuration) : BaseHikariConnection(config) {
    override fun getDriverConfig(): DatabaseDriver {
        return DatabaseDriver(
            className = "com.databricks.client.jdbc.Driver"
        )
    }
}
