package io.hasura.databricks.app

import io.hasura.app.base.*
import com.zaxxer.hikari.HikariConfig
import io.hasura.common.configuration.*
import io.hasura.databricks.common.DatabricksDataType

class DatabricksConnection(config: Configuration<DatabricksDataType>) : BaseHikariConnection<DatabricksDataType>(config) {
    override fun getDriverConfig(): DatabaseDriver {
        return DatabaseDriver(
            className = "com.databricks.client.jdbc.Driver"
        )
    }
}
