package io.hasura.phoenix.app

import io.hasura.app.base.BaseHikariConnection
import io.hasura.common.Configuration

class PhoenixConnection(config: Configuration) : BaseHikariConnection(config) {
    override fun getDriverConfig(): DatabaseDriver {
        return DatabaseDriver(
            className = "org.apache.phoenix.jdbc.PhoenixDriver",
        )
    }
}
