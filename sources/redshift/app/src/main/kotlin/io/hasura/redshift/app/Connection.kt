package io.hasura.redshift.app

import io.hasura.app.base.*
import com.zaxxer.hikari.HikariConfig

class RedshiftConnection(config: Configuration) : BaseHikariConnection(config) {
    override fun getDriverConfig(): DatabaseDriver {
        return DatabaseDriver(
            className = "com.amazon.redshift.jdbc.Driver"
        )
    }
}
