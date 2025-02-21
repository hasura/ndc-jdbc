package io.hasura.postgres.app

import io.hasura.app.base.*
import io.hasura.common.*

class PostgresConnection(config: Configuration) : BaseHikariConnection(config) {
    override fun getDriverConfig(): DatabaseDriver {
        return DatabaseDriver(
            className = "org.postgresql.Driver",
        )
    }
}
