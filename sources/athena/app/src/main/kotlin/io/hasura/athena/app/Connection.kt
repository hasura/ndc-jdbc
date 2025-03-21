package io.hasura.athena.app

import io.hasura.app.base.*
import io.hasura.common.*

class AthenaConnection(config: Configuration) : BaseHikariConnection(config) {
    override fun getDriverConfig(): DatabaseDriver {
        return DatabaseDriver(className = "com.amazon.athena.jdbc.AthenaDriver")
    }
}
