package io.hasura.bigquery.app

import io.hasura.app.base.*
import io.hasura.common.configuration.*

class BigQueryConnection(config: Configuration) : BaseHikariConnection(config) {
    override fun getDriverConfig(): DatabaseDriver {
        return DatabaseDriver(
            className = "com.simba.googlebigquery.jdbc.Driver"
        )
    }
}
