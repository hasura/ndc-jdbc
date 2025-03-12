package io.hasura.bigquery.app

import io.hasura.app.base.*
import io.hasura.common.configuration.*
import io.hasura.bigquery.common.BigQueryType

class BigQueryConnection(config: Configuration<BigQueryType>) : BaseHikariConnection<BigQueryType>(config) {
    override fun getDriverConfig(): DatabaseDriver {
        return DatabaseDriver(
            className = "com.simba.googlebigquery.jdbc.Driver"
        )
    }
}
