package hasura.source.bigquery

import hasura.base.*
import com.zaxxer.hikari.HikariConfig

class BigQueryConnection(config: Configuration) : BaseHikariConnection(config) {
    override fun getDriverConfig(): DatabaseDriver {
        return DatabaseDriver(
            className = "com.simba.googlebigquery.jdbc.Driver"
        )
    }
}
