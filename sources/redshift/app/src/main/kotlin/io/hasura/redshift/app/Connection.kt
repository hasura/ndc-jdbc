package io.hasura.redshift.app

import io.hasura.app.base.*
import com.zaxxer.hikari.HikariConfig
import io.hasura.common.configuration.*
import io.hasura.redshift.common.RedshiftDataType

class RedshiftConnection(config: Configuration<RedshiftDataType>) : BaseHikariConnection<RedshiftDataType>(config) {
    override fun getDriverConfig(): DatabaseDriver {
        return DatabaseDriver(
            className = "com.amazon.redshift.jdbc.Driver"
        )
    }
}
