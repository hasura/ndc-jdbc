package io.hasura.postgres.cli

import io.hasura.common.Configuration
import io.hasura.common.ConnectionUri

data class PostgresConfig(
    override val connectionUri: ConnectionUri,
) : Configuration