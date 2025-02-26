package io.hasura.postgres.cli

import io.hasura.common.ColumnType
import io.hasura.common.Configuration
import io.hasura.common.DefaultConfiguration

interface IConfigGenerator<T : Configuration, U : ColumnType> {
    fun generateConfig(config: T): DefaultConfiguration<U>
}