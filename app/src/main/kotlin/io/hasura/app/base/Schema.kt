package io.hasura.app.base

import io.hasura.ndc.ir.*
import io.hasura.common.configuration.*

interface ISchemaGenerator<U: ColumnType, T : Configuration<U>> {
    fun getSchema(configuration: T): SchemaResponse
}
