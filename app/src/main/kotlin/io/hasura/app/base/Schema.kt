package io.hasura.app.base

import io.hasura.ndc.ir.*
import io.hasura.common.configuration.*

interface ISchemaGenerator<T : Configuration> {
    fun getSchema(configuration: T): SchemaResponse
}
