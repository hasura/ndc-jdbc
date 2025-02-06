package io.hasura.app.base

import io.hasura.ndc.ir.*

interface ISchemaGenerator<T : Configuration> {
    fun getSchema(configuration: T): SchemaResponse
}