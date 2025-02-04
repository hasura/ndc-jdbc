package hasura.base

import hasura.ndc.ir.*

interface ISchemaGenerator<T : Configuration> {
    fun getSchema(configuration: T): SchemaResponse
}