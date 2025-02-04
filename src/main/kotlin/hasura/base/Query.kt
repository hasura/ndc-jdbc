package hasura.base

import hasura.ndc.connector.*
import hasura.ndc.ir.*

interface DatabaseQuery<T : Configuration> {
    fun generateQuery(source: DatabaseSource, configuration: T, request: QueryRequest): String
}
