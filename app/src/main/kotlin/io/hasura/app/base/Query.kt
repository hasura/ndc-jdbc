package io.hasura.app.base

import hasura.ndc.ir.*

interface DatabaseQuery<T : Configuration> {
    fun generateQuery(source: DatabaseSource, configuration: T, request: QueryRequest): String
}
