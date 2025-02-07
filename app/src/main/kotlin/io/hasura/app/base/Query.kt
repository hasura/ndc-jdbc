package io.hasura.app.base

import io.hasura.ndc.ir.*
import io.hasura.common.*

interface DatabaseQuery<T : Configuration> {
    fun generateQuery(source: DatabaseSource, configuration: T, request: QueryRequest): String
    fun generateExplainQuery(source: DatabaseSource, configuration: T, request: QueryRequest): String
}
