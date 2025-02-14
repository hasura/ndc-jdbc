package io.hasura.app.base

import io.hasura.ndc.ir.*
import io.hasura.common.*

interface DatabaseQuery<T : Configuration> {
    fun generateQuery(source: DatabaseSource, request: QueryRequest): String
    fun generateExplainQuery(source: DatabaseSource, request: QueryRequest): String
}
