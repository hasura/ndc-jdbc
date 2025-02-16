package io.hasura.app.base

import io.hasura.ndc.ir.*
import io.hasura.common.*

interface DatabaseQuery<T : Configuration> {
    fun generateQuery(): String
    fun generateAggregateQuery(): String
    fun generateExplainQuery(): String
}
