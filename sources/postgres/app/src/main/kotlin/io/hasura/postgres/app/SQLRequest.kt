package io.hasura.postgres.app

import kotlinx.serialization.Serializable

@Serializable
data class SQLRequest(
    val sql: String
)