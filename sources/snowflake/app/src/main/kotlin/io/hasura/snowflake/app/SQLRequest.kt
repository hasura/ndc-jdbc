package io.hasura.snowflake.app

import kotlinx.serialization.Serializable

@Serializable
data class SQLRequest(
    val sql: String
)