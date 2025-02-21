package io.hasura.postgres

import io.hasura.common.ColumnType
import kotlinx.serialization.Serializable

@Serializable
data class PGColumnType(
    override val typeName: String
) : ColumnType