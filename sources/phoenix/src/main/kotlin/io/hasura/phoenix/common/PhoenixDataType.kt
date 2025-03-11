package io.hasura.phoenix.common

import io.hasura.common.ColumnType
import kotlinx.serialization.Serializable

@Serializable
data class PhoenixDataType(
    override val typeName: String
) : ColumnType