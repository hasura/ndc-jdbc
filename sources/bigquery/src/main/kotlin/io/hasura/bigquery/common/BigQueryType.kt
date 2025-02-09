package io.hasura.bigquery.common

import io.hasura.common.ColumnType
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonClassDiscriminator

@Serializable
data class BigQueryType(
    @SerialName("scalar_type")
    val scalarType: BigQueryScalarType? = null,
    @SerialName("array_type")
    val arrayType: BigQueryType? = null,
    @SerialName("range_type")
    val rangeType: BigQueryRangeDataType? = null,
    @SerialName("struct_type")
    val structType: Map<String, BigQueryType>? = null
) : ColumnType {
    override val typeName: String
        get() = when {
            scalarType != null -> scalarType.toString()
            arrayType != null -> "array"
            structType != null -> "struct"
            rangeType != null -> "range"
            else -> throw IllegalStateException("Invalid BigQueryType")
        }
}

enum class BigQueryScalarType {
    // Scalar types
    ANY,
    BIGINT,
    BIGNUMERIC,
    BOOLEAN,
    BYTES,
    DATE,
    DATETIME,
    FLOAT64,
    FLOAT,
    GEOGRAPHY,
    INT64,
    JSON,
    NUMERIC,
    STRING,
    TIME,
    TIMESTAMP
}

enum class BigQueryRangeDataType {
    DATE,
    DATETIME,
    TIMESTAMP
}
