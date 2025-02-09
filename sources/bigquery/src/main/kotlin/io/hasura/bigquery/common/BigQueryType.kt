package io.hasura.bigquery.common

import io.hasura.common.ColumnType
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonClassDiscriminator

@OptIn(ExperimentalSerializationApi::class)
@Serializable
@JsonClassDiscriminator("type")
sealed class BigQueryType : ColumnType {
    @Serializable
    @SerialName("scalar_type")
    data class ScalarType(
        val value: BigQueryScalarType
    ) : BigQueryType()

    @Serializable
    @SerialName("array_type")
    data class ArrayType(
        val value: BigQueryType
    ) : BigQueryType()

    @Serializable
    @SerialName("range_type")
    data class RangeType(
        val value: BigQueryRangeDataType
    ) : BigQueryType()

    @Serializable
    @SerialName("struct_type")
    data class StructType(
        val fields: Map<String, BigQueryType>
    ) : BigQueryType()

    override val typeName: String
        get() = when (this) {
            is ScalarType -> value.toString()
            is ArrayType -> "array"
            is RangeType -> "range"
            is StructType -> "struct"
        }
}

@Serializable
enum class BigQueryScalarType : ColumnType {
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
    TIMESTAMP;

    override val typeName: String
        get() = toString()
}

@Serializable
enum class BigQueryRangeDataType : ColumnType {
    DATE,
    DATETIME,
    TIMESTAMP;

    override val typeName: String
        get() = toString()
}