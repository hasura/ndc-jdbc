package io.hasura.bigquery.common

import io.hasura.common.ColumnType
import kotlinx.serialization.*
import kotlinx.serialization.json.*

@Serializable(BigQueryTypeSerializer::class)
sealed class BigQueryType : ColumnType {
    @Serializable
    data class ScalarType(
        @SerialName("scalar_type")
        val scalarType: BigQueryScalarType
    ) : BigQueryType()

    @Serializable
    data class ArrayType(
        @SerialName("array_type")
        val arrayType: BigQueryType
    ) : BigQueryType()

    @Serializable
    data class RangeType(
        @SerialName("range_type")
        val rangeType: BigQueryRangeDataType
    ) : BigQueryType()

    @Serializable
    data class StructType(
        @SerialName("struct_type")
        val structType: Map<String, BigQueryType>
    ) : BigQueryType()

    override val typeName: String
        get() = when (this) {
            is ScalarType -> scalarType.toString()
            is ArrayType -> "array"
            is RangeType -> "range"
            is StructType -> "struct"
        }
}

object BigQueryTypeSerializer : JsonContentPolymorphicSerializer<BigQueryType>(
    BigQueryType::class,
) {
    override fun selectDeserializer(
        element: JsonElement,
    ): DeserializationStrategy<BigQueryType> {
        val jsonObject = element.jsonObject
        return when {
            jsonObject.containsKey("scalar_type") -> BigQueryType.ScalarType.serializer()
            jsonObject.containsKey("array_type") -> BigQueryType.ArrayType.serializer()
            jsonObject.containsKey("range_type") -> BigQueryType.RangeType.serializer()
            jsonObject.containsKey("struct_type") -> BigQueryType.StructType.serializer()
            else -> throw IllegalArgumentException(
                "Unsupported BigQueryType type.",
            )
        }
    }
}

@Serializable
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

@Serializable
enum class BigQueryRangeDataType {
    DATE,
    DATETIME,
    TIMESTAMP
}