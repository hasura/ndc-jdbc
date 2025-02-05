package io.hasura.bigquery.app

import io.hasura.app.default.*
import hasura.ndc.ir.*
import kotlinx.serialization.Serializable
import kotlinx.serialization.SerialName
import kotlinx.serialization.json.*

@OptIn(kotlinx.serialization.ExperimentalSerializationApi::class)
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

class BigQuerySchemaGenerator : DefaultSchemaGenerator<BigQueryType>() {
    override fun mapToTypeRepresentation(
        columnType: BigQueryType,
        numericPrecision: Int?,
        numericScale: Int?
    ): ScalarType {
        val representationType = when (columnType) {
            is BigQueryType.ScalarType -> when (columnType.value) {
                BigQueryScalarType.ANY -> null
                BigQueryScalarType.STRING -> RepresentationType.TypeString
                BigQueryScalarType.INT64 -> RepresentationType.Int64
                BigQueryScalarType.FLOAT64 -> RepresentationType.Float64
                BigQueryScalarType.FLOAT -> RepresentationType.Float64
                BigQueryScalarType.BIGINT -> RepresentationType.Biginteger
                BigQueryScalarType.NUMERIC, BigQueryScalarType.BIGNUMERIC -> RepresentationType.Bigdecimal
                BigQueryScalarType.BOOLEAN -> RepresentationType.TypeBoolean
                BigQueryScalarType.DATE -> RepresentationType.Date
                BigQueryScalarType.TIME -> RepresentationType.Timestamp
                BigQueryScalarType.DATETIME, BigQueryScalarType.TIMESTAMP -> RepresentationType.Timestamp
                BigQueryScalarType.BYTES -> RepresentationType.Bytes
                BigQueryScalarType.GEOGRAPHY -> RepresentationType.Geography
                BigQueryScalarType.JSON -> RepresentationType.JSON
            }
            is BigQueryType.ArrayType -> RepresentationType.JSON
            is BigQueryType.RangeType -> RepresentationType.JSON
            is BigQueryType.StructType -> RepresentationType.JSON
        }
        
        return createScalarType(representationType, columnType.typeName)
    }

    override fun mapAggregateFunctions(
        columnTypeStr: String,
        representation: TypeRepresentation?
    ): Map<String, AggregateFunctionDefinition> {
        return emptyMap()
    }

    override fun mapComparisonOperators(
        columnTypeStr: String,
        representation: TypeRepresentation?
    ): Map<String, ComparisonOperatorDefinition> {
        return emptyMap()
    }
}
