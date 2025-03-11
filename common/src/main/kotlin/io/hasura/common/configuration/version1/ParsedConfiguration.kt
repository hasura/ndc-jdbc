package io.hasura.common.configuration.version1

import io.hasura.common.configuration.ColumnType
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

@Serializable
data class ConnectionUri(
    val value: String? = null,
    val variable: String? = null
) {
    fun resolve(): String = when {
        value != null -> value
        variable != null -> System.getenv(variable) ?: throw IllegalStateException("Environment variable $variable not found")
        else -> throw IllegalStateException("Either value or variable must be set")
    }
}

interface Configuration {
    @SerialName("connection_uri") val connectionUri: ConnectionUri
}


@Serializable
data class ConfigurationV1(
    @SerialName("connection_uri")
    val connectionUri: ConnectionUri,
    val schemas: List<String> = emptyList(),
    val tables: List<TableInfoV1> = emptyList(),
    val functions: List<FunctionInfoV1> = emptyList(),
    @SerialName("native_operations")
    val nativeOperations: Map<String, NativeOperationV1> = emptyMap()
)

// V1-specific column type
@Serializable
data class ColumnTypeV1(
    @SerialName("scalar_type")
    val scalarType: String,
    val precision: Int? = null,
    val scale: Int? = null
) : ColumnType {
    override val typeName: String = scalarType
}

// Table info for V1
@Serializable
data class TableInfoV1(
    val name: String,
    val description: String?,
    val category: CategoryV1,
    val columns: List<ColumnV1>,
    @SerialName("primary_keys")
    val primaryKeys: List<String>,
    @SerialName("foreign_keys")
    val foreignKeys: Map<String, ForeignKeyInfoV1>
)

@Serializable
enum class CategoryV1 {
    TABLE,
    VIEW,
    MATERIALIZED_VIEW
}

@Serializable
data class ColumnV1(
    val name: String,
    val description: String? = null,
    val type: ColumnTypeV1,
    val nullable: Boolean,
    @SerialName("auto_increment")
    val autoIncrement: Boolean,
    @SerialName("is_primarykey")
    val isPrimaryKey: Boolean? = null,
)

@Serializable
data class ForeignKeyInfoV1(
    @SerialName("column_mapping")
    val columnMapping: Map<String, String>,
    @SerialName("foreign_collection")
    val foreignCollection: String
)

// Function info for V1
@Serializable
data class FunctionInfoV1(
    val name: String,
    val description: String?
)

// Native operations for V1
@Serializable
sealed class NativeOperationV1 {
    @Serializable
    data class NativeQueries(val queries: Map<String, NativeOperationInfoV1>) : NativeOperationV1()

    @Serializable
    data class NativeMutations(val mutations: Map<String, NativeOperationInfoV1>) : NativeOperationV1()
}

@Serializable
data class NativeOperationInfoV1(
    val sql: NativeOperationSqlV1,
    val columns: Map<String, NativeOperationColumnV1>,
    val arguments: Map<String, NativeOperationColumnV1>,
    val description: String?,
)

@Serializable
sealed class NativeOperationSqlV1 {
    @Serializable
    data class Literal(val value: String) : NativeOperationSqlV1()

    @Serializable
    data class File(val file: String) : NativeOperationSqlV1()
}

@Serializable
data class NativeOperationColumnV1(
    val name: String,
    val type: NativeOperationTypeV1,
    val nullable: Boolean,
    val description: String?
)

@Serializable
sealed class NativeOperationTypeV1 {
    @Serializable
    @SerialName("scalar_type")
    data class ScalarType(val value: String) : NativeOperationTypeV1()

    @Serializable
    @SerialName("array_type")
    data class ArrayType(val value: NativeOperationTypeV1) : NativeOperationTypeV1()
}
