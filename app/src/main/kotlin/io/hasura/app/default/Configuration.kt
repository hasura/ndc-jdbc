package io.hasura.app.default

import io.hasura.app.base.*
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

@Serializable
data class DefaultConfiguration<T : ColumnType>(
    @SerialName("connection_uri")
    override val connectionUri: ConnectionUri,
    val schemas: List<String> = emptyList(),
    val tables: List<TableInfo<T>> = emptyList(),
    val functions: List<FunctionInfo> = emptyList(),
    @SerialName("native_operations")
    val nativeOperations: Map<String, NativeOperation> = emptyMap()
) : Configuration

@Serializable
data class TableInfo<T : ColumnType>(
    val name: String,
    val description: String?,
    val category: Category,
    val columns: List<Column<T>>,
    @SerialName("primary_keys")
    val primaryKeys: List<String>,
    @SerialName("foreign_keys")
    val foreignKeys: Map<String, ForeignKeyInfo>
)

@Serializable
enum class Category {
    TABLE,
    VIEW,
    MATERIALIZED_VIEW
}

interface ColumnType {
    val typeName: String
}

@Serializable
data class Column<T : ColumnType>(
    val name: String,
    val description: String?,
    val type: T,
    @SerialName("numeric_precision")
    val numericPrecision: Int?,
    @SerialName("numeric_scale")
    val numericScale: Int?,
    val nullable: Boolean,
    @SerialName("auto_increment")
    val autoIncrement: Boolean,
    @SerialName("is_primarykey")
    val isPrimaryKey: Boolean?
)

// TODO: Not supported yet
@Serializable
data class FunctionInfo(
    val name: String,
    val description: String?
)

@Serializable
data class ForeignKeyInfo(
    @SerialName("column_mapping")
    val columnMapping: Map<String, String>,
    @SerialName("foreign_collection")
    val foreignCollection: String
)

@Serializable
sealed class NativeOperation {
    @Serializable
    data class NativeQueries(val queries: Map<String, NativeOperationInfo>) : NativeOperation()
    
    @Serializable
    data class NativeMutations(val mutations: Map<String, NativeOperationInfo>) : NativeOperation()
}

@Serializable
data class NativeOperationInfo(
    val sql: NativeOperationSql,
    val columns: Map<String, NativeOperationColumn>,
    val arguments: Map<String, NativeOperationColumn>,
    val description: String?,
)

@Serializable
sealed class NativeOperationSql {
    @Serializable
    data class Literal(val value: String) : NativeOperationSql()
    
    @Serializable
    data class File(val file: String) : NativeOperationSql()
}

@Serializable
data class NativeOperationColumn(
    val name: String,
    val type: NativeOperationType,
    val nullable: Boolean,
    val description: String?
)

@Serializable
sealed class NativeOperationType {
    @Serializable
    @SerialName("scalar_type")
    data class ScalarType(val value: String) : NativeOperationType()
    
    @Serializable
    @SerialName("array_type")
    data class ArrayType(val value: NativeOperationType) : NativeOperationType()
}
