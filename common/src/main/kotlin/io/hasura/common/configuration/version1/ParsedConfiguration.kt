package io.hasura.common.configuration.version1

import io.hasura.common.configuration.ColumnType
import io.hasura.common.configuration.Configuration
import io.hasura.common.configuration.ConnectionUri
import io.hasura.common.configuration.DefaultConfiguration
import io.hasura.common.configuration.Version
import io.hasura.common.configuration.Category
import io.hasura.common.configuration.Column
import io.hasura.common.configuration.ForeignKeyInfo
import io.hasura.common.configuration.FunctionInfo
import io.hasura.common.configuration.TableInfo
import io.hasura.common.configuration.NativeOperation
import io.hasura.common.configuration.NativeOperationColumn
import io.hasura.common.configuration.NativeOperationInfo
import io.hasura.common.configuration.NativeOperationSql
import io.hasura.common.configuration.NativeOperationType

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

@Serializable
data class ConfigurationV1<T: ColumnType>(
    @SerialName("version")
    override val version: Version = Version.V1,
    @SerialName("connection_uri")
    override val connectionUri: ConnectionUri,
    val schemas: List<String> = emptyList(),
    val tables: List<TableInfoV1<T>> = emptyList(),
    val functions: List<FunctionInfoV1> = emptyList(),
    @SerialName("native_operations")
    val nativeOperations: Map<String, NativeOperationV1> = emptyMap()
): Configuration<T> {
    override fun toDefaultConfiguration(): io.hasura.common.configuration.DefaultConfiguration<T>
    {
        return DefaultConfiguration(
            version = this.version,
            connectionUri = this.connectionUri,
            schemas = this.schemas,
            tables = this.tables.map { it.toDefaultTableInfo() },
            functions = this.functions.map { it.toDefaultFunctionInfo() },
            nativeOperations = this.nativeOperations.mapValues { it.value.toDefaultNativeOperation() })


    }
}

// Table info for V1
@Serializable
data class TableInfoV1<T: ColumnType>(
    val name: String,
    val description: String?,
    val category: CategoryV1,
    val columns: List<ColumnV1<T>>,
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
data class ColumnV1<T: ColumnType>(
    val name: String,
    val description: String? = null,
    val type: T,
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


/**
 * Converts a TableInfoV1 to TableInfo
 */
private fun <T: ColumnType> TableInfoV1<T>.toDefaultTableInfo(): TableInfo<T> {
    return TableInfo(
        name = this.name,
        description = this.description,
        category = this.category.toDefaultCategory(),
        columns = this.columns.map { it.toDefaultColumn() } ,
        primaryKeys = this.primaryKeys,
        foreignKeys = this.foreignKeys.mapValues { it.value.toDefaultForeignKeyInfo() }
    )
}

/**
 * Converts a CategoryV1 to Category
 */
private fun CategoryV1.toDefaultCategory(): Category {
    return when (this) {
        CategoryV1.TABLE -> Category.TABLE
        CategoryV1.VIEW -> Category.VIEW
        CategoryV1.MATERIALIZED_VIEW -> Category.MATERIALIZED_VIEW
    }
}

/**
 * Converts a ColumnV1 to Column
 */
private fun <T:ColumnType> ColumnV1<T>.toDefaultColumn(): Column<T> {
    return Column(
        name = this.name,
        description = this.description,
        type = this.type,
        nullable = this.nullable,
        autoIncrement = this.autoIncrement,
        isPrimaryKey = this.isPrimaryKey
    )
}

/**
 * Converts a FunctionInfoV1 to FunctionInfo
 */
private fun FunctionInfoV1.toDefaultFunctionInfo(): FunctionInfo {
    return FunctionInfo(
        name = this.name,
        description = this.description
    )
}

/**
 * Converts a ForeignKeyInfoV1 to ForeignKeyInfo
 */
private fun ForeignKeyInfoV1.toDefaultForeignKeyInfo(): ForeignKeyInfo {
    return ForeignKeyInfo(
        columnMapping = this.columnMapping,
        foreignCollection = this.foreignCollection
    )
}

/**
 * Converts a NativeOperationV1 to NativeOperation
 */
private fun NativeOperationV1.toDefaultNativeOperation(): NativeOperation {
    return when (this) {
        is NativeOperationV1.NativeQueries -> NativeOperation.NativeQueries(
            queries = this.queries.mapValues { it.value.toDefaultNativeOperationInfo() }
        )
        is NativeOperationV1.NativeMutations -> NativeOperation.NativeMutations(
            mutations = this.mutations.mapValues { it.value.toDefaultNativeOperationInfo() }
        )
    }
}

/**
 * Converts a NativeOperationInfoV1 to NativeOperationInfo
 */
private fun NativeOperationInfoV1.toDefaultNativeOperationInfo(): NativeOperationInfo {
    return NativeOperationInfo(
        sql = this.sql.toDefaultNativeOperationSql(),
        columns = this.columns.mapValues { it.value.toDefaultNativeOperationColumn() },
        arguments = this.arguments.mapValues { it.value.toDefaultNativeOperationColumn() },
        description = this.description
    )
}

/**
 * Converts a NativeOperationSqlV1 to NativeOperationSql
 */
private fun NativeOperationSqlV1.toDefaultNativeOperationSql(): NativeOperationSql {
    return when (this) {
        is NativeOperationSqlV1.Literal -> NativeOperationSql.Literal(this.value)
        is NativeOperationSqlV1.File -> NativeOperationSql.File(this.file)
    }
}

/**
 * Converts a NativeOperationColumnV1 to NativeOperationColumn
 */
private fun NativeOperationColumnV1.toDefaultNativeOperationColumn(): NativeOperationColumn {
    return NativeOperationColumn(
        name = this.name,
        type = this.type.toDefaultNativeOperationType(),
        nullable = this.nullable,
        description = this.description
    )
}

/**
 * Converts a NativeOperationTypeV1 to NativeOperationType
 */
private fun NativeOperationTypeV1.toDefaultNativeOperationType(): NativeOperationType {
    return when (this) {
        is NativeOperationTypeV1.ScalarType -> NativeOperationType.ScalarType(this.value)
        is NativeOperationTypeV1.ArrayType -> NativeOperationType.ArrayType(
            this.value.toDefaultNativeOperationType()
        )
    }
}
