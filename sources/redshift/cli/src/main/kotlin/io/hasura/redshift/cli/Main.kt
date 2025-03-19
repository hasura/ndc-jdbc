package io.hasura.redshift.cli

import io.hasura.redshift.common.*
import io.hasura.common.Category
import io.hasura.common.Column
import io.hasura.common.ColumnType
import io.hasura.common.Configuration
import io.hasura.common.ConnectionUri
import io.hasura.common.DefaultConfiguration
import io.hasura.common.ForeignKeyInfo
import io.hasura.common.FunctionInfo
import io.hasura.common.TableInfo
import io.hasura.ndc.ir.json
import kotlinx.cli.*
import kotlinx.serialization.Serializable
import kotlinx.serialization.SerialName
import kotlinx.serialization.json.Json
import org.jooq.DSLContext
import org.jooq.impl.DSL
import java.io.File
import kotlin.collections.joinToString
import kotlin.system.exitProcess

interface IConfigGenerator<T : Configuration, U : ColumnType> {
    fun generateConfig(config: T): DefaultConfiguration<U>
}

data class RedshiftConfiguration(
    override val connectionUri: ConnectionUri,
    val schemas: List<String> = emptyList()
) : Configuration


object RedshiftConfigGenerator : IConfigGenerator<RedshiftConfiguration, RedshiftDataType> {
    val jsonFormatter = Json { prettyPrint = true }

    data class IntrospectionResult(
        val tables: List<TableInfo<RedshiftDataType>> = emptyList(),
        val functions: List<FunctionInfo> = emptyList()
    )

    override fun generateConfig(config: RedshiftConfiguration): DefaultConfiguration<RedshiftDataType> {
        val introspectionResult = introspectSchemas(config)

        return DefaultConfiguration(
            connectionUri = config.connectionUri,
            tables = introspectionResult.tables,
            functions = introspectionResult.functions,
        )
    }

    @Serializable
    data class TableMetadata(
        val table_schema: String,
        val table_name: String,
        val table_type: String,
        val columns: MutableList<ColumnMetadata> = mutableListOf()
    )

    @Serializable
    data class ColumnMetadata(
        val column_name: String,
        val data_type: String,
        val is_nullable: String,
        val column_default: String? = null,
        val ordinal_position: Int,
        val character_maximum_length: Int? = null,
        val numeric_precision: Int? = null,
        val numeric_scale: Int? = null,
        val foreign_key: ForeignKeyMetadata? = null
    )

    @Serializable
    data class ForeignKeyMetadata(
        val constraint_name: String,
        val foreign_table_schema: String,
        val foreign_table_name: String,
        val foreign_column_name: String
    )

    private fun extractCatalog(jdbcUrl: String): String {
        return jdbcUrl.substringAfterLast("/").substringBefore("?")
    }

    fun transformData(rows: List<Map<String, Any?>>): List<TableMetadata> {
        val tables = mutableMapOf<String, TableMetadata>()

        // Process each row
        rows.forEach { row ->
            // Extract table info
            val schema = row["table_schema"] as String
            val name = row["table_name"] as String
            val type = row["table_type"] as String
            val tableKey = "$schema.$name"

            // Create column data
            val column = ColumnMetadata(
                column_name = row["column_name"] as String,
                data_type = row["data_type"] as String,
                is_nullable = row["is_nullable"] as String,
                column_default = row["column_default"] as? String,
                ordinal_position = (row["ordinal_position"] as Number).toInt(),
                character_maximum_length = (row["character_maximum_length"] as? Number)?.toInt(),
                numeric_precision = (row["numeric_precision"] as? Number)?.toInt(),
                numeric_scale = (row["numeric_scale"] as? Number)?.toInt(),
                foreign_key = if (row["fk_constraint_name"] != null) {
                    ForeignKeyMetadata(
                        constraint_name = row["fk_constraint_name"] as String,
                        foreign_table_schema = row["foreign_table_schema"] as String,
                        foreign_table_name = row["foreign_table_name"] as String,
                        foreign_column_name = row["foreign_column_name"] as String
                    )
                } else null
            )

            // Add to existing table or create new one
            if (tables.containsKey(tableKey)) {
                tables[tableKey]!!.columns.add(column)
            } else {
                tables[tableKey] = TableMetadata(
                    table_schema = schema,
                    table_name = name,
                    table_type = type,
                    columns = mutableListOf(column)
                )
            }
        }

        return tables.values.toList()
    }

    private fun introspectSchemas(config: RedshiftConfiguration): IntrospectionResult {
        val jdbcUrl = config.connectionUri.resolve()
        val catalog = extractCatalog(jdbcUrl)
        val ctx = DSL.using(jdbcUrl)

        val sql = """
            SELECT
                t.table_schema,
                t.table_name,
                t.table_type,
                c.column_name,
                c.data_type,
                c.is_nullable,
                c.column_default,
                c.ordinal_position,
                c.character_maximum_length,
                c.numeric_precision,
                c.numeric_scale,
                fk.constraint_name as fk_constraint_name,
                fk.foreign_table_schema,
                fk.foreign_table_name,
                fk.foreign_column_name
            FROM
                information_schema.tables t
            JOIN
                information_schema.columns c
                ON t.table_schema = c.table_schema AND t.table_name = c.table_name
            LEFT JOIN (
                SELECT
                    tc.constraint_name,
                    tc.table_schema,
                    tc.table_name,
                    kcu.column_name,
                    ccu.table_schema AS foreign_table_schema,
                    ccu.table_name AS foreign_table_name,
                    ccu.column_name AS foreign_column_name
                FROM
                    information_schema.table_constraints tc
                JOIN
                    information_schema.key_column_usage kcu
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                JOIN
                    information_schema.constraint_column_usage ccu
                    ON ccu.constraint_name = tc.constraint_name
                WHERE
                    tc.constraint_type = 'FOREIGN KEY'
            ) fk
            ON c.table_schema = fk.table_schema
              AND c.table_name = fk.table_name
              AND c.column_name = fk.column_name
            WHERE
                t.table_schema NOT IN ('information_schema', 'pg_catalog', 'pg_internal', 'pg_auto_copy')
                ${if (config.schemas.isNotEmpty())
                    "AND LOWER(c.table_schema) IN (${config.schemas.joinToString(",") { "'${it.lowercase()}'" }})"
                else ""}
            ORDER BY
                t.table_schema,
                t.table_name,
                c.ordinal_position;
        """

        val results = ctx.fetch(sql)
        val queryData = transformData(results.map { it.intoMap() })

        val tables = queryData.map { table ->
            TableInfo<RedshiftDataType>(
                name = "${catalog}.${table.table_schema}.${table.table_name}",
                category = when (table.table_type) {
                    "TABLE", "BASE TABLE", "MANAGED" -> Category.TABLE
                    "VIEW" -> Category.VIEW
                    "MATERIALIZED VIEW" -> Category.VIEW
                    else -> throw Exception("Unknown table type: ${table.table_type}")
                },
                description = null,
                columns = table.columns.map { column ->
                    Column<RedshiftDataType>(
                        name = column.column_name,
                        type = getRedshiftDataType(
                            column.data_type,
                            column.numeric_precision ?: 0,
                            column.numeric_scale ?: 0,
                        ),
                        nullable = column.is_nullable == "YES",
                        autoIncrement = false,
                        isPrimaryKey = false
                    )
                },
                primaryKeys = emptyList(),
                foreignKeys = emptyMap()
            )
        }

        return IntrospectionResult(tables = tables)
    }

    private fun getRedshiftDataType(
        column: String,
        precision: Int,
        scale: Int,
    ): RedshiftDataType {
        return when {
            column == "bigint" -> RedshiftDataType.BIGINT
            column == "int8" -> RedshiftDataType.BIGINT
            column == "boolean" -> RedshiftDataType.BOOLEAN
            column == "bool" -> RedshiftDataType.BOOLEAN
            column == "char" -> RedshiftDataType.CHAR
            column == "character" -> RedshiftDataType.CHAR
            column == "nchar" -> RedshiftDataType.CHAR
            column == "bpchar" -> RedshiftDataType.CHAR
            column == "date" -> RedshiftDataType.DATE
            column == "decimal" -> RedshiftDataType.DECIMAL(precision, scale)
            column == "double precision" -> RedshiftDataType.DOUBLE_PRECISION
            column == "float" -> RedshiftDataType.DOUBLE_PRECISION
            column == "float8" -> RedshiftDataType.DOUBLE_PRECISION
            column == "geometry" -> RedshiftDataType.GEOMETRY
            column == "geography" -> RedshiftDataType.GEOGRAPHY
            column == "hllsketch" -> RedshiftDataType.HLLSKETCH
            column == "integer" -> RedshiftDataType.INTEGER
            column == "int" -> RedshiftDataType.INTEGER
            column == "int4" -> RedshiftDataType.INTEGER
            column == "interval year to month" -> RedshiftDataType.INTERVAL_YEAR_TO_MONTH
            column == "interval day to second" -> RedshiftDataType.INTERVAL_DAY_TO_SECOND
            column == "real" -> RedshiftDataType.REAL
            column == "float4" -> RedshiftDataType.REAL
            column == "smallint" -> RedshiftDataType.SMALLINT
            column == "int2" -> RedshiftDataType.SMALLINT
            column == "super" -> RedshiftDataType.SUPER
            column == "text" -> RedshiftDataType.TEXT
            column == "time" -> RedshiftDataType.TIME
            column == "time without time zone" -> RedshiftDataType.TIME
            column == "timetz" -> RedshiftDataType.TIMETZ
            column == "time with time zone" -> RedshiftDataType.TIMETZ
            column == "timestamp" -> RedshiftDataType.TIMESTAMP
            column == "timestamp without time zone" -> RedshiftDataType.TIMESTAMP
            column == "timestamptz" -> RedshiftDataType.TIMESTAMPTZ
            column == "timestamptz with time zone" -> RedshiftDataType.TIMESTAMPTZ
            column == "varbyte" -> RedshiftDataType.VARBYTE
            column == "varbinary" -> RedshiftDataType.VARBYTE
            column == "binary varying" -> RedshiftDataType.VARBYTE
            column == "varchar" -> RedshiftDataType.VARCHAR
            column == "character varying" -> RedshiftDataType.VARCHAR
            column == "nvarchar" -> RedshiftDataType.VARCHAR
            column == "text" -> RedshiftDataType.VARCHAR
            else -> RedshiftDataType.SUPER
        }
    }

}

@OptIn(ExperimentalCli::class)
object UpdateCommand : Subcommand("update", "Update configuration file") {
    private val jdbcUrl by option(
        ArgType.String,
        shortName = "j",
        fullName = "jdbc-url",
        description = "JDBC URL or environment variable for Snowflake connection"
    ).required()

    private val schemas by option(
        ArgType.String,
        shortName = "s",
        fullName = "schemas",
        description = "Comma-separated list of schemas to introspect"
    )

    private val outfile by option(
        ArgType.String,
        shortName = "o",
        fullName = "outfile",
        description = "Output file for generated configuration"
    ).default("configuration.json")

    override fun execute() {
        val connectionUri = if (System.getenv(jdbcUrl) != null) {
            ConnectionUri(variable = jdbcUrl)
        } else {
            ConnectionUri(value = jdbcUrl)
        }

        val cleanedSchemas = schemas?.takeIf { it.isNotEmpty() }?.split(",") ?: emptyList()
        val config = RedshiftConfiguration(connectionUri, cleanedSchemas)
        val generatedConfig = RedshiftConfigGenerator.generateConfig(config)

        val json = RedshiftConfigGenerator.jsonFormatter.encodeToString(generatedConfig)

        // Write the generated configuration to the output file
        val file = File(outfile)
        try {
            file.writeText(json)
        } catch (e: Exception) {
            println("Failed to write configuration to file: ${e.message}")
            exitProcess(1)
        }

        exitProcess(0)
    }
}

@OptIn(ExperimentalCli::class)
fun main(args: Array<String>) {
    val parser = ArgParser("update", strictSubcommandOptionsOrder = true)
    parser.subcommands(UpdateCommand)

    val modifiedArgs = args.toMutableList()
    val schemasIndex = modifiedArgs.indexOf("--schemas")
    if (schemasIndex != -1 && schemasIndex + 1 < modifiedArgs.size) {
        val nextArg = modifiedArgs.getOrNull(schemasIndex + 1)
        if (nextArg?.startsWith("-") == true) {
            modifiedArgs.add(schemasIndex + 1, "")
        }
    }

    if (modifiedArgs.isEmpty()) {
        println("Subcommand is required (ex: update)")
        exitProcess(1)
    }

    parser.parse(modifiedArgs.toTypedArray())
}
