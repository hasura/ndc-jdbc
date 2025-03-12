package io.hasura.databricks.cli

import io.hasura.databricks.common.*
import io.hasura.common.configuration.Category
import io.hasura.common.configuration.Column
import io.hasura.common.configuration.ColumnType
import io.hasura.common.configuration.Configuration
import io.hasura.common.configuration.ConnectionUri
import io.hasura.common.configuration.DefaultConfiguration
import io.hasura.common.configuration.ForeignKeyInfo
import io.hasura.common.configuration.FunctionInfo
import io.hasura.common.configuration.TableInfo
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
import schemacrawler.inclusionrule.RegularExpressionInclusionRule;
import schemacrawler.schema.*
import schemacrawler.schema.Column as SchemaCrawlerColumn
import schemacrawler.schemacrawler.*
import schemacrawler.tools.utility.SchemaCrawlerUtility;
import us.fatehi.utility.datasource.*
import schemacrawler.tools.options.Config
import io.hasura.common.configuration.Version

interface IConfigGenerator<T : Configuration<DatabricksDataType>, U : ColumnType> {
    fun generateConfig(config: T): DefaultConfiguration<U>
}

data class DatabricksConfiguration(
    override val connectionUri: ConnectionUri,
    val schemas: List<String> = emptyList()
) : Configuration<DatabricksDataType> {
    override val version = Version.V1

    override fun toDefaultConfiguration(): DefaultConfiguration<DatabricksDataType> {
        return DatabricksConfigGenerator.generateConfig(this)
    }
}


object DatabricksConfigGenerator : IConfigGenerator<DatabricksConfiguration, DatabricksDataType> {
    val jsonFormatter = Json { prettyPrint = true }

    data class IntrospectionResult(
        val tables: List<TableInfo<DatabricksDataType>> = emptyList(),
        val functions: List<FunctionInfo> = emptyList()
    )

    override fun generateConfig(config: DatabricksConfiguration): DefaultConfiguration<DatabricksDataType> {
        val introspectionResult = introspectSchemas(config)

        return DefaultConfiguration(
            version = config.version,
            connectionUri = config.connectionUri,
            tables = introspectionResult.tables,
            functions = introspectionResult.functions,
        )
    }

    @Serializable
    data class QueryTable(
        val tableSchema: String,
        val tableName: String,
        val tableType: String,
        val description: String? = null,
        val columns: Map<String, QueryColumn>
    )

    @Serializable
    data class QueryColumn(
        val name: String,
        val type: String,
        val nullable: String,
        val precision: Int? = null,
        val scale: Int? = null,
    )

    private fun extractCatalog(jdbcUrl: String): String {
        return jdbcUrl.substringAfter("ConnCatalog=").substringBefore(";")
    }

    private fun introspectSchemas(config: DatabricksConfiguration): IntrospectionResult {
        val jdbcUrl = config.connectionUri.resolve()
        val catalog = extractCatalog(jdbcUrl)
        val ctx = DSL.using(jdbcUrl)

        val sql = """
            WITH column_data AS (
                SELECT
                    c.table_name AS table_name,
                    c.table_schema AS table_schema,
                    t.table_type AS table_type,
                    t.comment AS description,
                    c.column_name,
                    TO_JSON(STRUCT(
                        c.column_name AS name,
                        c.data_type AS type,
                        c.is_nullable AS nullable,
                        c.numeric_precision AS precision,
                        c.numeric_scale AS scale
                    )) AS column_info
                FROM information_schema.columns c
                JOIN information_schema.tables t
                ON c.table_schema = t.table_schema AND c.table_name = t.table_name
                WHERE c.table_schema NOT IN ('information_schema', 'sys')
                ${if (config.schemas.isNotEmpty())
                    "AND LOWER(c.table_schema) IN (${config.schemas.joinToString(",") { "'${it.lowercase()}'" }})"
                else ""}
            ),
            columns_struct AS (
                SELECT
                    table_schema,
                    table_name,
                    table_type,
                    description,
                    ARRAY_JOIN(
                        COLLECT_LIST(
                            CONCAT('"', column_name, '":', column_info)
                        ), ','
                    ) AS columns_json
                FROM column_data
                GROUP BY table_schema, table_name, table_type, description
            )
            SELECT
                CONCAT('{', ARRAY_JOIN(COLLECT_LIST(CONCAT(
                    '"', table_schema, '.', table_name, '": {',
                        '"tableSchema": "', table_schema, '", ',
                        '"tableName": "', table_name, '", ',
                        '"tableType": "', table_type, '", ',
                        '"description": "', description, '", ',
                        '"columns": {', columns_json, '}',
                    '}'
                )), ','), '}') AS result
            FROM columns_struct;
        """

        val results = ctx.fetchValue(sql)
        val json = results?.toString() ?: "{}"
        val queryTables = jsonFormatter.decodeFromString<Map<String, QueryTable>>(json)

        val tables = queryTables.map { (_, table) ->
            TableInfo<DatabricksDataType>(
                name = "${catalog}.${table.tableSchema}.${table.tableName}",
                category = when (table.tableType) {
                    "TABLE", "MANAGED" -> Category.TABLE
                    "VIEW" -> Category.VIEW
                    "MATERIALIZED VIEW" -> Category.VIEW
                    else -> throw Exception("Unknown table type: ${table.tableType}")
                },
                description = null,
                columns = table.columns.map { (_, column) ->
                    Column<DatabricksDataType>(
                        name = column.name,
                        type = getDatabricksDataType(
                            column.type,
                            column.precision,
                            column.scale,
                        ),
                        nullable = column.nullable == "YES",
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

    private fun getDatabricksDataType(
        column: String,
        precision: Int?,
        scale: Int?,
    ): DatabricksDataType {
        return when {
            column == "BOOLEAN" -> DatabricksDataType.BOOLEAN
            column == "TINYINT" -> DatabricksDataType.TINYINT
            column == "BYTE" -> DatabricksDataType.TINYINT
            column == "SMALLINT" -> DatabricksDataType.SMALLINT
            column == "SHORT" -> DatabricksDataType.SMALLINT
            column == "INT" -> DatabricksDataType.INT
            column == "BIGINT" -> DatabricksDataType.BIGINT
            column == "LONG" -> DatabricksDataType.BIGINT
            column == "FLOAT" -> DatabricksDataType.FLOAT
            column == "DOUBLE" -> DatabricksDataType.DOUBLE
            column == "DECIMAL" -> DatabricksDataType.DECIMAL(precision, scale)
            column == "NUMBER" -> DatabricksDataType.DECIMAL(precision, scale)
            column == "STRING" -> DatabricksDataType.STRING
            column == "TEXT" -> DatabricksDataType.STRING
            column == "CHAR" -> DatabricksDataType.CHAR
            column == "VARCHAR" -> DatabricksDataType.VARCHAR
            column == "BINARY" -> DatabricksDataType.BINARY
            column == "DATE" -> DatabricksDataType.DATE
            column == "TIMESTAMP" -> DatabricksDataType.TIMESTAMP
            column == "TIMESTAMP_NTZ" -> DatabricksDataType.TIMESTAMP_NTZ
            column == "VARIANT" -> DatabricksDataType.VARIANT
            column.contains("ARRAY") -> DatabricksDataType.ARRAY
            column.contains("MAP") -> DatabricksDataType.MAP
            column.contains("STRUCT") -> DatabricksDataType.STRUCT
            else -> DatabricksDataType.VARIANT
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

        // If schemas is empty string or null do empty list
        val cleanedSchemas = schemas?.takeIf { it.isNotEmpty() }?.split(",") ?: emptyList()
        val config = DatabricksConfiguration(connectionUri, cleanedSchemas)
        val generatedConfig = DatabricksConfigGenerator.generateConfig(config)

        val json = DatabricksConfigGenerator.jsonFormatter.encodeToString(generatedConfig)

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
