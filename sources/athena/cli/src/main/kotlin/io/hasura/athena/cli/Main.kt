package io.hasura.athena.cli

import io.hasura.common.*
import io.hasura.ndc.ir.json
import io.hasura.athena.common.AthenaDataType
import kotlinx.cli.*
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import org.jooq.impl.DSL
import kotlin.system.exitProcess

interface IConfigGenerator<T : Configuration, U : ColumnType> {
    fun generateConfig(config: T): DefaultConfiguration<U>
}

data class AthenaConfiguration(
    override val connectionUri: ConnectionUri,
    val catalogs: List<String> = emptyList(),
    val schemas: List<String> = emptyList(),
    val fullyQualifyTableNames: Boolean = false,
    val workgroup: String = "primary"
) : Configuration


object AthenaConfigGenerator : IConfigGenerator<AthenaConfiguration, AthenaDataType> {
    val jsonFormatter = Json { prettyPrint = true }

    override fun generateConfig(config: AthenaConfiguration): DefaultConfiguration<AthenaDataType> {
        val introspectionResult = introspectSchemas(config)

        return DefaultConfiguration(
            connectionUri = config.connectionUri,
            tables = introspectionResult.tables,
            functions = introspectionResult.functions,
        )
    }

    data class IntrospectionResult(
        val tables: List<TableInfo<AthenaDataType>> = emptyList(),
        val functions: List<FunctionInfo> = emptyList()
    )

    @Serializable
    data class RawForeignKeyConstraint(
        val foreign_collection: List<String>,
        val column_mapping: Map<String, String>
    )

    private fun introspectSchemas(config: AthenaConfiguration): IntrospectionResult {
        val jdbcUrl = config.connectionUri.resolve()

        val ctx = DSL.using(jdbcUrl)

        // Get current catalog
        val currentCatalog = ctx.fetchOne("SELECT current_catalog as catalog")
            ?.get("catalog", String::class.java)
            ?: throw Exception("Could not determine current catalog")

        // Define catalog/schema selection criteria
        val catalogSelection = if (config.catalogs.isEmpty()) {
            "'$currentCatalog'"
        } else {
            config.catalogs.joinToString("', '", prefix = "'", postfix = "'")
        }

        val schemaFilter = if (config.schemas.isEmpty()) {
            "table_schema NOT IN ('information_schema')"
        } else {
            "table_schema IN (${config.schemas.joinToString(", ") { "'$it'" }})"
        }

        // val tableNameSQL = if (config.fullyQualifyTableNames) {
        //     "array_join(tables.table_catalog, tables.table_schema, tables.table_name)"
        // } else {
        //     "tables.table_name"
        // }

        // Athena doesn't support foreign keys in the same way as Snowflake,
        // so we'll skip that part of the introspection

        // Query to get tables and columns
        val sql = """
        SELECT
            tables.table_name,
            tables.table_type,
            '' as description,
            ARRAY_AGG(
                JSON_FORMAT(
                CAST(
                    MAP(
                        ARRAY['name', 'description', 'type', 'nullable'],
                        ARRAY[
                            cols.column_name,
                            '',
                            JSON_FORMAT(
                                CAST(
                                    MAP(
                                        ARRAY['scalar_type'],
                                        ARRAY[cols.data_type]
                                    ) AS JSON
                                )
                            ),
                            CAST(CASE WHEN cols.is_nullable = 'YES' THEN 'true' ELSE 'false' END AS VARCHAR)
                        ]
                    ) AS JSON
                ))
            ) as columns
        FROM (
            SELECT
                table_catalog,
                table_schema,
                table_name,
                table_type
            FROM information_schema.tables
            WHERE table_catalog IN ($catalogSelection)
            AND $schemaFilter
            AND table_type IN ('BASE TABLE', 'VIEW')
        ) AS tables
        LEFT OUTER JOIN information_schema.columns cols
            ON cols.table_schema = tables.table_schema
            AND cols.table_name = tables.table_name
        GROUP BY tables.table_name, tables.table_type
        """

        val tables = try {
            println("Executing SQL: $sql")

            val tablesDebug = ctx.fetch(sql)
            println("tablesDebug: ${tablesDebug}")


            ctx.fetch(sql).map { row ->
                val columnsJson = row.get("columns", java.sql.Array::class.java)?.array as? Array<*> ?: emptyArray<Any>()


                val columns = if (columnsJson.isNullOrEmpty()) {
                    emptyList()
                } else {
                    try {
                        json.decodeFromString<List<Column<AthenaDataType>>>("[${columnsJson.joinToString(",")}]")
                    } catch (e: Exception) {
                        println("Error parsing columns JSON $columnsJson: $e")
                        emptyList()
                    }
                }

                TableInfo<AthenaDataType>(
                    name = row.get("table_name", String::class.java),
                    category = when (val tableType = row.get("table_type", String::class.java)) {
                        "BASE TABLE" -> Category.TABLE
                        "VIEW" -> Category.VIEW
                        else -> throw Exception("Unknown table type: $tableType")
                    },
                    description = row.get("description", String::class.java) ?: "",
                    columns = columns,
                    primaryKeys = emptyList(), // Athena doesn't support primary keys in the traditional sense
                    foreignKeys = emptyMap()  // Athena doesn't support foreign keys
                )
            }
        } catch (e: Exception) {
            println("Error executing SQL: $e")
            emptyList()
        }

        println("tables: ${tables}")

        return IntrospectionResult(tables = tables)
    }
}

@OptIn(ExperimentalCli::class)
class UpdateCommand : Subcommand("update", "Update configuration file") {
    private val jdbcUrl by option(
        ArgType.String,
        shortName = "j",
        fullName = "jdbc-url",
        description = "JDBC URL or environment variable for Athena connection"
    ).required()

    private val catalogs by option(
        ArgType.String,
        shortName = "c",
        fullName = "catalogs",
        description = "Comma-separated list of catalogs to introspect"
    )

    private val schemas by option(
        ArgType.String,
        shortName = "s",
        fullName = "schemas",
        description = "Comma-separated list of schemas to introspect"
    )

    private val workgroup by option(
        ArgType.String,
        shortName = "w",
        fullName = "workgroup",
        description = "Athena workgroup to use"
    ).default("primary")

    private val fullyQualifyTableNames by option(
        ArgType.Boolean,
        shortName = "f",
        fullName = "fully-qualify-table-names",
        description = "Whether to fully qualify table names in the configuration",
    ).default(false)

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

        val cleanedCatalogs = catalogs?.takeIf { it.isNotEmpty() }?.split(",") ?: emptyList()
        val cleanedSchemas = schemas?.takeIf { it.isNotEmpty() }?.split(",") ?: emptyList()

        val config = AthenaConfiguration(
            connectionUri = connectionUri,
            catalogs = cleanedCatalogs,
            schemas = cleanedSchemas,
            fullyQualifyTableNames = fullyQualifyTableNames,
            workgroup = workgroup
        )

        val generatedConfig = AthenaConfigGenerator.generateConfig(config)

        // Use the shared Json formatter
        val json = AthenaConfigGenerator.jsonFormatter.encodeToString(generatedConfig)

        // Write the generated configuration to the output file
        val file = java.io.File(outfile)
        try {
            file.writeText(json)
        } catch (e: Exception) {
            println("Failed to write configuration to file: ${e.message}")
            exitProcess(1)
        }

        exitProcess(0)
    }
}

fun main(args: Array<String>) {
    val parser = ArgParser("athena-cli", strictSubcommandOptionsOrder = true)
    parser.subcommands(UpdateCommand())

    val modifiedArgs = args.toMutableList()

    // Handle empty schema arguments
    val schemasIndex = modifiedArgs.indexOf("--schemas")
    if (schemasIndex != -1 && schemasIndex + 1 < modifiedArgs.size) {
        val nextArg = modifiedArgs.getOrNull(schemasIndex + 1)
        if (nextArg?.startsWith("-") == true) {
            modifiedArgs.add(schemasIndex + 1, "")
        }
    }

    // Handle empty catalog arguments
    val catalogsIndex = modifiedArgs.indexOf("--catalogs")
    if (catalogsIndex != -1 && catalogsIndex + 1 < modifiedArgs.size) {
        val nextArg = modifiedArgs.getOrNull(catalogsIndex + 1)
        if (nextArg?.startsWith("-") == true) {
            modifiedArgs.add(catalogsIndex + 1, "")
        }
    }

    if (modifiedArgs.isEmpty()) {
        println("Subcommand is required (ex: update)")
        exitProcess(1)
    }

    parser.parse(modifiedArgs.toTypedArray())
}
