package io.hasura.athena.cli

import io.hasura.common.*
import io.hasura.ndc.ir.json
import io.hasura.athena.common.AthenaDataType
import kotlinx.cli.*
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.*
import org.jooq.impl.DSL
import kotlin.system.exitProcess

interface IConfigGenerator<T : Configuration, U : ColumnType> {
    fun generateConfig(config: T): DefaultConfiguration<U>
}

data class AthenaConfiguration(
    override val connectionUri: ConnectionUri,
    val catalogs: List<String> = emptyList(),
    val schemas: List<String> = emptyList(),
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

        val tableNameSQL =
            "concat_ws('.' ,tables.table_catalog, tables.table_schema, tables.table_name)"


        // Athena doesn't support foreign keys in the same way as Snowflake,
        // so we'll skip that part of the introspection

        // Query to get tables and columns
        val sql = """
        SELECT
            $tableNameSQL as table_name_sql,
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
                            cols.data_type,
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
        GROUP BY $tableNameSQL, tables.table_type
        """

        val tables = try {
            ctx.fetch(sql).map { row ->
                val columnsJsonStr = row.get("columns", java.sql.Array::class.java)?.array as? Array<*>

                // Convert the array to a proper JSON string
                val columnsJsonString = columnsJsonStr?.joinToString(prefix = "[", postfix = "]") { it.toString() } ?: "[]"

                // Parse the entire JSON array at once
                val columns = try {
                    val columnsList = json.decodeFromString<List<JsonObject>>(columnsJsonString)

                    columnsList.mapNotNull { columnObj ->
                        try {
                            val name = columnObj["name"]?.jsonPrimitive?.content ?: return@mapNotNull null
                            val description = columnObj["description"]?.jsonPrimitive?.content ?: ""
                            val typeStr = columnObj["type"]?.jsonPrimitive?.content ?: return@mapNotNull null
                            val nullable = columnObj["nullable"]?.jsonPrimitive?.content?.toBoolean() ?: false

                            Column<AthenaDataType>(
                                name = name,
                                description = description,
                                type = mapStringToAthenaDataType(typeStr),
                                nullable = nullable,
                                autoIncrement = false
                            )
                        } catch (e: Exception) {
                            println("Error parsing column object: $columnObj")
                            null
                        }
                    }
                } catch (e: Exception) {
                    println("Error parsing columns JSON array: $e")
                    emptyList()
                }

                TableInfo<AthenaDataType>(
                    name = row.get("table_name_sql", String::class.java),
                    category = when (val tableType = row.get("table_type", String::class.java)) {
                        "BASE TABLE" -> Category.TABLE
                        "VIEW" -> Category.VIEW
                        else -> throw Exception("Unknown table type: $tableType")
                    },
                    description = row.get("description", String::class.java) ?: "",
                    columns = columns,
                    primaryKeys = emptyList(),
                    foreignKeys = emptyMap()
                )
            }
        } catch (e: Exception) {
            println("Error executing SQL: $e")
            throw e
        }

        return IntrospectionResult(tables = tables)
    }

    private fun mapStringToAthenaDataType(typeStr: String): AthenaDataType {
      val column = typeStr.lowercase()
      return when {
          column == "varchar" -> AthenaDataType.VARCHAR
          column == "string" -> AthenaDataType.STRING
          column == "tinyint" -> AthenaDataType.TINYINT
          column == "smallint" -> AthenaDataType.SMALLINT
          column == "integer" -> AthenaDataType.INTEGER
          column == "int" -> AthenaDataType.INTEGER
          column == "bigint" -> AthenaDataType.BIGINT
          column == "boolean" -> AthenaDataType.BOOLEAN
          column == "float" -> AthenaDataType.FLOAT
          column == "double" -> AthenaDataType.DOUBLE
          column == "date" -> AthenaDataType.DATE
          column == "timestamp" -> AthenaDataType.TIMESTAMP
          column == "binary" -> AthenaDataType.BINARY
          column.contains("map") -> AthenaDataType.MAP
          column == "json" -> AthenaDataType.JSON
          column.contains("decimal") -> {
              // Handle decimal with precision and scale
              if (typeStr.startsWith("decimal")) {
                  val regex = """decimal\((\d+),\s*(\d+)\)""".toRegex()
                  val matchResult = regex.find(typeStr)
                  if (matchResult != null) {
                      val (precision, scale) = matchResult.destructured
                      AthenaDataType.DECIMAL(precision.toInt(), scale.toInt())
                  } else {
                      throw IllegalStateException("Couldn't parse type string: $typeStr")
                  }
              } else {
                    throw IllegalStateException("Unknown type: $typeStr")
              }
          }
          else -> AthenaDataType.JSON
      }
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
