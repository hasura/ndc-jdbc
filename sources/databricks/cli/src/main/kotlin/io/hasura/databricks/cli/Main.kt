package io.hasura.databricks.cli

import io.hasura.databricks.common.*
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

interface IConfigGenerator<T : Configuration, U : ColumnType> {
    fun generateConfig(config: T): DefaultConfiguration<U>
}

data class DatabricksConfiguration(
    override val connectionUri: ConnectionUri,
    val schemas: List<String> = emptyList()
) : Configuration


object DatabricksConfigGenerator : IConfigGenerator<DatabricksConfiguration, DatabricksDataType> {
    val jsonFormatter = Json { prettyPrint = true }

    data class IntrospectionResult(
        val tables: List<TableInfo<DatabricksDataType>> = emptyList(),
        val functions: List<FunctionInfo> = emptyList()
    )

    override fun generateConfig(config: DatabricksConfiguration): DefaultConfiguration<DatabricksDataType> {
        val introspectionResult = introspectSchemas(config)

        return DefaultConfiguration(
            connectionUri = config.connectionUri,
            tables = introspectionResult.tables,
            functions = introspectionResult.functions,
        )
    }

    private fun extractCatalog(jdbcUrl: String): String {
        return jdbcUrl.substringAfter("ConnCatalog=").substringBefore(";")
    }

    private fun includeSchemas(catalog: String, configSchemas: List<String>): (String) -> Boolean {
        return { schema ->
            val schemaCatalog = schema.substringBefore(".")
            when {
                configSchemas.isEmpty() -> {
                    schemaCatalog.contains(catalog)
                    && !schema.contains("information_schema")
                }
                else -> {
                    configSchemas.any {
                        schemaCatalog.contains(catalog)
                        && schema.contains(it)
                        && !schema.contains("information_schema")
                    }
                }
            }
        }
    }

    private fun introspectSchemas(config: DatabricksConfiguration): IntrospectionResult {
        val jdbcUrl = config.connectionUri.resolve()
        val catalog = extractCatalog(jdbcUrl)

        // Schemacrawler options
        val options = SchemaCrawlerOptionsBuilder.newSchemaCrawlerOptions()
            .withLimitOptions(
                LimitOptionsBuilder.builder()
                    .tableTypes(null as String?)
                    .includeSchemas(includeSchemas(catalog, config.schemas))
                    .toOptions()
            )
        val results = SchemaCrawlerUtility.getCatalog(
            DatabaseConnectionSourceBuilder
                .builder(jdbcUrl)
                .build(),
            options
        )

        try {
            val tables = results.tables.map { table ->
                TableInfo<DatabricksDataType>(
                    name = "${catalog}.${table.schema.name}.${table.name}",
                    category = when (table.tableType.tableType) {
                        "TABLE" -> Category.TABLE
                        "VIEW" -> Category.VIEW
                        "MATERIALIZED VIEW" -> Category.VIEW
                        else -> throw Exception("Unknown table type: ${table.tableType}")
                    },
                    description = table.remarks,
                    columns = table.columns.map { column ->
                        Column<DatabricksDataType>(
                            name = column.name,
                            type = getDatabricksDataType(
                                column.columnDataType.name,
                                column.size,
                                column.decimalDigits,
                            ),
                            nullable = column.isNullable,
                            autoIncrement = column.isAutoIncremented,
                            isPrimaryKey = column.isPartOfPrimaryKey
                        )
                    },
                    primaryKeys = emptyList(),
                    foreignKeys = table.foreignKeys.filter {
                      table.name != it.primaryKeyTable.name
                    }.associate { fk ->
                      fk.name to ForeignKeyInfo(
                        columnMapping = fk.columnReferences.associate { 
                          it.primaryKeyColumn.name to it.foreignKeyColumn.name 
                        },
                        foreignCollection = fk.referencedTable.name
                      )
                    }
                )
            }

            return IntrospectionResult(tables = tables)
        } finally {
            // connection.close()
        }
    }

    private fun getDatabricksDataType(
        column: String,
        precision: Int?,
        scale: Int?,
    ): DatabricksDataType {
        return when {
            column == "BOOLEAN" -> DatabricksDataType.BOOLEAN
            column == "TINYINT" -> DatabricksDataType.TINYINT
            column == "SMALLINT" -> DatabricksDataType.SMALLINT
            column == "INT" -> DatabricksDataType.INT
            column == "BIGINT" -> DatabricksDataType.BIGINT
            column == "FLOAT" -> DatabricksDataType.FLOAT
            column == "DOUBLE" -> DatabricksDataType.DOUBLE
            column == "DECIMAL" -> DatabricksDataType.DECIMAL(precision, scale)
            column == "STRING" -> DatabricksDataType.STRING
            column == "CHAR" -> DatabricksDataType.CHAR
            column == "VARCHAR" -> DatabricksDataType.VARCHAR
            column == "BINARY" -> DatabricksDataType.BINARY
            column == "DATE" -> DatabricksDataType.DATE
            column == "TIMESTAMP" -> DatabricksDataType.TIMESTAMP
            column == "TIMESTAMP_NTZ" -> DatabricksDataType.TIMESTAMP_NTZ
            column == "VARIANT" -> DatabricksDataType.VARIANT
            column.contains("ARRAY") -> {
              val type = column.substringAfter("<").substringBefore(">")
              DatabricksDataType.ARRAY(getDatabricksDataType(type, null, null))
            }
            column.contains("MAP") -> {
              val keyType = column.substringAfter("<").substringBefore(",").trim()
              val valueType = column.substringAfter(",").substringBefore(">").trim()
              DatabricksDataType.MAP(
                getDatabricksDataType(keyType, null, null),
                getDatabricksDataType(valueType, null, null)
              )
            }
            column.contains("STRUCT") -> {
              val fields = column.substringAfter("<").substringBefore(">").split(",").map { field ->
                val fieldName = field.substringBefore(":").trim()
                val fieldType = field.substringAfter(":").trim()
                DatabricksDataType.StructField(fieldName, getDatabricksDataType(fieldType, null, null))
              }
              DatabricksDataType.STRUCT(fields)
            }
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
