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

    private fun extractCatalog(jdbcUrl: String): String {
        return jdbcUrl.substringAfterLast("/").substringBefore("?")
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

    private fun introspectSchemas(config: RedshiftConfiguration): IntrospectionResult {
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
                TableInfo<RedshiftDataType>(
                    name = "${catalog}.${table.schema.name}.${table.name}",
                    category = when (table.tableType.tableType) {
                        "TABLE" -> Category.TABLE
                        "VIEW" -> Category.VIEW
                        "MATERIALIZED VIEW" -> Category.VIEW
                        else -> throw Exception("Unknown table type: ${table.tableType}")
                    },
                    description = table.remarks,
                    columns = table.columns.map { column ->
                        Column<RedshiftDataType>(
                            name = column.name,
                            type = getRedshiftDataType(
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
        }
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
