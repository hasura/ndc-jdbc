package io.hasura.snowflake.cli

import io.hasura.common.configuration.*
import io.hasura.ndc.ir.json
import io.hasura.snowflake.common.SnowflakeDataType
import kotlinx.cli.*
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import org.jooq.impl.DSL
import kotlin.system.exitProcess
import kotlinx.serialization.json.*

interface IConfigGenerator<T : Configuration<U>, U : ColumnType> {
    fun generateConfig(config: T): DefaultConfiguration<U>
}

data class SnowflakeConfiguration(
    override val version: Version,
    override val connectionUri: ConnectionUri,
    val schemas: List<String> = emptyList(),
    val fullyQualifyTableNames: Boolean = false
) : Configuration<SnowflakeDataType> {
    override fun toDefaultConfiguration(): DefaultConfiguration<SnowflakeDataType> {
        return DefaultConfiguration(
            version = this.version,
            connectionUri = this.connectionUri,
            schemas = this.schemas,
            tables = emptyList(),
            functions = emptyList(),
            nativeOperations = emptyMap()
        )
    }
}


object SnowflakeConfigGenerator : IConfigGenerator<SnowflakeConfiguration, SnowflakeDataType> {
    val jsonFormatter = Json { prettyPrint = true }

    override fun generateConfig(config: SnowflakeConfiguration): DefaultConfiguration<SnowflakeDataType> {
        val introspectionResult = introspectSchemas(config)

        return DefaultConfiguration(
            version = config.version,
            connectionUri = config.connectionUri,
            tables = introspectionResult.tables,
            functions = introspectionResult.functions,
        )
    }

    data class IntrospectionResult(
        val tables: List<TableInfo<SnowflakeDataType>> = emptyList(),
        val functions: List<FunctionInfo> = emptyList()
    )

    @Serializable
    data class RawForeignKeyConstraint(
        val foreign_collection: List<String>,
        val column_mapping: Map<String, String>
    )

    private fun introspectSchemas(config: SnowflakeConfiguration): IntrospectionResult {
        val jdbcUrl = config.connectionUri.resolve()

        val modifiedJdbcUrl = jdbcUrl.find { it == '?' }
            ?.let { "$jdbcUrl&JDBC_QUERY_RESULT_FORMAT=JSON" }
            ?: "$jdbcUrl?JDBC_QUERY_RESULT_FORMAT=JSON"

        val ctx = DSL.using(modifiedJdbcUrl)

        val database = ctx.fetchOne("SELECT CURRENT_DATABASE() AS DATABASE")
            ?.get("DATABASE", String::class.java)
            ?: throw Exception("Could not determine current database")

        val schemaSelection = config.schemas.joinToString(", ") { "'$it'" }

        ctx.fetch("SHOW PRIMARY KEYS IN DATABASE $database")
        ctx.fetch("SHOW IMPORTED KEYS IN DATABASE $database")

        val tableNameSQL = if (config.fullyQualifyTableNames) {
            "array_construct(tables.TABLE_DATABASE, tables.TABLE_SCHEMA, tables.TABLE_NAME)"
        } else {
            "array_construct(tables.TABLE_NAME)"
        }

        val fkeySQL = if (config.fullyQualifyTableNames) {
            """array_construct(foreign_keys."pk_database_name", foreign_keys."pk_schema_name", foreign_keys."pk_table_name")"""
        } else {
            """array_construct(foreign_keys."pk_table_name")"""
        }

        //language=Snowflake
        val sql = """
        SELECT
            $tableNameSQL AS TABLE_NAME,
            tables.TABLE_TYPE,
            tables.COMMENT as DESCRIPTION,
            cols.COLUMNS,
            pks.PK_COLUMNS,
            fks.FOREIGN_KEYS
        FROM (
                SELECT CATALOG_NAME AS TABLE_DATABASE, db_tables.TABLE_SCHEMA, db_tables.TABLE_NAME, db_tables.COMMENT, db_tables.TABLE_TYPE
                FROM INFORMATION_SCHEMA.TABLES AS db_tables
                CROSS JOIN INFORMATION_SCHEMA.INFORMATION_SCHEMA_CATALOG_NAME
                WHERE db_tables.TABLE_TYPE IN ('BASE TABLE', 'VIEW')
                AND ${if (config.schemas.isEmpty()) "db_tables.TABLE_SCHEMA <> 'INFORMATION_SCHEMA'" else "db_tables.TABLE_SCHEMA IN ($schemaSelection)"}
        ) tables
        LEFT OUTER JOIN (
            SELECT
                columns.TABLE_SCHEMA,
                columns.TABLE_NAME,
                array_agg(object_construct(
                    'name', columns.column_name,
                    'description', columns.comment,
                    'type', case
                        when columns.data_type = 'NUMBER' then
                            object_construct(
                                'scalar_type', 'NUMBER',
                                'precision', columns.numeric_precision,
                                'scale', columns.numeric_scale
                            )
                        else
                            object_construct('scalar_type', columns.data_type)
                    end,
                    'nullable', to_boolean(columns.is_nullable),
                    'auto_increment', to_boolean(columns.is_identity)
                )) as COLUMNS
            FROM INFORMATION_SCHEMA.COLUMNS columns
            GROUP BY columns.TABLE_SCHEMA, columns.TABLE_NAME
        ) AS cols ON cols.TABLE_SCHEMA = tables.TABLE_SCHEMA AND cols.TABLE_NAME = tables.TABLE_NAME
        LEFT OUTER JOIN (
            SELECT
                primary_keys."schema_name" AS TABLE_SCHEMA,
                primary_keys."table_name" AS TABLE_NAME,
                array_agg(primary_keys."column_name") WITHIN GROUP (ORDER BY primary_keys."key_sequence" ASC) AS PK_COLUMNS
            FROM table(RESULT_SCAN(LAST_QUERY_ID(-2))) AS primary_keys
            GROUP BY primary_keys."schema_name", primary_keys."table_name"
        ) AS pks ON pks.TABLE_SCHEMA = tables.TABLE_SCHEMA AND pks.TABLE_NAME = tables.TABLE_NAME
        LEFT OUTER JOIN (
            SELECT
                fks."fk_schema_name" AS TABLE_SCHEMA,
                fks."fk_table_name" AS TABLE_NAME,
                object_agg(
                    fks."fk_name", to_variant(fks."constraint")
                ) AS FOREIGN_KEYS
            FROM (
                SELECT
                    foreign_keys."fk_schema_name",
                    foreign_keys."fk_table_name",
                    foreign_keys."fk_name",
                    object_construct(
                        'foreign_collection', $fkeySQL,
                        'column_mapping', object_agg(foreign_keys."fk_column_name", to_variant(foreign_keys."pk_column_name"))
                    ) AS "constraint"
                FROM table(RESULT_SCAN(LAST_QUERY_ID(-1))) AS foreign_keys
                GROUP BY foreign_keys."fk_schema_name", foreign_keys."fk_table_name", foreign_keys."fk_name", foreign_keys."pk_database_name", foreign_keys."pk_schema_name", foreign_keys."pk_table_name"
            ) AS fks
            GROUP BY fks."fk_schema_name", fks."fk_table_name"
        ) AS fks ON fks.TABLE_SCHEMA = tables.TABLE_SCHEMA AND fks.TABLE_NAME = tables.TABLE_NAME
        """.trimIndent()

        val tables = ctx.fetch(sql).map { row ->
            val columns = row
                .get("COLUMNS", String::class.java)
                .let { json.decodeFromString<List<Column<SnowflakeDataType>>>(it) }

            TableInfo<SnowflakeDataType>(
                name = row.get("TABLE_NAME", List::class.java).joinToString("."),
                category = when (val tableType = row.get("TABLE_TYPE", String::class.java)) {
                    "BASE TABLE" -> Category.TABLE
                    "VIEW" -> Category.VIEW
                    else -> throw Exception("Unknown table type: $tableType")
                },
                description = row.get("DESCRIPTION", String::class.java),
                columns = columns,
                primaryKeys = columns.filter { it.isPrimaryKey == true }.map { it.name },
                foreignKeys = row.get("FOREIGN_KEYS", String::class.java)
                    ?.let {
                        json.decodeFromString<Map<String, RawForeignKeyConstraint>>(it)
                            .mapValues { (_, value) ->
                                ForeignKeyInfo(
                                    columnMapping = value.column_mapping,
                                    foreignCollection = value.foreign_collection.joinToString(".")
                                )
                            }
                    } ?: emptyMap()
            )


        }

        return IntrospectionResult(tables = tables)
    }
}

@OptIn(ExperimentalCli::class)
class UpgradeCommand : Subcommand("upgrade", "Upgrade configuration file to V1") {
    private val configFile by option(
        ArgType.String,
        shortName = "c",
        fullName = "config-file",
        description = "Path to configuration file to upgrade"
    ).default("configuration.json")

    private val outfile by option(
        ArgType.String,
        shortName = "o",
        fullName = "outfile",
        description = "Output file for upgraded configuration"
    ).default("configuration.json")

    override fun execute() {
        // Read the existing configuration file
        val file = java.io.File(configFile)
        if (!file.exists()) {
            println("Configuration file not found: $configFile")
            exitProcess(1)
        }

        try {
            // Read the file as a string
            val jsonString = file.readText()

            // Parse the JSON to check if it has a version field
            val jsonElement = json.parseToJsonElement(jsonString)
            val jsonObject = jsonElement.jsonObject

            if (jsonObject.containsKey("version")) {
                println("Configuration already has a version field. No upgrade needed.")
                exitProcess(0)
            }

            // Add the version field to the JSON
            val mutableMap = jsonObject.toMutableMap()
            mutableMap["version"] = kotlinx.serialization.json.JsonPrimitive("v1")

            // Convert back to JSON string with pretty printing
            val upgradedJson = Json(json) {
                prettyPrint = true
            }.encodeToString(kotlinx.serialization.json.JsonObject(mutableMap))

            // Write the upgraded configuration
            val outputFile = java.io.File(outfile)
            outputFile.writeText(upgradedJson)

            println("Successfully upgraded configuration to version v1")
            exitProcess(0)
        } catch (e: Exception) {
            println("Failed to upgrade configuration: ${e.message}")
            exitProcess(1)
        }
    }
}

@OptIn(ExperimentalCli::class)
class UpdateCommand : Subcommand("update", "Update configuration file") {
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

        // If schemas is empty string or null do empty list
        val cleanedSchemas = schemas?.takeIf { it.isNotEmpty() }?.split(",") ?: emptyList()
        val config = SnowflakeConfiguration(Version.V1, connectionUri, cleanedSchemas, fullyQualifyTableNames)
        val generatedConfig = SnowflakeConfigGenerator.generateConfig(config)

        // Use the shared Json formatter
        val json = SnowflakeConfigGenerator.jsonFormatter.encodeToString(generatedConfig)

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
    val parser = ArgParser("snowflake-cli", strictSubcommandOptionsOrder = true)

    parser.subcommands(UpdateCommand(), UpgradeCommand())


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
