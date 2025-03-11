package io.hasura.phoenix.cli

import io.hasura.common.*
import io.hasura.phoenix.common.PhoenixDataType
import kotlinx.cli.*
import kotlinx.serialization.json.Json
import org.jooq.impl.DSL
import java.io.File
import java.sql.JDBCType
import java.sql.Types
import kotlin.system.exitProcess

interface IConfigGenerator<T : Configuration, U : ColumnType> {
    fun generateConfig(config: T): DefaultConfiguration<U>
}

data class PhoenixConfiguration(
    override val connectionUri: ConnectionUri,
    val schemas: List<String> = emptyList()
) : Configuration


object PhoenixConfigGenerator : IConfigGenerator<PhoenixConfiguration, PhoenixDataType> {
    val jsonFormatter = Json { prettyPrint = true }

    data class IntrospectionResult(
        val tables: List<TableInfo<PhoenixDataType>> = emptyList(),
        val functions: List<FunctionInfo> = emptyList()
    )

    override fun generateConfig(config: PhoenixConfiguration): DefaultConfiguration<PhoenixDataType> {
        val introspectionResult = introspectSchemas(config)

        return DefaultConfiguration(
            connectionUri = config.connectionUri,
            tables = introspectionResult.tables,
            functions = introspectionResult.functions,
        )
    }

    private fun introspectSchemas(config: PhoenixConfiguration): IntrospectionResult {
        val jdbcUrl = config.connectionUri.resolve()
        val ctx = DSL.using(jdbcUrl)

        val stmt = when {
            config.schemas.isNotEmpty() -> "SELECT * FROM SYSTEM.CATALOG WHERE TABLE_SCHEM IN (${config.schemas.joinToString { "'$it'" }})"
            else -> "SELECT * FROM SYSTEM.CATALOG WHERE TABLE_SCHEM != 'SYSTEM' OR TABLE_SCHEM IS NULL"
        }

        val result = ctx.fetch(stmt)

        val groupedBySchema = result.groupBy { it["TABLE_SCHEM"] as String? }

        val tables = groupedBySchema.flatMap { (schema, records) ->
            val tablesMap = records.groupBy { it["TABLE_NAME"] as String }

            tablesMap.map { (tableName, records) ->
                val columns = records
                    .filter { it["COLUMN_NAME"] != null }
                    .map {
                        val columnFamily = it["COLUMN_FAMILY"] as String?
                        val columnName = it["COLUMN_NAME"] as String

                        Column(
                            name = if (columnFamily != null && columnFamily != "0") "$columnFamily.$columnName" else columnName,
                            description = null,
                            type = PhoenixDataType(
                                translatePhoenixDataTypeToSqlType(
                                    it["DATA_TYPE"] as Int?,
                                    jdbcUrl.contains("phoenix:thin", ignoreCase = true)
                                )
                            ),
                            nullable = it["NULLABLE"] == 1,
                            autoIncrement = it["IS_AUTOINCREMENT"] == "YES",
                            isPrimaryKey = it["KEY_SEQ"] != null,
                        )
                    }

                TableInfo(
                    name = if (schema != null) "$schema.$tableName" else tableName,
                    description = null,
                    category = if (records.any { it["TABLE_TYPE"] == "u" }) Category.TABLE else Category.VIEW,
                    columns = columns,
                    primaryKeys = records
                        .filter { it["COLUMN_NAME"] != null && it["KEY_SEQ"] != null }
                        .map { it["COLUMN_NAME"] as String },
                    foreignKeys = emptyMap(),
                )
            }
        }

        return IntrospectionResult(tables = tables)
    }

    private fun translatePhoenixDataTypeToSqlType(phoenixDataType: Int?, isThinClient: Boolean): String {
        val t = phoenixDataType
        val sqlType = when {
            // For non-thin clients, use the original type or OTHER if null
            !isThinClient -> t ?: Types.OTHER

            // For thin clients, map Phoenix types to SQL types
            t == null -> Types.OTHER
            // Standard SQL types
            t == Types.TINYINT -> Types.TINYINT           // -6
            t == Types.BIGINT -> Types.BIGINT             // -5
            t == Types.VARBINARY -> Types.VARBINARY       // -3
            t == Types.BINARY -> Types.BINARY             // -2
            t == Types.CHAR -> Types.CHAR                 // 1
            t == Types.DECIMAL -> Types.DECIMAL           // 3
            t == Types.INTEGER -> Types.INTEGER           // 4
            t == Types.SMALLINT -> Types.SMALLINT         // 5
            t == Types.FLOAT -> Types.FLOAT               // 6
            t == Types.DOUBLE -> Types.DOUBLE             // 8
            t == Types.VARCHAR -> Types.VARCHAR           // 9
            // Phoenix-specific types
            t == 10 -> Types.SMALLINT                     // UNSIGNED_SMALLINT
            t == 11 -> Types.FLOAT                        // UNSIGNED_FLOAT
            t == 13 || t == 14 || t == 15 -> Types.VARCHAR // Custom/Unsupported
            t == 16 -> Types.BOOLEAN                      // BOOLEAN
            t == 18 -> Types.ARRAY                        // ARRAY
            t == 19 || t == 20 -> Types.VARBINARY         // Phoenix VARBINARY
            t == 91 -> Types.DATE                         // DATE
            t == 92 -> Types.TIME                         // TIME
            t == 93 -> Types.TIMESTAMP                    // TIMESTAMP

            // Try to use the JDBC type directly if it's a valid type
            JDBCType.valueOf(t) != null -> t

            // Unknown type
            else -> throw IllegalArgumentException("Unknown Phoenix data type: $t")
        }

        return JDBCType.valueOf(sqlType).name
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
        val config = PhoenixConfiguration(connectionUri, cleanedSchemas)
        val generatedConfig = PhoenixConfigGenerator.generateConfig(config)

        val json = PhoenixConfigGenerator.jsonFormatter.encodeToString(generatedConfig)

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
