package io.hasura.postgres.cli

import io.hasura.common.*
import io.hasura.postgres.PGColumnType
import kotlinx.cli.*
import org.jooq.impl.DSL
import java.io.File
import kotlin.system.exitProcess

interface IConfigGenerator<T : Configuration, U : ColumnType> {
    fun generateConfig(config: T): DefaultConfiguration<U>
}

data class PostgresConfig(
    override val connectionUri: ConnectionUri,
) : Configuration

object PostgresConfigGenerator : IConfigGenerator<PostgresConfig, PGColumnType> {
    override fun generateConfig(config: PostgresConfig): DefaultConfiguration<PGColumnType> {
        val ctx = DSL.using(config.connectionUri.resolve())

        val tables = ctx.meta()
            .filterSchemas { it.name == "public" }
            .tables
            .map {
                TableInfo(
                    name = it.name,
                    description = it.comment,
                    category = Category.TABLE,
                    columns = it.fields().map { field ->
                        Column(
                            name = field.name,
                            description = field.comment,
                            type = PGColumnType(
                                typeName = field.dataType.typeName,
                            ),
                            nullable = field.dataType.nullable(),
                            autoIncrement = field.dataType.identity(),
                            isPrimaryKey = it.references.any { ref -> ref.fields.contains(field) },
                        )
                    },
                    primaryKeys = it.primaryKey?.fields?.map { it.name } ?: emptyList(),
                    foreignKeys = emptyMap()
                )
            }

        return DefaultConfiguration(
            connectionUri = config.connectionUri,
            tables = tables,
        )
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

        val config = PostgresConfig(connectionUri)
        val generatedConfig = PostgresConfigGenerator.generateConfig(config)

        val json = io.hasura.ndc.ir.json.encodeToString(generatedConfig)

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

    if (args.isEmpty()) {
        println("Subcommand is required (ex: update)")
        exitProcess(1)
    }

    parser.parse(args)
}
