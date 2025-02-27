package io.hasura.bigquery.cli

import io.hasura.bigquery.common.BigQueryRangeDataType
import io.hasura.bigquery.common.BigQueryScalarType
import io.hasura.bigquery.common.BigQueryType
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

interface IConfigGenerator<T : Configuration, U : ColumnType> {
    fun generateConfig(config: T): DefaultConfiguration<U>
}

data class BigQueryConfiguration(
    override val connectionUri: ConnectionUri,
) : Configuration


object BigQueryConfigGenerator : IConfigGenerator<BigQueryConfiguration, BigQueryType> {
    val jsonFormatter = Json { prettyPrint = true }

    data class IntrospectionResult(
        val tables: List<TableInfo<BigQueryType>> = emptyList(),
        val functions: List<FunctionInfo> = emptyList()
    )

    // BIGQUERY SQL RESULT DATA CLASSES
    @Serializable
    data class QueryTable(
        val schemaName: String,
        val tableName: String,
        val columns: Map<String, QueryColumn>,
        val uniquenessConstraints: Map<String, List<String>>,
        val foreignRelations: Map<String, QueryRelation>
    )

    @Serializable
    data class QueryColumn(
        val name: String,
        val type: QueryColumnType,
        val nullable: String
    )

    @Serializable
    data class QueryColumnType(
        val scalarType: String? = null,
        val arrayType: ArrayType? = null,
        val rangeType: String? = null,
        val structType: Map<String, QueryColumnType>? = null
    )

    @Serializable
    data class ArrayType(
        val scalarType: String? = null,
        val rangeType: String? = null,
        val structType: Map<String, QueryColumnType>? = null
    )

    @Serializable
    data class QueryRelation(
        val foreignTable: String,
        val columnMapping: Map<String, String>
    )
    // END BIGQUERY SQL RESULT DATA CLASSES

    override fun generateConfig(config: BigQueryConfiguration): DefaultConfiguration<BigQueryType> {
        val introspectionResult = introspectSchemas(config)

        return DefaultConfiguration(
            connectionUri = config.connectionUri,
            tables = introspectionResult.tables,
            functions = introspectionResult.functions,
        )
    }

    private fun checkProjectAndDataset(jdbcUrl: String): Pair<String?, String?> {
        val project = jdbcUrl.takeIf { it.contains("ProjectId=") }
            ?.substringAfter("ProjectId=")
            ?.substringBefore(";")
        val dataset = jdbcUrl.takeIf { it.contains("DefaultDataset=") }
            ?.substringAfter("DefaultDataset=")
            ?.substringBefore(";")

        return Pair(project, dataset)
    }

    private fun introspectSchemas(config: BigQueryConfiguration): IntrospectionResult {
        val jdbcUrl = config.connectionUri.resolve()
        val (project, dataset) = checkProjectAndDataset(jdbcUrl)

        println("Project: $project")
        println("Dataset: $dataset")

        if (project == null) {
            throw IllegalArgumentException("ProjectId not found in connection string, but is required")
        }

        if (dataset == null) {
            throw IllegalArgumentException("DefaultDataset not found in connection string, but is required")
        }

        val ctx = DSL.using(jdbcUrl)

        //language=bigquery
        val sql = """
            WITH column_data AS (
              SELECT
                t.table_name,
                t.table_catalog,
                t.table_schema,
                c.column_name,
                TO_JSON_STRING(STRUCT(
                  c.column_name AS name,
                  CASE
                    WHEN LOWER(c.data_type) LIKE 'array%' THEN
                      JSON_OBJECT('arrayType',
                        # We need to case here on the inner type TRIM(REPLACE(REPLACE(LOWER(c.data_type), 'array<', ''), '>', ''))
                        CASE
                          WHEN LOWER(TRIM(REPLACE(REPLACE(LOWER(c.data_type), 'array<', ''), '>', ''))) LIKE 'struct%' THEN
                            JSON_OBJECT('structType',
                              (SELECT
                                JSON_OBJECT(
                                  ARRAY_AGG(TRIM(SPLIT(TRIM(field), ' ')[OFFSET(0)])),
                                  ARRAY_AGG(JSON_OBJECT('scalarType',
                                    LOWER(
                                        IF(
                                          ARRAY_LENGTH(SPLIT(TRIM(field), ' ')) > 1,
                                          case TRIM(SPLIT(TRIM(field), ' ')[OFFSET(1)])
                                            WHEN 'string' THEN 'string'
                                            WHEN 'bytes' THEN 'bytes'
                                            WHEN 'int64' THEN 'int64'
                                            WHEN 'float64' THEN 'float64'
                                            WHEN 'bool' THEN 'boolean'
                                            WHEN 'numeric' THEN 'numeric'
                                            WHEN 'bignumeric' THEN 'bignumeric'
                                            WHEN 'geography' THEN 'geography'
                                            WHEN 'date' THEN 'date'
                                            WHEN 'datetime' THEN 'datetime'
                                            WHEN 'time' THEN 'time'
                                            WHEN 'timestamp' THEN 'timestamp'
                                            WHEN 'json' THEN 'json'
                                            ELSE 'any'
                                          END,
                                          "any"
                                        )
                                      )
                                  ))
                                )
                              FROM UNNEST(SPLIT(TRIM(REPLACE(REPLACE(LOWER(c.data_type), 'array<struct<', ''), '>>', '')), ',')) AS field)
                            )
                          WHEN LOWER(TRIM(REPLACE(REPLACE(LOWER(c.data_type), 'array<', ''), '>', ''))) LIKE 'range%' THEN
                            JSON_OBJECT('rangeType', TRIM(REPLACE(REPLACE(LOWER(c.data_type), 'range<', ''), '>', '')))
                          ELSE
                            JSON_OBJECT('scalarType', TRIM(REPLACE(REPLACE(LOWER(c.data_type), 'array<', ''), '>', '')))
                        END
                      )
                    WHEN LOWER(c.data_type) LIKE 'range%' THEN
                      JSON_OBJECT('rangeType', TRIM(REPLACE(REPLACE(LOWER(c.data_type), 'range<', ''), '>', '')))
                    WHEN LOWER(c.data_type) LIKE 'struct%' THEN
                      JSON_OBJECT('structType',
                        (SELECT
                          JSON_OBJECT(
                            ARRAY_AGG(TRIM(SPLIT(TRIM(field), ' ')[OFFSET(0)])),
                            ARRAY_AGG(JSON_OBJECT('scalarType',
                              LOWER(
                                  IF(
                                    ARRAY_LENGTH(SPLIT(TRIM(field), ' ')) > 1,
                                    case TRIM(SPLIT(TRIM(field), ' ')[OFFSET(1)])
                                      WHEN 'string' THEN 'string'
                                      WHEN 'bytes' THEN 'bytes'
                                      WHEN 'int64' THEN 'int64'
                                      WHEN 'float64' THEN 'float64'
                                      WHEN 'bool' THEN 'boolean'
                                      WHEN 'numeric' THEN 'numeric'
                                      WHEN 'bignumeric' THEN 'bignumeric'
                                      WHEN 'geography' THEN 'geography'
                                      WHEN 'date' THEN 'date'
                                      WHEN 'datetime' THEN 'datetime'
                                      WHEN 'time' THEN 'time'
                                      WHEN 'timestamp' THEN 'timestamp'
                                      WHEN 'json' THEN 'json'
                                      ELSE 'any'
                                    END,
                                    "any"
                                  )
                                )
                            ))
                          )
                        FROM UNNEST(SPLIT(TRIM(REPLACE(REPLACE(LOWER(c.data_type), 'struct<', ''), '>', '')), ',')) AS field)
                      )
                    ELSE
                      JSON_OBJECT('scalarType',
                        CASE
                          WHEN LOWER(c.data_type) = 'string' THEN 'string'
                          WHEN LOWER(c.data_type) = 'bytes' THEN 'bytes'
                          WHEN LOWER(c.data_type) = 'int64' THEN 'int64'
                          WHEN LOWER(c.data_type) = 'float64' THEN 'float64'
                          WHEN LOWER(c.data_type) = 'bool' THEN 'boolean'
                          WHEN LOWER(c.data_type) = 'numeric' THEN 'numeric'
                          WHEN LOWER(c.data_type) = 'bignumeric' THEN 'bignumeric'
                          WHEN LOWER(c.data_type) = 'geography' THEN 'geography'
                          WHEN LOWER(c.data_type) = 'date' THEN 'date'
                          WHEN LOWER(c.data_type) = 'datetime' THEN 'datetime'
                          WHEN LOWER(c.data_type) = 'time' THEN 'time'
                          WHEN LOWER(c.data_type) = 'timestamp' THEN 'timestamp'
                          WHEN LOWER(c.data_type) = 'json' THEN 'json'
                          ELSE 'any'
                        END
                      )
                  END AS type,
                  CASE WHEN c.is_nullable = 'YES' THEN 'nullable' ELSE 'nonNullable' END AS nullable
                )) AS column_info
              FROM INFORMATION_SCHEMA.TABLES AS t
              JOIN INFORMATION_SCHEMA.COLUMNS AS c
                ON c.table_catalog = t.table_catalog
                AND c.table_schema = t.table_schema
                AND c.table_name = t.table_name
            ),
            columns_struct AS (
              SELECT
                table_name,
                table_catalog,
                table_schema,
                STRUCT(
                  STRING_AGG(
                    CONCAT('"', column_name, '":', column_info),
                    ','
                  ) AS columns_json
                ) AS columns
              FROM column_data
              GROUP BY table_name, table_catalog, table_schema
            ),
            relationship_data AS (
              SELECT
                t.table_name,
                t.table_catalog,
                t.table_schema,
                c.constraint_name,
                TO_JSON_STRING(STRUCT(
                    rc.table_name AS foreign_table,
                    json_object(fc.column_name, rc.column_name) as column_mapping
                )) AS relationship_info
              FROM INFORMATION_SCHEMA.TABLES AS t
              JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS as c
                ON c.table_catalog = t.table_catalog
                AND c.table_schema = t.table_schema
                AND c.table_name = t.table_name
              JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE as rc
                ON c.constraint_catalog = rc.constraint_catalog
                AND c.constraint_schema = rc.constraint_schema
                AND c.constraint_name = rc.constraint_name
              JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE as fc ON c.constraint_name = fc.constraint_name
              WHERE c.constraint_type = 'FOREIGN KEY'
              GROUP BY t.table_name, table_catalog, table_schema, constraint_name, rc.table_name, fc.column_name, rc.column_name
            ),
            relationship_struct AS (
              SELECT
                table_name,
                table_catalog,
                table_schema,
                STRUCT(
                  STRING_AGG(
                    CONCAT('"', constraint_name, '":', relationship_info),
                    ','
                  ) AS relationships_json
                ) AS relationships
              FROM relationship_data
              GROUP BY table_name, table_catalog, table_schema
            ),
            unique_constraint_data AS (
              SELECT
                t.table_name,
                t.table_catalog,
                t.table_schema,
                c.constraint_name,
                TO_JSON_STRING(JSON_ARRAY(cc.column_name)) AS unique_constraint_info
              FROM INFORMATION_SCHEMA.TABLES AS t
              JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS as c
                ON c.table_catalog = t.table_catalog
                AND c.table_schema = t.table_schema
                AND c.table_name = t.table_name
              JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE as cc
                ON c.constraint_name = cc.constraint_name
              WHERE c.constraint_type in ('PRIMARY KEY', 'UNIQUE')
                AND cc.constraint_catalog = c.constraint_catalog
                AND cc.constraint_schema = c.constraint_schema
            ),
            unique_constraint_struct AS (
              SELECT
                table_name,
                table_catalog,
                table_schema,
                STRUCT(
                  STRING_AGG(
                    CONCAT('"', constraint_name, '":', unique_constraint_info),
                    ','
                  ) AS unique_constraint_json
                ) AS unique_constraint
              FROM unique_constraint_data
              GROUP BY table_name, table_catalog, table_schema
            )
            SELECT
              CONCAT('{', STRING_AGG(CONCAT(
                '"', columns_struct.table_name, '": {',
                  '"schemaName": ',
                  '"', CONCAT(columns_struct.table_catalog , '.', columns_struct.table_schema), '", ',
                  '"tableName": ' , '"', columns_struct.table_name, '", '
                  '"columns": {',
                    columns_struct.columns.columns_json,
                  '},',
                  '"uniquenessConstraints": {',
                    coalesce(unique_constraint_struct.unique_constraint.unique_constraint_json, ""),
                  '},',
                  '"foreignRelations": {',
                    coalesce(relationship_struct.relationships.relationships_json, ""),
                  '}'
                '}'
              )), '}') AS result
            FROM columns_struct
            LEFT JOIN relationship_struct ON columns_struct.table_name = relationship_struct.table_name
            LEFT JOIN unique_constraint_struct ON columns_struct.table_name = unique_constraint_struct.table_name
        """.trimIndent()

        val result = ctx.fetchValue(sql)
        val json = result?.toString() ?: "{}"

        val queryTables = jsonFormatter.decodeFromString<Map<String, QueryTable>>(json)

        val introspectionResultTables = queryTables.map { (tableName, table) ->
            TableInfo<BigQueryType>(
                name = tableName,
                description = null,
                category = Category.TABLE,
                columns = table.columns.map { (_, column) ->
                    Column(
                        name = column.name,
                        type = mapToBigQueryType(column.type),
                        description = null,
                        nullable = column.nullable == "nullable",
                        autoIncrement = false,
                    )
                },
                primaryKeys = emptyList(),
                foreignKeys = emptyMap(),
            )
        }

        return IntrospectionResult(tables = introspectionResultTables)
    }

    private fun mapToBigQueryType(queryColumnType: QueryColumnType): BigQueryType {
        return when {
            queryColumnType.scalarType != null -> {
                val scalarType = queryColumnType.scalarType
                BigQueryType.ScalarType(BigQueryScalarType.valueOf(scalarType.uppercase()))
            }
            queryColumnType.arrayType != null -> {
                val arrayType = queryColumnType.arrayType
                BigQueryType.ArrayType(mapToBigQueryType(arrayType))
            }
            queryColumnType.rangeType != null -> {
                val rangeType = queryColumnType.rangeType
                BigQueryType.RangeType(BigQueryRangeDataType.valueOf(rangeType.uppercase()))
            }
            queryColumnType.structType != null -> {
                val structType = queryColumnType.structType
                val fields = structType.mapValues { (_, value) -> mapToBigQueryType(value) }
                BigQueryType.StructType(fields)
            }
            else -> throw IllegalArgumentException("Invalid QueryColumnType")
        }
    }

    private fun mapToBigQueryType(arrayType: ArrayType): BigQueryType {
        return when {
            arrayType.scalarType != null -> {
                BigQueryType.ScalarType(BigQueryScalarType.valueOf(arrayType.scalarType.uppercase()))
            }
            arrayType.rangeType != null -> {
                BigQueryType.RangeType(BigQueryRangeDataType.valueOf(arrayType.rangeType.uppercase()))
            }
            arrayType.structType != null -> {
                val structType = arrayType.structType
                val fields = structType.mapValues { (_, value) -> mapToBigQueryType(value) }
                BigQueryType.StructType(fields)
            }
            else -> throw IllegalArgumentException("Invalid QueryColumnType")
        }
    }

    private fun mapToBigQueryType(scalarType: String): BigQueryType {
        return BigQueryType.ScalarType(BigQueryScalarType.valueOf(scalarType.uppercase()))
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

        val config = BigQueryConfiguration(connectionUri)
        val generatedConfig = BigQueryConfigGenerator.generateConfig(config)

        val json = BigQueryConfigGenerator.jsonFormatter.encodeToString(generatedConfig)

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
