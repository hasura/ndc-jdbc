package io.hasura.app.default

import io.hasura.app.base.*
import io.hasura.ndc.connector.*
import io.hasura.ndc.ir.*
import io.micrometer.core.instrument.MeterRegistry
import kotlinx.serialization.json.*
import java.nio.file.Path
import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.Json
import io.hasura.common.*
import kotlinx.coroutines.*

class DefaultState<T : ColumnType>(
    val configuration: DefaultConfiguration<T>,
    val client: DatabaseConnection
)

class DefaultConnector<T : ColumnType>(
    private val source: DatabaseSource,
    private val connection: (DefaultConfiguration<T>) -> DatabaseConnection,
    private val schemaGenerator: DefaultSchemaGeneratorClass<T>,
    private val configSerializer: KSerializer<DefaultConfiguration<T>>,
) : Connector<DefaultConfiguration<T>, DefaultState<T>> {
    override suspend fun parseConfiguration(configurationDir: Path): DefaultConfiguration<T> {
        val configFile = configurationDir.resolve("configuration.json")
        return try {
            val jsonString = configFile.toFile().readText()
            Json.decodeFromString(configSerializer, jsonString)
        } catch (e: Exception) {
            ConnectorLogger.logger.error("Fatal error: Failed to parse configuration file: ${e.message}")
            System.exit(1)
            throw IllegalStateException()
        }
    }

    override suspend fun tryInitState(
        configuration: DefaultConfiguration<T>,
        metrics: MeterRegistry
    ): DefaultState<T> {
        return DefaultState(configuration, connection(configuration))
    }

    override fun getCapabilities(configuration: DefaultConfiguration<T>): Capabilities {
        return Capabilities(
            mutation = MutationCapabilities(
                explain = null,
                transactional = null
            ),
            query = QueryCapabilities(
                aggregates = null,
                variables = JsonObject(emptyMap()),
                explain = null,
                nestedFields = null
            ),
            relationships = null
        )
    }

    override suspend fun getSchema(configuration: DefaultConfiguration<T>): SchemaResponse {
        return Telemetry.withActiveSpan("getSchema") { _ ->
            schemaGenerator.getSchema(configuration)
        }
    }

    override suspend fun queryExplain(
        configuration: DefaultConfiguration<T>,
        state: DefaultState<T>,
        request: QueryRequest
    ): ExplainResponse {
        return Telemetry.withActiveSpan("queryExplain") { span ->
            ExplainResponse(
                details = mapOf(
                    "plan" to "Jdbc query plan"
                )
            )
        }
    }

    override suspend fun query(
        configuration: DefaultConfiguration<T>,
        state: DefaultState<T>,
        request: QueryRequest
    ): QueryResponse {
        return Telemetry.withActiveSpan("acquireDatabaseConnection") { _ ->
            val connection = state.client.getConnection()
            try {
                Telemetry.withActiveSpan("queryDatabase") { _ ->
                    coroutineScope {
                        val query: DefaultQuery<T> = DefaultQuery(configuration, state, schemaGenerator)
                        val queryExecutor = DefaultConnection(state.client)

                        val rowsAsync = async {
                            queryExecutor.executeQuery(
                                query.generateQuery(source, request)
                            )
                        }


                        val aggregatesAsync = request.query.aggregates?.let {
                            async {
                                queryExecutor.executeQuery(
                                    query.generateAggregateQuery(source, request)
                                ).firstOrNull()?.let { JsonObject(it) }
                            }
                        }

                        val rows = rowsAsync.await()
                        val aggregates = aggregatesAsync?.await()


                        val rowSets = if (request.variables.isNotEmpty()) {
                            val groupedRows = rows.groupBy { row ->
                                (row[indexName] as JsonPrimitive).content.toInt()
                            }
                            request.variables.indices.map { varIndex ->
                                val variableRows = groupedRows[varIndex]?.map { row ->
                                    row.filterKeys { it != indexName }
                                } ?: emptyList()
                                RowSet(rows = variableRows)
                            }

                        } else {
                            val rowSet = RowSet(rows = rows, aggregates = aggregates)
                            listOf(rowSet)
                        }

                        QueryResponse(rowSets = rowSets)
                    }}

                    } finally {
                        connection.close()
                    }

        }
    }

    override suspend fun mutationExplain(
        configuration: DefaultConfiguration<T>,
        state: DefaultState<T>,
        request: MutationRequest
    ): ExplainResponse {
        throw UnsupportedOperationException("Mutation explain is not supported")
    }

    override suspend fun mutation(
        configuration: DefaultConfiguration<T>,
        state: DefaultState<T>,
        request: MutationRequest
    ): MutationResponse {
        throw UnsupportedOperationException("Mutation is not supported")
    }
}
