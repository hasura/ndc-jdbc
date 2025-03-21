package io.hasura.app.default

import io.hasura.app.base.DatabaseConnection
import io.hasura.app.base.DatabaseSource
import io.hasura.common.ColumnType
import io.hasura.common.DefaultConfiguration
import io.hasura.ndc.connector.Connector
import io.hasura.ndc.connector.ConnectorLogger
import io.hasura.ndc.connector.Telemetry
import io.hasura.ndc.ir.*
import io.micrometer.core.instrument.MeterRegistry
import io.opentelemetry.context.Context
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import java.nio.file.Path

class DefaultState<T : ColumnType>(
    val configuration: DefaultConfiguration<T>,
    val client: DatabaseConnection
)

open class DefaultConnector<T : ColumnType>(
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
                aggregates = JsonObject(emptyMap()),
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
        return Telemetry.withActiveSpan("queryExplain") { _ ->
            ExplainResponse(
                details = mapOf(
                    "plan" to "Jdbc query plan"
                )
            )
        }
    }

    fun cleanUpRows(request: QueryRequest, rows: List<Map<String, JsonElement>>?): List<Map<String, JsonElement>>? {
        return rows?.map { row ->
            row.entries.mapNotNull { (key, value) ->
                request.query.fields?.keys?.find { it.equals(key, ignoreCase = true) }?.let { matchedKey ->
                    matchedKey to value
                }
            }.toMap()
        }?.map { row ->
            row.filterKeys { it != indexName }
        }
    }

    fun cleanUpAggregates(
        request: QueryRequest,
        aggregates: List<Map<String, JsonElement>>?
    ): List<Map<String, JsonElement>>? {
        return aggregates?.map { agg ->
            agg.entries.mapNotNull { (key, value) ->
                request.query.aggregates?.keys?.find { it.equals(key, ignoreCase = true) }?.let { matchedKey ->
                    matchedKey to value
                }
            }.toMap()
        }?.map { agg ->
            agg.filterKeys { it != indexName }
        }
    }

    override suspend fun query(
        configuration: DefaultConfiguration<T>,
        state: DefaultState<T>,
        request: QueryRequest
    ): QueryResponse {
        return coroutineScope {
            val currentContext = Context.current()
            val query = DefaultQuery(
                configuration,
                state,
                schemaGenerator,
                source,
                request
            )
            val queryExecutor = DefaultConnection(state.client)

            ConnectorLogger.logger.debug("Request: $request")

            // Handle regular query results
            val rowsAsync = async {
                request.query.fields?.let {
                    Telemetry.withActiveSpanContext(currentContext, "executeRowsQuery") { _ ->
                        queryExecutor.executeQuery(query.generateQuery())
                    }
                }
            }

            // Handle aggregates if present
            val aggregatesAsync = async {
                request.query.aggregates?.let {
                    Telemetry.withActiveSpanContext(currentContext, "executeAggregatesQuery") { _ ->
                        queryExecutor.executeQuery(query.generateAggregateQuery())
                    }
                }
            }

            val rows = rowsAsync.await()
            val aggregates = aggregatesAsync.await()

            ConnectorLogger.logger.debug("Rows: $rows")
            ConnectorLogger.logger.debug("Aggregates: $aggregates")

            ConnectorLogger.logger.debug(
                "Cleaned Aggregates: ${
                    cleanUpAggregates(
                        request,
                        aggregates
                    )?.map { JsonObject(it) }?.firstOrNull()
                }"
            )

            Telemetry.withActiveSpanContext(currentContext, "processResults") { _ ->
                val variables = request.variables
                when {
                    variables?.isEmpty() == true ->
                        QueryResponse(rowSets = emptyList())

                    variables == null ->
                        QueryResponse(
                            rowSets = listOf(
                                RowSet(
                                rows = cleanUpRows(request, rows),
                            aggregates = cleanUpAggregates(request, aggregates)
                                ?.map { JsonObject(it) }
                                ?.firstOrNull()
                        )))

                    else ->
                        QueryResponse(rowSets = variables.indices.map { index ->
                            RowSet(
                                rows = rows?.filter { row ->
                                    row[indexName]?.toString()?.toIntOrNull() == index
                                }?.let { filteredRows ->
                                    cleanUpRows(request, filteredRows)
                                },
                                aggregates = cleanUpAggregates(request, aggregates)
                                    ?.map { JsonObject(it) }
                                    ?.getOrNull(index)
                            )
                        })
                }
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

    override suspend fun queryRel(
        configuration: DefaultConfiguration<T>,
        state: DefaultState<T>,
        request: QueryRel
    ): JsonArray {
        throw UnsupportedOperationException("SQL is not supported")
    }
}
