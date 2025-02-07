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
        return Telemetry.withActiveSpan("getSchema") { span ->
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
        return Telemetry.withActiveSpan("acquireDatabaseConnection") { span ->
            val connection = state.client.getConnection()
            try {
                Telemetry.withActiveSpan("queryDatabase") { span ->
                    val query: DefaultQuery<T> = DefaultQuery()
                    val queryExecutor = DefaultConnection(state.client)
                    val rows = queryExecutor.executeQuery(
                        query.generateQuery(source, configuration, request)
                    )

                    val rowSet = RowSet(
                        rows = rows
                    )

                    QueryResponse(rowSets = listOf(rowSet))
                }
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
        return Telemetry.withActiveSpan("mutationExplain") { span ->
            ExplainResponse(
                details = mapOf(
                    "plan" to "Jdbc mutation plan"
                )
            )
        }
    }

    override suspend fun mutation(
        configuration: DefaultConfiguration<T>,
        state: DefaultState<T>,
        request: MutationRequest
    ): MutationResponse {
        return Telemetry.withActiveSpan("mutation") { span ->
            MutationResponse(
                operationResults = listOf(
                    MutationOperationResults.Procedure(
                        result = JsonPrimitive("Mutation result")
                    )
                )
            )
        }
    }
}
