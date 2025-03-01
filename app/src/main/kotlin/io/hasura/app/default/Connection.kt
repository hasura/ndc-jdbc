package io.hasura.app.default

import io.hasura.app.base.*
import io.hasura.app.util.JsonUtils
import io.hasura.ndc.connector.*
import kotlinx.serialization.json.*
import org.jooq.Record
import org.jooq.ResultQuery
import org.jooq.impl.DSL

interface QueryExecutor {
    suspend fun executeQuery(sql: String): List<Map<String, JsonElement>>
    suspend fun executeSQL(sql: ResultQuery<*>?): JsonArray
}

class DefaultConnection(
    private val connection: DatabaseConnection
) : QueryExecutor {
    override suspend fun executeQuery(sql: String): List<Map<String, JsonElement>> {
        return Telemetry.withActiveSpan("acquireDatabaseConnection") { _ ->
            connection.withConnection { conn ->
                Telemetry.withActiveSpan("queryDatabase") { _ ->
                    val ctx = DSL.using(conn)
                    val results = ctx.fetch(sql).intoMaps()

                    results.map { row ->
                        row.entries.associate { (key, value) ->
                            key to when (value) {
                                null -> JsonNull
                                is String -> JsonUtils.parseStringToJsonElement(value)
                                is Number -> JsonPrimitive(value)
                                is Boolean -> JsonPrimitive(value)
                                is Map<*, *> -> JsonUtils.mapToJsonObject(value.mapKeys { it.key.toString() })
                                is List<*> -> JsonUtils.listToJsonArray(value as List<Any?>)
                                is Array<*> -> JsonUtils.listToJsonArray(value.toList())
                                is ByteArray -> JsonPrimitive(value.contentToString())
                                is Char -> JsonPrimitive(value.toString())
                                else -> JsonUtils.parseStringToJsonElement(value.toString())
                            }
                        }
                    }
                }
            }
        }
    }

    override suspend fun executeSQL(sql: ResultQuery<*>?): JsonArray {
        return Telemetry.withActiveSpan("acquireDatabaseConnection") { _ ->
            connection.withConnection { conn ->
                Telemetry.withActiveSpan("queryDatabase") { _ ->
                    val ctx = DSL.using(conn)

                    val stmt = ctx.renderInlined(sql)
                    ConnectorLogger.logger.debug("SQL: $stmt")

                    val results = ctx.fetch(stmt).intoMaps()

                    JsonUtils.listToJsonArray(results)
                }
            }
        }
    }
}
