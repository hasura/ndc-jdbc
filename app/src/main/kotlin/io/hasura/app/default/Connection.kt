package io.hasura.app.default

import io.hasura.app.base.*
import io.hasura.ndc.connector.*
import kotlinx.serialization.json.*

interface QueryExecutor {
    suspend fun executeQuery(sql: String): List<Map<String, JsonElement>>
}

class DefaultConnection(
    private val connection: DatabaseConnection
) : QueryExecutor {
    override suspend fun executeQuery(sql: String): List<Map<String, JsonElement>> {
        return connection.withConnection { conn ->
            val statement = conn.prepareStatement(sql)
            val results = statement.executeQuery()

            val rows = mutableListOf<Map<String, JsonElement>>()

            // Get metadata about the columns
            val metaData = results.metaData
            val columnCount = metaData.columnCount

            Telemetry.withActiveSpan("processResults") { _ ->
                // Iterate over all rows
                while (results.next()) {
                    val row = mutableMapOf<String, JsonElement>()

                    // Iterate over all columns in the current row
                    for (columnIndex in 1..columnCount) {
                        val columnName = metaData.getColumnName(columnIndex)
                        val value = results.getObject(columnIndex)

                        row[columnName] = when (value) {
                            null -> JsonNull
                            is String -> {
                              if (value.startsWith("{") || value.startsWith("[")) {
                                parseStringToJsonElement(value)
                              } else {
                                JsonPrimitive(value)
                              }
                            }
                            is Number -> JsonPrimitive(value)
                            is Boolean -> JsonPrimitive(value)
                            else -> parseStringToJsonElement(value.toString())
                        }
                    }

                    rows.add(row)
                }

                results.close()
                statement.close()
            }

            rows // Last expression in the lambda is the return value
        }
    }

    private fun parseStringToJsonElement(value: String): JsonElement {
        return try {
            Json.parseToJsonElement(value)
        } catch (e: Exception) {
            JsonPrimitive(value)
        }
    }
}
