package hasura.default

import hasura.base.*
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

            // Iterate over all rows
            while (results.next()) {
                val row = mutableMapOf<String, JsonElement>()

                // Iterate over all columns in the current row
                for (columnIndex in 1..columnCount) {
                    val columnName = metaData.getColumnName(columnIndex)
                    val value = results.getObject(columnIndex)
                    // Convert the value to JsonElement
                    row[columnName] = when (value) {
                        null -> JsonNull
                        is String -> JsonPrimitive(value)
                        is Number -> JsonPrimitive(value)
                        is Boolean -> JsonPrimitive(value)
                        else -> JsonPrimitive(value.toString())
                    }
                }

                rows.add(row)
            }

            results.close()
            statement.close()

            rows
        }
    }
}
