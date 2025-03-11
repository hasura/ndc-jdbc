package io.hasura.snowflake.app

import io.hasura.app.base.*
import io.hasura.app.default.*
import io.hasura.common.configuration.*
import io.hasura.ndc.connector.ConnectorLogger
import io.hasura.ndc.ir.*
import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.*
import org.jooq.SQLDialect
import org.jooq.impl.DSL

class SQLConnector<T : ColumnType>(
    private val source: DatabaseSource,
    private val connection: (DefaultConfiguration<T>) -> DatabaseConnection,
    private val schemaGenerator: DefaultSchemaGeneratorClass<T>,
    private val configSerializer: KSerializer<DefaultConfiguration<T>>,
) : DefaultConnector<T>(
    source = source,
    connection = connection,
    schemaGenerator = schemaGenerator,
    configSerializer = configSerializer
) {
    override suspend fun sql(
        configuration: DefaultConfiguration<T>,
        state: DefaultState<T>,
        request: SQLRequest
    ): JsonArray {
        return state.client.getConnection().use { connection ->
            val dslContext = DSL.using(connection)

            val fixedSql = fixQuotesForFullyQualifiedTableNames(request.sql)
            val stmt = DSL.using(SQLDialect.DEFAULT)
                .parser()
                .parseResultQuery(fixedSql)

            val statement = dslContext.renderInlined(stmt)
            ConnectorLogger.logger.debug("SQL: $statement")

            val result = dslContext.fetch(stmt).intoMaps()
            result.toJsonArray()
        }
    }

    // Replace "DB.SCHEMA.TABLE" with "DB"."SCHEMA"."TABLE"
    private fun fixQuotesForFullyQualifiedTableNames(sql: String): String {
        return sql.replace(Regex("\"?([a-zA-Z0-9_]+)\\.([a-zA-Z0-9_]+)\\.([a-zA-Z0-9_]+)\"?")) {
            val (db, schema, table) = it.destructured
            "\"$db\".\"$schema\".\"$table\""
        }
    }

    fun Map<String, Any?>.toJsonObject(): JsonObject {
        return buildJsonObject {
            this@toJsonObject.forEach { (key, value) ->
                when (value) {
                    null -> put(key, JsonNull)
                    is String -> put(key, value)
                    is Number -> put(key, value)
                    is Boolean -> put(key, value)
                    is Map<*, *> -> put(key, (value as Map<String, Any>).toJsonObject())
                    is List<*> -> put(key, (value as List<Any>).toJsonArray())
                    else -> put(key, value.toString())
                }
            }
        }
    }

    fun List<Any?>.toJsonArray(): JsonArray {
        return buildJsonArray {
            this@toJsonArray.forEach { item ->
                when (item) {
                    null -> add(JsonNull)
                    is String -> add(item)
                    is Number -> add(item)
                    is Boolean -> add(item)
                    is Map<*, *> -> add((item as Map<String, Any>).toJsonObject())
                    is List<*> -> add((item as List<Any>).toJsonArray())
                    else -> add(item.toString())
                }
            }
        }
    }
}
