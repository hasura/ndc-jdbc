package io.hasura.postgres.app

import io.hasura.app.base.DatabaseConnection
import io.hasura.app.base.DatabaseSource
import io.hasura.app.default.DefaultConnector
import io.hasura.app.default.DefaultSchemaGeneratorClass
import io.hasura.app.default.DefaultState
import io.hasura.common.ColumnType
import io.hasura.common.DefaultConfiguration
import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.*
import org.jooq.SQLDialect
import org.jooq.impl.DSL

class ExperimentalSQLConnector<T : ColumnType>(
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

    fun experimentalSQL(
        configuration: DefaultConfiguration<out ColumnType>,
        state: DefaultState<out ColumnType>,
        sqlRequest: SQLRequest
    ): JsonArray {
        return state.client.getConnection().use { connection ->
            val dslContext = DSL.using(connection)

            val stmt = DSL.using(SQLDialect.DEFAULT).parser().parseResultQuery(sqlRequest.sql)

            dslContext.renderInlined(stmt)
            println("Postgres SQL: $stmt")

            val result = dslContext.fetch(stmt).intoMaps()
            result.toJsonArray()
        }
    }

    fun Map<String, Any>.toJsonObject(): JsonObject {
        return buildJsonObject {
            this@toJsonObject.forEach { (key, value) ->
                when (value) {
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

    fun List<Any>.toJsonArray(): JsonArray {
        return buildJsonArray {
            this@toJsonArray.forEach { item ->
                when (item) {
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