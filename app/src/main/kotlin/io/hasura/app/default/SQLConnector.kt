package io.hasura.app.default

import io.hasura.app.base.*
import io.hasura.app.default.*
import io.hasura.common.configuration.*
import io.hasura.app.util.JsonUtils
import io.hasura.ndc.connector.*
import io.hasura.ndc.connector.ConnectorLogger
import io.hasura.ndc.ir.*
import kotlinx.coroutines.*
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
        return coroutineScope {
            val queryExecutor = DefaultConnection(state.client)

            val fixedSql = fixQuotesForFullyQualifiedTableNames(request.sql)
            val sql = DSL.using(SQLDialect.DEFAULT)
                .parser()
                .parseResultQuery(fixedSql)

            queryExecutor.executeSQL(sql)
        }
    }

    // Replace "DB.SCHEMA.TABLE" with "DB"."SCHEMA"."TABLE"
    private fun fixQuotesForFullyQualifiedTableNames(sql: String): String {
        return sql.replace(Regex("\"?([a-zA-Z0-9_]+)\\.([a-zA-Z0-9_]+)\\.([a-zA-Z0-9_]+)\"?")) {
            val (db, schema, table) = it.destructured
            "\"$db\".\"$schema\".\"$table\""
        }
    }
}
