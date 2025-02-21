package io.hasura.snowflake.app

import io.hasura.app.base.DatabaseConnection
import io.hasura.app.base.DatabaseSource
import io.hasura.app.default.DefaultConnector
import io.hasura.app.default.DefaultSchemaGeneratorClass
import io.hasura.app.default.DefaultState
import io.hasura.common.ColumnType
import io.hasura.common.DefaultConfiguration
import io.hasura.ndc.ir.QueryResponse
import io.hasura.ndc.ir.RowSet
import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.JsonPrimitive
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
    ): QueryResponse {
        return state.client.getConnection().use { connection ->
            val dslContext = DSL.using(connection)

            val stmt = DSL.using(SQLDialect.DEFAULT)
                .parser()
                .parseResultQuery(sqlRequest.sql)

            val snowflakeStmt = dslContext.renderInlined(stmt)
            println("Snowflake SQL: $snowflakeStmt")

            val result = dslContext.fetch(stmt).intoMaps()

            QueryResponse(
                rowSets = listOf(
                    RowSet(
                        rows = result.map {
                            it.mapValues {
                                when (it.value) {
                                    is String -> JsonPrimitive(it.value as String)
                                    is Int -> JsonPrimitive(it.value as Int)
                                    is Long -> JsonPrimitive(it.value as Long)
                                    is Double -> JsonPrimitive(it.value as Double)
                                    else -> JsonPrimitive(it.value.toString())
                                }
                            }
                        }
                    )
                )
            )
        }
    }
}