package io.hasura.app.default

import io.hasura.app.base.*
import io.hasura.ndc.connector.*
import io.hasura.ndc.ir.*
import org.jooq.impl.DSL
import org.jooq.SQLDialect
import org.jooq.impl.DSL.*
import org.jooq.Query as JooqQuery
import org.jooq.Field as JooqField
import org.jooq.*
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.JsonArray

const val variablesCTEName = "vars"
const val resultsCTEName = "results"
const val collectionName = "coll"
const val variableName = "var"
const val indexName = "idx"
const val rowNumberName = "rn"

class DefaultQuery<T : ColumnType> : DatabaseQuery<DefaultConfiguration<T>> {
    override fun generateQuery(
        source: DatabaseSource,
        configuration: DefaultConfiguration<T>,
        request: QueryRequest
    ): String {
        ConnectorLogger.logger.info("generateSql: $request")

        val ctx = DSL.using(getDatabaseDialect(source))

        val query = when {
            request.variables.isNotEmpty() -> generateCTEQuery(ctx, request)
            else -> generateSingleQuery(ctx, request)
        }

        ConnectorLogger.logger.info("SQL: ${query.toString()}")

        return query.toString()
    }

    fun generateCTEQuery(
        ctx: DSLContext,
        request: QueryRequest,
    ): JooqQuery {
        val varsCTE = buildVariablesCTE(request)
        val resultsCTE = buildResultsCTE(request, varsCTE)
        val fields = getFieldSelects(request)
        var sql = ctx
            .with(varsCTE)
            .with(resultsCTE)
            .select(fields)
            .from(resultsCTE)
            .orderBy(DSL.field(DSL.name(indexName)))

        return sql
    }

    fun generateSingleQuery(
        ctx: DSLContext,
        request: QueryRequest,
    ): JooqQuery {
        val fields = getFieldSelects(request)
        val sql = ctx
            .select(fields)
            .from(request.collection)

        request.query.orderBy?.let { orderBy ->
            val orderFields = orderBy.elements.map { element ->
                when (val target = element.target) {
                    is OrderByTarget.Column -> {
                        if (target.path.isNotEmpty() || target.fieldPath != null) {
                            throw ConnectorError.NotSupported(
                                "Nested fields and relationships are not supported in order by"
                            )
                        }
                        when (element.orderDirection) {
                            OrderDirection.ASC -> DSL.field(target.name).asc()
                            OrderDirection.DESC -> DSL.field(target.name).desc()
                        }
                    }
                    is OrderByTarget.SingleColumnAggregate, 
                    is OrderByTarget.StarCountAggregate -> {
                        throw ConnectorError.NotSupported("Aggregate operations are not supported in order by")
                    }
                }
            }

            request.query.predicate?.let { predicate ->
                val condition = generateCondition(predicate)
                sql.where(condition)
            }

            sql.orderBy(orderFields)
        }

        request.query.limit?.let { sql.limit(it.toInt()) }
        request.query.offset?.let { sql.offset(it.toInt()) }

        return sql
    }

    private fun getDatabaseDialect(type: DatabaseSource): SQLDialect = when (type) {
        DatabaseSource.SNOWFLAKE -> SQLDialect.SNOWFLAKE
        DatabaseSource.BIGQUERY -> SQLDialect.BIGQUERY
        DatabaseSource.REDSHIFT -> SQLDialect.REDSHIFT
    }

    private fun getFieldSelects(
        request: QueryRequest,
    ): List<JooqField<*>> = getFieldSelectsHelper(request, true)

    private fun getFieldSelectsWithoutAlias(
        request: QueryRequest,
    ): List<JooqField<*>> = getFieldSelectsHelper(request, false)

    private fun buildVariablesCTE(
        request: QueryRequest,
    ): CommonTableExpression<Record> {
        return DSL
            .name(variablesCTEName)
            .`as`(
                request.variables.mapIndexed { index, variable ->
                    DSL.select(
                        *variable.map { (_, value) ->
                            DSL.inline(value).`as`(variableName)
                        }.toTypedArray(),
                        DSL.inline(index).`as`(indexName)
                    )
                }.reduce { acc: SelectOrderByStep<Record>, select: SelectSelectStep<Record> ->
                    acc.unionAll(select)
                }
            )
    }

    private fun buildResultsCTE(
        request: QueryRequest,
        varsCTE: CommonTableExpression<Record>
    ): CommonTableExpression<Record> {
        val fields = getFieldSelectsWithoutAlias(request)
        return DSL
            .name(resultsCTEName)
            .`as`(
                DSL.select(
                    *fields.toTypedArray() + arrayOf(
                        rowNumber()
                            .over()
                            .partitionBy(DSL.field(DSL.name(indexName)))
                            .orderBy(DSL.field(DSL.name(indexName)))
                            .`as`(rowNumberName),
                        DSL.field(DSL.name(indexName))
                    )
                )
                .from(request.collection)
                .join(varsCTE)
                .on(request.query.predicate?.let { generateCondition(it) } 
                    ?: throw ConnectorError.NotSupported("Predicate is required for variable queries")
                )
            )
    }

    private fun getVariableJoinColumn(request: QueryRequest): String =
        request.query.predicate?.let { predicate ->
            when (predicate) {
                is Expression.BinaryComparisonOperator -> {
                    val value = predicate.value
                    if (value is ComparisonValue.Variable) {
                        value.name
                    } else {
                        throw ConnectorError.NotSupported("Predicate must use a variable comparison")
                    }
                }
                else -> throw ConnectorError.NotSupported("Predicate must be a binary comparison")
            }
        } ?: throw ConnectorError.NotSupported("No predicate comparison found")

    private fun generateCondition(expr: Expression): Condition = when (expr) {
        is Expression.And -> 
            DSL.and(expr.expressions.map { generateCondition(it) })
        
        is Expression.Or -> 
            DSL.or(expr.expressions.map { generateCondition(it) })
        
        is Expression.Not -> 
            DSL.not(generateCondition(expr.expression))
        
        is Expression.UnaryComparisonOperator -> when (expr.operator) {
            UnaryComparisonOperatorType.IS_NULL -> 
                DSL.field(getColumnName(expr.column)).isNull
        }
        
        is Expression.BinaryComparisonOperator -> {
            val field = DSL.field(getColumnName(expr.column))
            when (val value = expr.value) {
                is ComparisonValue.Scalar -> {
                    val convertedValue = convertJsonValue(value)
                    
                    when (expr.operator) {
                        "eq" -> field.eq(convertedValue)
                        "neq" -> field.ne(convertedValue)
                        "gt" -> field.gt(convertedValue)
                        "lt" -> field.lt(convertedValue)
                        "gte" -> field.ge(convertedValue)
                        "lte" -> field.le(convertedValue)
                        "in" -> when (convertedValue) {
                            is List<*> -> field.`in`(convertedValue)
                            else -> throw ConnectorError.NotSupported("IN operator requires an array value")
                        }
                        else -> throw ConnectorError.NotSupported("Unsupported operator: ${expr.operator}")
                    }
                }
                is ComparisonValue.Column -> 
                    throw ConnectorError.NotSupported("Column comparisons not supported yet")
                is ComparisonValue.Variable -> {
                    DSL.field(getColumnName(expr.column))
                        .eq(DSL.field("${DSL.name(variablesCTEName)}.${DSL.name(variableName)}"))
                }
            }
        }
        
        is Expression.Exists -> 
            throw ConnectorError.NotSupported("Exists queries not supported yet")
    }

    private fun getColumnName(target: ComparisonTarget): String = when (target) {
        is ComparisonTarget.Column -> {
            if (target.path.isNotEmpty() || target.fieldPath != null) {
                throw ConnectorError.NotSupported("Nested fields and relationships are not supported in predicates")
            }
            target.name
        }
        is ComparisonTarget.RootCollectionColumn -> {
            if (target.fieldPath != null) {
                throw ConnectorError.NotSupported("Nested fields are not supported in predicates")
            }
            target.name
        }
    }

    private fun convertJsonPrimitive(primitive: JsonPrimitive): Any = when {
        primitive.isString -> primitive.content
        primitive.content.toLongOrNull() != null -> primitive.content.toLong()
        primitive.content.toDoubleOrNull() != null -> primitive.content.toDouble()
        primitive.content == "true" -> true
        primitive.content == "false" -> false
        else -> primitive.content
    }

    private fun convertJsonArray(array: JsonArray): List<Any> = 
        array.map { element ->
            when (element) {
                is JsonPrimitive -> convertJsonPrimitive(element)
                else -> element.toString()
            }
        }

    private fun convertJsonValue(value: ComparisonValue.Scalar): Any = 
        when (val jsonElement = value.value) {
            is JsonPrimitive -> convertJsonPrimitive(jsonElement)
            is JsonArray -> convertJsonArray(jsonElement)
            else -> jsonElement.toString()
        }

    private fun getFieldSelectsHelper(
        request: QueryRequest,
        applyAlias: Boolean = true,
        collection: String? = null
    ): List<JooqField<*>> =
        request.query.fields?.map { (alias, field) ->
            when (field) {
                is Field.Column -> {
                    val columnRef = if (collection != null) {
                        DSL.field("${collection}.${field.column}")
                    } else {
                        DSL.field(field.column)
                    }
                    if (applyAlias) {
                        columnRef.`as`(alias)
                    } else {
                        columnRef
                    }
                }
                else -> throw ConnectorError.NotSupported("Unsupported: Relationships are not supported")
            }
        } ?: emptyList()
}
