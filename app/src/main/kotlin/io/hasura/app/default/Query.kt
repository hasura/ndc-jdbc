package io.hasura.app.default

import io.hasura.app.base.*
import io.hasura.ndc.connector.*
import io.hasura.ndc.ir.*
import org.jooq.SQLDialect
import org.jooq.impl.DSL.*
import org.jooq.Query as JooqQuery
import org.jooq.Field as JooqField
import org.jooq.*
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.JsonArray
import io.hasura.common.*
import kotlinx.serialization.json.JsonElement

const val variablesCTEName = "vars"
const val resultsCTEName = "results"
const val variableName = "var"
const val indexName = "idx"
const val rowNumberName = "rn"

class DefaultQuery<T : ColumnType> : DatabaseQuery<DefaultConfiguration<T>> {
    override fun generateQuery(
        source: DatabaseSource,
        configuration: DefaultConfiguration<T>,
        request: QueryRequest,
    ): String = generateBaseQuery(source, request).toString()

    override fun generateExplainQuery(
        source: DatabaseSource,
        configuration: DefaultConfiguration<T>,
        request: QueryRequest,
    ): String = "EXPLAIN ${generateBaseQuery(source, request)}"

    private fun generateBaseQuery(
        source: DatabaseSource,
        request: QueryRequest,
    ): JooqQuery {
        val ctx = using(getDatabaseDialect(source))
        return when {
            request.variables.isNotEmpty() -> generateVariableQuery(ctx, request)
            else -> generateSingleQuery(ctx, request)
        }
    }

    fun generateVariableQuery(
        ctx: DSLContext,
        request: QueryRequest,
    ): JooqQuery {
        val varsCTE = buildVariablesCTE(request)
        val resultsCTE = buildResultsCTE(request, varsCTE)
        return buildFinalVariableQuery(ctx, request, varsCTE, resultsCTE)
    }

    fun generateSingleQuery(
        ctx: DSLContext,
        request: QueryRequest,
    ): JooqQuery = ctx
        .select(getFieldSelects(request))
        .from(name(request.collection.split(".")))
        .where(getPredicate(request))
        .orderBy(getOrderByFields(request))
        .limit(request.query.limit?.toInt())
        .offset(request.query.offset?.toInt())

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

    private fun getPredicate(
        request: QueryRequest,
    ): Condition = request.query.predicate?.let { generateCondition(it) } ?: noCondition()

    private fun generateCondition(expr: Expression): Condition = when (expr) {
        is Expression.And ->
            and(expr.expressions.map { generateCondition(it) })

        is Expression.Or ->
            or(expr.expressions.map { generateCondition(it) })

        is Expression.Not ->
            not(generateCondition(expr.expression))

        is Expression.UnaryComparisonOperator -> when (expr.operator) {
            UnaryComparisonOperatorType.IS_NULL ->
                field(getColumnName(expr.column)).isNull
        }

        is Expression.BinaryComparisonOperator -> {
            val field = field(name(getColumnName(expr.column)))
            when (val value = expr.value) {
                is ComparisonValue.Scalar -> {
                    val convertedValue = convertJsonValue(value)

                    when (expr.operator) {
                        "_eq" -> field.eq(convertedValue)
                        "_neq" -> field.ne(convertedValue)
                        "_gt" -> field.gt(convertedValue)
                        "_lt" -> field.lt(convertedValue)
                        "_gte" -> field.ge(convertedValue)
                        "_lte" -> field.le(convertedValue)
                        "_in" -> when (convertedValue) {
                            is List<*> -> field.`in`(convertedValue)
                            else -> throw ConnectorError.NotSupported("IN operator requires an array value")
                        }
                        else -> throw ConnectorError.NotSupported("Unsupported operator: ${expr.operator}")
                    }
                }
                is ComparisonValue.Column ->
                    throw ConnectorError.NotSupported("Column comparisons not supported yet")
                is ComparisonValue.Variable -> {
                    field(getColumnName(expr.column))
                        .eq(field(name(variablesCTEName, variableName)))
                }
            }
        }

        is Expression.Exists ->
            throw ConnectorError.NotSupported("Exists queries not supported yet")
    }

    private fun getOrderByFields(
        request: QueryRequest,
    ): List<SortField<*>> {
        return request.query.orderBy?.elements?.map { element ->
            when (val target = element.target) {
                is OrderByTarget.Column -> {
                    if (target.path.isNotEmpty() || target.fieldPath != null) {
                        throw ConnectorError.NotSupported("Nested fields and relationships are not supported in order by")
                    }
                    when (element.orderDirection) {
                        OrderDirection.ASC -> field(name(target.name)).asc()
                        OrderDirection.DESC -> field(name(target.name)).desc()
                    }
                }

                is OrderByTarget.SingleColumnAggregate,
                is OrderByTarget.StarCountAggregate -> {
                    throw ConnectorError.NotSupported("Aggregate operations are not supported in order by")
                }
            }
        }?: emptyList()
    }

    private fun buildVariablesCTE(
        request: QueryRequest,
    ): CommonTableExpression<Record> {
        return name(variablesCTEName)
            .`as`(
                request.variables.mapIndexed { index, variableMap ->
                    select(
                        *createVariableFields(variableMap),
                        inline(index).`as`(indexName)
                    )
                }.reduce { acc: SelectOrderByStep<Record>, select: SelectSelectStep<Record> ->
                    acc.unionAll(select)
                }
            )
    }

    private fun createVariableFields(variableMap: Map<String, JsonElement>): Array<JooqField<*>> {
        return variableMap.map { (_, value) ->
            inline(convertJsonValue(ComparisonValue.Scalar(value))).`as`(variableName)
        }.toTypedArray()
    }

    private fun buildResultsCTE(
        request: QueryRequest,
        varsCTE: CommonTableExpression<Record>
    ): CommonTableExpression<Record> {
        return name(resultsCTEName)
            .`as`(buildResultsCTEQuery(request, varsCTE))
    }

    private fun buildResultsCTEQuery(
        request: QueryRequest,
        varsCTE: CommonTableExpression<Record>
    ): SelectJoinStep<Record> {
        val fields = getFieldSelectsWithoutAlias(request)
        
        return select(
            *fields.toTypedArray() + buildPartitionFields()
        )
        .from(name(request.collection.split(".")))
        .join(varsCTE)
        .on(getVariablePredicate(request))
    }

    private fun buildPartitionFields(): Array<JooqField<*>> = arrayOf(
        rowNumber()
            .over()
            .partitionBy(field(name(variableName)))
            .orderBy(inline(1))
            .`as`(rowNumberName),
        field(name(indexName))
    )

    private fun getVariablePredicate(request: QueryRequest): Condition =
        request.query.predicate?.let { generateCondition(it) }
            ?: throw ConnectorError.NotSupported("Predicate is required for variable queries")

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

    private fun buildFinalVariableQuery(
        ctx: DSLContext,
        request: QueryRequest,
        varsCTE: CommonTableExpression<Record>,
        resultsCTE: CommonTableExpression<Record>
    ): JooqQuery {
        val fields = getFieldSelects(request)
        val baseQuery = buildBaseQuery(ctx, fields, varsCTE, resultsCTE)
        return baseQuery
            .let { applyVariableLimitOffset(it, request) }
            .let { applyVariableOrdering(it, request) }
    }

    private fun buildBaseQuery(
        ctx: DSLContext,
        fields: List<JooqField<*>>,
        varsCTE: CommonTableExpression<Record>,
        resultsCTE: CommonTableExpression<Record>
    ): SelectWhereStep<Record> = ctx
        .with(varsCTE)
        .with(resultsCTE)
        .select(
            *fields.toTypedArray() +
            arrayOf(
                field(name(indexName))
            )
        )
        .from(resultsCTE)

    private fun applyVariableLimitOffset(
        query: SelectWhereStep<Record>,
        request: QueryRequest
    ): SelectWhereStep<Record> {
        request.query.offset?.let { offset ->
            query.where(field(name(rowNumberName)).gt(inline(offset.toInt())))
        }
        
        request.query.limit?.let { limit ->
            val effectiveLimit = (limit.toInt()) + (request.query.offset?.toInt() ?: 0)
            query.where(field(name(rowNumberName)).le(inline(effectiveLimit)))
        }
        
        return query
    }

    private fun applyVariableOrdering(
        query: SelectWhereStep<Record>,
        request: QueryRequest
    ): SelectSeekStepN<Record> = query.orderBy(
        listOf(field(name(indexName))) +
        getOrderByFields(request)
    )

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
                        field(name(collection,field.column))
                    } else {
                        field(name(field.column))
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
