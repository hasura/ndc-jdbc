package io.hasura.app.default

import io.hasura.app.base.*
import io.hasura.common.*
import io.hasura.ndc.connector.*
import io.hasura.ndc.ir.*
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonPrimitive
import org.jooq.*
import org.jooq.Field as JooqField
import org.jooq.Query as JooqQuery
import org.jooq.SQLDialect
import org.jooq.impl.DSL
import org.jooq.impl.DSL.*
import org.jooq.impl.SQLDataType

const val variablesCTEName = "vars"
const val resultsCTEName = "results"
const val variableName = "var"
const val indexName = "idx"
const val rowNumberName = "rn"

class DefaultQuery<T : ColumnType>(
    private val configuration: DefaultConfiguration<T>,
    private val state: DefaultState<T>,
    private val schemaGenerator: DefaultSchemaGeneratorClass<T>,
    private val source: DatabaseSource,
    private val request: QueryRequest,
) : DatabaseQuery<DefaultConfiguration<T>> {
    override fun generateQuery(): String = generateBaseQuery().toString()

    override fun generateExplainQuery(): String = "EXPLAIN ${generateBaseQuery()}"

    private fun generateBaseQuery(): JooqQuery {
        val ctx = using(getDatabaseDialect(source))
        val sql = when {
            request.variables?.isNotEmpty() == true -> generateVariableQuery(ctx)
            else -> generateSingleQuery(ctx)
        }

        ConnectorLogger.logger.debug("Generated sql: ${sql}")

        return sql
    }

    fun generateVariableQuery(
        ctx: DSLContext,
    ): JooqQuery {
        val varsCTE = buildVariablesCTE()
        val resultsCTE = buildResultsCTE(varsCTE)
        return buildFinalVariableQuery(ctx, varsCTE, resultsCTE)
    }

    fun generateSingleQuery(
        ctx: DSLContext,
    ): JooqQuery = ctx
        .select(
            getFieldSelects()
                .withCasts(request.collection)
                .withAliases()
        )
        .from(name(request.collection.split(".")))
        .where(getPredicate())
        .orderBy(getOrderByFields())
        .limit(request.query.limit?.toInt())
        .offset(request.query.offset?.toInt())

    override fun generateAggregateQuery(): String {
        val ctx = using(getDatabaseDialect(source))
        val sql = ctx
            .select(translateIRAggregateFields(request.query.aggregates!!))
            .from(
                ctx
                    .select(asterisk())
                    .from(name(request.collection.split(".")))
                    .where(getPredicate())
                    .orderBy(getOrderByFields())
                    .limit(request.query.limit?.toInt())
                    .offset(request.query.offset?.toInt())
            )
            .toString()

        ConnectorLogger.logger.debug("Generated aggregate sql: ${sql}")

        return sql
    }

    private fun getDatabaseDialect(type: DatabaseSource): SQLDialect = when (type) {
        DatabaseSource.SNOWFLAKE -> SQLDialect.SNOWFLAKE
        DatabaseSource.BIGQUERY -> SQLDialect.BIGQUERY
        DatabaseSource.REDSHIFT -> SQLDialect.REDSHIFT
    }

    private fun getPredicate(): Condition = request.query.predicate?.let { generateCondition(it) } ?: noCondition()

    private fun generateCondition(expr: Expression): Condition = when (expr) {
        is Expression.And -> and(expr.expressions.map { generateCondition(it) })
        is Expression.Or -> or(expr.expressions.map { generateCondition(it) })
        is Expression.Not -> not(generateCondition(expr.expression))
        is Expression.UnaryComparisonOperator -> handleUnaryComparison(expr)
        is Expression.BinaryComparisonOperator -> handleBinaryComparison(expr)
        is Expression.Exists -> throw ConnectorError.NotSupported("Exists queries not supported yet")
    }

    private fun handleUnaryComparison(expr: Expression.UnaryComparisonOperator): Condition = when (expr.operator) {
        UnaryComparisonOperatorType.IS_NULL -> field(name(getColumnName(expr.column))).isNull
    }

    private fun handleBinaryComparison(expr: Expression.BinaryComparisonOperator): Condition {
        val field = field(name(getColumnName(expr.column)))
        
        return when (val value = expr.value) {
            is ComparisonValue.Scalar -> handleScalarComparison(field, expr.operator, value)
            is ComparisonValue.Column -> handleColumnComparison(field, expr.operator, value)
            is ComparisonValue.Variable -> handleVariableComparison(field, expr.operator, value)
        }
    }

    private fun handleBasicComparison(
        field: JooqField<Any>,
        operator: String,
        compareWith: JooqField<Any>
    ): Condition = when (operator) {
        "_eq" -> field.eq(compareWith)
        "_neq" -> field.ne(compareWith)
        "_gt" -> field.gt(compareWith)
        "_lt" -> field.lt(compareWith)
        "_gte" -> field.ge(compareWith)
        "_lte" -> field.le(compareWith)
        "_regex", "_iregex", "_nregex", "_niregex" -> handleRegexComparison(
            field,
            compareWith,
            operator.startsWith("_n"),
            operator.contains("i"),
            source
        )
        "_like" -> field.like(compareWith.cast(SQLDataType.VARCHAR))
        "_ilike" -> field.likeIgnoreCase(compareWith.cast(SQLDataType.VARCHAR))
        "_nlike" -> field.notLike(compareWith.cast(SQLDataType.VARCHAR))
        "_nilike" -> field.notLikeIgnoreCase(compareWith.cast(SQLDataType.VARCHAR))
        else -> throw ConnectorError.NotSupported("Unsupported operator: $operator")
    }

    private fun handleScalarComparison(
        field: JooqField<Any>,
        operator: String,
        value: ComparisonValue.Scalar
    ): Condition {
        val convertedValue = convertJsonValue(value)
        return when (operator) {
            "_in" -> when (convertedValue) {
                is List<*> -> field.`in`(convertedValue)
                else -> throw ConnectorError.NotSupported("IN operator requires an array value")
            }
            else -> handleBasicComparison(field, operator, inline(convertedValue))
        }
    }

    private fun handleColumnComparison(
        field: JooqField<Any>,
        operator: String,
        value: ComparisonValue.Column
    ): Condition {
        val otherField = field(name(getColumnName(value.column)))
        return when (operator) {
            "_in" -> throw ConnectorError.NotSupported("IN operator not supported in column comparison")
            else -> handleBasicComparison(field, operator, otherField)
        }
    }

    private fun handleVariableComparison(
        field: JooqField<Any>,
        operator: String,
        value: ComparisonValue.Variable
    ): Condition {
        return when (request.variables?.isNotEmpty() == true) {
            true -> {
                val varField = field(name(variablesCTEName, variableName))
                when (operator) {
                    "_like", "_ilike" -> handleBasicComparison(field, operator, varField)
                    else -> handleBasicComparison(field, operator, varField)
                }
            }
            false -> field.eq(value.name)
        }
    }

    private fun handleRegexComparison(
        field: org.jooq.Field<*>,
        compareWith: org.jooq.Field<*>,
        isNegated: Boolean,
        isCaseInsensitive: Boolean,
        source: DatabaseSource
    ): Condition {
        val expr = when (getDatabaseDialect(source)) {
            SQLDialect.SNOWFLAKE -> {
                if (isCaseInsensitive) {
                    condition("REGEXP_LIKE({0}, {1}, 'i')", field, compareWith)
                } else {
                    condition("REGEXP_LIKE({0}, {1})", field, compareWith)
                }
            }
            
            SQLDialect.BIGQUERY -> {
                if (isCaseInsensitive) {
                    condition("REGEXP_CONTAINS(LOWER({0}), LOWER({1}))", field, compareWith)
                } else {
                    condition("REGEXP_CONTAINS({0}, {1})", field, compareWith)
                }
            }
            
            SQLDialect.REDSHIFT -> {
                if (isCaseInsensitive) {
                    condition("REGEXP_LIKE(LOWER({0}), LOWER({1}))", field, compareWith)
                } else {
                    condition("REGEXP_LIKE({0}, {1})", field, compareWith)
                }
            }
            
            else -> throw ConnectorError.NotSupported("Regex operations not supported for this database")
        }
        return if (isNegated) not(expr) else expr
    }

    private fun getOrderByFields(): List<SortField<*>> {
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

    private fun buildVariablesCTE(): CommonTableExpression<Record> {
        return name(variablesCTEName)
            .`as`(
                request.variables?.mapIndexed { index, variableMap ->
                    select(
                        *createVariableFields(variableMap),
                        inline(index).`as`(indexName)
                    )
                }?.reduce { acc: SelectOrderByStep<Record>, select: SelectSelectStep<Record> ->
                    acc.unionAll(select)
                }
            )
    }

    private fun createVariableFields(variableMap: Map<String, JsonElement>): Array<JooqField<*>> {
        return variableMap.map { (_, value) ->
            when (value) {
                JsonNull -> inline(null as String?).`as`(variableName)
                else -> inline(convertJsonValue(ComparisonValue.Scalar(value))).`as`(variableName)
            }
        }.toTypedArray()
    }

    private fun buildResultsCTE(
        varsCTE: CommonTableExpression<Record>
    ): CommonTableExpression<Record> {
        return name(resultsCTEName)
            .`as`(buildResultsCTEQuery(varsCTE))
    }

    private fun buildResultsCTEQuery(
        varsCTE: CommonTableExpression<Record>
    ): SelectJoinStep<Record> {
        val fields = getFieldSelects().map { it.field }.toTypedArray()
        val partitionFields = buildPartitionFields()
        val resultTable = table(name(request.collection.split("."))).`as`("rtb")
        
        return select(resultTable.asterisk(), *partitionFields)
            .from(resultTable)
            .join(varsCTE)
            .on(getVariablePredicate())
    }

    private fun buildPartitionFields(): Array<JooqField<*>> = arrayOf(
        rowNumber()
            .over()
            .partitionBy(field(name(variableName)))
            .orderBy(inline(1))
            .`as`(rowNumberName),
        field(name(indexName))
    )

    private fun getVariablePredicate(): Condition =
        request.query.predicate?.let { generateCondition(it) }
            ?: throw ConnectorError.NotSupported("Predicate is required for variable queries")

    private fun getVariableJoinColumn(): String =
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
        varsCTE: CommonTableExpression<Record>,
        resultsCTE: CommonTableExpression<Record>
    ): JooqQuery {
        val fields = getFieldSelects()
            .withCasts(request.collection)
            .withAliases()
        val baseQuery = buildBaseQuery(ctx, fields, varsCTE, resultsCTE)
        return baseQuery
            .let { applyVariableLimitOffset(it) }
            .let { applyVariableOrdering(it) }
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
    ): SelectSeekStepN<Record> = query.orderBy(
        listOf(field(name(indexName))) +
        getOrderByFields()
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

    private data class FieldWithAlias(
        val field: JooqField<*>,
        val alias: String
    )

    private fun getFieldSelects(): List<FieldWithAlias> =
        request.query.fields?.map { (alias, field) ->
            when (field) {
                is Field.Column -> FieldWithAlias(
                    field = field(name(field.column)),
                    alias = alias
                )
                else -> throw ConnectorError.NotSupported("Unsupported: Relationships are not supported")
            }
        } ?: emptyList()
    
    private fun List<FieldWithAlias>.withCasts(collection: String): List<FieldWithAlias> =
        map { fieldWithAlias ->
            val columnType = columnTypeTojOOQType(collection, Field.Column(fieldWithAlias.field.name))
            fieldWithAlias.copy(
                field = schemaGenerator.castToSQLDataType(fieldWithAlias.field, columnType)
            )
        }

    private fun List<FieldWithAlias>.withAliases(): List<JooqField<*>> =
        map { it.field.`as`(it.alias) }

    private fun translateIRAggregateField(field: Aggregate): AggregateFunction<*> {
        return when (field) {
            is Aggregate.StarCount -> count()
            is Aggregate.ColumnCount ->
                if (field.distinct)
                    countDistinct(field(name(field.column)))
                else
                    count(field(name(field.column)))

            is Aggregate.SingleColumn -> {
                val jooqField =
                    field(name(field.column), SQLDataType.NUMERIC)
                when (field.function.lowercase()) {
                    "avg" -> avg(jooqField)
                    "sum" -> sum(jooqField)
                    "min" -> min(jooqField)
                    "max" -> max(jooqField)
                    "stddev_pop" -> stddevPop(jooqField)
                    "stddev_samp" -> stddevSamp(jooqField)
                    "var_pop" -> varPop(jooqField)
                    "var_samp" -> varSamp(jooqField)
                    else -> throw ConnectorError.NotSupported("Unsupported aggregate function: ${field.function}")
                }
            }
        }
    }

    private fun translateIRAggregateFields(fields: Map<String, Aggregate>): List<org.jooq.Field<*>> {
        return fields.map { (alias, field) ->
            translateIRAggregateField(field).`as`(alias)
        }
    }

    private fun columnTypeTojOOQType(
        collection: String,
        field: Field
    ): T {
        when (field) {
            is Field.Column -> {
                val table = configuration.tables.find { it.name == collection }
                    ?: error("Table $collection not found in connector configuration")

                val column = table.columns.find { it.name == field.column }
                    ?: error("Column ${field.column} not found in table $collection")

                return column.type
            }
            else -> throw ConnectorError.NotSupported("Unsupported: Relationships are not supported")
        }
    }
}
