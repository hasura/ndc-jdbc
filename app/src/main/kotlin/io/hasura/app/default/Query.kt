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
import org.jooq.conf.RenderNameCase
import org.jooq.conf.RenderQuotedNames
import org.jooq.conf.Settings
import org.jooq.Field as JooqField
import org.jooq.Query as JooqQuery
import org.jooq.SQLDialect
import org.jooq.impl.DSL
import org.jooq.impl.DSL.*
import org.jooq.impl.SQLDataType

const val variablesCTEName = "vars"
const val resultsCTEName = "rlts"
const val indexName = "idx"
const val rowNumberName = "rn"
const val tableName = "tb"
const val resultTableName = "rtb"

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
        val (dialect, settings) = getDatabaseDialect(source)
        val ctx = using(dialect, settings)
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
    ): JooqQuery {
        val resultsTable = table(name(request.collection.split("."))).`as`(tableName)

        return ctx
            .select(
                getFieldSelects(resultsTable)
                    .withCasts(request.collection)
                    .withAliases()
            )
            .from(resultsTable)
            .where(getPredicate())
            .orderBy(getOrderByFields(tableName))
            .limit(request.query.limit?.toInt())
            .offset(request.query.offset?.toInt())
    }

    override fun generateAggregateQuery(): String {
        val (dialect, settings) = getDatabaseDialect(source)
        val ctx = using(dialect, settings)
        val aggregateFields = request.query.aggregates!!.map { (alias, aggregate) ->
            FieldWithAlias(
                columnType = columnTypeTojOOQType(request.collection, FieldOrAggregate.AggregateType(aggregate)),
                field = translateIRAggregateField(aggregate),
                alias = alias
            )
        }
        val sql = ctx
            .select(
                aggregateFields
                    .withCasts(request.collection)
                    .withAliases()
            )
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

    private fun getDatabaseDialect(type: DatabaseSource): Pair<SQLDialect, Settings> = when (type) {
        DatabaseSource.BIGQUERY -> SQLDialect.BIGQUERY to Settings()

        DatabaseSource.DATABRICKS -> SQLDialect.MYSQL to Settings()
            .withRenderFormatted(true)
            .withRenderQuotedNames(RenderQuotedNames.EXPLICIT_DEFAULT_UNQUOTED)
            .withRenderNameCase(RenderNameCase.AS_IS)

        DatabaseSource.REDSHIFT -> SQLDialect.REDSHIFT to Settings()
        DatabaseSource.SNOWFLAKE -> SQLDialect.SNOWFLAKE to Settings()
    }

    private fun getPredicate(): Condition = request.query.predicate?.let { generateCondition(it) } ?: noCondition()

    private fun generateCondition(expr: Expression, table: Table<*>? = null): Condition = when (expr) {
        is Expression.And -> and(expr.expressions.map { generateCondition(it, table) })
        is Expression.Or -> or(expr.expressions.map { generateCondition(it, table) })
        is Expression.Not -> not(generateCondition(expr.expression, table))
        is Expression.UnaryComparisonOperator -> handleUnaryComparison(expr, table)
        is Expression.BinaryComparisonOperator -> handleBinaryComparison(expr, table)
        is Expression.Exists -> throw ConnectorError.NotSupported("Exists queries not supported yet")
    }

    private fun handleUnaryComparison(expr: Expression.UnaryComparisonOperator, table: Table<*>? = null): Condition = when (expr.operator) {
        UnaryComparisonOperatorType.IS_NULL -> {
            val fieldName = getColumnName(expr.column)
            if (table != null) field(name(table.name, fieldName)).isNull else field(name(fieldName)).isNull
        }
    }

    private fun handleBinaryComparison(expr: Expression.BinaryComparisonOperator, table: Table<*>? = null): Condition {
        val fieldName = getColumnName(expr.column)
        val field = if (table != null) field(name(table.name, fieldName)) else field(name(fieldName))
        
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
            operator.contains("i")
        )
        "_like", "_ilike", "_nlike", "_nilike" -> handleLikeComparison(
            field,
            compareWith,
            operator.startsWith("_n"),
            operator.contains("i")
        )
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
                val varField = field(name(variablesCTEName, value.name))
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
        isCaseInsensitive: Boolean
    ): Condition {
        val expr = when (source) {
            DatabaseSource.SNOWFLAKE -> {
                if (isCaseInsensitive) {
                    condition("REGEXP_LIKE({0}, {1}, 'i')", field, compareWith)
                } else {
                    condition("REGEXP_LIKE({0}, {1})", field, compareWith)
                }
            }
            
            DatabaseSource.BIGQUERY -> {
                if (isCaseInsensitive) {
                    condition("REGEXP_CONTAINS(LOWER({0}), LOWER({1}))", field, compareWith)
                } else {
                    condition("REGEXP_CONTAINS({0}, {1})", field, compareWith)
                }
            }
            
            DatabaseSource.REDSHIFT -> {
                if (isCaseInsensitive) {
                    condition("REGEXP_LIKE(LOWER({0}), LOWER({1}))", field, compareWith)
                } else {
                    condition("REGEXP_LIKE({0}, {1})", field, compareWith)
                }
            }
            
            DatabaseSource.DATABRICKS -> {
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

    private fun handleLikeComparison(
        field: org.jooq.Field<*>,
        compareWith: org.jooq.Field<*>,
        isNegated: Boolean,
        isCaseInsensitive: Boolean
    ): Condition {
        val expr = when (source) {
            DatabaseSource.SNOWFLAKE -> {
                if (isCaseInsensitive) {
                    field.likeIgnoreCase(compareWith.cast(SQLDataType.VARCHAR))
                } else {
                    field.like(compareWith.cast(SQLDataType.VARCHAR))
                }
            }
            
            DatabaseSource.BIGQUERY -> {
                if (isCaseInsensitive) {
                    condition("LOWER({0}) LIKE LOWER({1})", field, compareWith)
                } else {
                    condition("{0} LIKE {1}", field, compareWith)
                }
            }
            
            DatabaseSource.REDSHIFT -> {
                if (isCaseInsensitive) {
                    field.likeIgnoreCase(compareWith.cast(SQLDataType.VARCHAR))
                } else {
                    field.like(compareWith.cast(SQLDataType.VARCHAR))
                }
            }
            
            DatabaseSource.DATABRICKS -> {
                if (isCaseInsensitive) {
                    condition("LOWER({0}) LIKE LOWER({1})", field, compareWith)
                } else {
                    condition("{0} LIKE {1}", field, compareWith)
                }
            }
            
            else -> throw ConnectorError.NotSupported("Ilike operations not supported for this database")
        }
        return if (isNegated) not(expr) else expr
    }

    private fun getOrderByFields(prefix: String = ""): List<SortField<*>> {
        return request.query.orderBy?.elements?.map { element ->
            when (val target = element.target) {
                is OrderByTarget.Column -> {
                    if (target.path.isNotEmpty() || target.fieldPath != null) {
                        throw ConnectorError.NotSupported("Nested fields and relationships are not supported in order by")
                    }
                    when (element.orderDirection) {
                        OrderDirection.ASC -> field(name(listOf(prefix, target.name))).asc()
                        OrderDirection.DESC -> field(name(listOf(prefix, target.name))).desc()
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
        return variableMap.map { (index, value) ->
            when (value) {
                JsonNull -> inline(null as String?).`as`(index)
                else -> inline(convertJsonValue(ComparisonValue.Scalar(value))).`as`(index)
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
        val fields = getFieldSelects().map { field(name(resultTableName, it.field.name)) }.toTypedArray()
        val partitionFields = buildPartitionFields()
        val resultTable = table(name(request.collection.split("."))).`as`(resultTableName)
        
        return select(
                field("{0}.*", Any::class.java, name(resultTable.name)), // Why doesn't resultTable.asterisk() work here?
                *partitionFields
            )
            .from(resultTable)
            .join(varsCTE)
            .on(getVariablePredicate(resultTable))
    }

    private fun buildPartitionFields(): Array<JooqField<*>> = arrayOf(
        rowNumber()
            .over()
            .partitionBy(field(name(indexName)))
            .orderBy(inline(1))
            .`as`(rowNumberName),
        field(name(indexName))
    )

    private fun getVariablePredicate(table: Table<*>): Condition =
        request.query.predicate?.let { generateCondition(it, table) }
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
        val fields = getFieldSelects(resultsCTE)
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
                field(name(resultsCTEName, indexName))
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

    private sealed interface FieldOrAggregate {
        data class FieldType(val field: Field) : FieldOrAggregate
        data class AggregateType(val aggregate: Aggregate) : FieldOrAggregate
    }

    private data class FieldWithAlias<T>(
        val columnType: T?,
        val field: JooqField<*>,
        val alias: String
    )

    private fun getFieldSelects(table: Table<*>? = null): List<FieldWithAlias<T>> =
        request.query.fields?.map { (alias, field) ->
            when (field) {
                is Field.Column -> FieldWithAlias(
                    columnType = columnTypeTojOOQType(request.collection, FieldOrAggregate.FieldType(field)),
                    field = if (table == null) {
                        field(name(field.column))
                    } else {
                        field(name(table.name, field.column))
                    },
                    alias = alias
                )
                else -> throw ConnectorError.NotSupported("Unsupported: Relationships are not supported")
            }
        } ?: emptyList()
    
    private fun List<FieldWithAlias<T>>.withCasts(collection: String): List<FieldWithAlias<T>> =
        map { fieldWithAlias ->
            fieldWithAlias.copy(
                field = schemaGenerator.castToSQLDataType(fieldWithAlias.field, fieldWithAlias.columnType)
            )
        }

    private fun List<FieldWithAlias<T>>.withAliases(): List<JooqField<*>> =
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
        field: FieldOrAggregate
    ): T? {
        when (field) {
            is FieldOrAggregate.FieldType -> {
                when (val f = field.field) {
                    is Field.Column -> {
                        val table = configuration.tables.find { it.name == collection }
                            ?: error("Table $collection not found in connector configuration")

                        val column = table.columns.find { it.name == f.column }
                            ?: error("Column ${f.column} not found in table $collection")

                        return column.type
                    }
                    else -> throw ConnectorError.NotSupported("Unsupported: Relationships are not supported")
                }
            }
            is FieldOrAggregate.AggregateType -> {
                val table = configuration.tables.find { it.name == collection }
                    ?: error("Table $collection not found in connector configuration")
                when (val f = field.aggregate) {
                    is Aggregate.StarCount -> return null
                    is Aggregate.SingleColumn -> {
                        val column = table.columns.find { it.name == f.column }
                            ?: error("Column ${f.column} not found in table $collection")

                        return column.type
                    }
                    is Aggregate.ColumnCount -> return null
                }
            }
        }
    }
}
