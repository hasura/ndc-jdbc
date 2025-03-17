package io.hasura.app.default

import io.hasura.app.base.DatabaseConnection
import io.hasura.app.base.DatabaseSource
import io.hasura.common.ColumnType
import io.hasura.common.DefaultConfiguration
import io.hasura.ndc.ir.*
import io.hasura.ndc.ir.JoinType
import kotlinx.coroutines.coroutineScope
import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.JsonArray
import org.jooq.*
import org.jooq.Field
import org.jooq.impl.DSL
import org.jooq.impl.DSL.*

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
        plan: Plan
    ): JsonArray {
        println("Received plan: $plan")
        println(json.encodeToString(plan))
        println()

        val sql = PlanConverter.generateSQL(plan)
        println()

        println("Generated SQL:")
        println("==============")
        println(sql)

        return coroutineScope {
            // val queryExecutor = DefaultConnection(state.client)
            val emptyRows = JsonArray(emptyList())
            emptyRows
        }
    }
}

object PlanConverter {
    fun generateSQL(plan: Plan): String {
        val dsl = using(SQLDialect.SNOWFLAKE)
        return createDSLQuery(dsl, plan).toString()
    }

    private fun createDSLQuery(dsl: DSLContext, plan: Plan): Select<*> {
        return when (plan) {
            is Plan.From -> createFromQuery(dsl, plan)
            is Plan.Limit -> createLimitQuery(dsl, plan)
            is Plan.Project -> createProjectQuery(dsl, plan)
            is Plan.Filter -> createFilterQuery(dsl, plan)
            is Plan.Sort -> createSortQuery(dsl, plan)
            is Plan.Distinct -> createDistinctQuery(dsl, plan)
            is Plan.DistinctOn -> createDistinctOnQuery(dsl, plan)
            is Plan.Join -> createJoinQuery(dsl, plan)
            is Plan.Aggregate -> createAggregateQuery(dsl, plan)
        }
    }

    private fun createFromQuery(dsl: DSLContext, plan: Plan.From): Select<*> {
        val table = table(name(plan.collection))

        return dsl.select(
            plan.columns.mapIndexed { index, column ->
                field(name(column)).`as`(name("column_$index"))
            }
        ).from(table)
    }

    private fun createLimitQuery(dsl: DSLContext, plan: Plan.Limit): Select<*> {
        val innerQuery = createDSLQuery(dsl, plan.input)

        var query = dsl.select().from(innerQuery.asTable("limit"))

        return if (plan.fetch != null) {
            query.limit(plan.fetch).offset(plan.skip)
        } else {
            query.offset(plan.skip)
        }
    }

    private fun createProjectQuery(dsl: DSLContext, plan: Plan.Project): Select<*> {
        val innerQuery = createDSLQuery(dsl, plan.input)
        val innerTable = innerQuery.asTable("project")

        return dsl.select(
            plan.exprs.mapIndexed { index, expr ->
                createDSLNode(dsl, expr, innerTable).`as`(name("column_$index"))
            }
        ).from(innerTable)
    }

    private fun createFilterQuery(dsl: DSLContext, plan: Plan.Filter): Select<*> {
        val innerQuery = createDSLQuery(dsl, plan.input)
        val innerTable = innerQuery.asTable("filter")

        return dsl.select()
            .from(innerTable)
            .where(createDSLCondition(dsl, plan.predicate, innerTable))
    }

    private fun createSortQuery(dsl: DSLContext, plan: Plan.Sort): Select<*> {
        val innerQuery = createDSLQuery(dsl, plan.input)
        val innerTable = innerQuery.asTable("sort")

        var query = dsl.select().from(innerTable)

        return query.orderBy(
            plan.exprs.map { sortExpr ->
                val field = createDSLNode(dsl, sortExpr.expr, innerTable)
                if (sortExpr.asc) {
                    if (sortExpr.nulls_first) field.asc().nullsFirst() else field.asc().nullsLast()
                } else {
                    if (sortExpr.nulls_first) field.desc().nullsFirst() else field.desc().nullsLast()
                }
            }
        )
    }

    private fun createDistinctQuery(dsl: DSLContext, plan: Plan.Distinct): Select<*> {
        val innerQuery = createDSLQuery(dsl, plan.input)
        val innerTable = innerQuery.asTable("distinct")

        return dsl.selectDistinct().from(innerTable)
    }

    private fun createDistinctOnQuery(dsl: DSLContext, plan: Plan.DistinctOn): Select<*> {
        val innerQuery = createDSLQuery(dsl, plan.input)
        val innerTable = innerQuery.asTable("distinct_on")

        // jOOQ doesn't directly support DISTINCT ON, so we need to work around it
        // This is a simplified approach using GROUP BY
        val selectFields = mutableListOf<SelectField<*>>()

        // Add the distinct-on fields
        val distinctOnFields = plan.exprs.map { expr ->
            createDSLNode(dsl, expr, innerTable)
        }

        selectFields.addAll(distinctOnFields)

        // Include all columns from inner query
        for (field in innerTable.fields()) {
            if (!selectFields.contains(field)) {
                // Cast the field to SelectField<*>
                selectFields.add(field as SelectField<*>)
            }
        }

        return dsl.select(selectFields)
            .from(innerTable)
            .groupBy(distinctOnFields)
    }

    private fun createJoinQuery(dsl: DSLContext, plan: Plan.Join): Select<*> {
        val leftQuery = createDSLQuery(dsl, plan.left)
        val rightQuery = createDSLQuery(dsl, plan.right)
        val leftTable = leftQuery.asTable("left_t")
        val rightTable = rightQuery.asTable("right_t")

        // Start with a join
        var joinStep = when (plan.join_type) {
            JoinType.Inner -> dsl.select().from(leftTable).join(rightTable)
            JoinType.Left -> dsl.select().from(leftTable).leftOuterJoin(rightTable)
            JoinType.Right -> dsl.select().from(leftTable).rightOuterJoin(rightTable)
            JoinType.Full -> dsl.select().from(leftTable).fullOuterJoin(rightTable)
        }

        val joinFields = plan.on.fold(trueCondition() as Condition) { acc, joinOn ->
            val leftExpr = createDSLNode(dsl, joinOn.left, leftTable) as Field<Any>
            val rightExpr = createDSLNode(dsl, joinOn.right, rightTable) as Field<Any>
            acc.and(leftExpr.eq(rightExpr))
        }

        return joinStep.on(joinFields)
    }

    private fun createAggregateQuery(dsl: DSLContext, plan: Plan.Aggregate): Select<*> {
        val innerQuery = createDSLQuery(dsl, plan.input)
        val innerTable = innerQuery.asTable("t")

        val selectFields = mutableListOf<SelectField<*>>()

        // Add group by fields to the select
        val groupByFields = plan.group_by.mapIndexed { index, expr ->
            val field = createDSLNode(dsl, expr, innerTable).`as`(name("group_$index"))
            selectFields.add(field)
            field
        }

        // Add aggregate expressions to the select
        plan.aggregates.forEachIndexed { index, expr ->
            selectFields.add(createDSLAggregateExpression(dsl, expr, innerTable).`as`(name("agg_$index")))
        }

        return dsl.select(selectFields)
            .from(innerTable)
            .groupBy(groupByFields)
    }

    private fun createDSLNode(dsl: DSLContext, expr: PlanExpression, table: Table<*>): Field<*> {
        return when (expr) {
            is PlanExpression.Column -> {
                // Column references the index in the table
                // Force non-null return with !! or provide a default field
                table.field(expr.index) ?: field(DSL.name("column_${expr.index}"))
            }

            is PlanExpression.PlanLiteral -> {
                when (val literal = expr.literal) {
                    is Literal.Null -> `val`<Any?>(null)
                    is Literal.BooleanLiteral -> DSL.`val`(literal.value == true)
                    is Literal.Int32 -> DSL.`val`(literal.value ?: 0)
                    is Literal.Int64 -> DSL.`val`(literal.value ?: 0L)
                    is Literal.Float32 -> DSL.`val`(literal.value)
                    is Literal.Float64 -> DSL.`val`(literal.value)
                    is Literal.Utf8 -> DSL.`val`(literal.value ?: "")
                    is Literal.Int8 -> DSL.`val`(literal.value)
                    is Literal.Int16 -> DSL.`val`(literal.value)
                    is Literal.UInt8 -> DSL.`val`(literal.value?.toInt())
                    is Literal.UInt16 -> DSL.`val`(literal.value?.toInt())
                    is Literal.UInt32 -> DSL.`val`(literal.value?.toLong())
                    is Literal.UInt64 -> DSL.`val`(literal.value?.toLong())
                    is Literal.Date32 -> DSL.`val`(literal.value)
                    is Literal.Date64 -> DSL.`val`(literal.value)
                    is Literal.Time32Second -> DSL.`val`(literal.value)
                    is Literal.Time32Millisecond -> DSL.`val`(literal.value)
                    is Literal.Time64Microsecond -> DSL.`val`(literal.value)
                    is Literal.Time64Nanosecond -> DSL.`val`(literal.value)
                    is Literal.TimestampSecond -> DSL.`val`(literal.value)
                    is Literal.TimestampMillisecond -> DSL.`val`(literal.value)
                    is Literal.TimestampMicrosecond -> DSL.`val`(literal.value)
                    is Literal.TimestampNanosecond -> DSL.`val`(literal.value)
                    is Literal.DurationSecond -> DSL.`val`(literal.value)
                    is Literal.DurationMillisecond -> DSL.`val`(literal.value)
                    is Literal.DurationMicrosecond -> DSL.`val`(literal.value)
                    is Literal.DurationNanosecond -> DSL.`val`(literal.value)
                }
            }

            // Binary operators
            is PlanExpression.And -> {
                val left = createDSLCondition(dsl, expr.left, table)
                val right = createDSLCondition(dsl, expr.right, table)
                left.and(right)
            }

            is PlanExpression.Or -> {
                val left = createDSLCondition(dsl, expr.left, table)
                val right = createDSLCondition(dsl, expr.right, table)
                left.or(right)
            }

            is PlanExpression.Eq -> {
                val left = createDSLNode(dsl, expr.left, table) as Field<Any>
                val right = createDSLNode(dsl, expr.right, table)
                left.eq(right)
            }

            is PlanExpression.NotEq -> {
                val left = createDSLNode(dsl, expr.left, table) as Field<Any>
                val right = createDSLNode(dsl, expr.right, table)
                left.notEqual(right)
            }

            is PlanExpression.Lt -> {
                val left = createDSLNode(dsl, expr.left, table) as Field<Any>
                val right = createDSLNode(dsl, expr.right, table)
                left.lt(right)
            }

            is PlanExpression.LtEq -> {
                val left = createDSLNode(dsl, expr.left, table) as Field<Any>
                val right = createDSLNode(dsl, expr.right, table)
                left.lessOrEqual(right)
            }

            is PlanExpression.Gt -> {
                val left = createDSLNode(dsl, expr.left, table) as Field<Any>
                val right = createDSLNode(dsl, expr.right, table)
                left.gt(right)
            }

            is PlanExpression.GtEq -> {
                val left = createDSLNode(dsl, expr.left, table) as Field<Any>
                val right = createDSLNode(dsl, expr.right, table)
                left.greaterOrEqual(right)
            }

            is PlanExpression.Plus -> {
                val left = createDSLNode(dsl, expr.left, table)
                val right = createDSLNode(dsl, expr.right, table)
                left.plus(right)
            }

            is PlanExpression.Minus -> {
                val left = createDSLNode(dsl, expr.left, table)
                val right = createDSLNode(dsl, expr.right, table)
                left.minus(right)
            }

            is PlanExpression.Multiply -> {
                val left = createDSLNode(dsl, expr.left, table)
                val right = createDSLNode(dsl, expr.right, table) as Field<Number>
                left.multiply(right)
            }

            is PlanExpression.Divide -> {
                val left = createDSLNode(dsl, expr.left, table)
                val right = createDSLNode(dsl, expr.right, table) as Field<Number>
                left.div(right)
            }

            is PlanExpression.Modulo -> {
                val left = createDSLNode(dsl, expr.left, table)
                val right = createDSLNode(dsl, expr.right, table) as Field<Number>
                left.mod(right)
            }

            // Unary operators
            is PlanExpression.Not -> {
                createDSLCondition(dsl, expr.expr, table).not()
            }

            is PlanExpression.IsNotNull -> {
                createDSLNode(dsl, expr.expr, table).isNotNull
            }

            is PlanExpression.IsNull -> {
                createDSLNode(dsl, expr.expr, table).isNull
            }

            is PlanExpression.IsTrue -> {
                createDSLNode(dsl, expr.expr, table).isTrue
            }

            is PlanExpression.IsFalse -> {
                createDSLNode(dsl, expr.expr, table).isFalse
            }

            is PlanExpression.IsUnknown -> {
                createDSLNode(dsl, expr.expr, table).isNull
            }

            is PlanExpression.IsNotTrue -> {
                createDSLNode(dsl, expr.expr, table).isTrue.not()
            }

            is PlanExpression.IsNotFalse -> {
                createDSLNode(dsl, expr.expr, table).isFalse.not()
            }

            is PlanExpression.IsNotUnknown -> {
                createDSLNode(dsl, expr.expr, table).isNotNull
            }

            is PlanExpression.Negative -> {
                createDSLNode(dsl, expr.expr, table).neg()
            }

            // String functions
            is PlanExpression.ToLower -> {
                createDSLNode(dsl, expr.expr, table).lower()

            }

            is PlanExpression.ToUpper -> {
                createDSLNode(dsl, expr.expr, table).upper()
            }

            // Set operations
            is PlanExpression.In -> {
                val leftExpr = createDSLNode(dsl, expr.expr, table)
                val values = expr.list.map { createDSLNode(dsl, it, table) }
                leftExpr.`in`(values)
            }

            is PlanExpression.NotIn -> {
                val leftExpr = createDSLNode(dsl, expr.expr, table)
                val values = expr.list.map { createDSLNode(dsl, it, table) }
                leftExpr.notIn(values)
            }

            // Pattern matching operators
            is PlanExpression.Like -> {
                val leftExpr = createDSLNode(dsl, expr.expr, table)
                val pattern = createDSLNode(dsl, expr.pattern, table) as Field<String>
                leftExpr.like(pattern)
            }

            is PlanExpression.ILike -> {
                val leftExpr = createDSLNode(dsl, expr.expr, table)
                val pattern = createDSLNode(dsl, expr.pattern, table) as Field<String>
                leftExpr.likeIgnoreCase(pattern)
            }

            is PlanExpression.NotLike -> {
                val leftExpr = createDSLNode(dsl, expr.expr, table)
                val pattern = createDSLNode(dsl, expr.pattern, table) as Field<String>
                leftExpr.notLike(pattern)
            }

            is PlanExpression.NotILike -> {
                val leftExpr = createDSLNode(dsl, expr.expr, table)
                val pattern = createDSLNode(dsl, expr.pattern, table) as Field<String>
                leftExpr.notLikeIgnoreCase(pattern)
            }

            // Between operators
            is PlanExpression.Between -> {
                error("Between operator is not supported yet")
            }

            is PlanExpression.NotBetween -> {
                error("NotBetween operator is not supported yet")
            }

            // Aggregate functions - these should be handled separately
            is PlanExpression.Count,
            is PlanExpression.Sum,
            is PlanExpression.Average,
            is PlanExpression.Min,
            is PlanExpression.Max,
            is PlanExpression.Mean,
            is PlanExpression.Median,
            is PlanExpression.FirstValue,
            is PlanExpression.LastValue,
            is PlanExpression.BoolAnd,
            is PlanExpression.BoolOr,
            is PlanExpression.StringAgg,
            is PlanExpression.Var -> {
                // This should not be reached during normal expression evaluation
                // Aggregates should be handled by createDSLAggregateExpression
                DSL.field("INVALID_AGGREGATE")
            }
        }
    }

    private fun createDSLAggregateExpression(dsl: DSLContext, expr: PlanExpression, table: Table<*>): Field<*> {
        return when (expr) {
            is PlanExpression.Count -> {
                DSL.count(createDSLNode(dsl, expr.expr, table))
            }

            is PlanExpression.Sum -> {
                DSL.field("SUM({0})", createDSLNode(dsl, expr.expr, table))
            }

            is PlanExpression.Average -> {
                DSL.field("AVG({0})", createDSLNode(dsl, expr.expr, table))
            }

            is PlanExpression.Min -> {
                DSL.field("MIN({0})", createDSLNode(dsl, expr.expr, table))
            }

            is PlanExpression.Max -> {
                DSL.field("MAX({0})", createDSLNode(dsl, expr.expr, table))
            }

            is PlanExpression.FirstValue -> {
                DSL.field("FIRST_VALUE({0})", createDSLNode(dsl, expr.expr, table))
            }

            is PlanExpression.LastValue -> {
                DSL.field("LAST_VALUE({0})", createDSLNode(dsl, expr.expr, table))
            }

            is PlanExpression.BoolAnd -> {
                // Equivalent to BOOL_AND in PostgreSQL
                DSL.field("BOOL_AND({0})", createDSLNode(dsl, expr.expr, table))
            }

            is PlanExpression.BoolOr -> {
                // Equivalent to BOOL_OR in PostgreSQL
                DSL.field("BOOL_OR({0})", createDSLNode(dsl, expr.expr, table))
            }

            is PlanExpression.StringAgg -> {
                // GROUP_CONCAT in MySQL or STRING_AGG in PostgreSQL
                groupConcat(createDSLNode(dsl, expr.expr, table))
            }

            is PlanExpression.Mean -> {
                // Same as AVG
                DSL.field("AVG({0})", createDSLNode(dsl, expr.expr, table))
            }

            is PlanExpression.Median -> {
                // Most SQL dialects don't have direct median support
                DSL.field("MEDIAN({0})", createDSLNode(dsl, expr.expr, table))
            }

            is PlanExpression.Var -> {
                // Variance function
                DSL.field("VARIANCE({0})", createDSLNode(dsl, expr.expr, table))
            }

            else -> {
                createDSLNode(dsl, expr, table)
            }
        }
    }

    /**
     * Helper method to ensure we get a Condition from an expression
     */
    private fun createDSLCondition(dsl: DSLContext, expr: PlanExpression, table: Table<*>): Condition {
        return createDSLNode(dsl, expr, table) as Condition
    }
}
