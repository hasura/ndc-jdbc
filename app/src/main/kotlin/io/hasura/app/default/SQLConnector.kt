package io.hasura.app.default

import io.hasura.app.base.DatabaseConnection
import io.hasura.app.base.DatabaseSource
import io.hasura.common.ColumnType
import io.hasura.common.DefaultConfiguration
import io.hasura.ndc.connector.ConnectorLogger
import io.hasura.ndc.ir.*
import io.hasura.ndc.ir.JoinType
import kotlinx.coroutines.coroutineScope
import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.JsonArray
import org.jooq.*
import org.jooq.CaseConditionStep
import org.jooq.Field
import org.jooq.conf.Settings
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
    override suspend fun queryRel(
        configuration: DefaultConfiguration<T>,
        state: DefaultState<T>,
        request: QueryRel
    ): JsonArray {
        ConnectorLogger.logger.debug("Rel: ${request.rel}")

        val sql = RelConverter.generateSQL(request.rel)

        ConnectorLogger.logger.debug("Generated SQL: ${sql}")

            val queryExecutor = DefaultConnection(state.client)
            return coroutineScope {
            queryExecutor.executeSQL(sql)
        }
    }
}


object RelConverter {
    fun generateSQL(rel: Rel): Select<*> {
        val dsl = DSL.using(SQLDialect.DEFAULT, Settings().withRenderFormatted(true))
        return createDSLQuery(dsl, rel, "r")
    }

    private fun createDSLQuery(dsl: DSLContext, rel: Rel, path: String): Select<*> {
        return when (rel) {
            is Rel.From -> createFromQuery(dsl, rel, path)
            is Rel.Limit -> createLimitQuery(dsl, rel, path)
            is Rel.Project -> createProjectQuery(dsl, rel, path)
            is Rel.Filter -> createFilterQuery(dsl, rel, path)
            is Rel.Sort -> createSortQuery(dsl, rel, path)
            is Rel.Distinct -> createDistinctQuery(dsl, rel, path)
            is Rel.DistinctOn -> createDistinctOnQuery(dsl, rel, path)
            is Rel.Join -> createJoinQuery(dsl, rel, path)
            is Rel.Aggregate -> createAggregateQuery(dsl, rel, path)
        }
    }

    private fun createFromQuery(dsl: DSLContext, rel: Rel.From, path: String): Select<*> {
        val table = table(name(rel.collection))
        val prefix = "${path}f"

        return dsl.select(
            rel.columns.mapIndexed { index, column ->
                field(name(column)).`as`(name("${prefix}_col_$index"))
            }
        ).from(table)
    }

    private fun createLimitQuery(dsl: DSLContext, rel: Rel.Limit, path: String): Select<*> {
        val innerPath = "${path}l"
        val innerQuery = createDSLQuery(dsl, rel.input, innerPath)

        return dsl.select()
            .from(innerQuery.asTable("t$innerPath"))
            .limit(rel.fetch)
            .offset(rel.skip)
    }

    private fun createProjectQuery(dsl: DSLContext, rel: Rel.Project, path: String): Select<*> {
        val innerPath = "${path}p"
        val innerQuery = createDSLQuery(dsl, rel.input, innerPath)
        val innerTable = innerQuery.asTable("t$innerPath")

        return dsl.select(
            rel.exprs.mapIndexed { index, expr ->
                createDSLNode(dsl, expr, innerTable, innerPath, rel.input).`as`(name("${innerPath}_col_$index"))
            }
        ).from(innerTable)
    }

    private fun createFilterQuery(dsl: DSLContext, rel: Rel.Filter, path: String): Select<*> {
        val innerPath = "${path}f"
        val innerQuery = createDSLQuery(dsl, rel.input, innerPath)
        val innerTable = innerQuery.asTable("t$innerPath")

        return dsl.select()
            .from(innerTable)
            .where(createDSLCondition(dsl, rel.predicate, innerTable, innerPath, rel.input))
    }

    private fun createSortQuery(dsl: DSLContext, rel: Rel.Sort, path: String): Select<*> {
        val innerPath = "${path}s"
        val innerQuery = createDSLQuery(dsl, rel.input, innerPath)
        val innerTable = innerQuery.asTable("t$innerPath")

        var query = dsl.select().from(innerTable)

        return query.orderBy(
            rel.exprs.map { sortExpr ->
                val field = createDSLNode(dsl, sortExpr.expr, innerTable, innerPath, rel.input)
                val orderField = if (sortExpr.asc) field.asc() else field.desc()
                if (sortExpr.nulls_first) orderField.nullsFirst() else orderField.nullsLast()
            }
        )
    }

    private fun createDistinctQuery(dsl: DSLContext, rel: Rel.Distinct, path: String): Select<*> {
        val innerPath = "${path}d"
        val innerQuery = createDSLQuery(dsl, rel.input, innerPath)
        val innerTable = innerQuery.asTable("t$innerPath")

        return dsl.selectDistinct().from(innerTable)
    }

    private fun createDistinctOnQuery(dsl: DSLContext, rel: Rel.DistinctOn, path: String): Select<*> {
        val innerPath = "${path}do"
        val innerQuery = createDSLQuery(dsl, rel.input, innerPath)
        val innerTable = innerQuery.asTable("t$innerPath")

        return dsl
            .select(DSL.asterisk())
            .distinctOn(
                rel.exprs.mapIndexed { index, expr ->
                    createDSLNode(dsl, expr, innerTable, innerPath, rel.input).`as`(name("${innerPath}_col_$index"))
                }
            ).from(
                innerTable
            )
    }

    private fun createJoinQuery(dsl: DSLContext, rel: Rel.Join, path: String): Select<*> {
        // For joins, we need different path identifiers for left and right branches
        val leftPath = "${path}jl"
        val rightPath = "${path}jr"

        val leftQuery = createDSLQuery(dsl, rel.left, leftPath)
        val rightQuery = createDSLQuery(dsl, rel.right, rightPath)

        val leftTable = leftQuery.asTable("t${leftPath}_left")
        val rightTable = rightQuery.asTable("t${rightPath}_right")

        // Start with a join
        var joinStep = when (rel.join_type) {
            JoinType.Inner -> dsl.select().from(leftTable).join(rightTable)
            JoinType.Left -> dsl.select().from(leftTable).leftOuterJoin(rightTable)
            JoinType.Right -> dsl.select().from(leftTable).rightOuterJoin(rightTable)
            JoinType.Full -> dsl.select().from(leftTable).fullOuterJoin(rightTable)
        }

        val joinFields = rel.on.fold(trueCondition() as Condition) { acc, joinOn ->
            val leftExpr = createDSLNode(dsl, joinOn.left, leftTable, leftPath, rel.left) as Field<Any>
            val rightExpr = createDSLNode(dsl, joinOn.right, rightTable, rightPath, rel.right) as Field<Any>
            acc.and(leftExpr.eq(rightExpr))
        }

        return joinStep.on(joinFields)
    }

    private fun createAggregateQuery(dsl: DSLContext, rel: Rel.Aggregate, path: String): Select<*> {
        val innerPath = "${path}a"
        val innerQuery = createDSLQuery(dsl, rel.input, innerPath)
        val innerTable = innerQuery.asTable("t$innerPath")

        val groupByFields = rel.group_by.mapIndexed { index, expr ->
            createDSLNode(dsl, expr, innerTable, innerPath, rel.input).`as`(name("${innerPath}_grp_$index"))
        }

        val aggregateFields = rel.aggregates.mapIndexed { index, expr ->
            val agg = createDSLAggregateExpression(dsl, expr, innerTable, innerPath, rel.input)
            agg.`as`(name("${innerPath}_agg_$index"))
        }

        return dsl.select(groupByFields + aggregateFields)
            .from(innerTable)
            .groupBy(groupByFields)
    }

    private fun literalToValue(literal: Literal): Field<*> {
        return when (literal) {
            is Literal.Null -> DSL.`val`<Any?>(null)
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

    private fun createDSLNode(
        dsl: DSLContext,
        expr: RelExpression,
        table: Table<*>,
        path: String,
        parentRel: Rel
    ): Field<*> {
        return when (expr) {
            is RelExpression.Column -> {
                // Column references the index in the table
                // For column references, we need to use the rel-type specific column name format
                val columnPrefix = when (parentRel) {
                    is Rel.From -> "f"
                    is Rel.Project -> "p"
                    is Rel.Filter -> "f"
                    is Rel.Sort -> "s"
                    is Rel.Limit -> "l"
                    is Rel.Distinct -> "d"
                    is Rel.DistinctOn -> "do"
                    is Rel.Join -> "j"
                    is Rel.Aggregate -> "a"
                }

                table.field(expr.index) ?: field(name("${columnPrefix}${path}_col_${expr.index}"))
            }

            is RelExpression.RelLiteral -> {
                literalToValue(expr.literal)
            }

            is RelExpression.Cast -> {
                val inner = createDSLNode(dsl, expr.expr, table, path, parentRel)
                inner.cast(literalToValue(expr.as_type))
            }

            is RelExpression.TryCast -> {
                val inner = createDSLNode(dsl, expr.expr, table, path, parentRel)
                inner.cast(literalToValue(expr.as_type))
            }

            is RelExpression.Case -> {
                // TODO
                DSL.field("TODO")
            }

            // Binary operators
            is RelExpression.And -> {
                val left = createDSLCondition(dsl, expr.left, table, path, parentRel)
                val right = createDSLCondition(dsl, expr.right, table, path, parentRel)
                left.and(right)
            }

            is RelExpression.Or -> {
                val left = createDSLCondition(dsl, expr.left, table, path, parentRel)
                val right = createDSLCondition(dsl, expr.right, table, path, parentRel)
                left.or(right)
            }

            is RelExpression.Eq -> {
                val left = createDSLNode(dsl, expr.left, table, path, parentRel) as Field<Any>
                val right = createDSLNode(dsl, expr.right, table, path, parentRel)
                left.eq(right)
            }

            is RelExpression.NotEq -> {
                val left = createDSLNode(dsl, expr.left, table, path, parentRel) as Field<Any>
                val right = createDSLNode(dsl, expr.right, table, path, parentRel)
                left.notEqual(right)
            }

            is RelExpression.Lt -> {
                val left = createDSLNode(dsl, expr.left, table, path, parentRel) as Field<Any>
                val right = createDSLNode(dsl, expr.right, table, path, parentRel)
                left.lt(right)
            }

            is RelExpression.LtEq -> {
                val left = createDSLNode(dsl, expr.left, table, path, parentRel) as Field<Any>
                val right = createDSLNode(dsl, expr.right, table, path, parentRel)
                left.lessOrEqual(right)
            }

            is RelExpression.Gt -> {
                val left = createDSLNode(dsl, expr.left, table, path, parentRel) as Field<Any>
                val right = createDSLNode(dsl, expr.right, table, path, parentRel)
                left.gt(right)
            }

            is RelExpression.GtEq -> {
                val left = createDSLNode(dsl, expr.left, table, path, parentRel) as Field<Any>
                val right = createDSLNode(dsl, expr.right, table, path, parentRel)
                left.greaterOrEqual(right)
            }

            is RelExpression.Plus -> {
                val left = createDSLNode(dsl, expr.left, table, path, parentRel)
                val right = createDSLNode(dsl, expr.right, table, path, parentRel)
                left.plus(right)
            }

            is RelExpression.Minus -> {
                val left = createDSLNode(dsl, expr.left, table, path, parentRel)
                val right = createDSLNode(dsl, expr.right, table, path, parentRel)
                left.minus(right)
            }

            is RelExpression.Multiply -> {
                val left = createDSLNode(dsl, expr.left, table, path, parentRel)
                val right = createDSLNode(dsl, expr.right, table, path, parentRel) as Field<Number>
                left.multiply(right)
            }

            is RelExpression.Divide -> {
                val left = createDSLNode(dsl, expr.left, table, path, parentRel)
                val right = createDSLNode(dsl, expr.right, table, path, parentRel) as Field<Number>
                left.div(right)
            }

            is RelExpression.Modulo -> {
                val left = createDSLNode(dsl, expr.left, table, path, parentRel)
                val right = createDSLNode(dsl, expr.right, table, path, parentRel) as Field<Number>
                left.mod(right)
            }

            // Unary operators
            is RelExpression.Not -> {
                createDSLCondition(dsl, expr.expr, table, path, parentRel).not()
            }

            is RelExpression.IsNotNull -> {
                createDSLNode(dsl, expr.expr, table, path, parentRel).isNotNull
            }

            is RelExpression.IsNull -> {
                createDSLNode(dsl, expr.expr, table, path, parentRel).isNull
            }

            is RelExpression.IsTrue -> {
                createDSLNode(dsl, expr.expr, table, path, parentRel).isTrue
            }

            is RelExpression.IsFalse -> {
                createDSLNode(dsl, expr.expr, table, path, parentRel).isFalse
            }

            is RelExpression.IsUnknown -> {
                createDSLNode(dsl, expr.expr, table, path, parentRel).isNull
            }

            is RelExpression.IsNotTrue -> {
                createDSLNode(dsl, expr.expr, table, path, parentRel).isTrue.not()
            }

            is RelExpression.IsNotFalse -> {
                createDSLNode(dsl, expr.expr, table, path, parentRel).isFalse.not()
            }

            is RelExpression.IsNotUnknown -> {
                createDSLNode(dsl, expr.expr, table, path, parentRel).isNotNull
            }

            is RelExpression.Negative -> {
                createDSLNode(dsl, expr.expr, table, path, parentRel).neg()
            }

            // String functions
            is RelExpression.ToLower -> {
                createDSLNode(dsl, expr.expr, table, path, parentRel).lower()
            }

            is RelExpression.ToUpper -> {
                createDSLNode(dsl, expr.expr, table, path, parentRel).upper()
            }

            // Set operations
            is RelExpression.In -> {
                val leftExpr = createDSLNode(dsl, expr.expr, table, path, parentRel)
                val values = expr.list.map { createDSLNode(dsl, it, table, path, parentRel) }
                leftExpr.`in`(values)
            }

            is RelExpression.NotIn -> {
                val leftExpr = createDSLNode(dsl, expr.expr, table, path, parentRel)
                val values = expr.list.map { createDSLNode(dsl, it, table, path, parentRel) }
                leftExpr.notIn(values)
            }

            // Pattern matching operators
            is RelExpression.Like -> {
                val leftExpr = createDSLNode(dsl, expr.expr, table, path, parentRel)
                val pattern = createDSLNode(dsl, expr.pattern, table, path, parentRel) as Field<String>
                leftExpr.like(pattern)
            }

            is RelExpression.ILike -> {
                val leftExpr = createDSLNode(dsl, expr.expr, table, path, parentRel)
                val pattern = createDSLNode(dsl, expr.pattern, table, path, parentRel) as Field<String>
                leftExpr.likeIgnoreCase(pattern)
            }

            is RelExpression.NotLike -> {
                val leftExpr = createDSLNode(dsl, expr.expr, table, path, parentRel)
                val pattern = createDSLNode(dsl, expr.pattern, table, path, parentRel) as Field<String>
                leftExpr.notLike(pattern)
            }

            is RelExpression.NotILike -> {
                val leftExpr = createDSLNode(dsl, expr.expr, table, path, parentRel)
                val pattern = createDSLNode(dsl, expr.pattern, table, path, parentRel) as Field<String>
                leftExpr.notLikeIgnoreCase(pattern)
            }

            // Between operators
            is RelExpression.Between -> {
                error("Between operator is not supported yet")
            }

            is RelExpression.NotBetween -> {
                error("NotBetween operator is not supported yet")
            }

            // Aggregate functions - these should be handled separately
            is RelExpression.Count,
            is RelExpression.Sum,
            is RelExpression.Average,
            is RelExpression.Min,
            is RelExpression.Max,
            is RelExpression.Mean,
            is RelExpression.Median,
            is RelExpression.FirstValue,
            is RelExpression.LastValue,
            is RelExpression.BoolAnd,
            is RelExpression.BoolOr,
            is RelExpression.StringAgg,
            is RelExpression.Var -> {
                // This should not be reached during normal expression evaluation
                // Aggregates should be handled by createDSLAggregateExpression
                DSL.field("INVALID_AGGREGATE")
            }
        }
    }

    private fun createDSLAggregateExpression(
        dsl: DSLContext,
        expr: RelExpression,
        table: Table<*>,
        path: String,
        parentRel: Rel
    ): Field<*> {
        return when (expr) {
            is RelExpression.Count -> {
                DSL.count(createDSLNode(dsl, expr.expr, table, path, parentRel))
            }

            is RelExpression.Sum -> {
                DSL.sum(createDSLNode(dsl, expr.expr, table, path, parentRel) as Field<Number>)
            }

            is RelExpression.Average -> {
                DSL.avg(createDSLNode(dsl, expr.expr, table, path, parentRel) as Field<Number>)
            }

            is RelExpression.Min -> {
                DSL.min(createDSLNode(dsl, expr.expr, table, path, parentRel))
            }

            is RelExpression.Max -> {
                DSL.max(createDSLNode(dsl, expr.expr, table, path, parentRel))
            }

            // TODO: Figure out to how implement this such that it type-checks
            is RelExpression.FirstValue -> {
                DSL.field("FIRST_VALUE({0})", createDSLNode(dsl, expr.expr, table, path, parentRel))
            }

            // TODO: Figure out to how implement this such that it type-checks
            is RelExpression.LastValue -> {
                DSL.field("LAST_VALUE({0})", createDSLNode(dsl, expr.expr, table, path, parentRel))
            }

            is RelExpression.BoolAnd -> {
                DSL.boolAnd(createDSLCondition(dsl, expr.expr, table, path, parentRel))
            }

            is RelExpression.BoolOr -> {
                DSL.boolOr(createDSLCondition(dsl, expr.expr, table, path, parentRel))
            }

            is RelExpression.StringAgg -> {
                // GROUP_CONCAT in MySQL or STRING_AGG in PostgreSQL
                DSL.groupConcat(createDSLNode(dsl, expr.expr, table, path, parentRel))
            }

            is RelExpression.Mean -> {
                // Same as AVG
                DSL.avg(createDSLNode(dsl, expr.expr, table, path, parentRel) as Field<Number>)
            }

            is RelExpression.Median -> {
                // Most SQL dialects don't have direct median support
                DSL.median(createDSLNode(dsl, expr.expr, table, path, parentRel) as Field<Number>)
            }

            is RelExpression.Var -> {
                // Variance function
                DSL.varPop(createDSLNode(dsl, expr.expr, table, path, parentRel) as Field<Number>)
            }

            else -> {
                createDSLNode(dsl, expr, table, path, parentRel)
            }
        }
    }

    /**
     * Helper method to ensure we get a Condition from an expression
     */
    private fun createDSLCondition(
        dsl: DSLContext,
        expr: RelExpression,
        table: Table<*>,
        path: String,
        parentRel: Rel
    ): Condition {
        return createDSLNode(dsl, expr, table, path, parentRel) as Condition
    }
}
