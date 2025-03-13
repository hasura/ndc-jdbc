package io.hasura.app.default

import io.hasura.app.base.DatabaseConnection
import io.hasura.app.base.DatabaseSource
import io.hasura.common.ColumnType
import io.hasura.common.DefaultConfiguration
import io.hasura.ndc.ir.*
import kotlinx.coroutines.coroutineScope
import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.JsonArray

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

        val sql = PlanConverter.convertPlan(plan)
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

    fun convertPlan(plan: Plan): String {
        return when (plan) {
            is Plan.Aggregate -> convertAggregate(plan)
            is Plan.Distinct -> TODO()
            is Plan.DistinctOn -> TODO()
            is Plan.Filter -> convertFilter(plan)
            is Plan.From -> convertFrom(plan)
            is Plan.Join -> TODO()
            is Plan.Limit -> convertLimit(plan)
            is Plan.Project -> convertProject(plan)
            is Plan.Sort -> convertSort(plan)
        }
    }

    fun convertFrom(from: Plan.From): String {
        // id AS column_0, name AS column_1 ...
        val columnAliases = from.columns.mapIndexed { index, column -> "$column AS column_$index" }
        return "SELECT ${columnAliases.joinToString(", ")} FROM ${from.collection}"
    }

    fun convertFilter(filter: Plan.Filter): String {
        return "SELECT * FROM (${convertPlan(filter.input)}) WHERE ${convertExpr(filter.predicate)}"
    }

    fun convertAggregate(aggregate: Plan.Aggregate): String {
        return "SELECT ${aggregate.aggregates.joinToString(", ") { convertExpr(it) }} FROM (${convertPlan(aggregate.input)})"
    }

    fun convertSort(sort: Plan.Sort): String {
        fun convertSortExpr(sortExpr: SortExpr): String {
            return "${convertExpr(sortExpr.expr)} ${if (sortExpr.asc) "ASC" else "DESC"} ${if (sortExpr.nulls_first) "NULLS FIRST" else "NULLS LAST"}"
        }

        return "SELECT * FROM (${convertPlan(sort.input)}) ORDER BY ${sort.exprs.joinToString(", ") { convertSortExpr(it) }}"
    }

    fun convertLimit(limit: Plan.Limit): String {
        return "SELECT * FROM (${convertPlan(limit.input)}) LIMIT ${limit.fetch} OFFSET ${limit.skip}"
    }

    fun convertProject(project: Plan.Project): String {
        return "SELECT ${project.exprs.joinToString(", ") { convertExpr(it) }} FROM (${convertPlan(project.input)})"
    }

    fun convertExpr(expr: PlanExpression): String = when (expr) {
        is PlanExpression.And -> "${convertExpr(expr.left)} AND ${convertExpr(expr.right)}"
        is PlanExpression.Average -> "AVG(${convertExpr(expr.expr)})"
        is PlanExpression.Between -> "BETWEEN ${convertExpr(expr.low)} AND ${convertExpr(expr.high)}"
        is PlanExpression.BoolAnd -> TODO()
        is PlanExpression.BoolOr -> TODO()
        is PlanExpression.Column -> "column_${expr.index}"
        is PlanExpression.Count -> "COUNT(${convertExpr(expr.expr)})"
        is PlanExpression.Divide -> "${convertExpr(expr.left)} / ${convertExpr(expr.right)}"
        is PlanExpression.Eq -> "${convertExpr(expr.left)} = ${convertExpr(expr.right)}"
        is PlanExpression.FirstValue -> TODO()
        is PlanExpression.Gt -> "${convertExpr(expr.left)} > ${convertExpr(expr.right)}"
        is PlanExpression.GtEq -> "${convertExpr(expr.left)} >= ${convertExpr(expr.right)}"
        is PlanExpression.ILike -> "ILIKE ${expr.pattern}"
        is PlanExpression.In -> "IN (${expr.list.map { convertExpr(it) }.joinToString(", ")})"
        is PlanExpression.IsFalse -> "FALSE"
        is PlanExpression.IsNotFalse -> "IS NOT FALSE"
        is PlanExpression.IsNotNull -> "IS NOT NULL"
        is PlanExpression.IsNotTrue -> "IS NOT TRUE"
        is PlanExpression.IsNotUnknown -> "IS NOT UNKNOWN"
        is PlanExpression.IsNull -> "IS NULL"
        is PlanExpression.IsTrue -> "TRUE"
        is PlanExpression.IsUnknown -> "IS UNKNOWN"
        is PlanExpression.LastValue -> TODO()
        is PlanExpression.Like -> "LIKE ${expr.pattern}"
        is PlanExpression.PlanLiteral -> when (val x = expr.literal) {
            is Literal.BooleanLiteral -> x.value.toString()
            is Literal.Date32 -> x.value.toString()
            is Literal.Date64 -> x.value.toString()
            is Literal.DurationMicrosecond -> x.value.toString()
            is Literal.DurationMillisecond -> x.value.toString()
            is Literal.DurationNanosecond -> x.value.toString()
            is Literal.DurationSecond -> x.value.toString()
            is Literal.Float32 -> x.value.toString()
            is Literal.Float64 -> x.value.toString()
            is Literal.Int16 -> x.value.toString()
            is Literal.Int32 -> x.value.toString()
            is Literal.Int64 -> x.value.toString()
            is Literal.Int8 -> x.value.toString()
            Literal.Null -> "NULL"
            is Literal.Time32Millisecond -> x.value.toString()
            is Literal.Time32Second -> x.value.toString()
            is Literal.Time64Microsecond -> x.value.toString()
            is Literal.Time64Nanosecond -> x.value.toString()
            is Literal.TimestampMicrosecond -> x.value.toString()
            is Literal.TimestampMillisecond -> x.value.toString()
            is Literal.TimestampNanosecond -> x.value.toString()
            is Literal.TimestampSecond -> x.value.toString()
            is Literal.UInt16 -> x.value.toString()
            is Literal.UInt32 -> x.value.toString()
            is Literal.UInt64 -> x.value.toString()
            is Literal.UInt8 -> x.value.toString()
            is Literal.Utf8 -> x.value.toString()
        }

        is PlanExpression.Lt -> "${convertExpr(expr.left)} < ${convertExpr(expr.right)}"
        is PlanExpression.LtEq -> "${convertExpr(expr.left)} <= ${convertExpr(expr.right)}"
        is PlanExpression.Max -> "MAX(${convertExpr(expr.expr)})"
        is PlanExpression.Mean -> "MEAN(${convertExpr(expr.expr)})"
        is PlanExpression.Median -> "MEDIAN(${convertExpr(expr.expr)})"
        is PlanExpression.Min -> "MIN(${convertExpr(expr.expr)})"
        is PlanExpression.Minus -> "${convertExpr(expr.left)} - ${convertExpr(expr.right)}"
        is PlanExpression.Modulo -> "${convertExpr(expr.left)} % ${convertExpr(expr.right)}"
        is PlanExpression.Multiply -> "${convertExpr(expr.left)} * ${convertExpr(expr.right)}"
        is PlanExpression.Negative -> "-${convertExpr(expr.expr)}"
        is PlanExpression.Not -> "NOT ${convertExpr(expr.expr)}"
        is PlanExpression.NotBetween -> "NOT BETWEEN ${convertExpr(expr.low)} AND ${convertExpr(expr.high)}"
        is PlanExpression.NotEq -> "${convertExpr(expr.left)} != ${convertExpr(expr.right)}"
        is PlanExpression.NotILike -> "NOT ILIKE ${expr.pattern}"
        is PlanExpression.NotIn -> "NOT IN (${expr.list.map { convertExpr(it) }.joinToString(", ")})"
        is PlanExpression.NotLike -> "NOT LIKE ${expr.pattern}"
        is PlanExpression.Or -> "${convertExpr(expr.left)} OR ${convertExpr(expr.right)}"
        is PlanExpression.Plus -> "${convertExpr(expr.left)} + ${convertExpr(expr.right)}"
        is PlanExpression.StringAgg -> TODO()
        is PlanExpression.Sum -> "SUM(${convertExpr(expr.expr)})"
        is PlanExpression.ToLower -> "LOWER(${convertExpr(expr.expr)})"
        is PlanExpression.ToUpper -> "UPPER(${convertExpr(expr.expr)})"
        is PlanExpression.Var -> TODO()
    }

}



