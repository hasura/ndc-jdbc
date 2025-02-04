package hasura.default

import hasura.base.*
import hasura.ndc.connector.*
import hasura.ndc.ir.*
import org.jooq.impl.DSL
import org.jooq.SQLDialect
import org.jooq.impl.DSL.*
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.JsonArray

class DefaultQuery<T : ColumnType> : DatabaseQuery<DefaultConfiguration<T>> {
    override fun generateQuery(
        source: DatabaseSource,
        configuration: DefaultConfiguration<T>,
        request: QueryRequest
    ): String {
        ConnectorLogger.logger.info("generateSql: $request")

        val fields = request.query.fields?.mapNotNull { (alias, field) ->
            when (field) {
                is Field.Column -> DSL.field(field.column).`as`(alias)
                else -> throw ConnectorError.NotSupported("Unsupported: Relationships are not supported")
            }
        } ?: emptyList()

        val sql = DSL.using(getDatabaseDialect(source))
            .select(fields)
            .from(request.collection)

        request.query.orderBy?.let { orderBy ->
            val orderFields = orderBy.elements.map { element ->
                when (val target = element.target) {
                    is OrderByTarget.Column -> {
                        if (target.path.isNotEmpty() || target.fieldPath != null) {
                            throw ConnectorError.NotSupported("Nested fields and relationships are not supported in order by")
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

        ConnectorLogger.logger.info("SQL: ${sql.toString()}")

        return sql.toString()
    }

    private fun getDatabaseDialect(type: DatabaseSource): SQLDialect = when (type) {
        DatabaseSource.SNOWFLAKE -> SQLDialect.SNOWFLAKE
        DatabaseSource.BIGQUERY -> SQLDialect.BIGQUERY
        DatabaseSource.REDSHIFT -> SQLDialect.REDSHIFT
        else -> SQLDialect.DEFAULT
    }

    private fun generateCondition(expr: Expression): org.jooq.Condition = when (expr) {
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
                is ComparisonValue.Variable -> 
                    throw ConnectorError.NotSupported("Variable comparisons not supported yet")
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
}
