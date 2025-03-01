package io.hasura.app.util

import kotlinx.serialization.json.*

/**
 * Utility functions for JSON conversions
 */
object JsonUtils {
    /**
     * Attempts to parse a string as JSON, falling back to a simple string primitive if parsing fails
     */
    fun parseStringToJsonElement(value: String): JsonElement {
        return try {
            if (value.startsWith("[") || value.startsWith("{")) {
                Json.parseToJsonElement(value)
            } else {
                JsonPrimitive(value)
            }
        } catch (e: Exception) {
            JsonPrimitive(value)
        }
    }
    
    /**
     * Converts a Map with any key type to a JsonObject
     */
    fun mapToJsonObject(map: Map<*, Any?>): JsonObject {
        return buildJsonObject {
            map.forEach { (key, value) ->
                val keyStr = key.toString()
                when (value) {
                    null -> put(keyStr, JsonNull)
                    is String -> put(keyStr, parseStringToJsonElement(value))
                    is Number -> put(keyStr, value)
                    is Boolean -> put(keyStr, value)
                    is Map<*, *> -> put(keyStr, mapToJsonObject(value.mapKeys { it.key.toString() }))
                    is List<*> -> put(keyStr, listToJsonArray(value as List<Any?>))
                    is Array<*> -> put(keyStr, listToJsonArray(value.toList()))
                    is ByteArray -> put(keyStr, value.contentToString())
                    is Char -> put(keyStr, value.toString())
                    else -> put(keyStr, value.toString())
                }
            }
        }
    }
    
    /**
     * Converts a List of any type to a JsonArray
     */
    fun listToJsonArray(list: List<Any?>): JsonArray {
        return buildJsonArray {
            list.forEach { item ->
                when (item) {
                    null -> add(JsonNull)
                    is String -> add(parseStringToJsonElement(item))
                    is Number -> add(item)
                    is Boolean -> add(item)
                    is Map<*, *> -> add(mapToJsonObject(item.mapKeys { it.key.toString() }))
                    is List<*> -> add(listToJsonArray(item as List<Any?>))
                    is Array<*> -> add(listToJsonArray(item.toList()))
                    is ByteArray -> add(item.contentToString())
                    is Char -> add(item.toString())
                    else -> add(item.toString())
                }
            }
        }
    }
}

