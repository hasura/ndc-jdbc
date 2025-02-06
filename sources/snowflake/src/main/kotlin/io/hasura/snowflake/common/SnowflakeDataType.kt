package io.hasura.snowflake.common

import io.hasura.common.ColumnType
import kotlinx.serialization.Serializable

@Serializable
enum class SnowflakeDataType : ColumnType {
    ARRAY,
    BINARY,
    BOOLEAN,
    DATE,
    FLOAT,
    GEOGRAPHY,
    GEOMETRY,
    NUMBER,
    OBJECT,
    TEXT,
    TIME,
    TIMESTAMP_LTZ,
    TIMESTAMP_NTZ,
    TIMESTAMP_TZ,
    VARIANT,
    VECTOR;

    override val typeName: String
        get() = toString()
}