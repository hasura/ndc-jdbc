package io.hasura.snowflake.app

import io.hasura.app.default.*
import io.hasura.ndc.ir.*
import io.hasura.snowflake.common.SnowflakeDataType

class SnowflakeSchemaGenerator : DefaultSchemaGenerator<SnowflakeDataType>() {
    override fun mapToTypeRepresentation(
        columnType: SnowflakeDataType,
        numericPrecision: Int?,
        numericScale: Int?
    ): ScalarType {
        val representationType = when (columnType) {
            SnowflakeDataType.NUMBER -> when {
                numericScale == 0 -> when {
                    numericPrecision == null -> RepresentationType.Bigdecimal
                    numericPrecision <= 2 -> RepresentationType.Int8
                    numericPrecision <= 4 -> RepresentationType.Int16
                    numericPrecision <= 9 -> RepresentationType.Int32
                    else -> RepresentationType.Int64
                }
                numericScale != null && numericScale > 0 -> when {
                    numericPrecision == null -> RepresentationType.Float32
                    numericPrecision <= 15 -> RepresentationType.Float64
                    else -> RepresentationType.Bigdecimal
                }
                else -> RepresentationType.Bigdecimal
            }
            SnowflakeDataType.FLOAT -> 
                if (numericPrecision != null && numericPrecision <= 24) 
                    RepresentationType.Float32 
                else 
                    RepresentationType.Float64
            SnowflakeDataType.TEXT -> RepresentationType.TypeString
            SnowflakeDataType.BINARY -> RepresentationType.Bytes
            SnowflakeDataType.DATE -> RepresentationType.Date
            SnowflakeDataType.TIME -> RepresentationType.Timestamp
            SnowflakeDataType.TIMESTAMP_NTZ -> RepresentationType.Timestamp
            SnowflakeDataType.TIMESTAMP_LTZ, 
            SnowflakeDataType.TIMESTAMP_TZ -> RepresentationType.Timestamptz
            SnowflakeDataType.VARIANT,
            SnowflakeDataType.OBJECT,
            SnowflakeDataType.ARRAY -> RepresentationType.JSON
            SnowflakeDataType.GEOGRAPHY -> RepresentationType.Geography
            SnowflakeDataType.GEOMETRY -> RepresentationType.Geometry
            SnowflakeDataType.BOOLEAN -> RepresentationType.TypeBoolean
            else -> null
        }
        
        return createScalarType(representationType, columnType.name)
    }
}
