package io.hasura.app.base

import io.hasura.ndc.connector.*
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import io.hasura.common.*
import java.sql.Connection

interface DatabaseConnection {
    @Throws(ConnectorError::class)
    fun getConnection(): Connection
    fun close()
    suspend fun <T> withConnection(block: suspend (Connection) -> T): T
}

abstract class BaseHikariConnection(protected val config: Configuration) : DatabaseConnection {
    protected abstract fun getDriverConfig(): DatabaseDriver

    protected data class DatabaseDriver(
        val className: String,
        val configure: (HikariConfig) -> Unit = {}
    )

    private val dataSource: HikariDataSource by lazy {
        try {
            val driver = getDriverConfig()

            HikariDataSource(createBaseHikariConfig(driver.className).apply {
                driver.configure(this)
            }).also {
                ConnectorLogger.logger.info("Connection pool initialized")
            }
        } catch (e: Throwable) {
            throw ConnectorError.InternalServerError(
                "Failed to initialize connection pool: ${e.message}",
                details = mapOf("error" to (e.message ?: "Unknown error"))
            )
        }
    }

    private fun createBaseHikariConfig(className: String): HikariConfig {
        val poolMaxSize = System.getenv("CONNECTION_POOL_MAX_SIZE")?.toIntOrNull()
            ?: config.connectionPoolSettings?.maxConnections
            ?: 10
        val poolMinIdle = System.getenv("CONNECTION_POOL_MIN_IDLE")?.toIntOrNull()
            ?: config.connectionPoolSettings?.minIdle
            ?: 1
        val poolConnectionTimeout = (System.getenv("CONNECTION_POOL_TIMEOUT")?.toLongOrNull()
            ?: config.connectionPoolSettings?.connectionTimeout
            ?: 30000L).toLong()
        val poolFailTimeout = (System.getenv("CONNECTION_POOL_INITIALIZATION_FAIL_TIMEOUT")?.toLongOrNull()
            ?: config.connectionPoolSettings?.initializationFailTimeout
            ?: 30000L).toLong()

        return HikariConfig().apply {
            this.driverClassName = className
            jdbcUrl = config.connectionUri.resolve()
            maximumPoolSize = poolMaxSize
            minimumIdle = poolMinIdle
            connectionTimeout = poolConnectionTimeout
            initializationFailTimeout = poolFailTimeout
        }
    }

    override fun getConnection(): Connection {
        try {
            return dataSource.connection
        } catch (e: Throwable) {
            throw ConnectorError.InternalServerError(
                "Failed to get connection from pool: ${e.message}",
                details = mapOf("error" to (e.message ?: "Unknown error"))
            )
        }
    }

    override fun close() {
        try {
            dataSource.close()
        } catch (e: Throwable) {
            throw ConnectorError.InternalServerError(
                "Failed to close connection pool: ${e.message}",
                details = mapOf("error" to (e.message ?: "Unknown error"))
            )
        }
    }

    override suspend fun <T> withConnection(block: suspend (Connection) -> T): T {
        val connection = getConnection()
        try {
            return block(connection)
        } finally {
            connection.close()
        }
    }
}
