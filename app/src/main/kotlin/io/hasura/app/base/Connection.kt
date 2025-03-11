package io.hasura.app.base

import io.hasura.ndc.connector.*
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import io.hasura.common.configuration.*
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
        return HikariConfig().apply {
            this.driverClassName = className
            jdbcUrl = config.connectionUri.resolve()
            maximumPoolSize = 10
            minimumIdle = 1
            connectionTimeout = 30000L
            initializationFailTimeout = 30000L
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
