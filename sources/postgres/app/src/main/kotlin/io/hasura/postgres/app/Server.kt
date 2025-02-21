package io.hasura.postgres.app


import ch.qos.logback.classic.Logger
import com.vdurmont.semver4j.Requirement
import com.vdurmont.semver4j.Semver
import io.hasura.app.default.DefaultState
import io.hasura.common.ColumnType
import io.hasura.common.DefaultConfiguration
import io.hasura.ndc.connector.Connector
import io.hasura.ndc.connector.ServerOptions
import io.hasura.ndc.connector.Telemetry
import io.hasura.ndc.ir.*
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics
import io.micrometer.core.instrument.binder.system.ProcessorMetrics
import io.micrometer.prometheus.PrometheusConfig
import io.micrometer.prometheus.PrometheusMeterRegistry
import io.vertx.core.Vertx
import io.vertx.ext.web.Router
import io.vertx.ext.web.RoutingContext
import io.vertx.ext.web.handler.BodyHandler
import io.vertx.kotlin.coroutines.CoroutineRouterSupport
import io.vertx.kotlin.coroutines.CoroutineVerticle
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import org.slf4j.LoggerFactory
import java.util.*

/*
 * Logger for the sdk
 */
object ConnectorLogger {
    val logger = LoggerFactory.getLogger(ConnectorLogger::class.java)
}

/*
 * Start Server
 */
fun <Configuration, State> startServer(
    connector: Connector<Configuration, State>,
    args: Array<String>
) {
    runBlocking {
        val options = ServerOptions.fromArgs(args)
        startServer(connector, options)
    }
}

suspend fun <Configuration, State> startServer(
    connector: Connector<Configuration, State>,
    options: ServerOptions
) {
    val vertx = Vertx.vertx()

    // Set log level based on configuration
    val rootLogger = LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME) as Logger
    val logLevel = when (options.logLevel.uppercase()) {
        "TRACE" -> ch.qos.logback.classic.Level.TRACE
        "DEBUG" -> ch.qos.logback.classic.Level.DEBUG
        "INFO" -> ch.qos.logback.classic.Level.INFO
        "WARN" -> ch.qos.logback.classic.Level.WARN
        "ERROR" -> ch.qos.logback.classic.Level.ERROR
        else -> ch.qos.logback.classic.Level.INFO
    }
    rootLogger.level = logLevel

    vertx.deployVerticle(object : CoroutineVerticle(), CoroutineRouterSupport {
        private val logger = LoggerFactory.getLogger(this::class.java)

        override suspend fun start() {
            // Set default timezone to UTC
            TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

            // Initialize telemetry
            Telemetry.initTelemetry()

            // Initialize metrics
            val appMicrometerRegistry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)

            // Add JVM metrics collectors
            JvmMemoryMetrics().bindTo(appMicrometerRegistry)
            JvmGcMetrics().bindTo(appMicrometerRegistry)
            ProcessorMetrics().bindTo(appMicrometerRegistry)

            // Parse configuration
            val configuration = connector.parseConfiguration(java.nio.file.Path.of(options.configuration))

            // Initialize state
            val state = connector.tryInitState(configuration, appMicrometerRegistry)

            val router = Router.router(vertx)
            router.route().handler(BodyHandler.create())

            // Global error handler
            router.route().failureHandler { ctx ->
                val failure = ctx.failure()
                val statusCode = when (failure) {
                    is AuthenticationException -> 401
                    is IllegalArgumentException -> 400
                    else -> 500
                }

                ConnectorLogger.logger.error("Request failed", failure)
                Telemetry.recordError(failure)

                ctx.response()
                    .setStatusCode(statusCode)
                    .putHeader("content-type", "application/json")
                    .end(
                        Json.encodeToString(
                            ErrorResponse(
                                message = "Internal Error",
                                details = JsonObject(
                                    mapOf(
                                        "cause" to JsonPrimitive(
                                            failure?.message ?: "Unknown error"
                                        )
                                    )
                                )
                            )
                        )
                    )
            }

            // Authentication middleware
            router.route().handler { ctx ->
                authenticationHandler(ctx, options.serviceTokenSecret)
            }

            // Version check middleware
            router.route().handler { ctx ->
                versionCheckHandler(ctx)
            }

            // Routes
            router.get("/capabilities").coHandler { ctx ->
                val response = Telemetry.withActiveSpan("getCapabilities") {
                    CapabilitiesResponse(
                        version = VERSION,
                        capabilities = connector.getCapabilities(configuration)
                    )
                }
                ctx.sendJson(response)
            }

            router.get("/health").coHandler { ctx ->
                val response = withContext(Dispatchers.IO) {
                    connector.getHealthReadiness(configuration, state)
                }
                ctx.sendJson(response)
            }

            router.get("/metrics").coHandler { ctx ->
                val response = withContext(Dispatchers.IO) {
                    connector.fetchMetrics(configuration, state)
                    appMicrometerRegistry.scrape()
                }
                ctx.sendPlainText(response)
            }

            router.get("/schema").coHandler { ctx ->
                val response = withContext(Dispatchers.IO) {
                    Telemetry.withActiveSpan("getSchema") {
                        connector.getSchema(configuration)
                    }
                }
                ctx.sendJson(response)
            }

            router.post("/query").coHandler { ctx ->
                ctx.handleJsonRequest<QueryRequest, QueryResponse>("query") { request ->
                    connector.query(configuration, state, request)
                }
            }

            router.post("/query/explain").coHandler { ctx ->
                ctx.handleJsonRequest<QueryRequest, ExplainResponse>("queryExplain") { request ->
                    connector.queryExplain(configuration, state, request)
                }
            }

            router.post("/mutation").coHandler { ctx ->
                ctx.handleJsonRequest<MutationRequest, MutationResponse>("mutation") { request ->
                    connector.mutation(configuration, state, request)
                }
            }

            router.post("/mutation/explain").coHandler { ctx ->
                ctx.handleJsonRequest<MutationRequest, ExplainResponse>("mutationExplain") { request ->
                    connector.mutationExplain(configuration, state, request)
                }
            }

            if (connector is ExperimentalSQLConnector<*>) {
                println("SQL endpoint enabled")
                router.post("/sql").coHandler { ctx ->
                    ctx.handleJsonRequest<SQLRequest, QueryResponse>("sql") { request ->
                        connector.experimentalSQL(
                            configuration as DefaultConfiguration<out ColumnType>,
                            state as DefaultState<out ColumnType>,
                            request
                        )
                    }
                }
            }

            vertx.createHttpServer()
                .requestHandler(router)
                .listen(options.port, options.host)
                .onSuccess {
                    ConnectorLogger.logger.info("Server started on http://${options.host}:${options.port}")
                }
                .onFailure {
                    ConnectorLogger.logger.error("Failed to start server", it)
                    System.exit(1)
                }
        }
    })
}

private fun authenticationHandler(ctx: RoutingContext, serviceTokenSecret: String?) {
    if (ctx.request().method().name() == "GET" || ctx.request().path() == "/health") {
        ctx.next()
        return
    }

    val expectedAuthHeader = serviceTokenSecret?.let { "Bearer $it" }
    val authHeader =
        ctx.request().getHeader("Authorization")?.replace(Regex("^bearer", RegexOption.IGNORE_CASE), "Bearer")

    if (authHeader != expectedAuthHeader) {
        ctx.response()
            .setStatusCode(401)
            .putHeader("content-type", "application/json")
            .end(
                Json.encodeToString(
                    ErrorResponse(
                        details = JsonObject(mapOf("cause" to JsonPrimitive("Bearer token does not match."))),
                        message = "Internal Error"
                    )
                )
            )
        return
    }
    ctx.next()
}

private fun versionCheckHandler(ctx: RoutingContext) {
    val versionHeaders = ctx.request().headers()
        .names()
        .filter { it.equals("x-hasura-ndc-version", ignoreCase = true) }
        .flatMap { ctx.request().headers().getAll(it) }
    if (versionHeaders.size > 1) {
        handleVersionError(ctx, "Multiple X-Hasura-NDC-Version headers received. Only one is supported.")
        return
    }

    val versionHeader = versionHeaders.firstOrNull() ?: run {
        ctx.next()
        return
    }

    try {
        val wantedVersion = Semver(versionHeader)
        val currentVersion = Semver(VERSION)
        val requirement = Requirement.buildNPM("^${wantedVersion}")

        if (!requirement.isSatisfiedBy(currentVersion)) {
            handleVersionError(ctx, "The connector does not support the requested NDC version")
            return
        }
    } catch (e: IllegalArgumentException) {
        handleVersionError(ctx, "Invalid semver in X-Hasura-NDC-Version header")
        return
    }
    ctx.next()
}

private fun handleVersionError(ctx: RoutingContext, message: String) {
    ctx.response()
        .setStatusCode(400)
        .putHeader("content-type", "application/json")
        .end(
            Json.encodeToString(
                ErrorResponse(
                    message = message,
                    details = null
                )
            )
        )
}

private inline fun <reified T> RoutingContext.sendJson(response: T) {
    this.response()
        .putHeader("content-type", "application/json")
        .end(Json.encodeToString(response))
}

private fun RoutingContext.sendPlainText(response: String) {
    this.response()
        .putHeader("content-type", "text/plain")
        .end(response)
}

private suspend inline fun <reified T, reified R> RoutingContext.handleJsonRequest(
    spanName: String,
    crossinline action: suspend (T) -> R
) {
    try {
        val body = this.body().asString()
        val request = Json.decodeFromString<T>(body)
        val response = withContext(Dispatchers.IO) {
            Telemetry.withActiveSpan(spanName) {
                action(request)
            }
        }
        this.sendJson(response)
    } catch (e: Exception) {
        when (e) {
            is io.vertx.core.json.DecodeException,
            is kotlinx.serialization.SerializationException -> {
                this.response()
                    .setStatusCode(400)
                    .putHeader("content-type", "application/json")
                    .end(
                        Json.encodeToString(
                            ErrorResponse(
                                message = "Invalid JSON request body",
                                details = JsonObject(mapOf("cause" to JsonPrimitive(e.message ?: "Unknown error")))
                            )
                        )
                    )
            }

            else -> throw e
        }
    }
}

class AuthenticationException(message: String) : RuntimeException(message)