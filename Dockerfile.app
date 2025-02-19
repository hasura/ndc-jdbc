# 🔨 Builder stage: Uses an official JDK to build the fat JAR
FROM eclipse-temurin:21 AS builder

# Define an argument for the source and project module
ARG SOURCE

# Define arguments for JOOQ Pro credentials
ARG JOOQ_PRO_EMAIL
ARG JOOQ_PRO_LICENSE

ENV JOOQ_PRO_EMAIL=${JOOQ_PRO_EMAIL}
ENV JOOQ_PRO_LICENSE=${JOOQ_PRO_LICENSE}

WORKDIR /

# Copy the Gradle project
COPY . .

# Build the fat JAR
RUN ./gradlew clean :sources:${SOURCE}:app:shadowJar

# 🎯 Runtime stage: Uses Distroless for minimal final image
FROM eclipse-temurin:21-jre-alpine

# Define an argument for the source and project module
ARG SOURCE

WORKDIR /

# Set default empty JAVA_OPTS
ENV JAVA_OPTS=""

# Set the default configuration directory environment variable and allow it to be overridden
ENV HASURA_CONFIGURATION_DIRECTORY=/etc/connector

# Set the default port environment variable and allow it to be overridden
ENV HASURA_CONNECTOR_PORT=8080

# Expose the port specified by the HASURA_CONNECTOR_PORT environment variable
EXPOSE $HASURA_CONNECTOR_PORT

# Copy the app.properties file from the source directory
COPY sources/${SOURCE}/app/app.properties* /

# Copy only the built fat JAR from the builder stage
COPY --from=builder sources/${SOURCE}/app/build/libs/app-all.jar /app.jar

# Create an entrypoint script
COPY <<-"EOF" /entrypoint.sh
#!/bin/sh
if [ -f "/app.properties" ]; then
    DEFAULT_OPTS=$(grep "jvm.opts" "/app.properties" | cut -d'=' -f2-)
else
    DEFAULT_OPTS=""
fi
exec java $DEFAULT_OPTS $JAVA_OPTS -jar /app.jar "$@"
EOF

RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
