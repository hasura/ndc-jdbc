# 🔨 Builder stage: Uses an official JDK to build the fat JAR
FROM eclipse-temurin:21 AS builder

# Define an argument for the source and project module
ARG SOURCE
ARG PROJECT
# Define arguments for JOOQ Pro credentials
ARG JOOQ_PRO_EMAIL
ARG JOOQ_PRO_LICENSE

ENV JOOQ_PRO_EMAIL=${JOOQ_PRO_EMAIL}
ENV JOOQ_PRO_LICENSE=${JOOQ_PRO_LICENSE}

WORKDIR /

# Copy the Gradle project
COPY . .

# Build the fat JAR
RUN ./gradlew clean :sources:${SOURCE}:${PROJECT}:shadowJar

# 🎯 Runtime stage: Uses Distroless for minimal final image
FROM gcr.io/distroless/java21:nonroot

# Define an argument for the source and project module
ARG SOURCE
ARG PROJECT

WORKDIR /

# Copy only the built fat JAR from the builder stage
COPY --from=builder sources/${SOURCE}/${PROJECT}/build/libs/${PROJECT}-all.jar /app.jar

# Set entrypoint to run the application
ENTRYPOINT ["java", "-jar", "/app.jar"]
