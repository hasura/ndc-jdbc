.PHONY: build run-snowflake run-bigquery run-mysql run-oracle run-databricks run-redshift build-snowflake

build:
	./gradlew build

run-snowflake:
	OTEL_SERVICE_NAME=ndc-snowflake \
	HASURA_LOG_LEVEL=debug \
	HASURA_CONNECTOR_PORT=8081 \
	HASURA_CONFIGURATION_DIRECTORY=../../../configs/snowflake \
	./gradlew :sources:snowflake:app:run

run-bigquery:
	OTEL_SERVICE_NAME=ndc-bigquery \
	HASURA_LOG_LEVEL=debug \
	HASURA_CONNECTOR_PORT=8082 \
	HASURA_CONFIGURATION_DIRECTORY=../../../configs/bigquery \
	./gradlew :sources:bigquery:app:run

run-mysql:
	OTEL_SERVICE_NAME=ndc-mysql \
	HASURA_LOG_LEVEL=debug \
	HASURA_CONNECTOR_PORT=8083 \
	HASURA_CONFIGURATION_DIRECTORY=../../../configs/mysql \
	./gradlew :sources:mysql:app:run

run-oracle:
	OTEL_SERVICE_NAME=ndc-oracle \
	HASURA_LOG_LEVEL=debug \
	HASURA_CONNECTOR_PORT=8084 \
	HASURA_CONFIGURATION_DIRECTORY=../../../configs/oracle \
	./gradlew :sources:oracle:app:run

run-databricks:
	OTEL_SERVICE_NAME=ndc-databricks \
	HASURA_LOG_LEVEL=debug \
	HASURA_CONNECTOR_PORT=8085 \
	HASURA_CONFIGURATION_DIRECTORY=../../../configs/databricks \
	./gradlew :sources:databricks:app:run

run-redshift:
	OTEL_SERVICE_NAME=ndc-redshift \
	HASURA_LOG_LEVEL=debug \
	HASURA_CONNECTOR_PORT=8086 \
	HASURA_CONFIGURATION_DIRECTORY=../../../configs/redshift \
	./gradlew :sources:redshift:app:run

build-snowflake:
ifndef VERSION
	$(error VERSION is not set. Please set VERSION before running this target)
endif
	docker buildx \
		--build-arg SOURCE=snowflake \
		--build-arg JOOQ_PRO_EMAIL="${JOOQ_PRO_EMAIL}" \
		--build-arg JOOQ_PRO_LICENSE="${JOOQ_PRO_LICENSE}" \
		-f ./Dockerfile.app \
		-t ndc-snowflake-jdbc:v${VERSION} .
	docker build \
		--build-arg SOURCE=snowflake \
		--build-arg JOOQ_PRO_EMAIL="${JOOQ_PRO_EMAIL}" \
		--build-arg JOOQ_PRO_LICENSE="${JOOQ_PRO_LICENSE}" \
		-f ./Dockerfile.cli \
		-t ndc-snowflake-jdbc-cli:v${VERSION} .

publish-snowflake-app:
ifndef VERSION
	$(error VERSION is not set. Please set VERSION before running this target)
endif
	docker buildx build \
		--build-arg SOURCE=snowflake \
		--build-arg JOOQ_PRO_EMAIL="${JOOQ_PRO_EMAIL}" \
		--build-arg JOOQ_PRO_LICENSE="${JOOQ_PRO_LICENSE}" \
		-f ./Dockerfile.app \
		--platform linux/amd64,linux/arm64 \
		--label "org.opencontainers.image.source=https://github.com/hasura/ndc-jdbc" \
		-t ghcr.io/hasura/ndc-snowflake-jdbc:v${VERSION} \
		--push .

publish-snowflake-cli:
ifndef VERSION
	$(error VERSION is not set. Please set VERSION before running this target)
endif
	docker buildx build \
		--build-arg SOURCE=snowflake \
		--build-arg JOOQ_PRO_EMAIL="${JOOQ_PRO_EMAIL}" \
		--build-arg JOOQ_PRO_LICENSE="${JOOQ_PRO_LICENSE}" \
		-f ./Dockerfile.cli \
		--label "org.opencontainers.image.source=https://github.com/hasura/ndc-jdbc" \
		--platform linux/amd64,linux/arm64 \
		-t ghcr.io/hasura/ndc-snowflake-jdbc-cli:v${VERSION} \
		--push .

build-bigquery:
ifndef VERSION
	$(error VERSION is not set. Please set VERSION before running this target)
endif
	docker build \
		--build-arg SOURCE=bigquery \
		--build-arg JOOQ_PRO_EMAIL="${JOOQ_PRO_EMAIL}" \
		--build-arg JOOQ_PRO_LICENSE="${JOOQ_PRO_LICENSE}" \
		-f ./Dockerfile.app \
		-t ndc-bigquery-jdbc:v${VERSION} .
	docker build \
		--build-arg SOURCE=bigquery \
		--build-arg JOOQ_PRO_EMAIL="${JOOQ_PRO_EMAIL}" \
		--build-arg JOOQ_PRO_LICENSE="${JOOQ_PRO_LICENSE}" \
		-f ./Dockerfile.cli \
		-t ndc-bigquery-jdbc-cli:v${VERSION} .

build-redshift:
ifndef VERSION
	$(error VERSION is not set. Please set VERSION before running this target)
endif
	docker build \
		--build-arg SOURCE=redshift \
		--build-arg JOOQ_PRO_EMAIL="${JOOQ_PRO_EMAIL}" \
		--build-arg JOOQ_PRO_LICENSE="${JOOQ_PRO_LICENSE}" \
		-f ./Dockerfile.app \
		-t ndc-redshift-jdbc:v${VERSION} .
	docker build \
		--build-arg SOURCE=redshift \
		--build-arg JOOQ_PRO_EMAIL="${JOOQ_PRO_EMAIL}" \
		--build-arg JOOQ_PRO_LICENSE="${JOOQ_PRO_LICENSE}" \
		-f ./Dockerfile.cli \
		-t ndc-redshift-jdbc-cli:v${VERSION} .
