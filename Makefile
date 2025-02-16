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
	docker build \
		--build-arg SOURCE=snowflake \
		--build-arg PROJECT=app \
		--build-arg JOOQ_PRO_EMAIL="${JOOQ_PRO_EMAIL}" \
		--build-arg JOOQ_PRO_LICENSE="${JOOQ_PRO_LICENSE}" \
		-f ./Dockerfile \
		-t ndc-snowflake:v${VERSION} .
	docker build \
		--build-arg SOURCE=snowflake \
		--build-arg PROJECT=cli \
		--build-arg JOOQ_PRO_EMAIL="${JOOQ_PRO_EMAIL}" \
		--build-arg JOOQ_PRO_LICENSE="${JOOQ_PRO_LICENSE}" \
		-f ./Dockerfile \
		-t ndc-snowflake-cli:v${VERSION} .

build-bigquery:
ifndef VERSION
	$(error VERSION is not set. Please set VERSION before running this target)
endif
	docker build \
		--build-arg SOURCE=bigquery \
		--build-arg PROJECT=app \
		--build-arg JOOQ_PRO_EMAIL="${JOOQ_PRO_EMAIL}" \
		--build-arg JOOQ_PRO_LICENSE="${JOOQ_PRO_LICENSE}" \
		-f ./Dockerfile \
		-t ndc-bigquery:v${VERSION} .
	docker build \
		--build-arg SOURCE=bigquery \
		--build-arg PROJECT=cli \
		--build-arg JOOQ_PRO_EMAIL="${JOOQ_PRO_EMAIL}" \
		--build-arg JOOQ_PRO_LICENSE="${JOOQ_PRO_LICENSE}" \
		-f ./Dockerfile \
		-t ndc-bigquery-cli:v${VERSION} .

build-redshift:
ifndef VERSION
	$(error VERSION is not set. Please set VERSION before running this target)
endif
	docker build \
		--build-arg SOURCE=redshift \
		--build-arg PROJECT=app \
		--build-arg JOOQ_PRO_EMAIL="${JOOQ_PRO_EMAIL}" \
		--build-arg JOOQ_PRO_LICENSE="${JOOQ_PRO_LICENSE}" \
		-f ./Dockerfile \
		-t ndc-redshift:v${VERSION} .
	docker build \
		--build-arg SOURCE=redshift \
		--build-arg PROJECT=cli \
		--build-arg JOOQ_PRO_EMAIL="${JOOQ_PRO_EMAIL}" \
		--build-arg JOOQ_PRO_LICENSE="${JOOQ_PRO_LICENSE}" \
		-f ./Dockerfile \
		-t ndc-redshift-cli:v${VERSION} .
