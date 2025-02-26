.PHONY: build \
    build-snowflake build-snowflake-app build-snowflake-cli \
    build-bigquery build-bigquery-app build-bigquery-cli \
    build-redshift build-redshift-app build-redshift-cli \
    build-databricks build-databricks-app build-databricks-cli \
    run-snowflake run-bigquery run-mysql run-oracle run-databricks run-redshift \
    docker-snowflake docker-snowflake-app docker-snowflake-cli \
    publish-snowflake publish-snowflake-app publish-snowflake-cli \
    publish-bigquery publish-bigquery-app publish-bigquery-cli \
    publish-databricks publish-databricks-app publish-databricks-cli \
    docker-bigquery docker-bigquery-app docker-bigquery-cli \
    docker-redshift docker-redshift-app docker-redshift-cli \
    docker-databricks docker-databricks-app docker-databricks-cli \
	  run-snowflake-introspection run-bigquery-introspection \
	  run-databricks-introspection run-snowflake-cli


build:
	./gradlew build

build-snowflake:
	build-snowflake-app
	build-snowflake-cli

build-snowflake-app:
	./gradlew :sources:snowflake:app:build

build-snowflake-cli:
	./gradlew :sources:snowflake:app:build

build-bigquery:
	build-bigquery-app
	build-bigquery-cli

build-bigquery-app:
	./gradlew :sources:bigquery:app:build

build-bigquery-cli:
	./gradlew :sources:bigquery:app:build

build-redshift:
	build-redshift-app
	build-redshift-cli

build-redshift-app:
	./gradlew :sources:redshift:app:build

build-redshift-cli:
	./gradlew :sources:redshift:app:build

build-databricks:
	build-databricks-app
	build-databricks-cli

build-databricks-app:
	./gradlew :sources:databricks:app:build

build-databricks-cli:
	./gradlew :sources:databricks:app:build

run-snowflake:
	OTEL_SERVICE_NAME=ndc-snowflake \
	HASURA_LOG_LEVEL=debug \
	HASURA_CONNECTOR_PORT=8081 \
	HASURA_CONFIGURATION_DIRECTORY=../../../configs/snowflake \
	./gradlew :sources:snowflake:app:run

run-snowflake-introspection:
ifndef JDBC_URL
	$(error JDBC_URL environment variable is not set)
endif
	./gradlew ':sources:snowflake:cli:run' --args="update --jdbc-url JDBC_URL --outfile ../../../configs/snowflake/configuration.json"

run-bigquery-introspection:
ifndef JDBC_URL
	$(error JDBC_URL environment variable is not set)
endif
	./gradlew ':sources:bigquery:cli:run' --args="update --jdbc-url JDBC_URL --outfile ../../../configs/bigquery/configuration.json"

run-databricks-introspection:
ifndef JDBC_URL
	$(error JDBC_URL environment variable is not set)
endif
	./gradlew ':sources:databricks:cli:run' --args="update --jdbc-url JDBC_URL --outfile ../../../configs/databricks/configuration.json"

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

docker-snowflake:
ifndef VERSION
	$(error VERSION is not set. Please set VERSION before running this target)
endif
	$(MAKE) docker-snowflake-app
	$(MAKE) docker-snowflake-cli

docker-snowflake-app:
ifndef VERSION
	$(error VERSION is not set. Please set VERSION before running this target)
endif
	docker build \
		--build-arg SOURCE=snowflake \
		--build-arg JOOQ_PRO_EMAIL="${JOOQ_PRO_EMAIL}" \
		--build-arg JOOQ_PRO_LICENSE="${JOOQ_PRO_LICENSE}" \
		-f ./Dockerfile.app \
		-t ndc-snowflake-jdbc:v${VERSION} .

docker-snowflake-cli:
ifndef VERSION
	$(error VERSION is not set. Please set VERSION before running this target)
endif
	docker build \
		--build-arg SOURCE=snowflake \
		--build-arg JOOQ_PRO_EMAIL="${JOOQ_PRO_EMAIL}" \
		--build-arg JOOQ_PRO_LICENSE="${JOOQ_PRO_LICENSE}" \
		-f ./Dockerfile.cli \
		-t ndc-snowflake-jdbc-cli:v${VERSION} .

publish-snowflake:
	$(MAKE) publish-snowflake-app
	$(MAKE) publish-snowflake-cli

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

docker-bigquery:
	$(MAKE) docker-bigquery-app
	$(MAKE) docker-bigquery-cli

docker-bigquery-app:
ifndef VERSION
	$(error VERSION is not set. Please set VERSION before running this target)
endif
	docker build \
		--build-arg SOURCE=bigquery \
		--build-arg JOOQ_PRO_EMAIL="${JOOQ_PRO_EMAIL}" \
		--build-arg JOOQ_PRO_LICENSE="${JOOQ_PRO_LICENSE}" \
		-f ./Dockerfile.app \
		-t ndc-bigquery-jdbc:v${VERSION} .

docker-bigquery-cli:
ifndef VERSION
	$(error VERSION is not set. Please set VERSION before running this target)
endif
	docker build \
		--build-arg SOURCE=bigquery \
		--build-arg JOOQ_PRO_EMAIL="${JOOQ_PRO_EMAIL}" \
		--build-arg JOOQ_PRO_LICENSE="${JOOQ_PRO_LICENSE}" \
		-f ./Dockerfile.cli \
		-t ndc-bigquery-jdbc-cli:v${VERSION} .

publish-bigquery:
	$(MAKE) publish-bigquery-app
	$(MAKE) publish-bigquery-cli

publish-bigquery-app:
ifndef VERSION
	$(error VERSION is not set. Please set VERSION before running this target)
endif
	docker buildx build \
		--build-arg SOURCE=bigquery \
		--build-arg JOOQ_PRO_EMAIL="${JOOQ_PRO_EMAIL}" \
		--build-arg JOOQ_PRO_LICENSE="${JOOQ_PRO_LICENSE}" \
		-f ./Dockerfile.app \
		--platform linux/amd64,linux/arm64 \
		--label "org.opencontainers.image.source=https://github.com/hasura/ndc-jdbc" \
		-t ghcr.io/hasura/ndc-bigquery-jdbc:v${VERSION} \
		--push .

publish-bigquery-cli:
ifndef VERSION
	$(error VERSION is not set. Please set VERSION before running this target)
endif
	docker buildx build \
		--build-arg SOURCE=bigquery \
		--build-arg JOOQ_PRO_EMAIL="${JOOQ_PRO_EMAIL}" \
		--build-arg JOOQ_PRO_LICENSE="${JOOQ_PRO_LICENSE}" \
		-f ./Dockerfile.cli \
		--label "org.opencontainers.image.source=https://github.com/hasura/ndc-jdbc" \
		--platform linux/amd64,linux/arm64 \
		-t ghcr.io/hasura/ndc-bigquery-jdbc-cli:v${VERSION} \
		--push .

docker-redshift:
	$(MAKE) docker-redshift-app
	$(MAKE) docker-redshift-cli

docker-redshift-app:
ifndef VERSION
	$(error VERSION is not set. Please set VERSION before running this target)
endif
	docker build \
		--build-arg SOURCE=redshift \
		--build-arg JOOQ_PRO_EMAIL="${JOOQ_PRO_EMAIL}" \
		--build-arg JOOQ_PRO_LICENSE="${JOOQ_PRO_LICENSE}" \
		-f ./Dockerfile.app \
		-t ndc-redshift-jdbc:v${VERSION} .

docker-redshift-cli:
ifndef VERSION
	$(error VERSION is not set. Please set VERSION before running this target)
endif
	docker build \
		--build-arg SOURCE=redshift \
		--build-arg JOOQ_PRO_EMAIL="${JOOQ_PRO_EMAIL}" \
		--build-arg JOOQ_PRO_LICENSE="${JOOQ_PRO_LICENSE}" \
		-f ./Dockerfile.cli \
		-t ndc-redshift-jdbc-cli:v${VERSION} .

docker-databricks:
	$(MAKE) docker-databricks-app
	$(MAKE) docker-databricks-cli

docker-databricks-app:
ifndef VERSION
	$(error VERSION is not set. Please set VERSION before running this target)
endif
	docker build \
		--build-arg SOURCE=databricks \
		--build-arg JOOQ_PRO_EMAIL="${JOOQ_PRO_EMAIL}" \
		--build-arg JOOQ_PRO_LICENSE="${JOOQ_PRO_LICENSE}" \
		-f ./Dockerfile.app \
		-t ndc-databricks-jdbc:v${VERSION} .

docker-databricks-cli:
ifndef VERSION
	$(error VERSION is not set. Please set VERSION before running this target)
endif
	docker build \
		--build-arg SOURCE=databricks \
		--build-arg JOOQ_PRO_EMAIL="${JOOQ_PRO_EMAIL}" \
		--build-arg JOOQ_PRO_LICENSE="${JOOQ_PRO_LICENSE}" \
		-f ./Dockerfile.cli \
		-t ndc-databricks-jdbc-cli:v${VERSION} .

publish-databricks:
	$(MAKE) publish-databricks-app
	$(MAKE) publish-databricks-cli

publish-databricks-app:
ifndef VERSION
	$(error VERSION is not set. Please set VERSION before running this target)
endif
	docker buildx build \
		--build-arg SOURCE=databricks \
		--build-arg JOOQ_PRO_EMAIL="${JOOQ_PRO_EMAIL}" \
		--build-arg JOOQ_PRO_LICENSE="${JOOQ_PRO_LICENSE}" \
		-f ./Dockerfile.app \
		--platform linux/amd64,linux/arm64 \
		--label "org.opencontainers.image.source=https://github.com/hasura/ndc-jdbc" \
		-t ghcr.io/hasura/ndc-databricks-jdbc:v${VERSION} \
		--push .

publish-databricks-cli:
ifndef VERSION
	$(error VERSION is not set. Please set VERSION before running this target)
endif
	docker buildx build \
		--build-arg SOURCE=databricks \
		--build-arg JOOQ_PRO_EMAIL="${JOOQ_PRO_EMAIL}" \
		--build-arg JOOQ_PRO_LICENSE="${JOOQ_PRO_LICENSE}" \
		-f ./Dockerfile.cli \
		--label "org.opencontainers.image.source=https://github.com/hasura/ndc-jdbc" \
		--platform linux/amd64,linux/arm64 \
		-t ghcr.io/hasura/ndc-databricks-jdbc-cli:v${VERSION} \
		--push .
