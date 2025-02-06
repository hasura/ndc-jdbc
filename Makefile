.PHONY: build run-snowflake run-bigquery run-mysql run-oracle run-databricks

build:
	./gradlew build

run-snowflake:
	OTEL_SERVICE_NAME=ndc-snowflake HASURA_CONNECTOR_PORT=8081 HASURA_CONFIGURATION_DIRECTORY=../../../configs/snowflake ./gradlew :sources:snowflake:app:run

run-bigquery:
	OTEL_SERVICE_NAME=ndc-bigquery HASURA_CONNECTOR_PORT=8082 HASURA_CONFIGURATION_DIRECTORY=../../../configs/bigquery ./gradlew :sources:bigquery:app:run

run-mysql:
	OTEL_SERVICE_NAME=ndc-mysql HASURA_CONNECTOR_PORT=8083 HASURA_CONFIGURATION_DIRECTORY=../../../configs/mysql ./gradlew :sources:mysql:app:run

run-oracle:
	OTEL_SERVICE_NAME=ndc-oracle HASURA_CONNECTOR_PORT=8084 HASURA_CONFIGURATION_DIRECTORY=../../../configs/oracle ./gradlew :sources:oracle:app:run

run-databricks:
	OTEL_SERVICE_NAME=ndc-databricks HASURA_CONNECTOR_PORT=8085 HASURA_CONFIGURATION_DIRECTORY=../../../configs/databricks ./gradlew :sources:databricks:app:run

run-redshift:
	OTEL_SERVICE_NAME=ndc-redshift HASURA_CONNECTOR_PORT=8086 HASURA_CONFIGURATION_DIRECTORY=../../../configs/redshift ./gradlew :sources:redshift:app:run