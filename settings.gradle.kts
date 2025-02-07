rootProject.name = "ndc-jdbc"

include(":app")
include(":common")
include(":sources:bigquery", ":sources:bigquery:app", ":sources:bigquery:cli")
include(":sources:snowflake", ":sources:snowflake:app", ":sources:snowflake:cli")
include(":sources:redshift", ":sources:redshift:app", ":sources:redshift:cli")
