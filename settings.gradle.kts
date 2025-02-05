rootProject.name = "ndc-jdbc"

include(":ndc-sdk-kotlin")
project(":ndc-sdk-kotlin").projectDir = file("../ndc-sdk-kotlin/sdk")

include(":app")
include(":bigquery", ":bigquery:app", ":bigquery:cli")