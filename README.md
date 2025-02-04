# Hasura NDC JDBC

A Hasura Native Data Connector (NDC) implementation for JDBC databases, currently supporting:

- Snowflake
- BigQuery 
- Redshift

## Overview

This connector allows Hasura to connect to various JDBC data sources using a common architecture. It implements the Hasura NDC specification while providing database-specific optimizations and type mappings.

## Features

- Common JDBC connection pooling using HikariCP
- Database-specific type mappings and schema generation
- Support for basic queries with:
  - Field selection
  - Filtering
  - Sorting
  - Pagination
- Configuration via JSON and environment variables

## Usage

1. Set the required environment variable to specify your database:

```bash
export NDC_JDBC_SOURCE=snowflake
```

2. Create a configuration file (`configuration.json`) with your connection details:
```json
{
  "connection_uri": {
    "value": "jdbc:snowflake://..."  // or use "variable" for env var
  },
  "schemas": ["public"],
  "tables": [...]
}
```

3. Run the connector:

```bash
make run-snowflake
```

## Architecture

The connector is built with a modular architecture:

- `base/` - Core interfaces and base classes
- `default/` - Default implementations for common JDBC functionality
- `source/` - Database-specific implementations
  - `snowflake/`
  - `bigquery/`
  - `redshift/`

Each database implementation provides:
- Custom connection configuration
- Type mapping
- Schema generation
- Query optimization (where applicable)

## Development

To add support for a new database:

1. Create a new package under `source/`
2. Implement the required components:
   - Connection configuration
   - Data type mappings
   - Schema generator
   - Any database-specific optimizations
3. Add the new source to the `DatabaseSource` enum
4. Register the connector in `Main.kt`