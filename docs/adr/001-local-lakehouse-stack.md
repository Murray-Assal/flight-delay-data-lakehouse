# ADR 001: Use Iceberg on MinIO with Spark, Trino, and Metabase

- Status: accepted
- Date: 2026-08-22

## Context

The project must demonstrate a local, no-cost medallion lakehouse with schema evolution, time travel, and BI consumption. The stack should be reproducible from a fresh checkout and resemble common production patterns without copying a cloud platform.

## Decision

Use the following locally containerized components:

| Concern | Component |
|---|---|
| Object storage | MinIO (S3-compatible) |
| Table format | Apache Iceberg |
| Transformation engine | Apache Spark with PySpark |
| Iceberg catalog | JDBC catalog backed by PostgreSQL |
| Query engine | Trino with the Iceberg connector |
| BI | Metabase, connected to Trino |
| Environment | Docker Compose |

Spark will own data writes. Trino will serve the Gold layer to Metabase. PostgreSQL is metadata infrastructure and the Metabase application database; it is not the analytics warehouse.

## Rationale

- Iceberg supports atomic table commits, schema evolution, and snapshot-based time travel.
- MinIO preserves the S3 object-store workflow without cloud cost.
- Spark is the transformation engine most relevant to typical data-engineering roles.
- Trino can query Iceberg tables and is a direct, credible connector for Metabase.
- The JDBC catalog avoids running a Hive Metastore or separate catalog server for a focused local project.

## Consequences

- The project requires several containers and sufficient local Docker resources.
- PostgreSQL credentials remain local-only configuration.
- The JDBC catalog is appropriate for this learning project, but a production deployment could instead choose a governed REST catalog, managed catalog, or cloud-native catalog.
- Airflow is deferred until the CLI pipeline is reliable and idempotent.

## Alternatives considered

| Alternative | Why not selected now |
|---|---|
| Delta Lake | Valid table format, but Iceberg has a direct Trino integration path for this stack. |
| Hive Metastore | Adds a service without adding learning value for this focused project. |
| REST catalog or Nessie | Good production options; adds a catalog service and configuration surface to the first milestone. |
| OpenSky as primary data source | Its official API does not provide schedules or commercial delay data. |
