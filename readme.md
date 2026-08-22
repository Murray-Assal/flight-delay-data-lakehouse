# Flight Delay Lakehouse

> A local, end-to-end data lakehouse that ingests flight-status data, applies the medallion architecture, and serves delay analytics to Metabase.

## Project status

**Planning — not yet implemented.** This README is the build checklist and will be updated with runnable commands, screenshots, and results as each milestone is completed.

## Why this project?

This project is designed to demonstrate practical data-engineering skills that are common in lakehouse roles:

- Medallion data modelling: Bronze, Silver, and Gold layers.
- Apache Iceberg tables on object storage, including ACID transactions and time travel.
- Schema evolution without breaking ingestion.
- Late-arriving data processing and idempotent reruns.
- Slowly changing dimensions (SCD Type 2).
- Analytics consumption through Trino and Metabase.

## Business question

> Which airlines, airports, and hours of the day have the highest flight-delay rates?

For this project, a delayed flight is one with `delay_minutes > 15`. The source fields and calculation will be recorded in the data contract before ingestion is built.

## Architecture

```text
Aviationstack API
       |
       v
Bronze ── raw, immutable JSON in MinIO + ingestion metadata
       |
       v
Silver ── typed, deduplicated Iceberg flight events
       |      + late-data correction + schema evolution
       |
       +── Iceberg SCD Type 2 airport and airline dimensions
       |
       v
Gold   ── hourly delay metrics by airline and airport (Iceberg)
       |
       v
Trino ──> Metabase dashboard
```

### Technology choices

| Area | Choice | Reason |
|---|---|---|
| Flight data | Aviationstack API | Supplies flight-status data usable for delay analysis. |
| Object storage | MinIO | Local, S3-compatible, and free. |
| Table format | Apache Iceberg | Provides schema evolution, snapshots, and time travel. |
| Compute | Apache Spark (PySpark) | Industry-standard distributed data processing. |
| SQL serving | Trino | Queries Iceberg data and connects cleanly to BI. |
| BI | Metabase | Visualizes Gold-layer metrics. |
| Local environment | Docker Compose | Reproducible setup with no cloud cost. |

OpenSky is intentionally not the primary source because it provides aircraft-tracking data but does not provide commercial schedule or delay data.

## Scope

### Included

- Low-frequency Aviationstack ingestion, retaining every API response as immutable JSON.
- A small, focused airport/airline scope suitable for the API's free request allowance.
- Reproducible local data fixtures for development and demonstrations.
- A Bronze-to-Silver-to-Gold batch pipeline.
- A Metabase dashboard backed by Gold tables through Trino.

### Explicitly deferred

- Cloud deployment and paid cloud services.
- Streaming and Kafka.
- Kubernetes.
- Airflow orchestration, until the command-line pipeline is complete and idempotent.

## Data layers

| Layer | Storage | Purpose | Main output |
|---|---|---|---|
| Bronze | JSON objects in MinIO and an Iceberg ingestion manifest | Preserve the source exactly as received; append only. | Raw API response, source URL, ingestion timestamp, file path, payload hash. |
| Silver | Iceberg | Parse, cast, validate, deduplicate, and reconcile late updates. | One current record per flight instance, with audit columns. |
| Gold | Iceberg | Aggregate and model data for analytics. | Hourly airport and airline delay metrics; SCD Type 2 dimensions. |

### Core data model

The Silver pipeline will create a stable `flight_instance_key` from the flight number, departure airport, and scheduled departure timestamp. It will retain `raw_payload`, `source_file`, `ingested_at`, `source_updated_at`, and `payload_hash` for traceability.

Gold will include:

- `fact_flight_delay` — flight-level delay facts.
- `dim_airport_scd2` — airport history with `effective_from`, `effective_to`, and `is_current`.
- `dim_airline_scd2` — airline history with the same SCD Type 2 fields.
- `agg_delay_by_airport_hour` — total flights, delayed flights, and delay rate.
- `agg_delay_by_airline_hour` — total flights, delayed flights, and delay rate.

## Build plan

Work is deliberately ordered so that the data pipeline works before additional platform complexity is introduced.

### Milestone 0 — define the contract

- [ ] Register for Aviationstack and store the API key only in a local `.env` file.
- [ ] Add `.env.example` with the required variable names but no secrets.
- [ ] Choose a small initial set of airports or airlines.
- [ ] Capture representative API responses as sanitized fixtures.
- [ ] Create `docs/data-contract.md` describing fields, data types, business keys, and quality rules.
- [ ] Create architecture and decision records in `docs/`.

**Done when:** the source payload, delay calculation, table schemas, and acceptance criteria are written down.

### Milestone 1 — local platform

- [ ] Create `docker-compose.yml`.
- [ ] Start MinIO, PostgreSQL, Spark, Trino, and Metabase locally.
- [ ] Configure Spark and Trino to use an Iceberg catalog with MinIO as object storage.
- [ ] Add a `Makefile` or equivalent task runner for `up`, `down`, and `test`.
- [ ] Confirm a sample Iceberg table can be created and queried through Trino.

**Done when:** a new developer can start the full local stack with one documented command and query a sample Iceberg table.

### Milestone 2 — Bronze ingestion

- [ ] Implement a Python API client with timeouts, retries, structured logging, and API-key configuration.
- [ ] Write each response as a uniquely named JSON object in MinIO, partitioned by ingestion date and endpoint.
- [ ] Append a manifest record containing request parameters, object path, ingestion time, HTTP status, and payload hash.
- [ ] Make the command safe to rerun without overwriting raw data.
- [ ] Add fixture-based tests that do not call the API.

**Done when:** the raw source response can be traced from its manifest record to a MinIO object.

### Milestone 3 — Silver transformation

- [ ] Parse Bronze JSON into a typed Iceberg `silver_flights` table.
- [ ] Standardize timestamps in UTC and cast numeric fields.
- [ ] Flag invalid records instead of silently discarding them.
- [ ] Deduplicate by `flight_instance_key`, retaining the newest valid source update.
- [ ] Reprocess a rolling 48-hour lookback window to capture late-arriving data.
- [ ] Prove idempotency: running the job twice produces the same Silver result.

**Done when:** Silver contains clean, traceable, deduplicated data and automated tests cover duplicate and late records.

### Milestone 4 — lakehouse capabilities

- [ ] Add a version-two fixture with a new additive field, such as `aircraft_type`.
- [ ] Evolve the Iceberg table schema and backfill or default the new field safely.
- [ ] Add a late correction fixture for an existing flight instance.
- [ ] Record the Iceberg snapshot before the correction.
- [ ] Query the table both before and after that snapshot, saving the commands and outputs in documentation.

**Done when:** the repository demonstrates schema evolution and time travel with repeatable scripts or tests, not just prose.

### Milestone 5 — Gold modelling

- [ ] Build `fact_flight_delay` and hourly aggregate tables.
- [ ] Implement SCD Type 2 merges for airport and airline dimensions.
- [ ] Add data-quality checks: non-null business key, non-negative delays, one current dimension row per natural key, and valid effective-date ranges.
- [ ] Validate aggregate counts against Silver data.

**Done when:** Gold answers the business question and preserves dimension history correctly.

### Milestone 6 — analytics and orchestration

- [ ] Connect Metabase to Trino.
- [ ] Build a dashboard with delay rate by airport, airline, and hour.
- [ ] Add dashboard filters and clear metric definitions.
- [ ] Add screenshots and a short walkthrough to this README.
- [ ] Wrap the tested commands in an Airflow DAG as an optional final enhancement.

**Done when:** a viewer can run the stack, open the dashboard, and understand the data lineage.

### Milestone 7 — portfolio polish

- [ ] Add an architecture diagram and a data-lineage diagram.
- [ ] Add setup, run, test, and teardown commands.
- [ ] Add an end-to-end demo script.
- [ ] Run the demo from a clean checkout.
- [ ] Add a brief section explaining trade-offs and future improvements.
- [ ] Add concise CV-ready project bullets.

**Done when:** the repository is understandable and demonstrable without an oral explanation.

## Advanced demonstrations

The API may not naturally change schema or deliver a convenient late correction during the project window. These scenarios will therefore use versioned fixtures that mimic real operational behaviour; the production code path remains identical.

| Capability | Demonstration |
|---|---|
| Schema evolution | Ingest v1 payloads, then ingest v2 payloads containing a new field. Add the Iceberg column without breaking historical records. |
| Time travel | Query `silver_flights` at a saved Iceberg snapshot before a late correction, then compare it with the current snapshot. |
| Late data | Re-ingest a flight instance with a newer `source_updated_at` and prove that Silver selects the corrected record. |
| SCD Type 2 | Change an airport or airline attribute in a fixture and prove the prior row is closed while a new current row is created. |
| Idempotency | Run each pipeline step twice and assert that row counts and current-state results remain unchanged. |

## Planned repository layout

```text
.
├── docker-compose.yml
├── .env.example
├── Makefile
├── src/flight_lakehouse/
│   ├── ingest.py
│   ├── bronze_to_silver.py
│   ├── silver_to_gold.py
│   └── dimensions.py
├── sql/
├── tests/
├── data/fixtures/
├── docs/
└── dashboards/
```

## Quality and security rules

- Never commit API keys, `.env`, MinIO credentials, or real production payloads containing sensitive values.
- Keep Bronze immutable; corrections are new objects, never overwritten source files.
- Use fixtures for tests so routine development does not consume API requests.
- Log request metadata and failures without logging secrets.
- Keep all timestamps in UTC in Bronze, Silver, and Gold.

## Evidence to collect

The final repository should contain or link to:

- A local architecture diagram.
- A successful end-to-end run from clean setup to Metabase dashboard.
- Automated test results.
- A schema-evolution run and its resulting table schema.
- A time-travel query showing a table before and after a late update.
- Dashboard screenshots and a metric glossary.

## CV-ready summary (draft)

Built a containerized local flight-delay lakehouse using Python, Spark, Apache Iceberg, MinIO, Trino, and Metabase; implemented Bronze/Silver/Gold pipelines, idempotent late-data processing, Iceberg schema evolution and time travel, SCD Type 2 dimensions, and delay-rate analytics.
