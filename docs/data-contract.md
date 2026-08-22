# Flight-data contract

## Purpose

This contract defines the inputs, business rules, and quality expectations for the Flight Delay Lakehouse. It is deliberately versioned before implementing ingestion so transformations are deterministic and reviewable.

## Source and collection policy

| Item | Decision |
|---|---|
| Primary source | Aviationstack flight-status API |
| Collection mode | Batch, low frequency |
| Free-tier budget | At most three API calls per day (up to 93 in a 31-day month) |
| Development source | Versioned fixtures in `data/fixtures/aviationstack/` |
| Raw retention | Every successful or failed source response is retained as a new immutable Bronze object |
| Timestamps | UTC only |

API responses are not committed to the repository unless they are sanitized fixtures. The API key is supplied only through the local `.env` file.

## Scope for the first release

The first production-like run will ingest flight-status snapshots for a small, intentionally selected set of airports or airlines. The selection must be recorded here before live ingestion starts:

| Scope item | Initial selection | Reason |
|---|---|---|
| Airports | _To be selected_ | Keep response volume and dashboard scope manageable. |
| Airlines | _To be selected_ | Ensure meaningful comparison in Gold metrics. |
| Polling times | _To be selected_ | Stay within the free API-request allowance. |

## Bronze contract

Each source response is saved exactly as received to an object path shaped as:

```text
bronze/source=aviationstack/endpoint=flights/ingestion_date=YYYY-MM-DD/<uuid>.json
```

The append-only `bronze_ingestion_manifest` Iceberg table records:

| Column | Type | Rule |
|---|---|---|
| `ingestion_id` | string | UUID; unique and non-null. |
| `source_name` | string | Always `aviationstack` for the first release. |
| `endpoint` | string | Source endpoint name; non-null. |
| `request_parameters` | string | Sanitized JSON; never includes an API key. |
| `object_path` | string | Non-null immutable MinIO path. |
| `ingested_at` | timestamp | UTC timestamp of the request completion. |
| `http_status` | integer | HTTP response status. |
| `payload_sha256` | string | SHA-256 checksum of the exact raw response body. |
| `record_count` | integer | Number of records in the source `data` array, if present. |
| `error_message` | string | Populated for failed requests; must not contain credentials. |

## Silver contract

`silver_flights` represents the latest valid observation of a flight instance. A flight may appear in multiple Bronze snapshots as its status, delay, or actual timestamps change.

### Business key

```text
flight_instance_key = SHA-256(
  normalized flight.iata + departure.iata + departure.scheduled (UTC)
)
```

If `flight.iata`, `departure.iata`, or `departure.scheduled` is missing, the record is quarantined with a quality reason rather than used for analytics.

### Core columns

| Column | Type | Source or rule |
|---|---|---|
| `flight_instance_key` | string | Deterministic business key above. |
| `flight_iata` | string | `flight.iata`, normalized to upper case. |
| `flight_status` | string | `flight_status`, normalized to lower case. |
| `airline_iata` | string | `airline.iata`, normalized to upper case. |
| `airline_name` | string | `airline.name`, retained for the Gold airline dimension. |
| `departure_airport_iata` | string | `departure.iata`, normalized to upper case. |
| `departure_airport_name` | string | `departure.airport`, retained for the Gold airport dimension. |
| `arrival_airport_iata` | string | `arrival.iata`, normalized to upper case. |
| `arrival_airport_name` | string | `arrival.airport`, retained for the Gold airport dimension. |
| `scheduled_departure_at` | timestamp | Parsed from `departure.scheduled`, in UTC. |
| `actual_departure_at` | timestamp | Parsed from `departure.actual`, in UTC; nullable. |
| `scheduled_arrival_at` | timestamp | Parsed from `arrival.scheduled`, in UTC. |
| `actual_arrival_at` | timestamp | Parsed from `arrival.actual`, in UTC; nullable. |
| `delay_minutes` | integer | `arrival.delay`; fall back to `departure.delay`; nullable when neither is supplied. |
| `is_delayed` | boolean | `delay_minutes > 15`; false only when a non-null delay is 15 or less. |
| `observed_at` | timestamp | UTC timestamp when the API snapshot was ingested. |
| `source_file` | string | Bronze object path. |
| `payload_sha256` | string | Bronze manifest checksum. |
| `quality_status` | string | `valid` or `quarantined`. |
| `quality_reason` | string | Nullable explanation for a quarantined record. |

### Deduplication and late arrivals

The Silver job reads a rolling 48-hour Bronze lookback window. For each `flight_instance_key`, it selects the newest valid record ordered by `observed_at` and uses `payload_sha256` as a deterministic tie breaker. Older valid observations remain available in Bronze.

The Silver merge must be idempotent: rerunning it with the same Bronze files must not change the resulting current-state rows.

## Gold contract

### Delay aggregates

`agg_delay_by_airport_hour` and `agg_delay_by_airline_hour` have a UTC `hour_start_at` grain and include:

- `total_flights`: count of valid Silver flights with a non-null `delay_minutes`.
- `delayed_flights`: count where `is_delayed` is true.
- `delay_rate`: `delayed_flights / total_flights`; null if `total_flights = 0`.
- `average_delay_minutes`: average of non-null `delay_minutes`.

### SCD Type 2 dimensions

`dim_airport_scd2` and `dim_airline_scd2` use their IATA code as the natural key and contain a surrogate key, tracked attributes, `effective_from`, `effective_to`, and `is_current`.

For a changed tracked attribute, the current row is closed and a new current row is created. The pipeline must guarantee at most one current row per natural key.

## Quality rules

| Rule | Handling |
|---|---|
| Missing business-key field | Quarantine the row; retain the raw payload. |
| Invalid timestamp | Quarantine the row; retain the raw payload. |
| Negative delay | Quarantine the row. |
| Duplicate source payload | Preserve Bronze object; prevent duplicate current-state Silver output. |
| Unknown additive source field | Preserve it in Bronze; handle it through the documented schema-evolution path. |
| API failure | Append a failed manifest record; do not create a false data record. |

## Contract changes

Additive changes are implemented as a versioned fixture first, followed by an Iceberg `ALTER TABLE ... ADD COLUMN` migration and transformation update. Breaking changes require a new decision record and a migration plan before code is merged.
