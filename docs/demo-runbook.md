# Repeatable local demo

This runbook demonstrates the implemented Bronze, Silver, and Gold paths using only sanitized local fixtures. It does not consume Aviationstack requests.

## Prerequisites

```powershell
.\scripts\lakehouse.ps1 configure
# Edit .env if you have not already set the local passwords.
.\scripts\lakehouse.ps1 up
.\scripts\lakehouse.ps1 platform-check
.\scripts\lakehouse.ps1 test
```

## Bronze to Silver

Ingest the baseline fixture and transform it:

```powershell
.\scripts\lakehouse.ps1 bronze-fixture
.\scripts\lakehouse.ps1 silver
```

Verify the current Silver record from Trino:

```powershell
docker compose exec -T trino trino --server http://localhost:8080 --user lakehouse --execute "SELECT flight_iata, delay_minutes, quality_status FROM iceberg.silver.silver_flights"
```

## Schema evolution and time travel

1. Ingest the additive v2 payload and rerun Silver.

```powershell
.\scripts\lakehouse.ps1 bronze-v2-fixture
.\scripts\lakehouse.ps1 silver
```

2. Confirm `aircraft_type` exists, then save the current snapshot ID.

```powershell
docker compose exec -T trino trino --server http://localhost:8080 --user lakehouse --execute "DESCRIBE iceberg.silver.silver_flights"
docker compose exec -T trino trino --server http://localhost:8080 --user lakehouse --execute 'SELECT snapshot_id, committed_at FROM iceberg.silver."silver_flights$snapshots" ORDER BY committed_at DESC'
```

3. Ingest the late correction and rerun Silver. Replace `<v2_snapshot_id>` with the ID captured above.

```powershell
.\scripts\lakehouse.ps1 bronze-late-fixture
.\scripts\lakehouse.ps1 silver
docker compose exec -T trino trino --server http://localhost:8080 --user lakehouse --execute "SELECT flight_iata, delay_minutes, aircraft_type FROM iceberg.silver.silver_flights"
docker compose exec -T trino trino --server http://localhost:8080 --user lakehouse --execute "SELECT flight_iata, delay_minutes, aircraft_type FROM iceberg.silver.silver_flights FOR VERSION AS OF <v2_snapshot_id>"
```

The fixture’s current delay is 41 minutes; the saved v2 snapshot reports 27 minutes and `a321neo`.

## Gold and SCD Type 2

Build the Gold fact, aggregates, and initial dimension versions:

```powershell
.\scripts\lakehouse.ps1 gold
```

Then demonstrate a tracked-label change while preserving the IATA natural keys:

```powershell
.\scripts\lakehouse.ps1 bronze-dimension-fixture
.\scripts\lakehouse.ps1 silver
.\scripts\lakehouse.ps1 gold
docker compose exec -T trino trino --server http://localhost:8080 --user lakehouse --execute "SELECT airline_iata, airline_name, effective_from, effective_to, is_current FROM iceberg.gold.dim_airline_scd2 ORDER BY effective_from"
docker compose exec -T trino trino --server http://localhost:8080 --user lakehouse --execute "SELECT airport_iata, airport_name, effective_from, effective_to, is_current FROM iceberg.gold.dim_airport_scd2 ORDER BY airport_iata, effective_from"
docker compose exec -T trino trino --server http://localhost:8080 --user lakehouse --execute "SELECT * FROM iceberg.gold.agg_delay_by_airline_hour"
```

Each changed dimension has one closed row and one current row. The Gold job validates non-negative delays, one current row per natural key, and valid SCD effective-date ranges before succeeding.

## Metabase

The remaining manual step is to create a local Metabase administrator and add a Trino connection:

- Host: `trino`
- Port: `8080`
- Catalog: `iceberg`
- Schema: `gold`
- User: `lakehouse`

Use `agg_delay_by_airline_hour` and `agg_delay_by_airport_hour` for the dashboard. Both tables expose `total_flights`, `delayed_flights`, `delay_rate`, and `average_delay_minutes` at an hourly UTC grain.