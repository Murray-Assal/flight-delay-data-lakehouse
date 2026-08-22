"""Build Gold fact, SCD Type 2 dimensions, and delay aggregates from Silver."""

from __future__ import annotations

import json
from typing import Any

from verify_lakehouse import create_spark_session, required


SILVER_TABLE = "silver_flights"
FACT_TABLE = "fact_flight_delay"
AIRLINE_DIMENSION = "dim_airline_scd2"
AIRPORT_DIMENSION = "dim_airport_scd2"
AIRLINE_AGGREGATE = "agg_delay_by_airline_hour"
AIRPORT_AGGREGATE = "agg_delay_by_airport_hour"


def ensure_gold_tables(spark: Any, catalog: str) -> None:
    """Create the Gold tables once; subsequent invocations only merge changes."""

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.gold")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {catalog}.gold.{FACT_TABLE} (
            flight_instance_key STRING,
            airline_iata STRING,
            departure_airport_iata STRING,
            arrival_airport_iata STRING,
            scheduled_departure_at TIMESTAMP,
            delay_minutes INT,
            is_delayed BOOLEAN,
            observed_at TIMESTAMP,
            source_file STRING
        ) USING iceberg
        """
    )
    for table, key_column, label_column in (
        (AIRLINE_DIMENSION, "airline_iata", "airline_name"),
        (AIRPORT_DIMENSION, "airport_iata", "airport_name"),
    ):
        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {catalog}.gold.{table} (
                {table}_sk STRING,
                {key_column} STRING,
                {label_column} STRING,
                effective_from TIMESTAMP,
                effective_to TIMESTAMP,
                is_current BOOLEAN
            ) USING iceberg
            """
        )
    for table, key_column in (
        (AIRLINE_AGGREGATE, "airline_iata"),
        (AIRPORT_AGGREGATE, "airport_iata"),
    ):
        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {catalog}.gold.{table} (
                {key_column} STRING,
                hour_start_at TIMESTAMP,
                total_flights BIGINT,
                delayed_flights BIGINT,
                delay_rate DOUBLE,
                average_delay_minutes DOUBLE
            ) USING iceberg
            """
        )


def build_fact(spark: Any, catalog: str) -> None:
    spark.sql(
        f"""
        CREATE OR REPLACE TEMP VIEW gold_flight_stage AS
        SELECT
            flight_instance_key,
            airline_iata,
            departure_airport_iata,
            arrival_airport_iata,
            scheduled_departure_at,
            delay_minutes,
            is_delayed,
            observed_at,
            source_file
        FROM {catalog}.silver.{SILVER_TABLE}
        WHERE quality_status = 'valid'
          AND delay_minutes IS NOT NULL
        """
    )
    spark.sql(
        f"""
        MERGE INTO {catalog}.gold.{FACT_TABLE} AS target
        USING gold_flight_stage AS source
          ON target.flight_instance_key = source.flight_instance_key
        WHEN MATCHED AND (
            source.observed_at > target.observed_at
            OR (source.observed_at = target.observed_at AND source.source_file > target.source_file)
        ) THEN UPDATE SET
            airline_iata = source.airline_iata,
            departure_airport_iata = source.departure_airport_iata,
            arrival_airport_iata = source.arrival_airport_iata,
            scheduled_departure_at = source.scheduled_departure_at,
            delay_minutes = source.delay_minutes,
            is_delayed = source.is_delayed,
            observed_at = source.observed_at,
            source_file = source.source_file
        WHEN NOT MATCHED THEN INSERT (
            flight_instance_key, airline_iata, departure_airport_iata, arrival_airport_iata,
            scheduled_departure_at, delay_minutes, is_delayed, observed_at, source_file
        ) VALUES (
            source.flight_instance_key, source.airline_iata, source.departure_airport_iata,
            source.arrival_airport_iata, source.scheduled_departure_at, source.delay_minutes,
            source.is_delayed, source.observed_at, source.source_file
        )
        """
    )


def build_dimension_stages(spark: Any, catalog: str) -> None:
    """Select one latest source observation per natural key for each dimension."""

    spark.sql(
        f"""
        CREATE OR REPLACE TEMP VIEW airline_dimension_stage AS
        SELECT airline_iata, COALESCE(airline_name, airline_iata) AS airline_name, observed_at
        FROM (
            SELECT
                airline_iata,
                airline_name,
                observed_at,
                ROW_NUMBER() OVER (
                    PARTITION BY airline_iata
                    ORDER BY observed_at DESC, source_file DESC
                ) AS row_number
            FROM {catalog}.silver.{SILVER_TABLE}
            WHERE quality_status = 'valid' AND airline_iata IS NOT NULL
        )
        WHERE row_number = 1
        """
    )
    spark.sql(
        f"""
        CREATE OR REPLACE TEMP VIEW airport_dimension_stage AS
        SELECT airport_iata, COALESCE(airport_name, airport_iata) AS airport_name, observed_at
        FROM (
            SELECT
                airport_iata,
                airport_name,
                observed_at,
                source_file,
                ROW_NUMBER() OVER (
                    PARTITION BY airport_iata
                    ORDER BY observed_at DESC, source_file DESC
                ) AS row_number
            FROM (
                SELECT departure_airport_iata AS airport_iata,
                       departure_airport_name AS airport_name,
                       observed_at,
                       source_file
                FROM {catalog}.silver.{SILVER_TABLE}
                WHERE quality_status = 'valid' AND departure_airport_iata IS NOT NULL
                UNION ALL
                SELECT arrival_airport_iata AS airport_iata,
                       arrival_airport_name AS airport_name,
                       observed_at,
                       source_file
                FROM {catalog}.silver.{SILVER_TABLE}
                WHERE quality_status = 'valid' AND arrival_airport_iata IS NOT NULL
            ) AS airport_source
        )
        WHERE row_number = 1
        """
    )


def merge_scd2_dimension(
    spark: Any,
    *,
    catalog: str,
    table: str,
    stage: str,
    key_column: str,
    label_column: str,
) -> None:
    """Close changed current rows, then insert their new current SCD Type 2 version."""

    qualified_table = f"{catalog}.gold.{table}"
    spark.sql(
        f"""
        MERGE INTO {qualified_table} AS target
        USING {stage} AS source
          ON target.{key_column} = source.{key_column} AND target.is_current
        WHEN MATCHED
          AND NOT (target.{label_column} <=> source.{label_column})
          AND source.observed_at > target.effective_from
        THEN UPDATE SET
            effective_to = source.observed_at,
            is_current = false
        """
    )
    spark.sql(
        f"""
        INSERT INTO {qualified_table}
        SELECT
            sha2(concat_ws('|', source.{key_column}, CAST(source.observed_at AS STRING)), 256),
            source.{key_column},
            source.{label_column},
            source.observed_at,
            CAST(NULL AS TIMESTAMP),
            true
        FROM {stage} AS source
        WHERE NOT EXISTS (
            SELECT 1
            FROM {qualified_table} AS target
            WHERE target.{key_column} = source.{key_column}
              AND target.is_current
              AND (
                  target.{label_column} <=> source.{label_column}
                  OR target.effective_from >= source.observed_at
              )
        )
        """
    )


def build_aggregates(spark: Any, catalog: str) -> None:
    aggregate_specs = (
        (AIRLINE_AGGREGATE, "airline_iata", "airline_aggregate_stage"),
        (AIRPORT_AGGREGATE, "departure_airport_iata", "airport_aggregate_stage"),
    )
    for table, grouping_column, stage in aggregate_specs:
        output_key = grouping_column.replace("departure_", "")
        spark.sql(
            f"""
            CREATE OR REPLACE TEMP VIEW {stage} AS
            SELECT
                {grouping_column} AS {output_key},
                date_trunc('hour', scheduled_departure_at) AS hour_start_at,
                COUNT(*) AS total_flights,
                SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) AS delayed_flights,
                CAST(SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) AS delay_rate,
                AVG(CAST(delay_minutes AS DOUBLE)) AS average_delay_minutes
            FROM {catalog}.gold.{FACT_TABLE}
            WHERE {grouping_column} IS NOT NULL
            GROUP BY {grouping_column}, date_trunc('hour', scheduled_departure_at)
            """
        )
        spark.sql(
            f"""
            MERGE INTO {catalog}.gold.{table} AS target
            USING {stage} AS source
              ON target.{output_key} = source.{output_key}
             AND target.hour_start_at = source.hour_start_at
            WHEN MATCHED AND NOT (
                target.total_flights <=> source.total_flights
                AND target.delayed_flights <=> source.delayed_flights
                AND target.delay_rate <=> source.delay_rate
                AND target.average_delay_minutes <=> source.average_delay_minutes
            ) THEN UPDATE SET
                total_flights = source.total_flights,
                delayed_flights = source.delayed_flights,
                delay_rate = source.delay_rate,
                average_delay_minutes = source.average_delay_minutes
            WHEN NOT MATCHED THEN INSERT (
                {output_key}, hour_start_at, total_flights, delayed_flights,
                delay_rate, average_delay_minutes
            ) VALUES (
                source.{output_key}, source.hour_start_at, source.total_flights,
                source.delayed_flights, source.delay_rate, source.average_delay_minutes
            )
            """
        )


def assert_gold_quality(spark: Any, catalog: str) -> None:
    """Fail the run when a contract-level Gold invariant is violated."""

    checks = {
        "fact has invalid delay": f"""
            SELECT COUNT(*) FROM {catalog}.gold.{FACT_TABLE}
            WHERE flight_instance_key IS NULL OR delay_minutes < 0
        """,
        "airport dimension has duplicate current keys": f"""
            SELECT COUNT(*) FROM (
                SELECT airport_iata FROM {catalog}.gold.{AIRPORT_DIMENSION}
                WHERE is_current GROUP BY airport_iata HAVING COUNT(*) > 1
            )
        """,
        "airline dimension has duplicate current keys": f"""
            SELECT COUNT(*) FROM (
                SELECT airline_iata FROM {catalog}.gold.{AIRLINE_DIMENSION}
                WHERE is_current GROUP BY airline_iata HAVING COUNT(*) > 1
            )
        """,
        "airport dimension has invalid date range": f"""
            SELECT COUNT(*) FROM {catalog}.gold.{AIRPORT_DIMENSION}
            WHERE effective_to IS NOT NULL AND effective_to <= effective_from
        """,
        "airline dimension has invalid date range": f"""
            SELECT COUNT(*) FROM {catalog}.gold.{AIRLINE_DIMENSION}
            WHERE effective_to IS NOT NULL AND effective_to <= effective_from
        """,
    }
    failed = {name: spark.sql(query).first()[0] for name, query in checks.items()}
    invalid = {name: count for name, count in failed.items() if count}
    if invalid:
        raise RuntimeError(f"Gold data-quality checks failed: {invalid}")


def main() -> None:
    spark = create_spark_session()
    try:
        catalog = required("ICEBERG_CATALOG_NAME")
        ensure_gold_tables(spark, catalog)
        build_fact(spark, catalog)
        build_dimension_stages(spark, catalog)
        merge_scd2_dimension(
            spark,
            catalog=catalog,
            table=AIRLINE_DIMENSION,
            stage="airline_dimension_stage",
            key_column="airline_iata",
            label_column="airline_name",
        )
        merge_scd2_dimension(
            spark,
            catalog=catalog,
            table=AIRPORT_DIMENSION,
            stage="airport_dimension_stage",
            key_column="airport_iata",
            label_column="airport_name",
        )
        build_aggregates(spark, catalog)
        assert_gold_quality(spark, catalog)
        table_counts = {
            table: spark.table(f"{catalog}.gold.{table}").count()
            for table in (
                FACT_TABLE,
                AIRLINE_DIMENSION,
                AIRPORT_DIMENSION,
                AIRLINE_AGGREGATE,
                AIRPORT_AGGREGATE,
            )
        }
        print(json.dumps({"gold_table_counts": table_counts}, sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()