"""Build the current-state Silver Iceberg table from manifest-backed Bronze objects."""

from __future__ import annotations

import argparse
from datetime import timezone
import json
import logging
from typing import Any

from flight_lakehouse.silver import BronzePayload, ParsedSilverBatch, parse_bronze_payloads
from verify_lakehouse import create_spark_session, required


MANIFEST_TABLE = "bronze_ingestion_manifest"
SILVER_TABLE = "silver_flights"
BASE_COLUMNS = (
    "flight_instance_key",
    "flight_iata",
    "flight_status",
    "airline_iata",
    "airline_name",
    "departure_airport_iata",
    "departure_airport_name",
    "arrival_airport_iata",
    "arrival_airport_name",
    "scheduled_departure_at",
    "actual_departure_at",
    "scheduled_arrival_at",
    "actual_arrival_at",
    "delay_minutes",
    "is_delayed",
    "observed_at",
    "source_file",
    "payload_sha256",
    "quality_status",
    "quality_reason",
)


class SparkS3ObjectReader:
    """Read Bronze objects through the same Iceberg S3FileIO used to write them."""

    def __init__(self, spark: Any) -> None:
        self._jvm = spark._jvm

    def read_bytes(self, object_path: str) -> bytes:
        properties = self._jvm.java.util.HashMap()
        for name, value in {
            "client.region": required("AWS_REGION"),
            "s3.endpoint": required("MINIO_ENDPOINT"),
            "s3.access-key-id": required("MINIO_ROOT_USER"),
            "s3.secret-access-key": required("MINIO_ROOT_PASSWORD"),
            "s3.path-style-access": "true",
        }.items():
            properties.put(name, value)
        file_io = self._jvm.org.apache.iceberg.aws.s3.S3FileIO()
        try:
            file_io.initialize(properties)
            stream = file_io.newInputFile(object_path).newStream()
            try:
                return bytes(stream.readAllBytes())
            finally:
                stream.close()
        finally:
            file_io.close()


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--lookback-hours",
        type=int,
        default=48,
        help="Reprocess this rolling Bronze window to capture late-arriving updates.",
    )
    return parser.parse_args()


def bronze_payloads(spark: Any, reader: SparkS3ObjectReader, catalog: str, lookback_hours: int) -> list[BronzePayload]:
    if lookback_hours < 1:
        raise ValueError("lookback-hours must be positive")
    rows = spark.sql(
        f"""
        SELECT object_path, ingested_at, payload_sha256
        FROM {catalog}.bronze.{MANIFEST_TABLE}
        WHERE http_status BETWEEN 200 AND 299
          AND object_path IS NOT NULL
          AND ingested_at >= current_timestamp() - INTERVAL {lookback_hours} HOURS
        ORDER BY ingested_at, object_path
        """
    ).collect()
    return [
        BronzePayload(
            object_path=row.object_path,
            observed_at=row.ingested_at.replace(tzinfo=timezone.utc),
            payload_sha256=row.payload_sha256,
            body=reader.read_bytes(row.object_path),
        )
        for row in rows
    ]


def ensure_silver_table(spark: Any, catalog: str, *, include_aircraft_type: bool) -> tuple[str, bool]:
    """Create the base table and evolve it when the additive field first appears."""

    table = f"{catalog}.silver.{SILVER_TABLE}"
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.silver")
    exists = spark.catalog.tableExists(table)
    if not exists:
        spark.sql(
            f"""
            CREATE TABLE {table} (
                flight_instance_key STRING,
                flight_iata STRING,
                flight_status STRING,
                airline_iata STRING,
                airline_name STRING,
                departure_airport_iata STRING,
                departure_airport_name STRING,
                arrival_airport_iata STRING,
                arrival_airport_name STRING,
                scheduled_departure_at TIMESTAMP,
                actual_departure_at TIMESTAMP,
                scheduled_arrival_at TIMESTAMP,
                actual_arrival_at TIMESTAMP,
                delay_minutes INT,
                is_delayed BOOLEAN,
                observed_at TIMESTAMP,
                source_file STRING,
                payload_sha256 STRING,
                quality_status STRING,
                quality_reason STRING
            ) USING iceberg
            """
        )
    columns = {field.name for field in spark.table(table).schema.fields}
    for column in ("airline_name", "departure_airport_name", "arrival_airport_name"):
        if column not in columns:
            spark.sql(f"ALTER TABLE {table} ADD COLUMN {column} STRING")
            columns.add(column)
    if include_aircraft_type and "aircraft_type" not in columns:
        spark.sql(f"ALTER TABLE {table} ADD COLUMN aircraft_type STRING")
        columns.add("aircraft_type")
    return table, "aircraft_type" in columns


def silver_dataframe(spark: Any, batch: ParsedSilverBatch, *, include_aircraft_type: bool) -> Any:
    from pyspark.sql.types import BooleanType, IntegerType, StringType, StructField, StructType, TimestampType

    fields = [
        StructField("flight_instance_key", StringType(), False),
        StructField("flight_iata", StringType(), True),
        StructField("flight_status", StringType(), True),
        StructField("airline_iata", StringType(), True),
        StructField("airline_name", StringType(), True),
        StructField("departure_airport_iata", StringType(), True),
        StructField("departure_airport_name", StringType(), True),
        StructField("arrival_airport_iata", StringType(), True),
        StructField("arrival_airport_name", StringType(), True),
        StructField("scheduled_departure_at", TimestampType(), True),
        StructField("actual_departure_at", TimestampType(), True),
        StructField("scheduled_arrival_at", TimestampType(), True),
        StructField("actual_arrival_at", TimestampType(), True),
        StructField("delay_minutes", IntegerType(), True),
        StructField("is_delayed", BooleanType(), True),
        StructField("observed_at", TimestampType(), False),
        StructField("source_file", StringType(), False),
        StructField("payload_sha256", StringType(), False),
        StructField("quality_status", StringType(), False),
        StructField("quality_reason", StringType(), True),
    ]
    if include_aircraft_type:
        fields.append(StructField("aircraft_type", StringType(), True))
    return spark.createDataFrame(
        [record.as_dict(include_aircraft_type=include_aircraft_type) for record in batch.records],
        StructType(fields),
    )


def merge_current_state(
    spark: Any, table: str, dataframe: Any, *, include_aircraft_type: bool
) -> tuple[int, int]:
    """Deduplicate the staged window and merge only rows that would change state."""

    from pyspark.sql import Window
    from pyspark.sql import functions as F

    columns = list(BASE_COLUMNS)
    if include_aircraft_type:
        columns.append("aircraft_type")
    window = Window.partitionBy("flight_instance_key").orderBy(
        F.col("observed_at").desc(), F.col("payload_sha256").desc(), F.col("source_file").desc()
    )
    stage = dataframe.withColumn("_rank", F.row_number().over(window)).where("_rank = 1").drop("_rank")
    stage_count = stage.count()
    stage.createOrReplaceTempView("silver_stage")
    assignments = ", ".join(f"{column} = s.{column}" for column in columns)
    enrichment_condition = " OR ".join(
        f"(t.{column} IS NULL AND s.{column} IS NOT NULL)"
        for column in ("airline_name", "departure_airport_name", "arrival_airport_name")
    )
    change_condition = f"""
        t.flight_instance_key IS NULL
        OR s.observed_at > t.observed_at
        OR (s.observed_at = t.observed_at AND s.payload_sha256 > t.payload_sha256)
        OR (s.observed_at = t.observed_at AND s.payload_sha256 = t.payload_sha256 AND s.source_file > t.source_file)
        OR (
            s.observed_at = t.observed_at
            AND s.payload_sha256 = t.payload_sha256
            AND s.source_file = t.source_file
            AND ({enrichment_condition})
        )
    """
    changes = spark.sql(
        f"""
        SELECT s.*
        FROM silver_stage AS s
        LEFT JOIN {table} AS t
          ON t.flight_instance_key = s.flight_instance_key
        WHERE {change_condition}
        """
    ).cache()
    try:
        changed_row_count = changes.count()
        if changed_row_count == 0:
            return stage_count, 0
        changes.createOrReplaceTempView("silver_changes")
        insert_columns = ", ".join(columns)
        insert_values = ", ".join(f"s.{column}" for column in columns)
        spark.sql(
            f"""
            MERGE INTO {table} AS t
            USING silver_changes AS s
            ON t.flight_instance_key = s.flight_instance_key
            WHEN MATCHED THEN UPDATE SET {assignments}
            WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})
            """
        )
        return stage_count, changed_row_count
    finally:
        changes.unpersist()

def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    args = parse_arguments()
    spark = create_spark_session()
    try:
        catalog = required("ICEBERG_CATALOG_NAME")
        payloads = bronze_payloads(spark, SparkS3ObjectReader(spark), catalog, args.lookback_hours)
        if not payloads:
            print(json.dumps({"processed_bronze_files": 0, "staged_silver_rows": 0}, sort_keys=True))
            return
        batch = parse_bronze_payloads(payloads)
        table, has_aircraft_type = ensure_silver_table(
            spark, catalog, include_aircraft_type=batch.saw_aircraft_type
        )
        stage_count, changed_row_count = merge_current_state(
            spark,
            table,
            silver_dataframe(spark, batch, include_aircraft_type=has_aircraft_type),
            include_aircraft_type=has_aircraft_type,
        )
        print(
            json.dumps(
                {
                    "processed_bronze_files": len(payloads),
                    "parsed_silver_rows": len(batch.records),
                    "schema_evolved_aircraft_type": batch.saw_aircraft_type,
                    "staged_silver_rows": stage_count,
                    "merged_silver_rows": changed_row_count,
                },
                sort_keys=True,
            )
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
