"""Ingest one live or fixture Aviationstack response into the Bronze layer."""

from __future__ import annotations

import argparse
from datetime import timezone
import logging
import json
from pathlib import Path
from typing import Any

from flight_lakehouse.bronze import AviationstackClient, BronzeIngestor, BronzeManifestRecord, SourceResponse
from verify_lakehouse import create_spark_session, required


class SparkS3ObjectStore:
    """Write exact response bytes using the same Iceberg S3FileIO as table metadata."""

    def __init__(self, spark: Any, bucket: str) -> None:
        self._jvm, self._bucket = spark._jvm, bucket

    def put_bytes(self, key: str, payload: bytes) -> None:
        properties = self._jvm.java.util.HashMap()
        for name, value in {
            "client.region": "us-east-1",
            "s3.endpoint": required("MINIO_ENDPOINT"),
            "s3.access-key-id": required("MINIO_ROOT_USER"),
            "s3.secret-access-key": required("MINIO_ROOT_PASSWORD"),
            "s3.path-style-access": "true",
        }.items():
            properties.put(name, value)
        file_io = self._jvm.org.apache.iceberg.aws.s3.S3FileIO()
        try:
            file_io.initialize(properties)
            stream = file_io.newOutputFile(f"s3://{self._bucket}/{key}").create()
            try:
                stream.write(bytearray(payload))
            finally:
                stream.close()
        finally:
            file_io.close()


class SparkManifestWriter:
    """Create and append the append-only Iceberg ingestion manifest."""

    def __init__(self, spark: Any, catalog: str) -> None:
        self._spark, self._table = spark, f"{catalog}.bronze.bronze_ingestion_manifest"
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.bronze")
        spark.sql(
            f"""CREATE TABLE IF NOT EXISTS {self._table} (
                ingestion_id STRING, source_name STRING, endpoint STRING,
                request_parameters STRING, object_path STRING, ingested_at TIMESTAMP,
                http_status INT, payload_sha256 STRING, record_count INT, error_message STRING
            ) USING iceberg"""
        )

    def append(self, record: BronzeManifestRecord) -> None:
        from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType

        schema = StructType([
            StructField("ingestion_id", StringType(), False),
            StructField("source_name", StringType(), False),
            StructField("endpoint", StringType(), False),
            StructField("request_parameters", StringType(), False),
            StructField("object_path", StringType(), True),
            StructField("ingested_at", TimestampType(), False),
            StructField("http_status", IntegerType(), False),
            StructField("payload_sha256", StringType(), True),
            StructField("record_count", IntegerType(), False),
            StructField("error_message", StringType(), True),
        ])
        self._spark.createDataFrame([record.as_dict()], schema).writeTo(self._table).append()


def parse_parameters(values: list[str]) -> dict[str, str]:
    parameters: dict[str, str] = {}
    for value in values:
        if "=" not in value:
            raise ValueError(f"invalid parameter {value!r}; use KEY=VALUE")
        key, parameter_value = value.split("=", 1)
        if not key:
            raise ValueError("parameter keys must not be empty")
        parameters[key] = parameter_value
    return parameters


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", default="flights")
    parser.add_argument("--param", action="append", default=[], metavar="KEY=VALUE")
    parser.add_argument("--fixture", type=Path, help="Ingest a local fixture without calling the API.")
    args = parser.parse_args()
    parameters = parse_parameters(args.param)
    spark = create_spark_session()
    try:
        bucket, catalog = required("MINIO_BUCKET"), required("ICEBERG_CATALOG_NAME")
        ingestor = BronzeIngestor(SparkS3ObjectStore(spark, bucket), SparkManifestWriter(spark, catalog), bucket=bucket)
        if args.fixture:
            record = ingestor.ingest_response(SourceResponse(200, args.fixture.read_bytes()), args.endpoint, parameters)
        else:
            client = AviationstackClient(required("AVIATIONSTACK_API_KEY"), required("AVIATIONSTACK_BASE_URL"))
            record = ingestor.ingest(client, args.endpoint, parameters)
        print(json.dumps({**record.as_dict(), "ingested_at": record.ingested_at.astimezone(timezone.utc).isoformat()}, sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
