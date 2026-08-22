"""Create and query a small Iceberg table to verify the local platform."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession


def required(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"{name} must be configured")
    return value


def create_spark_session() -> SparkSession:
    catalog = required("ICEBERG_CATALOG_NAME")
    builder = (
        SparkSession.builder.appName("flight-delay-lakehouse-platform-check")
        .master("local[*]")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog}.type", "jdbc")
        .config(f"spark.sql.catalog.{catalog}.uri", required("ICEBERG_CATALOG_URI"))
        .config(f"spark.sql.catalog.{catalog}.jdbc.user", required("POSTGRES_USER"))
        .config(
            f"spark.sql.catalog.{catalog}.jdbc.password",
            required("POSTGRES_PASSWORD"),
        )
        .config(f"spark.sql.catalog.{catalog}.warehouse", required("ICEBERG_WAREHOUSE"))
        .config(f"spark.sql.catalog.{catalog}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{catalog}.s3.endpoint", required("MINIO_ENDPOINT"))
        .config(
            f"spark.sql.catalog.{catalog}.s3.access-key-id",
            required("MINIO_ROOT_USER"),
        )
        .config(
            f"spark.sql.catalog.{catalog}.s3.secret-access-key",
            required("MINIO_ROOT_PASSWORD"),
        )
        .config(f"spark.sql.catalog.{catalog}.s3.path-style-access", "true")
        .config("spark.sql.defaultCatalog", catalog)
        .config(f"spark.sql.catalog.{catalog}.client.region", "us-east-1")
    )
    return builder.getOrCreate()


def main() -> None:
    catalog = required("ICEBERG_CATALOG_NAME")
    spark = create_spark_session()
    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.platform")
        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {catalog}.platform.connection_check (
                id INT,
                checked_at TIMESTAMP
            ) USING iceberg
            """
        )
        spark.sql(
            f"""
            INSERT INTO {catalog}.platform.connection_check
            VALUES (1, current_timestamp())
            """
        )
        spark.sql(
            f"SELECT id, checked_at FROM {catalog}.platform.connection_check ORDER BY checked_at"
        ).show(truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
