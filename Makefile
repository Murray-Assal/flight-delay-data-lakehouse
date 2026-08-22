.DEFAULT_GOAL := help

SPARK_PACKAGES := org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.1,org.apache.iceberg:iceberg-aws-bundle:1.10.1,org.postgresql:postgresql:42.7.5

.PHONY: help configure up down logs platform-check bronze-fixture bronze-v2-fixture bronze-late-fixture bronze-dimension-fixture silver gold test

help:
	@echo "configure            Copy .env.example to .env (run once)"
	@echo "up                   Start MinIO, PostgreSQL, Trino, and Metabase"
	@echo "platform-check       Create and query an Iceberg table through Spark"
	@echo "bronze-fixture       Ingest the baseline fixture into Bronze"
	@echo "bronze-v2-fixture    Ingest the additive-schema fixture into Bronze"
	@echo "bronze-late-fixture  Ingest the late-correction fixture into Bronze"
	@echo "bronze-dimension-fixture  Ingest the SCD Type 2 change fixture into Bronze"
	@echo "silver               Transform rolling Bronze data into Silver"
	@echo "gold                 Build Gold facts, SCD2 dimensions, and aggregates"
	@echo "test                 Run offline fixture tests"
	@echo "down                 Stop the local stack (preserves volumes)"

configure:
	@test -f .env || cp .env.example .env

up:
	docker compose up -d postgres minio minio-init trino metabase

down:
	docker compose down

logs:
	docker compose logs -f --tail=100

platform-check:
	docker compose run --rm spark --conf spark.jars.ivy=/tmp/.ivy2 --packages $(SPARK_PACKAGES) jobs/verify_lakehouse.py

bronze-fixture:
	docker compose run --rm spark --conf spark.jars.ivy=/tmp/.ivy2 --packages $(SPARK_PACKAGES) jobs/ingest_bronze.py --fixture data/fixtures/aviationstack/flights_v1.json

bronze-v2-fixture:
	docker compose run --rm spark --conf spark.jars.ivy=/tmp/.ivy2 --packages $(SPARK_PACKAGES) jobs/ingest_bronze.py --fixture data/fixtures/aviationstack/flights_v2_schema_addition.json

bronze-late-fixture:
	docker compose run --rm spark --conf spark.jars.ivy=/tmp/.ivy2 --packages $(SPARK_PACKAGES) jobs/ingest_bronze.py --fixture data/fixtures/aviationstack/flights_late_correction.json

bronze-dimension-fixture:
	docker compose run --rm spark --conf spark.jars.ivy=/tmp/.ivy2 --packages $(SPARK_PACKAGES) jobs/ingest_bronze.py --fixture data/fixtures/aviationstack/flights_dimension_change.json

silver:
	docker compose run --rm spark --conf spark.jars.ivy=/tmp/.ivy2 --packages $(SPARK_PACKAGES) jobs/bronze_to_silver.py

gold:
	docker compose run --rm spark --conf spark.jars.ivy=/tmp/.ivy2 --packages $(SPARK_PACKAGES) jobs/silver_to_gold.py

test:
	python -m pytest