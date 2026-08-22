.DEFAULT_GOAL := help

SPARK_PACKAGES := org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.1,org.apache.iceberg:iceberg-aws-bundle:1.10.1,org.postgresql:postgresql:42.7.5

.PHONY: help configure up down logs platform-check test

help:
	@echo "configure       Copy .env.example to .env (run once)"
	@echo "up              Start MinIO, PostgreSQL, Trino, and Metabase"
	@echo "platform-check  Create and query an Iceberg table through Spark"
	@echo "test            Run offline fixture tests"
	@echo "down            Stop the local stack (preserves volumes)"

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

test:
	python -m pytest
