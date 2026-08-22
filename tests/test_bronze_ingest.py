"""Offline Bronze ingestion tests."""

from datetime import datetime, timezone
from hashlib import sha256
from urllib.error import URLError
import json
from pathlib import Path
from uuid import UUID
import pytest

from flight_lakehouse.bronze import BronzeIngestor, SourceResponse

FIXTURE = Path(__file__).parents[1] / "data" / "fixtures" / "aviationstack" / "flights_v1.json"


class ObjectStore:
    def __init__(self):
        self.objects = {}

    def put_bytes(self, key, payload):
        self.objects[key] = payload


class ManifestWriter:
    def __init__(self):
        self.records = []

    def append(self, record):
        self.records.append(record)


def test_fixture_is_immutable_and_manifest_is_sanitized():
    payload, objects, manifests = FIXTURE.read_bytes(), ObjectStore(), ManifestWriter()
    ingestor = BronzeIngestor(
        objects, manifests, bucket="flight-lakehouse",
        clock=lambda: datetime(2026, 8, 22, 10, tzinfo=timezone.utc),
        id_factory=lambda: UUID("00000000-0000-0000-0000-000000000123"),
    )
    record = ingestor.ingest_response(SourceResponse(200, payload), "flights", {"dep_iata": "JFK", "access_key": "secret"})

    key = "bronze/source=aviationstack/endpoint=flights/ingestion_date=2026-08-22/00000000-0000-0000-0000-000000000123.json"
    assert objects.objects == {key: payload}
    assert record.object_path == f"s3://flight-lakehouse/{key}"
    assert record.payload_sha256 == sha256(payload).hexdigest()
    assert record.record_count == 1
    assert record.error_message is None
    assert json.loads(record.request_parameters) == {"dep_iata": "JFK"}
    assert manifests.records == [record]


def test_failed_response_is_retained_and_manifested():
    objects, manifests = ObjectStore(), ManifestWriter()
    record = BronzeIngestor(objects, manifests, bucket="flight-lakehouse").ingest_response(
        SourceResponse(429, b'{"error":{"code":"usage_limit_reached"}}', "HTTP 429: Too Many Requests"),
        "flights", {},
    )
    assert len(objects.objects) == 1
    assert record.http_status == 429
    assert record.record_count == 0
    assert record.error_message == "HTTP 429: Too Many Requests"

def test_client_retries_transient_failures_without_accepting_credential_parameters():
    class Response:
        def getcode(self):
            return 200

        def read(self):
            return b'{"data": []}'

        def __enter__(self):
            return self

        def __exit__(self, *_):
            return None

    attempts, pauses = [], []

    def opener(request, timeout):
        attempts.append((request.full_url, timeout))
        if len(attempts) == 1:
            raise URLError("temporary network failure")
        return Response()

    from flight_lakehouse.bronze import AviationstackClient

    response = AviationstackClient("secret-key", "https://example.test/v1", opener=opener, sleep=pauses.append).fetch(
        "flights", {"dep_iata": "JFK"}
    )

    assert response.http_status == 200
    assert pauses == [1]
    assert len(attempts) == 2
    with pytest.raises(ValueError, match="credentials"):
        AviationstackClient("secret-key", "https://example.test/v1").fetch("flights", {"access_key": "not-allowed"})
