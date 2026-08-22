"""Offline tests for the Silver typing, validation, and late-data rules."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from hashlib import sha256
import json
from pathlib import Path

from flight_lakehouse.silver import BronzePayload, normalize_flight, parse_bronze_payloads


FIXTURE_DIR = Path(__file__).parents[1] / "data" / "fixtures" / "aviationstack"


def payload(name: str, observed_at: datetime) -> BronzePayload:
    body = (FIXTURE_DIR / name).read_bytes()
    return BronzePayload(
        object_path=f"s3://flight-lakehouse/bronze/{name}",
        observed_at=observed_at,
        payload_sha256=sha256(body).hexdigest(),
        body=body,
    )


def flight(name: str) -> dict:
    return json.loads((FIXTURE_DIR / name).read_text(encoding="utf-8"))["data"][0]


def test_baseline_is_typed_and_has_the_contract_business_key() -> None:
    bronze = payload("flights_v1.json", datetime(2026, 8, 22, 10, tzinfo=timezone.utc))
    record = normalize_flight(flight("flights_v1.json"), bronze, 0)

    assert record.quality_status == "valid"
    assert record.flight_iata == "EA101"
    assert record.departure_airport_iata == "JFK"
    assert record.scheduled_departure_at == datetime(2026, 8, 20, 14, tzinfo=timezone.utc)
    assert record.delay_minutes == 27
    assert record.is_delayed is True
    assert record.aircraft_type is None


def test_late_correction_keeps_the_business_key_and_changes_the_delay() -> None:
    first = normalize_flight(
        flight("flights_v1.json"),
        payload("flights_v1.json", datetime(2026, 8, 22, 10, tzinfo=timezone.utc)),
        0,
    )
    correction = normalize_flight(
        flight("flights_late_correction.json"),
        payload("flights_late_correction.json", datetime(2026, 8, 22, 11, tzinfo=timezone.utc)),
        0,
    )

    assert correction.flight_instance_key == first.flight_instance_key
    assert correction.observed_at > first.observed_at
    assert correction.delay_minutes == 41


def test_additive_aircraft_type_is_detected_without_breaking_v1_records() -> None:
    batch = parse_bronze_payloads(
        [
            payload("flights_v1.json", datetime(2026, 8, 22, 10, tzinfo=timezone.utc)),
            payload("flights_v2_schema_addition.json", datetime(2026, 8, 22, 11, tzinfo=timezone.utc)),
        ]
    )

    assert batch.saw_aircraft_type is True
    assert batch.records[0].aircraft_type is None
    assert batch.records[1].aircraft_type == "a321neo"


def test_bad_business_key_and_negative_delay_are_quarantined() -> None:
    bronze = payload("flights_v1.json", datetime(2026, 8, 22, 10, tzinfo=timezone.utc))
    invalid = deepcopy(flight("flights_v1.json"))
    invalid["flight"]["iata"] = None
    invalid["arrival"]["delay"] = -1

    record = normalize_flight(invalid, bronze, 0)

    assert record.quality_status == "quarantined"
    assert record.flight_instance_key.startswith("quarantine:")
    assert "missing flight.iata" in record.quality_reason
    assert "negative delay" in record.quality_reason


def test_invalid_json_becomes_a_quarantined_silver_row() -> None:
    batch = parse_bronze_payloads(
        [
            BronzePayload(
                object_path="s3://flight-lakehouse/bronze/invalid.json",
                observed_at=datetime(2026, 8, 22, 10, tzinfo=timezone.utc),
                payload_sha256="invalid",
                body=b"not-json",
            )
        ]
    )

    assert batch.records[0].quality_status == "quarantined"
    assert batch.records[0].quality_reason == "response body is not valid JSON"
