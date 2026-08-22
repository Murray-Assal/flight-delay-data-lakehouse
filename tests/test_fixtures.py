"""Contract tests for the versioned, offline Aviationstack fixtures."""

from __future__ import annotations

import json
from pathlib import Path


FIXTURE_DIR = Path(__file__).parents[1] / "data" / "fixtures" / "aviationstack"


def read_fixture(name: str) -> dict:
    return json.loads((FIXTURE_DIR / name).read_text(encoding="utf-8"))


def flight_record(name: str) -> dict:
    payload = read_fixture(name)
    assert payload["pagination"]["count"] == len(payload["data"])
    assert len(payload["data"]) == 1
    return payload["data"][0]


def test_baseline_fixture_has_the_business_key_inputs() -> None:
    flight = flight_record("flights_v1.json")

    assert flight["flight"]["iata"] == "EA101"
    assert flight["departure"]["iata"] == "JFK"
    assert flight["departure"]["scheduled"] == "2026-08-20T14:00:00+00:00"


def test_schema_evolution_fixture_is_additive() -> None:
    baseline = flight_record("flights_v1.json")
    evolved = flight_record("flights_v2_schema_addition.json")

    assert set(baseline).issubset(evolved)
    assert evolved["aircraft_type"] == "A321neo"


def test_late_correction_has_same_business_key_and_new_delay() -> None:
    baseline = flight_record("flights_v1.json")
    correction = flight_record("flights_late_correction.json")

    assert correction["flight"]["iata"] == baseline["flight"]["iata"]
    assert correction["departure"]["iata"] == baseline["departure"]["iata"]
    assert correction["departure"]["scheduled"] == baseline["departure"]["scheduled"]
    assert correction["arrival"]["delay"] > baseline["arrival"]["delay"]


def test_dimension_change_keeps_natural_keys_and_changes_tracked_labels() -> None:
    baseline = flight_record("flights_late_correction.json")
    changed = flight_record("flights_dimension_change.json")

    assert changed["flight"]["iata"] == baseline["flight"]["iata"]
    assert changed["airline"]["iata"] == baseline["airline"]["iata"]
    assert changed["airline"]["name"] != baseline["airline"]["name"]
    assert changed["departure"]["iata"] == baseline["departure"]["iata"]
    assert changed["departure"]["airport"] != baseline["departure"]["airport"]