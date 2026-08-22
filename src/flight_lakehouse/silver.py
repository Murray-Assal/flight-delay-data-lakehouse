"""Pure normalization and data-quality rules for the Silver flight layer."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from hashlib import sha256
import json
from typing import Any, Iterable


@dataclass(frozen=True)
class BronzePayload:
    """A manifest-backed raw payload selected for Silver processing."""

    object_path: str
    observed_at: datetime
    payload_sha256: str
    body: bytes


@dataclass(frozen=True)
class SilverFlightRecord:
    """The typed Silver representation of one source flight observation."""

    flight_instance_key: str
    flight_iata: str | None
    flight_status: str | None
    airline_iata: str | None
    airline_name: str | None
    departure_airport_iata: str | None
    departure_airport_name: str | None
    arrival_airport_iata: str | None
    arrival_airport_name: str | None
    scheduled_departure_at: datetime | None
    actual_departure_at: datetime | None
    scheduled_arrival_at: datetime | None
    actual_arrival_at: datetime | None
    delay_minutes: int | None
    is_delayed: bool | None
    observed_at: datetime
    source_file: str
    payload_sha256: str
    quality_status: str
    quality_reason: str | None
    aircraft_type: str | None

    def as_dict(self, *, include_aircraft_type: bool) -> dict[str, object]:
        result = asdict(self)
        if not include_aircraft_type:
            result.pop("aircraft_type")
        return result


@dataclass(frozen=True)
class ParsedSilverBatch:
    records: list[SilverFlightRecord]
    saw_aircraft_type: bool


def parse_bronze_payloads(payloads: Iterable[BronzePayload]) -> ParsedSilverBatch:
    """Parse raw API payloads while retaining invalid records as quarantined rows."""

    records: list[SilverFlightRecord] = []
    saw_aircraft_type = False
    for bronze in payloads:
        try:
            document = json.loads(bronze.body)
        except (UnicodeDecodeError, json.JSONDecodeError):
            records.append(_quarantine(bronze, 0, "response body is not valid JSON"))
            continue

        data = document.get("data") if isinstance(document, dict) else None
        if not isinstance(data, list):
            records.append(_quarantine(bronze, 0, "response data field is not an array"))
            continue

        for index, raw_flight in enumerate(data):
            if not isinstance(raw_flight, dict):
                records.append(_quarantine(bronze, index, "flight record is not an object"))
                continue
            saw_aircraft_type = saw_aircraft_type or "aircraft_type" in raw_flight
            records.append(normalize_flight(raw_flight, bronze, index))
    return ParsedSilverBatch(records, saw_aircraft_type)


def normalize_flight(
    raw: dict[str, Any], bronze: BronzePayload, record_index: int
) -> SilverFlightRecord:
    """Type one API flight record and attach every contract-level quality result."""

    flight_iata = _normalised_text(_nested(raw, "flight", "iata"), upper=True)
    departure_iata = _normalised_text(_nested(raw, "departure", "iata"), upper=True)
    arrival_iata = _normalised_text(_nested(raw, "arrival", "iata"), upper=True)
    airline_iata = _normalised_text(_nested(raw, "airline", "iata"), upper=True)
    airline_name = _display_text(_nested(raw, "airline", "name"))
    flight_status = _normalised_text(raw.get("flight_status"), upper=False)
    departure_airport_name = _display_text(_nested(raw, "departure", "airport"))
    arrival_airport_name = _display_text(_nested(raw, "arrival", "airport"))

    scheduled_departure, scheduled_departure_error = _timestamp(
        _nested(raw, "departure", "scheduled"), "departure.scheduled"
    )
    actual_departure, actual_departure_error = _timestamp(
        _nested(raw, "departure", "actual"), "departure.actual"
    )
    scheduled_arrival, scheduled_arrival_error = _timestamp(
        _nested(raw, "arrival", "scheduled"), "arrival.scheduled"
    )
    actual_arrival, actual_arrival_error = _timestamp(
        _nested(raw, "arrival", "actual"), "arrival.actual"
    )
    delay_minutes, delay_error = _delay_minutes(raw)

    reasons = [
        item
        for item in (
            None if flight_iata else "missing flight.iata",
            None if departure_iata else "missing departure.iata",
            None if scheduled_departure else "missing departure.scheduled",
            scheduled_departure_error,
            actual_departure_error,
            scheduled_arrival_error,
            actual_arrival_error,
            delay_error,
        )
        if item
    ]
    valid_business_key = flight_iata and departure_iata and scheduled_departure
    business_key = (
        flight_instance_key(flight_iata, departure_iata, scheduled_departure)
        if valid_business_key
        else f"quarantine:{bronze.payload_sha256}:{record_index}"
    )
    status = "valid" if not reasons else "quarantined"
    if status == "quarantined":
        business_key = f"quarantine:{bronze.payload_sha256}:{record_index}"

    return SilverFlightRecord(
        flight_instance_key=business_key,
        flight_iata=flight_iata,
        flight_status=flight_status,
        airline_iata=airline_iata,
        airline_name=airline_name,
        departure_airport_iata=departure_iata,
        departure_airport_name=departure_airport_name,
        arrival_airport_iata=arrival_iata,
        arrival_airport_name=arrival_airport_name,
        scheduled_departure_at=scheduled_departure,
        actual_departure_at=actual_departure,
        scheduled_arrival_at=scheduled_arrival,
        actual_arrival_at=actual_arrival,
        delay_minutes=delay_minutes,
        is_delayed=(delay_minutes > 15) if delay_minutes is not None else None,
        observed_at=_utc(bronze.observed_at),
        source_file=bronze.object_path,
        payload_sha256=bronze.payload_sha256,
        quality_status=status,
        quality_reason="; ".join(reasons) or None,
        aircraft_type=_normalised_text(raw.get("aircraft_type"), upper=False),
    )


def flight_instance_key(
    flight_iata: str, departure_iata: str, scheduled_departure_at: datetime
) -> str:
    """Build the stable SHA-256 business key defined in the data contract."""

    value = "|".join(
        (flight_iata.upper(), departure_iata.upper(), _utc(scheduled_departure_at).isoformat())
    )
    return sha256(value.encode("utf-8")).hexdigest()


def _quarantine(bronze: BronzePayload, record_index: int, reason: str) -> SilverFlightRecord:
    return SilverFlightRecord(
        flight_instance_key=f"quarantine:{bronze.payload_sha256}:{record_index}",
        flight_iata=None,
        flight_status=None,
        airline_iata=None,
        airline_name=None,
        departure_airport_iata=None,
        departure_airport_name=None,
        arrival_airport_iata=None,
        arrival_airport_name=None,
        scheduled_departure_at=None,
        actual_departure_at=None,
        scheduled_arrival_at=None,
        actual_arrival_at=None,
        delay_minutes=None,
        is_delayed=None,
        observed_at=_utc(bronze.observed_at),
        source_file=bronze.object_path,
        payload_sha256=bronze.payload_sha256,
        quality_status="quarantined",
        quality_reason=reason,
        aircraft_type=None,
    )


def _nested(payload: dict[str, Any], group: str, key: str) -> object | None:
    value = payload.get(group)
    return value.get(key) if isinstance(value, dict) else None


def _normalised_text(value: object | None, *, upper: bool) -> str | None:
    if not isinstance(value, str) or not value.strip():
        return None
    result = value.strip()
    return result.upper() if upper else result.lower()


def _display_text(value: object | None) -> str | None:
    """Retain human-readable source labels for Gold SCD dimensions."""

    if not isinstance(value, str) or not value.strip():
        return None
    return value.strip()


def _timestamp(value: object | None, field: str) -> tuple[datetime | None, str | None]:
    if value is None:
        return None, None
    if not isinstance(value, str):
        return None, f"invalid {field}"
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None, f"invalid {field}"
    if parsed.tzinfo is None:
        return None, f"invalid {field}"
    return parsed.astimezone(timezone.utc), None


def _delay_minutes(raw: dict[str, Any]) -> tuple[int | None, str | None]:
    value = _nested(raw, "arrival", "delay")
    if value is None:
        value = _nested(raw, "departure", "delay")
    if value is None:
        return None, None
    if isinstance(value, bool):
        return None, "invalid delay"
    try:
        delay = int(value)
    except (TypeError, ValueError):
        return None, "invalid delay"
    if str(value).strip() not in {str(delay), f"{delay}.0"}:
        return None, "invalid delay"
    if delay < 0:
        return None, "negative delay"
    return delay, None


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("timestamps must be timezone-aware")
    return value.astimezone(timezone.utc)
