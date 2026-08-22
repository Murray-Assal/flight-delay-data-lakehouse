"""Immutable Bronze ingestion primitives for Aviationstack responses."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from hashlib import sha256
import json
import logging
from pathlib import PurePosixPath
import time
from typing import Callable, Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from uuid import UUID, uuid4

SOURCE_NAME = "aviationstack"
SENSITIVE_PARAMETERS = frozenset({"access_key", "api_key", "token"})
LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class SourceResponse:
    http_status: int
    body: bytes | None
    error_message: str | None = None


@dataclass(frozen=True)
class BronzeManifestRecord:
    ingestion_id: str
    source_name: str
    endpoint: str
    request_parameters: str
    object_path: str | None
    ingested_at: datetime
    http_status: int
    payload_sha256: str | None
    record_count: int
    error_message: str | None

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


class ObjectStore(Protocol):
    def put_bytes(self, key: str, payload: bytes) -> None: ...


class ManifestWriter(Protocol):
    def append(self, record: BronzeManifestRecord) -> None: ...


class AviationstackClient:
    """A small HTTP client with timeouts, retries, and no credential logging."""

    def __init__(self, api_key: str, base_url: str, *, timeout_seconds: float = 20,
                 max_attempts: int = 3, opener: Callable[..., object] = urlopen,
                 sleep: Callable[[float], None] = time.sleep) -> None:
        if not api_key:
            raise ValueError("AVIATIONSTACK_API_KEY must be configured")
        if max_attempts < 1:
            raise ValueError("max_attempts must be positive")
        self._api_key, self._base_url = api_key, base_url.rstrip("/")
        self._timeout_seconds, self._max_attempts = timeout_seconds, max_attempts
        self._opener, self._sleep = opener, sleep

    def fetch(self, endpoint: str, parameters: dict[str, str]) -> SourceResponse:
        endpoint = _endpoint(endpoint)
        if any(name.lower() in SENSITIVE_PARAMETERS for name in parameters):
            raise ValueError("credentials must not be passed as request parameters")
        request = Request(f"{self._base_url}/{endpoint}?{urlencode({**parameters, 'access_key': self._api_key})}")
        for attempt in range(1, self._max_attempts + 1):
            _log("aviationstack_request", endpoint=endpoint, attempt=attempt, parameters=json.loads(sanitized_parameters(parameters)))
            try:
                with self._opener(request, timeout=self._timeout_seconds) as response:
                    result = SourceResponse(int(response.getcode()), response.read())
            except HTTPError as error:
                result = SourceResponse(error.code, error.read(), f"HTTP {error.code}: {error.reason}")
            except (OSError, TimeoutError, URLError) as error:
                result = SourceResponse(0, None, f"request failed: {error}")
            _log("aviationstack_response", endpoint=endpoint, attempt=attempt, http_status=result.http_status, body_bytes=len(result.body or b""), error_message=result.error_message)
            if 0 < result.http_status < 500 or attempt == self._max_attempts:
                return result
            self._sleep(2 ** (attempt - 1))
        raise AssertionError("unreachable")


class BronzeIngestor:
    """Writes an exact payload before appending its traceability manifest record."""

    def __init__(self, object_store: ObjectStore, manifest_writer: ManifestWriter, *, bucket: str,
                 clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
                 id_factory: Callable[[], UUID] = uuid4) -> None:
        self._object_store, self._manifest_writer, self._bucket = object_store, manifest_writer, bucket
        self._clock, self._id_factory = clock, id_factory

    def ingest(self, client: AviationstackClient, endpoint: str,
               parameters: dict[str, str]) -> BronzeManifestRecord:
        return self.ingest_response(client.fetch(endpoint, parameters), endpoint, parameters)

    def ingest_response(self, response: SourceResponse, endpoint: str,
                        parameters: dict[str, str]) -> BronzeManifestRecord:
        ingested_at, ingestion_id = _utc(self._clock()), str(self._id_factory())
        object_path, storage_error = None, None
        if response.body is not None:
            key = bronze_object_key(endpoint, ingested_at, ingestion_id)
            try:
                self._object_store.put_bytes(key, response.body)
                object_path = f"s3://{self._bucket}/{key}"
            except OSError as error:
                storage_error = f"object storage failed: {error}"
        count, payload_error = record_count(response.body)
        errors = [item for item in (response.error_message, storage_error, payload_error) if item]
        if not 200 <= response.http_status < 300 and not errors:
            errors.append(f"HTTP {response.http_status}")
        record = BronzeManifestRecord(
            ingestion_id, SOURCE_NAME, _endpoint(endpoint), sanitized_parameters(parameters), object_path,
            ingested_at, response.http_status,
            sha256(response.body).hexdigest() if response.body is not None else None,
            count, "; ".join(errors) or None,
        )
        self._manifest_writer.append(record)
        _log("bronze_manifest_appended", ingestion_id=record.ingestion_id, endpoint=record.endpoint, object_path=record.object_path, http_status=record.http_status, record_count=record.record_count)
        return record


def bronze_object_key(endpoint: str, ingested_at: datetime, ingestion_id: str) -> str:
    return str(PurePosixPath("bronze", f"source={SOURCE_NAME}", f"endpoint={_endpoint(endpoint)}",
                             f"ingestion_date={_utc(ingested_at):%Y-%m-%d}", f"{ingestion_id}.json"))


def sanitized_parameters(parameters: dict[str, str]) -> str:
    return json.dumps({key: value for key, value in parameters.items() if key.lower() not in SENSITIVE_PARAMETERS},
                      sort_keys=True, separators=(",", ":"))



def _log(event: str, **fields: object) -> None:
    """Emit a secret-safe JSON event when the application enables INFO logging."""
    LOGGER.info(json.dumps({"event": event, **fields}, sort_keys=True, default=str))


def record_count(payload: bytes | None) -> tuple[int, str | None]:
    if payload is None:
        return 0, None
    try:
        data = json.loads(payload).get("data")
    except (AttributeError, UnicodeDecodeError, json.JSONDecodeError):
        return 0, "response body is not valid JSON"
    if data is None:
        return 0, None
    return (len(data), None) if isinstance(data, list) else (0, "response data field is not an array")


def _endpoint(value: str) -> str:
    value = value.strip("/")
    if not value or "/" in value or value == "..":
        raise ValueError("endpoint must be a single relative path segment")
    return value


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("timestamps must be timezone-aware")
    return value.astimezone(timezone.utc)
