"""Record validation and ingestion-time enrichment for the simulation engine."""

from collections import Counter
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from math import isfinite
from types import MappingProxyType

from app.engine.generator import RECORD_SCHEMA_VERSION, TimeSeriesRecord


class ValidationReason(StrEnum):
    """Explicit classifications for rejected source records."""

    MISSING_FIELD = "missing_field"
    INVALID_TYPE = "invalid_type"
    INVALID_VALUE = "invalid_value"
    INVALID_TIMESTAMP = "invalid_timestamp"


class _RecordValidationError(ValueError):
    def __init__(self, reason: ValidationReason, field: str, message: str) -> None:
        super().__init__(message)
        self.reason = reason
        self.field = field


def _format_utc(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


@dataclass(frozen=True, slots=True)
class ValidatedRecord:
    """Canonical record after required-field validation and enrichment."""

    event_id: str
    device_id: str
    metric: str
    value: float
    timestamp: datetime
    ingested_at: datetime
    schema_version: str = RECORD_SCHEMA_VERSION

    def to_dict(self) -> dict[str, str | float]:
        return {
            "schema_version": self.schema_version,
            "event_id": self.event_id,
            "device_id": self.device_id,
            "metric": self.metric,
            "value": self.value,
            "timestamp": _format_utc(self.timestamp),
            "ingested_at": _format_utc(self.ingested_at),
        }


@dataclass(frozen=True, slots=True)
class ValidationResult:
    """The accepted record or the explicit reason it was rejected."""

    record: ValidatedRecord | None
    reason: ValidationReason | None = None
    field: str | None = None

    @property
    def accepted(self) -> bool:
        return self.record is not None


@dataclass(frozen=True, slots=True)
class ValidationMetrics:
    """Counts produced by one validator instance."""

    processed_records: int
    accepted_records: int
    rejected_records: int
    rejection_counts: Mapping[ValidationReason, int]


class RecordValidator:
    """Validate source records and attach a validator-owned ingestion timestamp.

    The validator catches only expected malformed-input errors. Unexpected failures
    remain visible to the caller rather than being silently converted to rejects.
    """

    def __init__(self, *, clock: Callable[[], datetime] = lambda: datetime.now(UTC)) -> None:
        self._clock = clock
        self._processed_records = 0
        self._accepted_records = 0
        self._rejection_counts: Counter[ValidationReason] = Counter()

    def validate(self, raw_record: object) -> ValidationResult:
        """Validate one mapping or generated record without mutating the input."""

        try:
            source = self._as_mapping(raw_record)
            event_id = self._required_text(source, "event_id")
            device_id = self._required_text(source, "device_id")
            metric = self._required_text(source, "metric")
            value = self._required_number(source, "value")
            timestamp = self._required_timestamp(source, "timestamp")
        except _RecordValidationError as error:
            self._processed_records += 1
            self._rejection_counts[error.reason] += 1
            return ValidationResult(record=None, reason=error.reason, field=error.field)

        record = ValidatedRecord(
            event_id=event_id,
            device_id=device_id,
            metric=metric,
            value=value,
            timestamp=timestamp,
            ingested_at=self._ingested_at_after(timestamp),
            schema_version=self._schema_version(source),
        )
        self._processed_records += 1
        self._accepted_records += 1
        return ValidationResult(record=record)

    def metrics(self) -> ValidationMetrics:
        """Return immutable copies of validation counts and classifications."""

        rejected_records = self._processed_records - self._accepted_records
        return ValidationMetrics(
            processed_records=self._processed_records,
            accepted_records=self._accepted_records,
            rejected_records=rejected_records,
            rejection_counts=MappingProxyType(dict(self._rejection_counts)),
        )

    @staticmethod
    def _as_mapping(raw_record: object) -> Mapping[str, object]:
        if isinstance(raw_record, TimeSeriesRecord):
            return raw_record.to_dict()
        if not isinstance(raw_record, Mapping):
            raise _RecordValidationError(
                ValidationReason.INVALID_TYPE,
                "record",
                "record must be a mapping or TimeSeriesRecord",
            )
        return raw_record

    @staticmethod
    def _required_value(source: Mapping[str, object], field: str) -> object:
        if field not in source:
            raise _RecordValidationError(ValidationReason.MISSING_FIELD, field, f"{field} is required")
        return source[field]

    @classmethod
    def _required_text(cls, source: Mapping[str, object], field: str) -> str:
        value = cls._required_value(source, field)
        if not isinstance(value, str):
            raise _RecordValidationError(ValidationReason.INVALID_TYPE, field, f"{field} must be a string")
        value = value.strip()
        if not value:
            raise _RecordValidationError(ValidationReason.INVALID_VALUE, field, f"{field} cannot be empty")
        return value

    @classmethod
    def _required_number(cls, source: Mapping[str, object], field: str) -> float:
        value = cls._required_value(source, field)
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise _RecordValidationError(ValidationReason.INVALID_TYPE, field, f"{field} must be numeric")
        number = float(value)
        if not isfinite(number):
            raise _RecordValidationError(ValidationReason.INVALID_VALUE, field, f"{field} must be finite")
        return number

    @classmethod
    def _required_timestamp(cls, source: Mapping[str, object], field: str) -> datetime:
        value = cls._required_value(source, field)
        if not isinstance(value, str):
            raise _RecordValidationError(ValidationReason.INVALID_TYPE, field, f"{field} must be an ISO 8601 string")
        try:
            timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as error:
            raise _RecordValidationError(
                ValidationReason.INVALID_TIMESTAMP,
                field,
                f"{field} must be ISO 8601",
            ) from error
        if timestamp.tzinfo is None or timestamp.utcoffset() is None:
            raise _RecordValidationError(
                ValidationReason.INVALID_TIMESTAMP,
                field,
                f"{field} must include a timezone",
            )
        return timestamp.astimezone(UTC)

    @staticmethod
    def _schema_version(source: Mapping[str, object]) -> str:
        version = source.get("schema_version", RECORD_SCHEMA_VERSION)
        if isinstance(version, str) and version.strip():
            return version.strip()
        return RECORD_SCHEMA_VERSION

    def _ingested_at_after(self, timestamp: datetime) -> datetime:
        ingested_at = self._clock()
        if ingested_at.tzinfo is None or ingested_at.utcoffset() is None:
            raise ValueError("validator clock must return a timezone-aware datetime")
        ingested_at = ingested_at.astimezone(UTC)
        if ingested_at <= timestamp:
            return timestamp + timedelta(microseconds=1)
        return ingested_at
