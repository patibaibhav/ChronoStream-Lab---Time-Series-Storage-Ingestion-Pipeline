from datetime import UTC, datetime
from math import nan

import pytest

from app.core.config import FeedSettings
from app.engine.generator import SeededRecordGenerator
from app.engine.validator import RecordValidator, ValidationReason


INGESTED_AT = datetime(2030, 1, 1, tzinfo=UTC)


def valid_record() -> dict[str, object]:
    return {
        "event_id": "event-001",
        "device_id": "server_01",
        "metric": "cpu_usage",
        "value": 73.5,
        "timestamp": "2026-10-12T10:00:01Z",
    }


def test_valid_record_is_normalized_and_enriched() -> None:
    validator = RecordValidator(clock=lambda: INGESTED_AT)

    result = validator.validate(valid_record())

    assert result.accepted is True
    assert result.reason is None
    assert result.record is not None
    assert result.record.value == 73.5
    assert result.record.timestamp == datetime(2026, 10, 12, 10, 0, 1, tzinfo=UTC)
    assert result.record.ingested_at == INGESTED_AT
    assert result.record.ingested_at > result.record.timestamp
    assert result.record.to_dict()["ingested_at"].endswith("Z")


def test_generated_record_is_validated_and_reenriched_by_validator_clock() -> None:
    generated = next(SeededRecordGenerator(FeedSettings()).generate(1))
    validator = RecordValidator(clock=lambda: INGESTED_AT)

    result = validator.validate(generated)

    assert result.record is not None
    assert result.record.event_id == generated.event_id
    assert result.record.ingested_at == INGESTED_AT
    assert result.record.ingested_at != generated.ingested_at


@pytest.mark.parametrize(
    ("record", "reason", "field"),
    [
        ({key: value for key, value in valid_record().items() if key != "metric"}, ValidationReason.MISSING_FIELD, "metric"),
        ({**valid_record(), "event_id": 1}, ValidationReason.INVALID_TYPE, "event_id"),
        ({**valid_record(), "device_id": "  "}, ValidationReason.INVALID_VALUE, "device_id"),
        ({**valid_record(), "value": "73.5"}, ValidationReason.INVALID_TYPE, "value"),
        ({**valid_record(), "value": True}, ValidationReason.INVALID_TYPE, "value"),
        ({**valid_record(), "value": nan}, ValidationReason.INVALID_VALUE, "value"),
        ({**valid_record(), "timestamp": "not-a-timestamp"}, ValidationReason.INVALID_TIMESTAMP, "timestamp"),
        ({**valid_record(), "timestamp": "2026-10-12T10:00:01"}, ValidationReason.INVALID_TIMESTAMP, "timestamp"),
        ([], ValidationReason.INVALID_TYPE, "record"),
    ],
)
def test_malformed_records_are_classified(
    record: object, reason: ValidationReason, field: str
) -> None:
    result = RecordValidator(clock=lambda: INGESTED_AT).validate(record)

    assert result.accepted is False
    assert result.record is None
    assert result.reason is reason
    assert result.field == field


def test_malformed_records_are_counted_without_stopping_valid_records() -> None:
    validator = RecordValidator(clock=lambda: INGESTED_AT)

    invalid = validator.validate({**valid_record(), "value": "invalid"})
    valid = validator.validate(valid_record())
    missing = validator.validate({"event_id": "only-id"})
    metrics = validator.metrics()

    assert invalid.accepted is False
    assert valid.accepted is True
    assert missing.accepted is False
    assert metrics.processed_records == 3
    assert metrics.accepted_records == 1
    assert metrics.rejected_records == 2
    assert metrics.rejection_counts == {
        ValidationReason.INVALID_TYPE: 1,
        ValidationReason.MISSING_FIELD: 1,
    }


def test_ingested_time_is_made_distinct_when_clock_matches_event_time() -> None:
    timestamp = datetime(2030, 1, 1, tzinfo=UTC)
    record = {**valid_record(), "timestamp": "2030-01-01T00:00:00Z"}

    result = RecordValidator(clock=lambda: timestamp).validate(record)

    assert result.record is not None
    assert result.record.ingested_at > result.record.timestamp


def test_validator_rejects_a_naive_clock() -> None:
    validator = RecordValidator(clock=lambda: datetime(2030, 1, 1))

    with pytest.raises(ValueError, match="timezone-aware"):
        validator.validate(valid_record())
    assert validator.metrics().processed_records == 0
    assert validator.metrics().accepted_records == 0
    assert validator.metrics().rejected_records == 0
