from datetime import UTC, datetime

import pytest

from app.core.config import FeedSettings, TrafficPattern
from app.engine.generator import (
    DEFAULT_SIMULATION_START,
    RECORD_SCHEMA_VERSION,
    SeededRecordGenerator,
)


def test_generated_records_follow_the_canonical_schema() -> None:
    records = list(SeededRecordGenerator(FeedSettings(seed=7)).generate(3))

    assert len(records) == 3
    assert len({record.event_id for record in records}) == 3
    for record in records:
        serialized = record.to_dict()
        assert serialized["schema_version"] == RECORD_SCHEMA_VERSION
        assert serialized["device_id"].startswith("server_")
        assert isinstance(serialized["value"], float)
        assert serialized["timestamp"].endswith("Z")
        assert serialized["ingested_at"].endswith("Z")
        assert record.ingested_at > record.timestamp


def test_same_seed_and_settings_reproduce_the_same_sequence() -> None:
    settings = FeedSettings(seed=123, records_per_second=200, pattern=TrafficPattern.BURST)

    first = [record.to_dict() for record in SeededRecordGenerator(settings).generate(25)]
    second = [record.to_dict() for record in SeededRecordGenerator(settings).generate(25)]

    assert first == second


def test_generator_restarts_deterministically_for_each_generate_call() -> None:
    generator = SeededRecordGenerator(FeedSettings(seed=99))

    assert [record.to_dict() for record in generator.generate(5)] == [
        record.to_dict() for record in generator.generate(5)
    ]


def test_different_seeds_produce_different_records() -> None:
    first = list(SeededRecordGenerator(FeedSettings(seed=1)).generate(1))
    second = list(SeededRecordGenerator(FeedSettings(seed=2)).generate(1))

    assert first[0].event_id != second[0].event_id


@pytest.mark.parametrize(
    "pattern",
    [TrafficPattern.STEADY, TrafficPattern.BURST, TrafficPattern.SPIKE, TrafficPattern.RAMP],
)
def test_all_traffic_patterns_are_repeatable(pattern: TrafficPattern) -> None:
    settings = FeedSettings(seed=5, records_per_second=100, pattern=pattern)

    first = list(SeededRecordGenerator(settings).generate(40))
    second = list(SeededRecordGenerator(settings).generate(40))

    assert first == second


@pytest.mark.parametrize(
    "pattern",
    [TrafficPattern.BURST, TrafficPattern.SPIKE, TrafficPattern.RAMP],
)
def test_non_steady_patterns_change_inter_arrival_time(pattern: TrafficPattern) -> None:
    records = list(
        SeededRecordGenerator(FeedSettings(seed=1, records_per_second=100, pattern=pattern)).generate(40)
    )
    intervals = {
        records[index + 1].timestamp - records[index].timestamp
        for index in range(len(records) - 1)
    }

    assert len(intervals) > 1


def test_steady_pattern_uses_a_constant_inter_arrival_time() -> None:
    records = list(
        SeededRecordGenerator(FeedSettings(records_per_second=100, pattern=TrafficPattern.STEADY)).generate(3)
    )

    assert records[1].timestamp - records[0].timestamp == records[2].timestamp - records[1].timestamp


def test_generator_rejects_invalid_count_and_naive_start_time() -> None:
    generator = SeededRecordGenerator(FeedSettings())

    with pytest.raises(ValueError, match="non-negative"):
        list(generator.generate(-1))
    with pytest.raises(TypeError, match="integer"):
        list(generator.generate(True))
    with pytest.raises(ValueError, match="timezone-aware"):
        SeededRecordGenerator(FeedSettings(), start_at=datetime(2026, 1, 1))


def test_custom_utc_start_time_is_used() -> None:
    start_at = datetime(2030, 1, 1, tzinfo=UTC)

    record = next(SeededRecordGenerator(FeedSettings(), start_at=start_at).generate(1))

    assert record.timestamp == start_at
    assert DEFAULT_SIMULATION_START.tzinfo is UTC
