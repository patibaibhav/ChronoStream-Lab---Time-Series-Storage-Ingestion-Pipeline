import pytest
from pydantic import ValidationError

from app.core.config import (
    DEFAULT_SIMULATION_SETTINGS,
    BufferSettings,
    ChunkingSettings,
    Compression,
    FeedSettings,
    PartitionKey,
    SimulationSettings,
    TrafficPattern,
)


def test_default_settings_are_safe_and_serializable() -> None:
    settings = DEFAULT_SIMULATION_SETTINGS

    assert settings.feed.records_per_second == 1_000
    assert settings.feed.pattern is TrafficPattern.STEADY
    assert settings.buffer.max_records == 5_000
    assert settings.chunking.max_records == 1_000
    assert settings.storage.compression is Compression.SNAPPY
    assert settings.model_dump(mode="json")["buffer"]["overflow_policy"] == "backpressure"


def test_valid_settings_accept_all_supported_options() -> None:
    settings = SimulationSettings.model_validate(
        {
            "feed": {
                "records_per_second": 25_000,
                "pattern": "burst",
                "seed": 123,
                "invalid_record_ratio": 0.25,
            },
            "buffer": {
                "max_records": 50_000,
                "partition_by": ["device_id"],
                "overflow_policy": "backpressure",
            },
            "chunking": {"max_records": 10_000, "max_age_seconds": 60},
            "processing": {"validation_delay_ms": 5, "writer_delay_ms": 100},
            "storage": {"compression": "zstd", "output_path": "parquet/experiments"},
        }
    )

    assert settings.feed.pattern is TrafficPattern.BURST
    assert settings.buffer.partition_by == (PartitionKey.DEVICE_ID,)
    assert settings.storage.compression is Compression.ZSTD
    assert settings.storage.output_path == "parquet/experiments"


@pytest.mark.parametrize(
    ("model", "payload"),
    [
        (FeedSettings, {"records_per_second": 0}),
        (FeedSettings, {"records_per_second": 100_001}),
        (FeedSettings, {"invalid_record_ratio": -0.01}),
        (FeedSettings, {"invalid_record_ratio": 1.01}),
        (BufferSettings, {"max_records": 0}),
        (BufferSettings, {"partition_by": []}),
        (BufferSettings, {"partition_by": ["device_id", "device_id"]}),
        (ChunkingSettings, {"max_age_seconds": 0}),
        (SimulationSettings, {"storage": {"output_path": "../outside-data"}}),
        (SimulationSettings, {"storage": {"output_path": "C:\\data"}}),
        (SimulationSettings, {"storage": {"output_path": "   "}}),
        (SimulationSettings, {"unexpected_setting": True}),
    ],
)
def test_invalid_settings_are_rejected(
    model: type[SimulationSettings] | type[FeedSettings] | type[BufferSettings] | type[ChunkingSettings],
    payload: dict[str, object],
) -> None:
    with pytest.raises(ValidationError):
        model.model_validate(payload)


def test_boundary_values_are_accepted() -> None:
    settings = SimulationSettings(
        feed=FeedSettings(
            records_per_second=100_000,
            pattern=TrafficPattern.RAMP,
            seed=2**32 - 1,
            invalid_record_ratio=1.0,
        ),
        buffer=BufferSettings(max_records=1_000_000, partition_by=(PartitionKey.METRIC,)),
        chunking=ChunkingSettings(max_records=1_000_000, max_age_seconds=3_600),
    )

    assert settings.feed.seed == 2**32 - 1
    assert settings.chunking.max_age_seconds == 3_600


def test_chunk_threshold_cannot_exceed_total_buffer_capacity() -> None:
    with pytest.raises(ValidationError, match="chunking.max_records"):
        SimulationSettings.model_validate(
            {
                "buffer": {"max_records": 999},
                "chunking": {"max_records": 1_000},
            }
        )
