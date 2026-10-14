"""Validated settings for a single local simulation run.

The model is intentionally independent of API handlers and engine modules so both
can share one authoritative configuration contract as they are introduced.
"""

from enum import StrEnum
from pathlib import PurePosixPath, PureWindowsPath

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class TrafficPattern(StrEnum):
    """Supported deterministic feed shapes."""

    STEADY = "steady"
    BURST = "burst"
    SPIKE = "spike"
    RAMP = "ramp"


class OverflowPolicy(StrEnum):
    """Queue-full policies available to the local simulation."""

    BACKPRESSURE = "backpressure"


class PartitionKey(StrEnum):
    """Record fields that can form a buffer partition."""

    DEVICE_ID = "device_id"
    METRIC = "metric"


class Compression(StrEnum):
    """Compression modes intended for the later Parquet writer."""

    SNAPPY = "snappy"
    GZIP = "gzip"
    ZSTD = "zstd"
    UNCOMPRESSED = "uncompressed"


class SimulationModel(BaseModel):
    """Base model that rejects unknown settings and is immutable after validation."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
        validate_default=True,
    )


class FeedSettings(SimulationModel):
    """Input feed settings."""

    records_per_second: int = Field(default=1_000, ge=1, le=100_000)
    pattern: TrafficPattern = TrafficPattern.STEADY
    seed: int = Field(default=42, ge=0, le=2**32 - 1)
    invalid_record_ratio: float = Field(default=0.0, ge=0.0, le=1.0)


class BufferSettings(SimulationModel):
    """Bounds and grouping settings for in-memory buffering."""

    max_records: int = Field(default=5_000, ge=1, le=1_000_000)
    max_partitions: int = Field(default=100, ge=1, le=10_000)
    idle_partition_seconds: int = Field(default=300, ge=1, le=86_400)
    partition_by: tuple[PartitionKey, ...] = (
        PartitionKey.DEVICE_ID,
        PartitionKey.METRIC,
    )
    overflow_policy: OverflowPolicy = OverflowPolicy.BACKPRESSURE

    @field_validator("partition_by")
    @classmethod
    def partition_keys_must_be_unique_and_present(
        cls, value: tuple[PartitionKey, ...]
    ) -> tuple[PartitionKey, ...]:
        if not value:
            raise ValueError("at least one partition key is required")
        if len(value) != len(set(value)):
            raise ValueError("partition keys must be unique")
        return value


class ChunkingSettings(SimulationModel):
    """Size- and time-based chunk sealing thresholds."""

    max_records: int = Field(default=1_000, ge=1, le=1_000_000)
    max_age_seconds: int = Field(default=10, ge=1, le=3_600)


class ProcessingSettings(SimulationModel):
    """Optional simulated stage delays, expressed in milliseconds."""

    validation_delay_ms: int = Field(default=0, ge=0, le=60_000)
    writer_delay_ms: int = Field(default=0, ge=0, le=60_000)


class StorageSettings(SimulationModel):
    """Local artifact settings relative to the configured data directory."""

    compression: Compression = Compression.SNAPPY
    output_path: str = "parquet"

    @field_validator("output_path")
    @classmethod
    def output_path_must_be_a_safe_relative_path(cls, value: str) -> str:
        posix_path = PurePosixPath(value)
        windows_path = PureWindowsPath(value)
        if (
            not value
            or value == "."
            or ".." in posix_path.parts
            or ".." in windows_path.parts
            or posix_path.is_absolute()
            or windows_path.is_absolute()
        ):
            raise ValueError("output_path must be a non-empty relative path")
        return value


class SimulationSettings(SimulationModel):
    """Complete settings snapshot used to create one simulation run."""

    feed: FeedSettings = Field(default_factory=FeedSettings)
    buffer: BufferSettings = Field(default_factory=BufferSettings)
    chunking: ChunkingSettings = Field(default_factory=ChunkingSettings)
    processing: ProcessingSettings = Field(default_factory=ProcessingSettings)
    storage: StorageSettings = Field(default_factory=StorageSettings)

    @model_validator(mode="after")
    def chunk_threshold_must_fit_in_the_buffer(self) -> "SimulationSettings":
        if self.chunking.max_records > self.buffer.max_records:
            raise ValueError("chunking.max_records cannot exceed buffer.max_records")
        return self


DEFAULT_SIMULATION_SETTINGS = SimulationSettings()
