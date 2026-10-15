"""Deterministic tests for buffer occupancy and partition behavior.

These tests verify that records actually accumulate in the buffer between
flushes, that the buffer drains only when the chunk builder signals readiness,
and that per-partition occupancies are tracked independently.
"""

from datetime import UTC, datetime

from app.core.config import BufferSettings, ChunkingSettings, SimulationSettings
from app.engine.buffer_manager import PartitionedBufferManager
from app.engine.chunk_builder import ChunkBuilder
from app.engine.validator import ValidatedRecord


class Clock:
    def __init__(self, value: float = 2_000_000_000.0) -> None:
        self.value = value

    def __call__(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += seconds


def make_record(device: str = "server_01", metric: str = "cpu_usage") -> ValidatedRecord:
    ts = datetime.fromtimestamp(2_000_000_000.0, UTC)
    return ValidatedRecord(f"id-{device}-{metric}", device, metric, 1.0, ts, ts)


def test_buffer_accumulates_records_before_flush() -> None:
    """Insert 30 records into one partition with chunk size 100.
    Buffer occupancy must become 30, not 0."""
    settings = SimulationSettings(
        chunking=ChunkingSettings(max_records=100, max_age_seconds=3600),
        buffer=BufferSettings(max_records=1000, max_partitions=10),
    )
    buffer = PartitionedBufferManager(settings.buffer, clock=Clock())
    builder = ChunkBuilder("run-test", settings.chunking)

    # Insert 30 records into the same partition
    for _ in range(30):
        result = buffer.append(make_record())
        assert result.accepted

    # Buffer must hold all 30 records
    metrics = buffer.metrics()
    assert metrics.record_count == 30, f"Expected 30 buffered, got {metrics.record_count}"
    assert metrics.partition_count == 1
    assert metrics.utilization == 30 / 1000

    # No chunk should be created yet (chunk size is 100)
    assert builder.pending_count == 0


def test_buffer_drains_when_chunk_builder_ready() -> None:
    """Insert 70 more records (total 100), verify size-triggered flush
    produces one 100-record chunk and occupancy returns to 0."""
    settings = SimulationSettings(
        chunking=ChunkingSettings(max_records=100, max_age_seconds=3600),
        buffer=BufferSettings(max_records=1000, max_partitions=10),
    )
    buffer = PartitionedBufferManager(settings.buffer, clock=Clock())
    builder = ChunkBuilder("run-test", settings.chunking)

    # Insert 100 records into the same partition
    for _ in range(100):
        result = buffer.append(make_record())
        assert result.accepted

    # Buffer must hold all 100 records
    assert buffer.metrics().record_count == 100

    # Now drain and feed to chunk builder (simulating what orchestrator does)
    drained = buffer.drain(("server_01", "cpu_usage"))
    assert len(drained) == 100

    # Buffer must be empty after drain
    assert buffer.metrics().record_count == 0

    # Feed to chunk builder
    chunk = None
    for record in drained:
        chunk = builder.add(("server_01", "cpu_usage"), record)

    # Chunk builder should have sealed a 100-record chunk
    assert chunk is not None
    assert chunk.record_count == 100
    assert chunk.sealed_reason == "size"


def test_multiple_partitions_tracked_independently() -> None:
    """Insert records into multiple partitions and verify their occupancies
    are tracked independently."""
    settings = SimulationSettings(
        chunking=ChunkingSettings(max_records=100, max_age_seconds=3600),
        buffer=BufferSettings(max_records=1000, max_partitions=10),
    )
    buffer = PartitionedBufferManager(settings.buffer, clock=Clock())

    # Insert 30 records into partition A
    for _ in range(30):
        buffer.append(make_record(device="server_01", metric="cpu_usage"))

    # Insert 20 records into partition B
    for _ in range(20):
        buffer.append(make_record(device="server_02", metric="cpu_usage"))

    # Insert 10 records into partition C
    for _ in range(10):
        buffer.append(make_record(device="server_03", metric="cpu_usage"))

    metrics = buffer.metrics()
    assert metrics.record_count == 60, f"Expected 60 total, got {metrics.record_count}"
    assert metrics.partition_count == 3

    # Drain partition A only
    drained_a = buffer.drain(("server_01", "cpu_usage"))
    assert len(drained_a) == 30

    # Remaining should be 30 (20 + 10)
    assert buffer.metrics().record_count == 30
    assert buffer.metrics().partition_count == 3  # partition A still exists (empty)


def test_end_of_run_flush_uses_seal_all() -> None:
    """Verify that end-of-run flushing uses seal_all with END_OF_RUN reason."""
    settings = SimulationSettings(
        chunking=ChunkingSettings(max_records=100, max_age_seconds=3600),
        buffer=BufferSettings(max_records=1000, max_partitions=10),
    )
    buffer = PartitionedBufferManager(settings.buffer, clock=Clock())
    builder = ChunkBuilder("run-eor", settings.chunking)

    # Insert 45 records (less than chunk size)
    for _ in range(45):
        buffer.append(make_record())

    # Drain and feed to builder
    drained = buffer.drain(("server_01", "cpu_usage"))
    for record in drained:
        builder.add(("server_01", "cpu_usage"), record)

    # No size-triggered chunk yet
    assert builder.pending_count == 45

    # End-of-run flush
    chunks = builder.seal_all("end_of_run")
    assert len(chunks) == 1
    assert chunks[0].record_count == 45
    assert chunks[0].sealed_reason == "end_of_run"


def test_age_based_flush_with_controlled_clock() -> None:
    """Verify age-based flushing using a controlled clock."""
    clock = Clock()
    settings = SimulationSettings(
        chunking=ChunkingSettings(max_records=100, max_age_seconds=5),
        buffer=BufferSettings(max_records=1000, max_partitions=10),
    )
    buffer = PartitionedBufferManager(settings.buffer, clock=clock)
    builder = ChunkBuilder("run-age", settings.chunking)

    # Insert 30 records
    for _ in range(30):
        buffer.append(make_record())

    # Drain and feed to builder
    drained = buffer.drain(("server_01", "cpu_usage"))
    for record in drained:
        builder.add(("server_01", "cpu_usage"), record)

    assert builder.pending_count == 30

    # Before advancing clock: no age-based flush
    now_before = datetime.fromtimestamp(clock.value, UTC)
    chunks_before = builder.seal_expired(now_before)
    assert len(chunks_before) == 0

    # Advance clock beyond age threshold (5 seconds)
    clock.advance(6)
    now_after = datetime.fromtimestamp(clock.value, UTC)
    chunks_after = builder.seal_expired(now_after)
    assert len(chunks_after) == 1
    assert chunks_after[0].record_count == 30
    assert chunks_after[0].sealed_reason == "age"


def test_orchestrator_buffer_occupancy_nonzero() -> None:
    """Integration test: verify that during a real run, the buffer
    actually holds records between flushes."""
    import tempfile
    from pathlib import Path

    from app.engine.orchestrator import SimulationEngine

    settings = SimulationSettings(
        chunking=ChunkingSettings(max_records=100, max_age_seconds=3600),
        buffer=BufferSettings(max_records=1000, max_partitions=10),
    )

    with tempfile.TemporaryDirectory() as td:
        engine = SimulationEngine(settings, Path(td))
        # Run a small simulation
        summary = engine.run("run-buffer-test", 250)

        # All records should be persisted
        assert summary.generated_records == 250
        assert summary.accepted_records == 250
        assert summary.persisted_records == 250
        assert summary.persisted_chunks == 3  # 2 full + 1 partial
        assert summary.full_chunks == 2
        assert summary.partial_chunks == 1
