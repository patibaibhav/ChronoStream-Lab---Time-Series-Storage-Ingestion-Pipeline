from datetime import UTC, datetime
from pathlib import Path

from app.core.config import Compression
from app.engine.chunk_builder import Chunk
from app.engine.chunk_lifecycle import ChunkLifecycleManager, ChunkState, RetryPolicy
from app.engine.local_writer import PersistedChunk
from app.engine.validator import ValidatedRecord


class StubWriter:
    def __init__(self, failures_before_success: int = 0) -> None:
        self.failures_before_success = failures_before_success
        self.calls = 0

    def write(self, _chunk: Chunk) -> PersistedChunk:
        self.calls += 1
        if self.calls <= self.failures_before_success:
            raise OSError(f"temporary failure {self.calls}")
        return PersistedChunk(Path("chunk.parquet"), 1, 10, "checksum", Compression.SNAPPY)


def make_chunk() -> Chunk:
    now = datetime(2030, 1, 1, tzinfo=UTC)
    record = ValidatedRecord("event", "server_01", "cpu_usage", 1.0, now, now)
    return Chunk("chunk", "run", ("server_01",), (record,), now, now, "size")


def test_records_all_successful_lifecycle_states() -> None:
    lifecycle = ChunkLifecycleManager(StubWriter()).persist(make_chunk())

    assert lifecycle.state is ChunkState.PERSISTED
    assert lifecycle.attempts == 1
    assert [event.state for event in lifecycle.events] == [
        ChunkState.CREATED,
        ChunkState.SEALED,
        ChunkState.WRITING,
        ChunkState.PERSISTED,
    ]
    assert lifecycle.persisted_chunk is not None


def test_retries_transient_failures_with_bounded_exponential_backoff() -> None:
    sleeps: list[float] = []
    writer = StubWriter(failures_before_success=2)
    lifecycle = ChunkLifecycleManager(
        writer,
        RetryPolicy(max_retries=3, initial_backoff_seconds=0.25, max_backoff_seconds=0.5),
        sleeper=sleeps.append,
    ).persist(make_chunk())

    assert writer.calls == 3
    assert sleeps == [0.25, 0.5]
    assert lifecycle.state is ChunkState.PERSISTED
    assert lifecycle.attempts == 3
    assert [event.state for event in lifecycle.events].count(ChunkState.FAILED) == 2


def test_leaves_chunk_failed_after_retry_limit() -> None:
    sleeps: list[float] = []
    writer = StubWriter(failures_before_success=10)
    lifecycle = ChunkLifecycleManager(
        writer,
        RetryPolicy(max_retries=2, initial_backoff_seconds=0.1, max_backoff_seconds=1),
        sleeper=sleeps.append,
    ).persist(make_chunk())

    assert writer.calls == 3
    assert sleeps == [0.1, 0.2]
    assert lifecycle.state is ChunkState.FAILED
    assert lifecycle.persisted_chunk is None
    assert lifecycle.last_error == "temporary failure 3"
