from datetime import UTC, datetime, timedelta

from app.core.config import ChunkingSettings
from app.engine.chunk_builder import ChunkBuilder
from app.engine.validator import ValidatedRecord


def record(seconds: int) -> ValidatedRecord:
    time = datetime(2030, 1, 1, tzinfo=UTC) + timedelta(seconds=seconds)
    return ValidatedRecord(str(seconds), "server_01", "cpu_usage", 1.0, time, time + timedelta(milliseconds=1))


def test_size_threshold_seals_an_immutable_chunk_with_stable_id() -> None:
    builder = ChunkBuilder("run-1", ChunkingSettings(max_records=2, max_age_seconds=10))
    assert builder.add(("server_01",), record(0)) is None
    chunk = builder.add(("server_01",), record(1))
    assert chunk is not None
    assert chunk.record_count == 2 and chunk.sealed_reason == "size"
    assert chunk.records == (record(0), record(1))
    repeat = ChunkBuilder("run-1", ChunkingSettings(max_records=2, max_age_seconds=10))
    repeat.add(("server_01",), record(0))
    repeated_chunk = repeat.add(("server_01",), record(1))
    assert repeated_chunk is not None and chunk.chunk_id == repeated_chunk.chunk_id


def test_age_threshold_seals_pending_chunk() -> None:
    builder = ChunkBuilder("run-1", ChunkingSettings(max_records=10, max_age_seconds=5))
    builder.add(("server_01",), record(0))
    chunks = builder.seal_expired(datetime(2030, 1, 1, 0, 0, 5, tzinfo=UTC))
    assert len(chunks) == 1 and chunks[0].sealed_reason == "age" and chunks[0].record_count == 1
