import sqlite3
from datetime import UTC, datetime
from pathlib import Path

from app.core.config import Compression
from app.engine.chunk_builder import Chunk
from app.engine.local_writer import PersistedChunk
from app.engine.validator import ValidatedRecord
from app.storage.metadata import SCHEMA_VERSION, SQLiteMetadataRepository


def test_metadata_repository_persists_versioned_run_data(tmp_path: Path) -> None:
    database_path = tmp_path / "lab.sqlite"
    repository = SQLiteMetadataRepository(database_path)
    now = datetime(2030, 1, 1, tzinfo=UTC)
    repository.create_run("run-1", now, {"feed": {"records_per_second": 10}})
    repository.record_event("run-1", "event-1", "accepted", None, {"metric": "cpu_usage"})
    repository.record_metric_sample("run-1", "event-1", "cpu_usage", 42.5, now)
    record = ValidatedRecord("event-1", "server_01", "cpu_usage", 42.5, now, now)
    chunk = Chunk("chunk-1", "run-1", ("server_01",), (record,), now, now, "size")
    persisted = PersistedChunk(Path("chunk.parquet"), 1, 100, "checksum", Compression.SNAPPY)
    repository.record_chunk("run-1", chunk, "persisted", persisted)
    repository.finish_run("run-1", now, "completed")

    with sqlite3.connect(database_path) as connection:
        assert connection.execute("SELECT version FROM schema_version").fetchone() == (
            SCHEMA_VERSION,
        )
        assert connection.execute("SELECT status FROM runs").fetchone() == ("completed",)
    assert repository.count("events", "run-1") == 1
    assert repository.count("metric_samples", "run-1") == 1
    assert repository.count("chunks", "run-1") == 1
