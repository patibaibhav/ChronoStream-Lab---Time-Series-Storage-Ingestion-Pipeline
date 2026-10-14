from datetime import UTC, datetime
from hashlib import sha256

import pyarrow.parquet as pq
import pytest

from app.core.config import StorageSettings
from app.engine.chunk_builder import Chunk
from app.engine.local_writer import LocalParquetWriter
from app.engine.validator import ValidatedRecord


def test_writes_readable_parquet(tmp_path) -> None:
    now = datetime(2030, 1, 1, tzinfo=UTC)
    record = ValidatedRecord("event", "server_01", "cpu_usage", 1.0, now, now)
    chunk = Chunk("chunk", "run", ("server_01",), (record,), now, now, "size")
    result = LocalParquetWriter(tmp_path, StorageSettings()).write(chunk)
    table = pq.read_table(result.local_path)
    metadata = pq.ParquetFile(result.local_path).metadata

    assert result.local_path.exists()
    assert result.record_count == 1
    assert result.byte_size == result.local_path.stat().st_size
    assert result.checksum == sha256(result.local_path.read_bytes()).hexdigest()
    assert table.schema.names == [
        "event_id",
        "device_id",
        "metric",
        "value",
        "timestamp",
        "ingested_at",
    ]
    assert table.num_rows == 1
    assert table.column("event_id")[0].as_py() == "event"
    assert metadata.row_group(0).column(0).compression == "SNAPPY"


def test_removes_temporary_file_when_write_fails(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime(2030, 1, 1, tzinfo=UTC)
    record = ValidatedRecord("event", "server_01", "cpu_usage", 1.0, now, now)
    chunk = Chunk("chunk", "run", ("server_01",), (record,), now, now, "size")
    writer = LocalParquetWriter(tmp_path, StorageSettings())

    def fail_write(*_args, **_kwargs) -> None:
        raise OSError("disk unavailable")

    monkeypatch.setattr("app.engine.local_writer.pq.write_table", fail_write)

    with pytest.raises(OSError, match="disk unavailable"):
        writer.write(chunk)

    assert not (tmp_path / "parquet" / "run_run" / "chunk_chunk.parquet").exists()
    assert not (tmp_path / "parquet" / "run_run" / "chunk_chunk.parquet.tmp").exists()
