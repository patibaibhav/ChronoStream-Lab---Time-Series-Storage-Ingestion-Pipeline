"""Atomic local Parquet persistence for sealed chunks."""

from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from app.core.config import Compression, StorageSettings
from app.engine.chunk_builder import Chunk


@dataclass(frozen=True, slots=True)
class PersistedChunk:
    local_path: Path
    record_count: int
    byte_size: int
    checksum: str
    compression: Compression


class LocalParquetWriter:
    def __init__(self, data_dir: Path, settings: StorageSettings) -> None:
        self._data_dir = data_dir
        self._settings = settings

    def write(self, chunk: Chunk) -> PersistedChunk:
        directory = self._data_dir / self._settings.output_path / f"run_{chunk.run_id}"
        directory.mkdir(parents=True, exist_ok=True)
        final_path = directory / f"chunk_{chunk.chunk_id}.parquet"
        temporary_path = final_path.with_suffix(".parquet.tmp")
        table = pa.table({
            "event_id": [r.event_id for r in chunk.records], "device_id": [r.device_id for r in chunk.records],
            "metric": [r.metric for r in chunk.records], "value": [r.value for r in chunk.records],
            "timestamp": pa.array([r.timestamp for r in chunk.records], type=pa.timestamp("us", tz="UTC")),
            "ingested_at": pa.array([r.ingested_at for r in chunk.records], type=pa.timestamp("us", tz="UTC")),
        })
        compression = None if self._settings.compression is Compression.UNCOMPRESSED else self._settings.compression.value
        try:
            pq.write_table(table, temporary_path, compression=compression)
            verified = pq.read_table(temporary_path)
            if verified.num_rows != chunk.record_count or verified.schema != table.schema:
                raise ValueError("Parquet round-trip verification failed")
            temporary_path.replace(final_path)
        except Exception:
            temporary_path.unlink(missing_ok=True)
            raise
        content = final_path.read_bytes()
        return PersistedChunk(final_path, chunk.record_count, len(content), sha256(content).hexdigest(), self._settings.compression)
