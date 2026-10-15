"""Immutable size- and age-sealed chunks of validated records."""

from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import NAMESPACE_URL, uuid5

from app.core.config import ChunkingSettings
from app.engine.buffer_manager import PartitionId
from app.engine.validator import ValidatedRecord


@dataclass(frozen=True, slots=True)
class Chunk:
    chunk_id: str
    run_id: str
    partition_id: PartitionId
    records: tuple[ValidatedRecord, ...]
    min_event_time: datetime
    max_event_time: datetime
    sealed_reason: str

    @property
    def record_count(self) -> int:
        return len(self.records)


class ChunkBuilder:
    def __init__(self, run_id: str, settings: ChunkingSettings) -> None:
        if not run_id.strip():
            raise ValueError("run_id is required")
        self._run_id = run_id
        self._settings = settings
        self._pending: list[tuple[PartitionId, ValidatedRecord]] = []
        self._sequences: dict[PartitionId, int] = defaultdict(int)

    def add(self, partition_id: PartitionId, record: ValidatedRecord) -> Chunk | None:
        self._pending.append((partition_id, record))
        if len(self._pending) >= self._settings.max_records:
            return self._seal("size")
        return None

    def seal_expired(self, now: datetime) -> tuple[Chunk, ...]:
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("now must be timezone-aware")
        now = now.astimezone(UTC)
        sealed: list[Chunk] = []
        while self._pending:
            if (now - self._pending[0][1].timestamp).total_seconds() >= self._settings.max_age_seconds:
                sealed.append(self._seal("age"))
            else:
                break
        return tuple(sealed)

    def seal_all(self, reason: str) -> tuple[Chunk, ...]:
        if not self._pending:
            return ()
        return (self._seal(reason),)

    @property
    def pending_count(self) -> int:
        return len(self._pending)

    def _seal(self, reason: str) -> Chunk:
        records = tuple(record for _, record in self._pending)
        partitions = tuple(partition_id for partition_id, _ in self._pending)
        self._pending.clear()
        sequence = self._sequences[partitions[0]]
        self._sequences[partitions[0]] += 1
        chunk_id = str(uuid5(NAMESPACE_URL, f"flowstore:{self._run_id}:{partitions[0]}:{sequence}"))
        timestamps = [record.timestamp for record in records]
        return Chunk(chunk_id, self._run_id, partitions[0], records, min(timestamps), max(timestamps), reason)
