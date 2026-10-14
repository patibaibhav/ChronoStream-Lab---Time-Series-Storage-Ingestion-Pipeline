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
        self._pending: dict[PartitionId, list[ValidatedRecord]] = defaultdict(list)
        self._sequences: dict[PartitionId, int] = defaultdict(int)

    def add(self, partition_id: PartitionId, record: ValidatedRecord) -> Chunk | None:
        pending = self._pending[partition_id]
        pending.append(record)
        if len(pending) >= self._settings.max_records:
            return self._seal(partition_id, "size")
        return None

    def seal_expired(self, now: datetime) -> tuple[Chunk, ...]:
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("now must be timezone-aware")
        now = now.astimezone(UTC)
        sealed: list[Chunk] = []
        for partition_id, pending in tuple(self._pending.items()):
            if pending and (now - pending[0].timestamp).total_seconds() >= self._settings.max_age_seconds:
                sealed.append(self._seal(partition_id, "age"))
        return tuple(sealed)

    def _seal(self, partition_id: PartitionId, reason: str) -> Chunk:
        records = tuple(self._pending[partition_id])
        del self._pending[partition_id]
        sequence = self._sequences[partition_id]
        self._sequences[partition_id] += 1
        chunk_id = str(uuid5(NAMESPACE_URL, f"flowstore:{self._run_id}:{partition_id}:{sequence}"))
        timestamps = [record.timestamp for record in records]
        return Chunk(chunk_id, self._run_id, partition_id, records, min(timestamps), max(timestamps), reason)
