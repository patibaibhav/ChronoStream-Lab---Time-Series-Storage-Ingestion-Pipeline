"""Bounded partitioned buffers for validated records."""

from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from time import monotonic

from app.core.config import BufferSettings, PartitionKey
from app.engine.validator import ValidatedRecord


PartitionId = tuple[str, ...]


@dataclass(frozen=True, slots=True)
class BufferAppendResult:
    accepted: bool
    partition_id: PartitionId
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class BufferMetrics:
    capacity: int
    record_count: int
    partition_count: int
    utilization: float
    oldest_record_age_seconds: float | None


class PartitionedBufferManager:
    """Bounded buffers keyed by configured record fields.

    Empty partitions are removed after the configured idle period. Non-empty
    partitions are retained until a later chunking task seals/drains them; this
    manager never evicts records to clean up idle state.
    """

    def __init__(self, settings: BufferSettings, *, clock: Callable[[], float] = monotonic) -> None:
        self._settings = settings
        self._clock = clock
        self._buffers: dict[PartitionId, deque[ValidatedRecord]] = {}
        self._last_active: dict[PartitionId, float] = {}
        self._record_count = 0

    def append(self, record: ValidatedRecord) -> BufferAppendResult:
        partition_id = self._partition_id(record)
        now = self._clock()
        if self._record_count >= self._settings.max_records:
            return BufferAppendResult(False, partition_id, "global_capacity")
        if partition_id not in self._buffers and len(self._buffers) >= self._settings.max_partitions:
            return BufferAppendResult(False, partition_id, "partition_limit")
        buffer = self._buffers.setdefault(partition_id, deque())
        buffer.append(record)
        self._record_count += 1
        self._last_active[partition_id] = now
        return BufferAppendResult(True, partition_id)

    def drain(self, partition_id: PartitionId) -> tuple[ValidatedRecord, ...]:
        buffer = self._buffers.get(partition_id)
        if buffer is None:
            return ()
        records = tuple(buffer)
        buffer.clear()
        self._record_count -= len(records)
        self._last_active[partition_id] = self._clock()
        return records

    def cleanup_idle_partitions(self) -> tuple[PartitionId, ...]:
        now = self._clock()
        removed: list[PartitionId] = []
        for partition_id, buffer in tuple(self._buffers.items()):
            if buffer or now - self._last_active[partition_id] < self._settings.idle_partition_seconds:
                continue
            del self._buffers[partition_id]
            del self._last_active[partition_id]
            removed.append(partition_id)
        return tuple(removed)

    def metrics(self) -> BufferMetrics:
        now = self._clock()
        oldest_timestamp = min(
            (record.timestamp for buffer in self._buffers.values() for record in buffer),
            default=None,
        )
        oldest_age = None if oldest_timestamp is None else max(0.0, now - oldest_timestamp.timestamp())
        return BufferMetrics(
            capacity=self._settings.max_records,
            record_count=self._record_count,
            partition_count=len(self._buffers),
            utilization=self._record_count / self._settings.max_records,
            oldest_record_age_seconds=oldest_age,
        )

    def _partition_id(self, record: ValidatedRecord) -> PartitionId:
        values = {
            PartitionKey.DEVICE_ID: record.device_id,
            PartitionKey.METRIC: record.metric,
        }
        return tuple(values[key] for key in self._settings.partition_by)
