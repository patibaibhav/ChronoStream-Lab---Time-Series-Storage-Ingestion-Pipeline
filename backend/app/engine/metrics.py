"""Bounded metrics collection for one simulation run."""

from dataclasses import dataclass
from statistics import quantiles
from time import monotonic

from app.engine.buffer_manager import BufferMetrics
from app.engine.queue import QueueMetrics


@dataclass(frozen=True, slots=True)
class MetricsSnapshot:
    input_records: int
    persisted_records: int
    rejected_records: int
    retries: int
    chunk_count: int
    chunk_records: int
    chunk_bytes: int
    queue_depth: int
    queue_capacity: int
    buffer_records: int
    buffer_capacity: int
    input_rate_records_per_second: float
    persisted_rate_records_per_second: float
    p50_latency_seconds: float | None
    p95_latency_seconds: float | None
    p99_latency_seconds: float | None


class MetricsCollector:
    """Collect counters and latency samples without retaining source records."""

    def __init__(self, *, clock=monotonic) -> None:
        self._clock = clock
        self._started_at = clock()
        self._input_records = 0
        self._persisted_records = 0
        self._rejected_records = 0
        self._retries = 0
        self._chunk_count = 0
        self._chunk_records = 0
        self._chunk_bytes = 0
        self._latencies: list[float] = []
        self._queue = QueueMetrics(0, 0, 0, 0, 0, 0.0)
        self._buffer = BufferMetrics(0, 0, 0, 0.0, None)

    def record_input(self) -> None:
        self._input_records += 1

    def record_persisted(self, record_count: int) -> None:
        self._persisted_records += record_count

    def record_rejected(self) -> None:
        self._rejected_records += 1

    def record_retry(self) -> None:
        self._retries += 1

    def record_retries(self, count: int) -> None:
        if count < 0:
            raise ValueError("retry count cannot be negative")
        self._retries += count

    def record_chunk(self, record_count: int, byte_size: int) -> None:
        self._chunk_count += 1
        self._chunk_records += record_count
        self._chunk_bytes += byte_size

    def record_latency(self, seconds: float) -> None:
        if seconds < 0:
            raise ValueError("latency cannot be negative")
        self._latencies.append(seconds)

    def observe_queue(self, metrics: QueueMetrics) -> None:
        self._queue = metrics

    def observe_buffer(self, metrics: BufferMetrics) -> None:
        self._buffer = metrics

    def snapshot(self) -> MetricsSnapshot:
        elapsed = max(self._clock() - self._started_at, 1e-9)
        return MetricsSnapshot(
            input_records=self._input_records,
            persisted_records=self._persisted_records,
            rejected_records=self._rejected_records,
            retries=self._retries,
            chunk_count=self._chunk_count,
            chunk_records=self._chunk_records,
            chunk_bytes=self._chunk_bytes,
            queue_depth=self._queue.depth,
            queue_capacity=self._queue.capacity,
            buffer_records=self._buffer.record_count,
            buffer_capacity=self._buffer.capacity,
            input_rate_records_per_second=self._input_records / elapsed,
            persisted_rate_records_per_second=self._persisted_records / elapsed,
            p50_latency_seconds=self._percentile(0.5),
            p95_latency_seconds=self._percentile(0.95),
            p99_latency_seconds=self._percentile(0.99),
        )

    def _percentile(self, percentile: float) -> float | None:
        if not self._latencies:
            return None
        if len(self._latencies) == 1:
            return self._latencies[0]
        return quantiles(self._latencies, n=100, method="inclusive")[
            round(percentile * 100) - 1
        ]
