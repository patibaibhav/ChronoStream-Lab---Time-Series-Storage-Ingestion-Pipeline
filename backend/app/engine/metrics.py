"""Bounded metrics collection for one simulation run."""

from dataclasses import dataclass
from statistics import quantiles
from time import monotonic

from app.engine.buffer_manager import BufferMetrics
from app.engine.queue import QueueMetrics


@dataclass(frozen=True, slots=True)
class MetricsSnapshot:
    # Record counters
    input_records: int
    persisted_records: int
    rejected_records: int
    enqueued_records: int
    pending_records: int
    failed_records: int

    # Rates
    input_rate_records_per_second: float
    persisted_rate_records_per_second: float

    # Queue
    queue_depth: int
    queue_capacity: int
    queue_utilization: float
    backpressure_active: bool
    backpressure_events: int
    producer_wait_seconds: float

    # Buffer
    buffer_records: int
    buffer_capacity: int
    buffer_utilization: float
    buffer_partition_count: int

    # Chunks
    chunk_count: int
    chunk_records: int
    chunk_bytes: int
    chunks_created: int
    chunks_persisted: int
    chunks_failed: int
    chunks_open: int
    chunks_awaiting_write: int
    chunks_being_written: int
    full_chunks: int
    age_chunks: int
    partial_chunks: int

    # Storage
    file_count: int
    bytes_written: int

    # Retries
    retries: int

    # Latency
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
        self._enqueued_records = 0
        self._failed_records = 0
        self._retries = 0
        self._chunk_count = 0
        self._chunk_records = 0
        self._chunk_bytes = 0
        self._chunks_created = 0
        self._chunks_persisted = 0
        self._chunks_failed = 0
        self._chunks_open = 0
        self._chunks_awaiting_write = 0
        self._chunks_being_written = 0
        self._full_chunks = 0
        self._age_chunks = 0
        self._partial_chunks = 0
        self._file_count = 0
        self._bytes_written = 0
        self._latencies: list[float] = []
        self._queue = QueueMetrics(0, 0, 0, 0, 0, 0.0)
        self._buffer = BufferMetrics(0, 0, 0, 0.0, None)

    def record_input(self) -> None:
        self._input_records += 1

    def record_persisted(self, record_count: int) -> None:
        self._persisted_records += record_count

    def record_rejected(self) -> None:
        self._rejected_records += 1

    def record_enqueued(self) -> None:
        self._enqueued_records += 1

    def record_failed(self) -> None:
        self._failed_records += 1

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

    def record_chunks_created(self, count: int = 1) -> None:
        self._chunks_created += count

    def record_chunks_persisted(self, count: int = 1) -> None:
        self._chunks_persisted += count

    def record_chunks_failed(self, count: int = 1) -> None:
        self._chunks_failed += count

    def record_chunks_open(self, count: int) -> None:
        self._chunks_open = count

    def record_chunks_awaiting_write(self, count: int) -> None:
        self._chunks_awaiting_write = count

    def record_chunks_being_written(self, count: int) -> None:
        self._chunks_being_written = count

    def record_full_chunk(self) -> None:
        self._full_chunks += 1

    def record_age_chunk(self) -> None:
        self._age_chunks += 1

    def record_partial_chunk(self) -> None:
        self._partial_chunks += 1

    def record_file_written(self, byte_size: int) -> None:
        self._file_count += 1
        self._bytes_written += byte_size

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
            enqueued_records=self._enqueued_records,
            pending_records=self._enqueued_records - self._persisted_records - self._failed_records,
            failed_records=self._failed_records,
            input_rate_records_per_second=self._input_records / elapsed,
            persisted_rate_records_per_second=self._persisted_records / elapsed,
            queue_depth=self._queue.depth,
            queue_capacity=self._queue.capacity,
            queue_utilization=self._queue.depth / self._queue.capacity if self._queue.capacity > 0 else 0.0,
            backpressure_active=self._queue.depth >= self._queue.capacity,
            backpressure_events=self._queue.backpressure_events,
            producer_wait_seconds=self._queue.producer_wait_seconds,
            buffer_records=self._buffer.record_count,
            buffer_capacity=self._buffer.capacity,
            buffer_utilization=self._buffer.utilization,
            buffer_partition_count=self._buffer.partition_count,
            chunk_count=self._chunk_count,
            chunk_records=self._chunk_records,
            chunk_bytes=self._chunk_bytes,
            chunks_created=self._chunks_created,
            chunks_persisted=self._chunks_persisted,
            chunks_failed=self._chunks_failed,
            chunks_open=self._chunks_open,
            chunks_awaiting_write=self._chunks_awaiting_write,
            chunks_being_written=self._chunks_being_written,
            full_chunks=self._full_chunks,
            age_chunks=self._age_chunks,
            partial_chunks=self._partial_chunks,
            file_count=self._file_count,
            bytes_written=self._bytes_written,
            retries=self._retries,
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
