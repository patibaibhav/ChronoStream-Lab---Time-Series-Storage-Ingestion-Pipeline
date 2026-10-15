"""Synchronous, API-independent orchestration of one simulation run."""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Event, Lock
from time import sleep
from collections.abc import Callable

from app.core.config import SimulationSettings
from app.engine.buffer_manager import PartitionId, PartitionedBufferManager
from app.engine.chunk_builder import Chunk, ChunkBuilder
from app.engine.chunk_lifecycle import ChunkLifecycleManager, ChunkState, RetryPolicy
from app.engine.generator import SeededRecordGenerator
from app.engine.local_writer import LocalParquetWriter
from app.engine.metrics import MetricsCollector, MetricsSnapshot
from app.engine.queue import BoundedStreamQueue
from app.engine.validator import RecordValidator
from app.storage.metadata import SQLiteMetadataRepository


@dataclass(frozen=True, slots=True)
class RunSummary:
    """Stable summary returned when a simulation run stops."""

    run_id: str
    requested_records: int
    generated_records: int
    accepted_records: int
    rejected_records: int
    persisted_chunks: int
    failed_chunks: int
    persisted_records: int
    stopped: bool
    chunk_ids: tuple[str, ...]
    metrics: MetricsSnapshot
    chunks_created: int
    chunks_awaiting_write: int
    full_chunks: int
    age_chunks: int
    partial_chunks: int


class SimulationEngine:
    """Connect the local pipeline stages without depending on the web layer."""

    def __init__(
        self,
        settings: SimulationSettings,
        data_dir: Path,
        *,
        retry_policy: RetryPolicy | None = None,
        metadata_repository: SQLiteMetadataRepository | None = None,
        sleeper: Callable[[float], None] = sleep,
    ) -> None:
        self._settings = settings
        self._data_dir = data_dir
        self._metadata = metadata_repository or SQLiteMetadataRepository(data_dir / "lab.sqlite")
        self._lock = Lock()
        self._stop_requested = Event()
        self._running = False

        self._queue: BoundedStreamQueue[object] = BoundedStreamQueue.from_settings(settings)
        self._validator = RecordValidator()
        self._buffer = PartitionedBufferManager(settings.buffer)
        self._run_id = ""
        self._builder: ChunkBuilder | None = None
        self._lifecycle: ChunkLifecycleManager | None = None
        self._metrics = MetricsCollector()

        self._retry_policy = retry_policy
        self._sleeper = sleeper

    def run(self, run_id: str, record_count: int) -> RunSummary:
        """Generate and persist up to ``record_count`` records, then return a summary."""
        if not run_id.strip():
            raise ValueError("run_id is required")
        if isinstance(record_count, bool) or not isinstance(record_count, int):
            raise TypeError("record_count must be an integer")
        if record_count < 0:
            raise ValueError("record_count must be non-negative")

        with self._lock:
            if self._running:
                raise RuntimeError("a simulation is already running")
            self._running = True

        self._stop_requested.clear()
        self._run_id = run_id
        self._queue = BoundedStreamQueue.from_settings(self._settings)
        self._validator = RecordValidator()
        self._buffer = PartitionedBufferManager(self._settings.buffer)
        self._builder = ChunkBuilder(run_id, self._settings.chunking)
        writer = LocalParquetWriter(self._data_dir, self._settings.storage)
        self._lifecycle = ChunkLifecycleManager(
            writer,
            self._retry_policy or RetryPolicy(),
            sleeper=self._sleeper,
            writer_delay_seconds=self._settings.processing.writer_delay_ms / 1_000,
        )
        self._metrics = MetricsCollector()
        self._metadata.create_run(
            run_id,
            datetime.now(UTC),
            self._settings.model_dump(mode="json"),
        )

        generated = 0
        persisted: list[Chunk] = []
        failed: list[Chunk] = []
        latest_event_time: datetime | None = None
        try:
            previous_event_time: datetime | None = None
            for source_record in SeededRecordGenerator(self._settings.feed).generate(record_count):
                if self._stop_requested.is_set():
                    break
                current_timestamp = (
                    source_record.timestamp
                    if hasattr(source_record, "timestamp")
                    else datetime.fromisoformat(
                        str(source_record["timestamp"]).replace("Z", "+00:00")
                    )
                )
                if previous_event_time is not None:
                    self._sleeper(
                        max(0.0, (current_timestamp - previous_event_time).total_seconds())
                    )
                generated += 1
                latest_event_time = current_timestamp
                previous_event_time = current_timestamp
                self._offer_and_process(source_record, persisted, failed)

            if not self._stop_requested.is_set():
                # Drain any remaining buffered records into the chunk builder
                if self._buffer.metrics().record_count > 0:
                    buffered_records: list[tuple[PartitionId, ValidatedRecord]] = []
                    for partition_id in list(self._buffer._buffers.keys()):
                        buffered_records.extend(
                            (partition_id, r) for r in self._buffer.drain(partition_id)
                        )
                    for partition_id, record in buffered_records:
                        chunk = self._builder.add(partition_id, record)
                        if chunk is not None:
                            self._persist_chunks((chunk,), persisted, failed)
                expired = self._builder.seal_expired(
                    datetime.now(UTC)
                    + timedelta(seconds=self._settings.chunking.max_age_seconds)
                )
                self._persist_chunks(expired, persisted, failed)
                remaining = self._builder.seal_all("end_of_run")
                self._persist_chunks(remaining, persisted, failed)
        finally:
            self._metadata.finish_run(
                run_id,
                datetime.now(UTC),
                "stopped" if self._stop_requested.is_set() else "completed",
            )
            with self._lock:
                self._running = False

        validation_metrics = self._validator.metrics()
        metrics_snapshot = self._metrics.snapshot()
        return RunSummary(
            run_id=run_id,
            requested_records=record_count,
            generated_records=generated,
            accepted_records=validation_metrics.accepted_records,
            rejected_records=validation_metrics.rejected_records,
            persisted_chunks=len(persisted),
            failed_chunks=len(failed),
            persisted_records=sum(chunk.record_count for chunk in persisted),
            stopped=self._stop_requested.is_set(),
            chunk_ids=tuple(chunk.chunk_id for chunk in persisted),
            metrics=metrics_snapshot,
            chunks_created=metrics_snapshot.chunks_created,
            chunks_awaiting_write=metrics_snapshot.chunks_awaiting_write,
            full_chunks=metrics_snapshot.full_chunks,
            age_chunks=metrics_snapshot.age_chunks,
            partial_chunks=metrics_snapshot.partial_chunks,
        )

    def stop(self) -> None:
        """Request a clean stop at the next record boundary."""
        self._stop_requested.set()

    @property
    def running(self) -> bool:
        return self._running

    @property
    def metrics(self) -> MetricsSnapshot:
        return self._metrics.snapshot()

    def _offer_and_process(
        self,
        source_record: object,
        persisted: list[Chunk],
        failed: list[Chunk],
    ) -> None:
        self._metrics.record_input()
        while not self._stop_requested.is_set():
            offered = self._queue.offer(source_record)
            if offered.accepted:
                self._process_one(persisted, failed)
                self._metrics.observe_queue(self._queue.metrics())
                self._metrics.observe_buffer(self._buffer.metrics())
                return
            self._process_one(persisted, failed)

    def _process_one(self, persisted: list[Chunk], failed: list[Chunk]) -> None:
        raw_record = self._queue.get()
        if raw_record is None:
            return
        if self._settings.processing.validation_delay_ms:
            self._sleeper(self._settings.processing.validation_delay_ms / 1_000)
        result = self._validator.validate(raw_record)
        payload = raw_record.to_dict() if hasattr(raw_record, "to_dict") else None
        self._metadata.record_event(
            self._run_id,
            result.record.event_id if result.record is not None else None,
            "accepted" if result.accepted else "rejected",
            result.reason.value if result.reason is not None else None,
            payload,
        )
        if not result.accepted or result.record is None:
            self._metrics.record_rejected()
            return
        self._metadata.record_metric_sample(
            self._run_id,
            result.record.event_id,
            result.record.metric,
            result.record.value,
            result.record.timestamp,
        )
        append_result = self._buffer.append(result.record)
        if not append_result.accepted:
            raise RuntimeError(f"buffer rejected a validated record: {append_result.reason}")
        if self._builder is None:
            raise RuntimeError("engine is not initialized")
        # Only drain the buffer when it has accumulated enough records for a chunk.
        # This allows the buffer to actually hold records between flushes.
        if self._buffer.metrics().record_count >= self._settings.chunking.max_records:
            buffered_records: list[tuple[PartitionId, ValidatedRecord]] = []
            for partition_id in list(self._buffer._buffers.keys()):
                buffered_records.extend(
                    (partition_id, r) for r in self._buffer.drain(partition_id)
                )
            for partition_id, record in buffered_records:
                chunk = self._builder.add(partition_id, record)
                if chunk is not None:
                    self._persist_chunks((chunk,), persisted, failed)

    def _persist_chunks(
        self,
        chunks: tuple[Chunk, ...],
        persisted: list[Chunk],
        failed: list[Chunk],
    ) -> None:
        if self._lifecycle is None:
            raise RuntimeError("engine is not initialized")
        for chunk in chunks:
            self._metrics.record_chunks_created()
            self._metrics.record_chunks_awaiting_write(1)
            if chunk.sealed_reason == "size":
                self._metrics.record_full_chunk()
            elif chunk.sealed_reason == "age":
                self._metrics.record_age_chunk()
            elif chunk.sealed_reason == "end_of_run":
                self._metrics.record_partial_chunk()
            lifecycle = self._lifecycle.persist(chunk)
            self._metrics.record_chunks_awaiting_write(0)
            if lifecycle.state is ChunkState.PERSISTED:
                persisted.append(chunk)
                self._metrics.record_persisted(chunk.record_count)
                self._metrics.record_chunks_persisted()
                if lifecycle.persisted_chunk is None:
                    raise RuntimeError("persisted lifecycle is missing writer metadata")
                self._metrics.record_chunk(
                    chunk.record_count,
                    lifecycle.persisted_chunk.byte_size,
                )
                self._metrics.record_file_written(lifecycle.persisted_chunk.byte_size)
                self._metrics.record_retries(
                    sum(event.state is ChunkState.FAILED for event in lifecycle.events)
                )
                self._metadata.record_chunk(
                    self._run_id,
                    chunk,
                    lifecycle.state.value,
                    lifecycle.persisted_chunk,
                    flush_reason=chunk.sealed_reason,
                    retry_attempts=sum(event.state is ChunkState.FAILED for event in lifecycle.events),
                )
            elif lifecycle.state is ChunkState.FAILED:
                failed.append(chunk)
                self._metrics.record_chunks_failed()
                self._metrics.record_failed(chunk.record_count)
                self._metadata.record_chunk(
                    self._run_id,
                    chunk,
                    lifecycle.state.value,
                    error=lifecycle.last_error,
                    flush_reason=chunk.sealed_reason,
                    retry_attempts=sum(event.state is ChunkState.FAILED for event in lifecycle.events),
                )
