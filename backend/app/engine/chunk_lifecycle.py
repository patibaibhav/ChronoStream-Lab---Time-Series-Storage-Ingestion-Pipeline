"""Lifecycle tracking and bounded retries for local chunk persistence."""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from time import sleep

from app.engine.chunk_builder import Chunk
from app.engine.local_writer import LocalParquetWriter, PersistedChunk


class ChunkState(StrEnum):
    CREATED = "created"
    SEALED = "sealed"
    WRITING = "writing"
    PERSISTED = "persisted"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """A finite exponential-backoff retry policy for one chunk write."""

    max_retries: int = 3
    initial_backoff_seconds: float = 0.1
    max_backoff_seconds: float = 1.0

    def __post_init__(self) -> None:
        if self.max_retries < 0:
            raise ValueError("max_retries must be non-negative")
        if self.initial_backoff_seconds < 0:
            raise ValueError("initial_backoff_seconds must be non-negative")
        if self.max_backoff_seconds < self.initial_backoff_seconds:
            raise ValueError("max_backoff_seconds must be at least initial_backoff_seconds")

    def backoff_seconds(self, retry_number: int) -> float:
        if retry_number < 1 or retry_number > self.max_retries:
            raise ValueError("retry_number must be within the configured retry range")
        return min(
            self.initial_backoff_seconds * 2 ** (retry_number - 1),
            self.max_backoff_seconds,
        )


@dataclass(frozen=True, slots=True)
class LifecycleEvent:
    state: ChunkState
    occurred_at: datetime
    attempt: int
    error: str | None = None


@dataclass(frozen=True, slots=True)
class ChunkLifecycle:
    chunk_id: str
    state: ChunkState
    attempts: int
    events: tuple[LifecycleEvent, ...]
    persisted_chunk: PersistedChunk | None = None
    last_error: str | None = None


class ChunkLifecycleManager:
    """Persist chunks only after a writer completes its own verification."""

    def __init__(
        self,
        writer: LocalParquetWriter,
        retry_policy: RetryPolicy = RetryPolicy(),
        *,
        clock: Callable[[], datetime] | None = None,
        sleeper: Callable[[float], None] = sleep,
        writer_delay_seconds: float = 0.0,
    ) -> None:
        if writer_delay_seconds < 0:
            raise ValueError("writer_delay_seconds must be non-negative")
        self._writer = writer
        self._retry_policy = retry_policy
        self._clock = clock or (lambda: datetime.now(UTC))
        self._sleeper = sleeper
        self._writer_delay_seconds = writer_delay_seconds
        self._lifecycles: dict[str, ChunkLifecycle] = {}

    def persist(self, chunk: Chunk) -> ChunkLifecycle:
        """Write a sealed chunk, retrying transient errors a finite number of times."""
        lifecycle = self._lifecycles.get(chunk.chunk_id)
        if lifecycle is None:
            lifecycle = self._transition(chunk.chunk_id, ChunkState.CREATED, 0)
        if lifecycle.state is ChunkState.CREATED:
            lifecycle = self._transition(chunk.chunk_id, ChunkState.SEALED, lifecycle.attempts)
        if lifecycle.state is ChunkState.PERSISTED:
            return lifecycle
        if lifecycle.state is ChunkState.WRITING:
            raise RuntimeError(f"chunk {chunk.chunk_id} is already being written")

        while True:
            lifecycle = self._transition(
                chunk.chunk_id, ChunkState.WRITING, lifecycle.attempts + 1
            )
            try:
                if self._writer_delay_seconds:
                    self._sleeper(self._writer_delay_seconds)
                persisted_chunk = self._writer.write(chunk)
            except Exception as error:
                lifecycle = self._transition(
                    chunk.chunk_id,
                    ChunkState.FAILED,
                    lifecycle.attempts,
                    error=str(error),
                )
                retries_used = lifecycle.attempts - 1
                if retries_used >= self._retry_policy.max_retries:
                    return lifecycle
                self._sleeper(self._retry_policy.backoff_seconds(retries_used + 1))
                continue

            return self._transition(
                chunk.chunk_id,
                ChunkState.PERSISTED,
                lifecycle.attempts,
                persisted_chunk=persisted_chunk,
            )

    def get(self, chunk_id: str) -> ChunkLifecycle | None:
        return self._lifecycles.get(chunk_id)

    def _transition(
        self,
        chunk_id: str,
        state: ChunkState,
        attempts: int,
        *,
        error: str | None = None,
        persisted_chunk: PersistedChunk | None = None,
    ) -> ChunkLifecycle:
        previous = self._lifecycles.get(chunk_id)
        event = LifecycleEvent(state, self._clock(), attempts, error)
        lifecycle = ChunkLifecycle(
            chunk_id=chunk_id,
            state=state,
            attempts=attempts,
            events=(*(previous.events if previous else ()), event),
            persisted_chunk=persisted_chunk or (previous.persisted_chunk if previous else None),
            last_error=error,
        )
        self._lifecycles[chunk_id] = lifecycle
        return lifecycle
