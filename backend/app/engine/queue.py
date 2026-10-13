"""Bounded in-process queue with explicit backpressure accounting."""

from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from time import monotonic
from typing import Generic, TypeVar

from app.core.config import SimulationSettings


Item = TypeVar("Item")


@dataclass(frozen=True, slots=True)
class QueueOfferResult:
    """Outcome of a producer's attempt to place one item on the queue."""

    accepted: bool
    depth: int
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class QueueMetrics:
    """Snapshot of bounded-queue state and producer backpressure."""

    capacity: int
    depth: int
    accepted_records: int
    dequeued_records: int
    backpressure_events: int
    producer_wait_seconds: float


class BoundedStreamQueue(Generic[Item]):
    """A controlled-loop queue that never accepts more than its capacity.

    ``offer`` is deliberately nonblocking. A full queue returns an explicit
    backpressure result so the future orchestrator can pause or slow the source
    without dropping the item or creating untested concurrency. Wait time is the
    elapsed monotonic time from the first rejected offer until the next accepted
    offer, including an active wait in metric snapshots.
    """

    def __init__(self, capacity: int, *, clock: Callable[[], float] = monotonic) -> None:
        if isinstance(capacity, bool) or not isinstance(capacity, int) or capacity < 1:
            raise ValueError("capacity must be a positive integer")
        self._capacity = capacity
        self._clock = clock
        self._items: deque[Item] = deque()
        self._accepted_records = 0
        self._dequeued_records = 0
        self._backpressure_events = 0
        self._completed_wait_seconds = 0.0
        self._backpressure_started_at: float | None = None

    @classmethod
    def from_settings(cls, settings: SimulationSettings) -> "BoundedStreamQueue[object]":
        """Build a queue using the configured total buffer capacity."""

        return cls(capacity=settings.buffer.max_records)

    @property
    def capacity(self) -> int:
        return self._capacity

    @property
    def depth(self) -> int:
        return len(self._items)

    def offer(self, item: Item) -> QueueOfferResult:
        """Attempt to enqueue an item without dropping or blocking it."""

        now = self._clock()
        if self.depth >= self._capacity:
            self._backpressure_events += 1
            if self._backpressure_started_at is None:
                self._backpressure_started_at = now
            return QueueOfferResult(accepted=False, depth=self.depth, reason="backpressure")

        self._complete_backpressure_wait(now)
        self._items.append(item)
        self._accepted_records += 1
        return QueueOfferResult(accepted=True, depth=self.depth)

    def get(self) -> Item | None:
        """Remove and return the next item, or ``None`` when the queue is empty."""

        if not self._items:
            return None
        self._dequeued_records += 1
        return self._items.popleft()

    def metrics(self) -> QueueMetrics:
        """Return a snapshot without fabricating unavailable or dropped records."""

        active_wait_seconds = 0.0
        if self._backpressure_started_at is not None:
            active_wait_seconds = self._elapsed_since(self._backpressure_started_at, self._clock())
        return QueueMetrics(
            capacity=self.capacity,
            depth=self.depth,
            accepted_records=self._accepted_records,
            dequeued_records=self._dequeued_records,
            backpressure_events=self._backpressure_events,
            producer_wait_seconds=self._completed_wait_seconds + active_wait_seconds,
        )

    def _complete_backpressure_wait(self, now: float) -> None:
        if self._backpressure_started_at is None:
            return
        self._completed_wait_seconds += self._elapsed_since(self._backpressure_started_at, now)
        self._backpressure_started_at = None

    @staticmethod
    def _elapsed_since(started_at: float, now: float) -> float:
        elapsed = now - started_at
        if elapsed < 0:
            raise ValueError("clock must be monotonic")
        return elapsed
