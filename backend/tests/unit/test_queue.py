import pytest

from app.core.config import BufferSettings, ChunkingSettings, SimulationSettings
from app.engine.queue import BoundedStreamQueue


class FakeClock:
    def __init__(self) -> None:
        self.current = 0.0

    def __call__(self) -> float:
        return self.current

    def advance(self, seconds: float) -> None:
        self.current += seconds


def test_queue_never_exceeds_configured_capacity_or_drops_items() -> None:
    queue: BoundedStreamQueue[str] = BoundedStreamQueue(capacity=2)

    first = queue.offer("first")
    second = queue.offer("second")
    blocked = queue.offer("third")

    assert first.accepted is True
    assert second.accepted is True
    assert blocked == type(blocked)(accepted=False, depth=2, reason="backpressure")
    assert queue.depth == 2
    assert queue.get() == "first"
    assert queue.get() == "second"
    assert queue.get() is None

    metrics = queue.metrics()
    assert metrics.depth == 0
    assert metrics.accepted_records == 2
    assert metrics.dequeued_records == 2
    assert metrics.backpressure_events == 1


def test_backpressure_wait_is_exposed_until_the_item_is_accepted() -> None:
    clock = FakeClock()
    queue: BoundedStreamQueue[str] = BoundedStreamQueue(capacity=1, clock=clock)
    queue.offer("first")

    clock.advance(2.5)
    assert queue.offer("second").accepted is False
    clock.advance(1.5)
    assert queue.get() == "first"

    assert queue.metrics().producer_wait_seconds == 1.5

    clock.advance(0.5)
    accepted = queue.offer("second")

    assert accepted.accepted is True
    assert queue.metrics().producer_wait_seconds == 2.0
    assert queue.metrics().backpressure_events == 1


def test_multiple_backpressure_episodes_accumulate_wait_time() -> None:
    clock = FakeClock()
    queue: BoundedStreamQueue[int] = BoundedStreamQueue(capacity=1, clock=clock)
    queue.offer(1)
    clock.advance(1)
    queue.offer(2)
    clock.advance(2)
    queue.get()
    queue.offer(2)
    clock.advance(3)
    queue.offer(3)
    clock.advance(4)
    queue.get()
    queue.offer(3)

    metrics = queue.metrics()
    assert metrics.backpressure_events == 2
    assert metrics.producer_wait_seconds == 6.0
    assert metrics.accepted_records - metrics.dequeued_records == metrics.depth


def test_queue_uses_buffer_capacity_from_simulation_settings() -> None:
    settings = SimulationSettings(
        buffer=BufferSettings(max_records=3),
        chunking=ChunkingSettings(max_records=3),
    )

    queue = BoundedStreamQueue.from_settings(settings)

    assert queue.capacity == 3


@pytest.mark.parametrize("capacity", [0, -1, True, 1.5, "2"])
def test_invalid_queue_capacity_is_rejected(capacity: object) -> None:
    with pytest.raises(ValueError, match="positive integer"):
        BoundedStreamQueue(capacity=capacity)  # type: ignore[arg-type]


def test_queue_rejects_a_non_monotonic_clock() -> None:
    clock = FakeClock()
    queue: BoundedStreamQueue[str] = BoundedStreamQueue(capacity=1, clock=clock)
    queue.offer("first")
    clock.advance(1)
    queue.offer("second")
    clock.current = 0.0

    with pytest.raises(ValueError, match="monotonic"):
        queue.metrics()
