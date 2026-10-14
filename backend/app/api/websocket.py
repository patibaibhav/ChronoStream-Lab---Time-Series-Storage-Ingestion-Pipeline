"""Bounded WebSocket snapshot/event fan-out."""

from collections import deque
from collections.abc import Iterable
from threading import Lock
from typing import Any


class SnapshotHub:
    """Keep a bounded event history and publish immutable snapshots."""

    def __init__(self, max_events: int = 100) -> None:
        if max_events < 1:
            raise ValueError("max_events must be positive")
        self._events: deque[dict[str, Any]] = deque(maxlen=max_events)
        self._lock = Lock()

    def publish(self, event: dict[str, Any]) -> None:
        with self._lock:
            self._events.append(dict(event))

    def snapshot(self) -> tuple[dict[str, Any], ...]:
        with self._lock:
            return tuple(dict(event) for event in self._events)

    def extend(self, events: Iterable[dict[str, Any]]) -> None:
        for event in events:
            self.publish(event)
