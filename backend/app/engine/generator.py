"""Seeded generation of canonical time-series records."""

from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from random import Random
from uuid import NAMESPACE_URL, uuid5

from app.core.config import FeedSettings, TrafficPattern


RECORD_SCHEMA_VERSION = "1.0"
DEFAULT_SIMULATION_START = datetime(2026, 1, 1, tzinfo=UTC)
SIMULATED_INGEST_DELAY = timedelta(milliseconds=25)

_METRIC_RANGES: tuple[tuple[str, float, float], ...] = (
    ("cpu_usage", 0.0, 100.0),
    ("memory_usage", 0.0, 100.0),
    ("disk_io", 0.0, 500.0),
    ("request_rate", 0.0, 1_000.0),
)


def _format_utc(value: datetime) -> str:
    """Serialize a timezone-aware value as an ISO 8601 UTC timestamp."""

    return value.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


@dataclass(frozen=True, slots=True)
class TimeSeriesRecord:
    """Versioned canonical record produced by the simulated source."""

    event_id: str
    device_id: str
    metric: str
    value: float
    timestamp: datetime
    ingested_at: datetime
    schema_version: str = RECORD_SCHEMA_VERSION

    def to_dict(self) -> dict[str, str | float]:
        """Return the stable API/storage representation of this record."""

        return {
            "schema_version": self.schema_version,
            "event_id": self.event_id,
            "device_id": self.device_id,
            "metric": self.metric,
            "value": self.value,
            "timestamp": _format_utc(self.timestamp),
            "ingested_at": _format_utc(self.ingested_at),
        }


class SeededRecordGenerator:
    """Generate a repeatable finite record sequence from feed settings.

    Timestamps represent simulation time. The generator does not sleep or measure
    wall-clock time, so callers can exercise high configured feed rates deterministically.
    """

    def __init__(
        self,
        settings: FeedSettings,
        *,
        start_at: datetime = DEFAULT_SIMULATION_START,
    ) -> None:
        if start_at.tzinfo is None or start_at.utcoffset() is None:
            raise ValueError("start_at must be timezone-aware")
        self._settings = settings
        self._start_at = start_at.astimezone(UTC)

    def generate(self, count: int) -> Iterator[TimeSeriesRecord | dict[str, object]]:
        """Yield ``count`` records, restarting deterministically on every call."""

        if isinstance(count, bool) or not isinstance(count, int):
            raise TypeError("count must be an integer")
        if count < 0:
            raise ValueError("count must be non-negative")

        random = Random(self._settings.seed)
        timestamp = self._start_at
        for sequence_number in range(count):
            device_id = f"server_{random.randint(1, 8):02d}"
            metric, lower_bound, upper_bound = random.choice(_METRIC_RANGES)
            value = round(random.uniform(lower_bound, upper_bound), 3)
            event_id = str(
                uuid5(
                    NAMESPACE_URL,
                    (
                        f"flowstore:{RECORD_SCHEMA_VERSION}:{self._settings.seed}:"
                        f"{sequence_number}:{timestamp.isoformat()}:{device_id}:{metric}:{value}"
                    ),
                )
            )
            if (
                self._settings.invalid_record_ratio > 0
                and random.random() < self._settings.invalid_record_ratio
            ):
                yield {
                    "event_id": event_id,
                    "device_id": device_id,
                    "metric": metric,
                    "timestamp": _format_utc(timestamp),
                }
            else:
                yield TimeSeriesRecord(
                    event_id=event_id,
                    device_id=device_id,
                    metric=metric,
                    value=value,
                    timestamp=timestamp,
                    ingested_at=timestamp + SIMULATED_INGEST_DELAY,
                )
            timestamp += timedelta(
                seconds=1 / (self._settings.records_per_second * self._rate_multiplier(sequence_number, count))
            )

    def _rate_multiplier(self, sequence_number: int, count: int) -> float:
        """Return a deterministic multiplier whose patterns average near the target rate."""

        pattern = self._settings.pattern
        if pattern is TrafficPattern.STEADY:
            return 1.0
        if pattern is TrafficPattern.BURST:
            return 2.0 if sequence_number % 20 < 5 else 2 / 3
        if pattern is TrafficPattern.SPIKE:
            return 6.0 if sequence_number % 30 == 0 else 24 / 29
        if pattern is TrafficPattern.RAMP:
            return 1.0 if count <= 1 else 0.5 + sequence_number / (count - 1)
        raise ValueError(f"unsupported traffic pattern: {pattern}")
