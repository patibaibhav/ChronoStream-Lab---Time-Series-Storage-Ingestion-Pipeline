from datetime import UTC, datetime

from app.core.config import BufferSettings
from app.engine.buffer_manager import PartitionedBufferManager
from app.engine.validator import ValidatedRecord


class Clock:
    def __init__(self, value: float = 2_000_000_000.0) -> None:
        self.value = value
    def __call__(self) -> float:
        return self.value
    def advance(self, seconds: float) -> None:
        self.value += seconds


def record(device: str = "server_01", metric: str = "cpu_usage") -> ValidatedRecord:
    timestamp = datetime.fromtimestamp(2_000_000_000.0, UTC)
    return ValidatedRecord("id", device, metric, 1.0, timestamp, timestamp)


def test_partition_and_global_limits_are_explicit() -> None:
    manager = PartitionedBufferManager(BufferSettings(max_records=2, max_partitions=1), clock=Clock())
    assert manager.append(record()).accepted
    assert manager.append(record()).accepted
    assert manager.append(record()).reason == "global_capacity"
    assert manager.metrics().record_count == 2


def test_partition_limit_rejects_new_partition_without_dropping_existing() -> None:
    manager = PartitionedBufferManager(BufferSettings(max_records=5, max_partitions=1), clock=Clock())
    assert manager.append(record()).accepted
    result = manager.append(record(metric="memory_usage"))
    assert result.accepted is False and result.reason == "partition_limit"
    assert manager.drain(("server_01", "cpu_usage")) == (record(),)


def test_empty_idle_partitions_are_cleaned_but_records_are_retained() -> None:
    clock = Clock()
    manager = PartitionedBufferManager(BufferSettings(idle_partition_seconds=5), clock=clock)
    partition = manager.append(record()).partition_id
    clock.advance(6)
    assert manager.cleanup_idle_partitions() == ()
    manager.drain(partition)
    clock.advance(5)
    assert manager.cleanup_idle_partitions() == (partition,)


def test_utilization_and_oldest_age_are_observable() -> None:
    clock = Clock()
    manager = PartitionedBufferManager(BufferSettings(max_records=4), clock=clock)
    manager.append(record())
    clock.advance(2)
    metrics = manager.metrics()
    assert metrics.utilization == 0.25
    assert metrics.oldest_record_age_seconds == 2.0
