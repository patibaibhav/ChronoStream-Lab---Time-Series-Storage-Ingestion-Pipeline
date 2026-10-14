from app.engine.buffer_manager import BufferMetrics
from app.engine.metrics import MetricsCollector
from app.engine.queue import QueueMetrics


def test_metrics_snapshot_reports_counters_rates_and_percentiles() -> None:
    now = iter([10.0, 12.0])
    metrics = MetricsCollector(clock=lambda: next(now))
    metrics.record_input()
    metrics.record_input()
    metrics.record_persisted(2)
    metrics.record_rejected()
    metrics.record_retry()
    metrics.record_chunk(2, 100)
    metrics.record_latency(0.1)
    metrics.record_latency(0.2)
    metrics.record_latency(0.4)
    metrics.observe_queue(QueueMetrics(10, 3, 2, 1, 1, 0.5))
    metrics.observe_buffer(BufferMetrics(20, 4, 1, 0.2, 1.5))

    snapshot = metrics.snapshot()

    assert snapshot.input_records == 2
    assert snapshot.persisted_records == 2
    assert snapshot.rejected_records == 1
    assert snapshot.retries == 1
    assert snapshot.chunk_bytes == 100
    assert snapshot.queue_depth == 3
    assert snapshot.buffer_records == 4
    assert snapshot.input_rate_records_per_second == 1
    assert snapshot.p50_latency_seconds == 0.2
    assert snapshot.p95_latency_seconds == 0.38
