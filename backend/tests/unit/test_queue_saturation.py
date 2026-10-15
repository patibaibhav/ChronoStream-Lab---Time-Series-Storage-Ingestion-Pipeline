"""Deterministic tests for queue saturation, backpressure, and clean shutdown.

Uses the actual BoundedStreamQueue and a controlled consumer to verify:
- Queue becomes nonempty under load
- Queue depth reaches capacity under sustained overload
- Queue depth never exceeds capacity
- Producer is slowed when capacity is exhausted
- Queue drains after production stops
- All records are accounted for (no loss, no duplication)
"""

import tempfile
import threading
import time
from pathlib import Path

from app.core.config import (
    BufferSettings,
    ChunkingSettings,
    FeedSettings,
    ProcessingSettings,
    SimulationSettings,
)
from app.engine.orchestrator import SimulationEngine


def test_queue_saturates_under_overload() -> None:
    """Producer target 1000 rec/s, consumer ~100 rec/s, queue capacity 100.
    Queue must reach capacity and never exceed it."""
    settings = SimulationSettings(
        feed=FeedSettings(records_per_second=1000, invalid_record_ratio=0.0),
        buffer=BufferSettings(max_records=100, max_partitions=100),
        chunking=ChunkingSettings(max_records=100, max_age_seconds=3600),
        processing=ProcessingSettings(validation_delay_ms=10),  # ~100 rec/s consumer
    )

    with tempfile.TemporaryDirectory() as td:
        engine = SimulationEngine(settings, Path(td))
        summaries: list = []
        worker = threading.Thread(
            target=lambda: summaries.append(engine.run("run-q", 1000)),
            daemon=True,
        )
        worker.start()

        # Monitor queue depth while running
        peak_depth = 0
        saw_backpressure = False
        while worker.is_alive():
            metrics = engine.metrics
            if metrics is not None:
                peak_depth = max(peak_depth, metrics.queue_depth)
                if metrics.backpressure_active:
                    saw_backpressure = True
            time.sleep(0.01)

        worker.join(timeout=30)
        summary = summaries[0]

        # Queue must have reached capacity
        assert peak_depth == 100, f"Expected peak depth 100, got {peak_depth}"
        # Backpressure must have activated
        assert saw_backpressure, "Backpressure never activated"
        # All records must be accounted for
        assert summary.generated_records == 1000
        assert summary.accepted_records == 1000
        assert summary.persisted_records == 1000


def test_queue_never_exceeds_capacity() -> None:
    """Verify queue depth never exceeds configured capacity."""
    settings = SimulationSettings(
        feed=FeedSettings(records_per_second=10000, invalid_record_ratio=0.0),
        buffer=BufferSettings(max_records=50, max_partitions=100),
        chunking=ChunkingSettings(max_records=50, max_age_seconds=3600),
        processing=ProcessingSettings(validation_delay_ms=5),
    )

    with tempfile.TemporaryDirectory() as td:
        engine = SimulationEngine(settings, Path(td))
        summaries: list = []
        worker = threading.Thread(
            target=lambda: summaries.append(engine.run("run-qcap", 500)),
            daemon=True,
        )
        worker.start()

        peak_depth = 0
        while worker.is_alive():
            metrics = engine.metrics
            if metrics is not None:
                peak_depth = max(peak_depth, metrics.queue_depth)
                assert metrics.queue_depth <= 50, (
                    f"Queue exceeded capacity: {metrics.queue_depth} > 50"
                )
            time.sleep(0.005)

        worker.join(timeout=30)
        summary = summaries[0]
        assert summary.persisted_records == 500


def test_queue_drains_after_production_stops() -> None:
    """After generation completes, queue must drain to zero."""
    settings = SimulationSettings(
        feed=FeedSettings(records_per_second=100000, invalid_record_ratio=0.0),
        buffer=BufferSettings(max_records=500, max_partitions=100),
        chunking=ChunkingSettings(max_records=100, max_age_seconds=3600),
    )

    with tempfile.TemporaryDirectory() as td:
        engine = SimulationEngine(settings, Path(td))
        summary = engine.run("run-drain", 200)

        # After completion, queue must be empty
        final_metrics = engine.metrics
        assert final_metrics is not None
        assert final_metrics.queue_depth == 0
        assert final_metrics.buffer_records == 0
        assert summary.persisted_records == 200


def test_stop_while_queue_full() -> None:
    """Stop must unblock a producer waiting for queue space."""
    settings = SimulationSettings(
        feed=FeedSettings(records_per_second=10000, invalid_record_ratio=0.0),
        buffer=BufferSettings(max_records=100, max_partitions=100),
        chunking=ChunkingSettings(max_records=100, max_age_seconds=3600),
        processing=ProcessingSettings(validation_delay_ms=50),  # slow consumer
    )

    with tempfile.TemporaryDirectory() as td:
        engine = SimulationEngine(settings, Path(td))
        summaries: list = []
        worker = threading.Thread(
            target=lambda: summaries.append(engine.run("run-stop-q", 10000)),
            daemon=True,
        )
        worker.start()

        # Wait for queue to fill
        time.sleep(0.5)
        metrics = engine.metrics
        assert metrics is not None
        assert metrics.queue_depth > 0

        # Stop must work even while producer is blocked
        engine.stop()
        worker.join(timeout=10)

        assert not worker.is_alive(), "Thread did not terminate after stop"
        summary = summaries[0]
        assert summary.stopped is True
        assert summary.generated_records < 10000
        # All generated records must be accounted for
        assert summary.generated_records == (
            summary.accepted_records + summary.rejected_records
        )
