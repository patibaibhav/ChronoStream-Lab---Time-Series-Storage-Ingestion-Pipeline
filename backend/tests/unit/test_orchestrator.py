from pathlib import Path
from threading import Thread
from time import sleep

from app.core.config import ChunkingSettings, SimulationSettings
from app.engine.orchestrator import SimulationEngine
from app.storage.metadata import SQLiteMetadataRepository


def test_engine_runs_all_pipeline_stages_and_persists_chunks(tmp_path: Path) -> None:
    settings = SimulationSettings(
        chunking=ChunkingSettings(max_records=2, max_age_seconds=10),
    )
    summary = SimulationEngine(settings, tmp_path).run("run-1", 5)

    assert summary.run_id == "run-1"
    assert summary.requested_records == 5
    assert summary.generated_records == 5
    assert summary.accepted_records == 5
    assert summary.rejected_records == 0
    assert summary.persisted_chunks == 4
    assert summary.failed_chunks == 0
    assert summary.persisted_records == 5
    assert summary.stopped is False
    assert len(summary.chunk_ids) == 4
    assert len(list(tmp_path.glob("parquet/run_run-1/*.parquet"))) == 4
    metadata = SQLiteMetadataRepository(tmp_path / "lab.sqlite")
    assert metadata.count("events", "run-1") == 5
    assert metadata.count("metric_samples", "run-1") == 5
    assert metadata.count("chunks", "run-1") == 4


def test_engine_can_be_stopped_cleanly(tmp_path: Path) -> None:
    engine = SimulationEngine(SimulationSettings(), tmp_path)
    summaries = []
    worker = Thread(target=lambda: summaries.append(engine.run("run-stopped", 100_000)))
    worker.start()
    while not engine.running:
        sleep(0.001)
    engine.stop()
    worker.join(timeout=5)

    assert not worker.is_alive()
    assert summaries[0].stopped is True
    assert summaries[0].generated_records < 100_000
