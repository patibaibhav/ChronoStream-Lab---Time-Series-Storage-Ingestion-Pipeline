"""ASGI application and minimal REST control surface for the local lab."""

import os
import csv
import io
from pathlib import Path
from threading import Lock, Thread
from asyncio import sleep
from contextlib import suppress

from fastapi import FastAPI
from fastapi import HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from pydantic import BaseModel, Field

from app.core.config import SimulationSettings
from app.api.websocket import SnapshotHub
from app.engine.orchestrator import RunSummary, SimulationEngine
from app.storage.metadata import SQLiteMetadataRepository

app = FastAPI(
    title="Time-Series Virtual Lab",
    version="0.1.0",
    description="Local time-series ingestion and storage virtual lab.",
)


class RunStartRequest(BaseModel):
    run_id: str = Field(min_length=1, max_length=100)
    record_count: int = Field(default=100, ge=0, le=100_000)
    settings: SimulationSettings = Field(default_factory=SimulationSettings)


class LabController:
    def __init__(self, data_dir: Path) -> None:
        self.data_dir = data_dir
        self.metadata = SQLiteMetadataRepository(data_dir / "lab.sqlite")
        self._lock = Lock()
        self._engine: SimulationEngine | None = None
        self._thread: Thread | None = None
        self._summary: RunSummary | None = None
        self._active_run_id: str | None = None
        self._stopping = False
        self._starting = False

    @property
    def running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    @property
    def stopping(self) -> bool:
        return self._stopping

    @property
    def starting(self) -> bool:
        return self._starting

    def start(self, request: RunStartRequest) -> None:
        with self._lock:
            if self.running:
                raise HTTPException(status_code=409, detail="a simulation is already running")
            self._engine = SimulationEngine(
                request.settings,
                self.data_dir,
                metadata_repository=self.metadata,
            )
            self._summary = None
            self._active_run_id = request.run_id
            self._stopping = False
            self._starting = True
            self._thread = Thread(
                target=self._run,
                args=(request.run_id, request.record_count),
                daemon=True,
            )
            self._thread.start()
            # Clear starting flag immediately — the thread is now running
            self._starting = False

    @property
    def metrics(self) -> object:
        return self._engine.metrics if self._engine is not None else None

    def stop(self) -> None:
        with self._lock:
            if self._engine is None or not self.running:
                raise HTTPException(status_code=409, detail="no simulation is running")
            if self._stopping:
                return  # idempotent: already stopping
            self._stopping = True
            self._engine.stop()

    def _run(self, run_id: str, record_count: int) -> None:
        if self._engine is None:
            return
        try:
            self._summary = self._engine.run(run_id, record_count)
            self._active_run_id = run_id
            snapshots.publish({"type": "run_completed", "run_id": run_id, "summary": self._summary})
        finally:
            with self._lock:
                self._stopping = False
                self._starting = False


controller = LabController(Path(os.getenv("DATA_DIR", "data")))
snapshots = SnapshotHub()


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/api/config/validate")
def validate_config(settings: SimulationSettings) -> dict[str, object]:
    return {"valid": True, "settings": settings.model_dump(mode="json")}


@app.post("/api/runs/start", status_code=202)
def start_run(request: RunStartRequest) -> dict[str, object]:
    controller.start(request)
    return {"run_id": request.run_id, "status": "running"}


@app.post("/api/runs/stop")
def stop_run() -> dict[str, str]:
    controller.stop()
    return {"status": "stopping"}


@app.get("/api/runs/status")
def run_status() -> dict[str, object]:
    if controller.stopping:
        return {
            "status": "stopping",
            "run_id": controller._active_run_id,
            "metrics": controller.metrics,
        }
    if controller.starting:
        return {
            "status": "starting",
            "run_id": controller._active_run_id,
        }
    if controller.running:
        return {
            "status": "running",
            "run_id": controller._active_run_id,
            "metrics": controller.metrics,
        }
    if controller._summary is not None:
        return {
            "status": "completed",
            "run_id": controller._summary.run_id,
            "summary": controller._summary,
        }
    return {"status": "idle"}


@app.get("/api/runs/history")
def run_history() -> list[dict[str, object]]:
    return controller.metadata.list_runs()


@app.get("/api/chunks")
def chunks(run_id: str | None = None) -> list[dict[str, object]]:
    return controller.metadata.list_chunks(run_id)


@app.get("/api/runs/{run_id}/files")
def run_files(run_id: str) -> list[dict[str, object]]:
    return controller.metadata.list_files(run_id)


@app.get("/api/metrics/aggregate")
def aggregate_metrics() -> dict[str, object]:
    return controller.metadata.aggregate_metrics()


@app.get("/api/metrics/samples")
def metric_samples(run_id: str | None = None) -> list[dict[str, object]]:
    """Return time-series samples of key metrics for charting."""
    if run_id is None:
        run_id = controller._active_run_id
    if run_id is None:
        return []
    samples: list[dict[str, object]] = []
    for event in controller.metadata.list_events(run_id):
        payload = event.get("payload") or {}
        samples.append({
            "timestamp": event.get("event_id", "")[:8] if event.get("event_id") else "",
            "run_id": run_id,
            "status": event.get("status"),
            "metric": payload.get("metric", ""),
            "value": payload.get("value", 0),
        })
    return samples[-200:]  # bound response size


@app.get("/api/events")
def events(run_id: str | None = None) -> list[dict[str, object]]:
    return controller.metadata.list_events(run_id)


@app.get("/api/events/{event_id}")
def event_detail(event_id: str) -> dict[str, object]:
    event = controller.metadata.get_event(event_id)
    if event is None:
        raise HTTPException(status_code=404, detail="event not found")
    return event


@app.get("/api/runs/compare")
def compare_runs(run_id: str, baseline_run_id: str) -> dict[str, object]:
    runs = {str(run["run_id"]): run for run in controller.metadata.list_runs()}
    if run_id not in runs:
        raise HTTPException(status_code=404, detail="run not found")
    if baseline_run_id not in runs:
        raise HTTPException(status_code=404, detail="baseline run not found")
    current = controller.metadata.run_summary(run_id)
    baseline = controller.metadata.run_summary(baseline_run_id)
    metrics = {
        key: {
            "run_a": current[key],
            "run_b": baseline[key],
            "delta": current[key] - baseline[key],
        }
        for key in (
            "input_records",
            "accepted_records",
            "rejected_records",
            "persisted_chunks",
            "persisted_records",
            "persisted_bytes",
            "failed_chunks",
        )
    }
    return {
        "run_id": run_id,
        "baseline_run_id": baseline_run_id,
        "run_a_label": runs[run_id].get("started_at", run_id),
        "run_b_label": runs[baseline_run_id].get("started_at", baseline_run_id),
        "metrics": metrics,
    }


@app.get("/api/runs/export")
def export_runs() -> StreamingResponse:
    output = io.StringIO()
    writer = csv.DictWriter(
        output,
        fieldnames=[
            "run_id",
            "started_at",
            "finished_at",
            "status",
            "input_records",
            "accepted_records",
            "rejected_records",
            "persisted_chunks",
            "persisted_records",
        ],
        extrasaction="ignore",
    )
    writer.writeheader()
    for run in controller.metadata.list_runs():
        row = {key: run[key] for key in writer.fieldnames if key in run}
        row.update(controller.metadata.run_summary(str(run["run_id"])))
        writer.writerow(row)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=chronostream-runs.csv"},
    )


@app.get("/api/chunks/{chunk_id}/download")
def download_chunk(chunk_id: str) -> FileResponse:
    chunk = next(
        (item for item in controller.metadata.list_chunks() if item["chunk_id"] == chunk_id),
        None,
    )
    if chunk is None or chunk["path"] is None:
        raise HTTPException(status_code=404, detail="chunk file not found")
    path = Path(str(chunk["path"])).resolve()
    data_root = controller.data_dir.resolve()
    if data_root not in path.parents or not path.is_file():
        raise HTTPException(status_code=404, detail="chunk file not found")
    return FileResponse(path, media_type="application/octet-stream", filename=path.name)


@app.get("/api/presets")
def presets() -> dict[str, SimulationSettings]:
    return {"default": SimulationSettings()}


_active_ws_lock = Lock()
_active_ws_connections: set[WebSocket] = set()


def _build_snapshot(run_id: str | None, status: str, metrics: object, sequence: int) -> dict[str, object]:
    """Build a structured snapshot with run_id and sequence for stale-state prevention."""
    snapshot: dict[str, object] = {
        "type": "metrics",
        "run_id": run_id,
        "status": status,
        "sequence": sequence,
        "metrics": metrics,
    }
    return snapshot


@app.websocket("/api/ws")
async def websocket_snapshots(websocket: WebSocket) -> None:
    await websocket.accept()
    with _active_ws_lock:
        _active_ws_connections.add(websocket)
    sequence = 0
    try:
        while True:
            sequence += 1
            if controller.running:
                snapshot = _build_snapshot(
                    controller._active_run_id,
                    "running",
                    controller.metrics,
                    sequence,
                )
            elif controller._summary is not None:
                snapshot = _build_snapshot(
                    controller._summary.run_id,
                    "completed",
                    controller._summary.metrics,
                    sequence,
                )
            else:
                snapshot = _build_snapshot(None, "idle", None, sequence)
            await websocket.send_json(snapshot)
            await sleep(1)
    except (WebSocketDisconnect, RuntimeError):
        pass
    finally:
        with _active_ws_lock:
            _active_ws_connections.discard(websocket)
        with suppress(Exception):
            await websocket.close()


from contextlib import asynccontextmanager


@asynccontextmanager
async def lifespan(_: FastAPI):
    yield
    with _active_ws_lock:
        for ws in list(_active_ws_connections):
            with suppress(Exception):
                await ws.close()
        _active_ws_connections.clear()


app.router.lifespan_context = lifespan
