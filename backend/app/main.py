"""ASGI application and minimal REST control surface for the local lab."""

import os
from pathlib import Path
from threading import Lock, Thread
from asyncio import sleep
from contextlib import suppress

from fastapi import FastAPI
from fastapi import HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
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

    @property
    def running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

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
            self._thread = Thread(
                target=self._run,
                args=(request.run_id, request.record_count),
                daemon=True,
            )
            self._thread.start()

    @property
    def metrics(self) -> object:
        return self._engine.metrics if self._engine is not None else None

    def stop(self) -> None:
        with self._lock:
            if self._engine is None or not self.running:
                raise HTTPException(status_code=409, detail="no simulation is running")
            self._engine.stop()

    def _run(self, run_id: str, record_count: int) -> None:
        if self._engine is None:
            return
        self._summary = self._engine.run(run_id, record_count)
        snapshots.publish({"type": "run_completed", "run_id": run_id, "summary": self._summary})


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
    if controller.running:
        return {"status": "running", "metrics": controller.metrics}
    if controller._summary is not None:
        return {"status": "completed", "summary": controller._summary}
    return {"status": "idle"}


@app.get("/api/runs/history")
def run_history() -> list[dict[str, object]]:
    return controller.metadata.list_runs()


@app.get("/api/chunks")
def chunks(run_id: str | None = None) -> list[dict[str, object]]:
    return controller.metadata.list_chunks(run_id)


@app.get("/api/events")
def events(run_id: str | None = None) -> list[dict[str, object]]:
    return controller.metadata.list_events(run_id)


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


@app.websocket("/api/ws")
async def websocket_snapshots(websocket: WebSocket) -> None:
    await websocket.accept()
    try:
        while True:
            await websocket.send_json({"events": snapshots.snapshot()})
            await sleep(1)
    except (WebSocketDisconnect, RuntimeError):
        with suppress(Exception):
            await websocket.close()
