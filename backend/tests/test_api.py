from fastapi.testclient import TestClient
from uuid import uuid4

from app.main import app


client = TestClient(app)


def test_health_and_config_validation() -> None:
    assert client.get("/health").json() == {"status": "ok"}
    response = client.post("/api/config/validate", json={})

    assert response.status_code == 200
    assert response.json()["valid"] is True


def test_start_status_and_history_endpoints() -> None:
    run_id = f"api-test-{uuid4()}"
    response = client.post(
        "/api/runs/start",
        json={"run_id": run_id, "record_count": 2},
    )

    assert response.status_code == 202
    status = client.get("/api/runs/status")
    assert status.status_code == 200
    assert status.json()["status"] in {"running", "completed"}
    assert any(run["run_id"] == run_id for run in client.get("/api/runs/history").json())


def test_run_export_and_missing_comparison_are_explicit() -> None:
    export = client.get("/api/runs/export")
    assert export.status_code == 200
    assert "text/csv" in export.headers["content-type"]
    assert "run_id" in export.text

    comparison = client.get(
        "/api/runs/compare",
        params={"run_id": "missing", "baseline_run_id": "missing-baseline"},
    )
    assert comparison.status_code == 404
