from fastapi.testclient import TestClient

from app.main import app


client = TestClient(app)


def test_health_and_config_validation() -> None:
    assert client.get("/health").json() == {"status": "ok"}
    response = client.post("/api/config/validate", json={})

    assert response.status_code == 200
    assert response.json()["valid"] is True


def test_start_status_and_history_endpoints() -> None:
    response = client.post(
        "/api/runs/start",
        json={"run_id": "api-test", "record_count": 2},
    )

    assert response.status_code == 202
    status = client.get("/api/runs/status")
    assert status.status_code == 200
    assert status.json()["status"] in {"running", "completed"}
    assert any(run["run_id"] == "api-test" for run in client.get("/api/runs/history").json())
