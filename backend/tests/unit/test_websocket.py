import pytest

from app.api.websocket import SnapshotHub


def test_snapshot_hub_bounds_history_and_copies_events() -> None:
    hub = SnapshotHub(max_events=2)
    event = {"type": "chunk", "chunk_id": "one"}
    hub.publish(event)
    event["chunk_id"] = "changed"
    hub.publish({"type": "metric"})
    hub.publish({"type": "done"})

    assert hub.snapshot() == (
        {"type": "metric"},
        {"type": "done"},
    )


def test_snapshot_hub_rejects_invalid_capacity() -> None:
    with pytest.raises(ValueError, match="positive"):
        SnapshotHub(0)
