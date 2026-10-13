from app.main import app


def test_application_placeholder_has_expected_metadata() -> None:
    assert app.title == "Time-Series Virtual Lab"
    assert app.version == "0.1.0"
