"""Regression coverage for external U.S. Lacey uptime probes."""
from fastapi.testclient import TestClient

from litoral_trace.web.us_lacey_unified_app import app


def test_us_lacey_health_accepts_get_and_head() -> None:
    client = TestClient(app)

    get_response = client.get("/health")
    assert get_response.status_code == 200
    assert get_response.json() == {"status": "healthy", "service": "us-lacey-pilot"}

    head_response = client.head("/health")
    assert head_response.status_code == 200
    assert head_response.content == b""
    assert head_response.headers["cache-control"] == "no-store, max-age=0"
