"""Northflank-only corporate/domain preparation must preserve Lacey and sessions."""
from __future__ import annotations

from fastapi.testclient import TestClient
from litoral_trace.web.us_lacey_unified_app import app

client = TestClient(app)


def test_northflank_corporate_preview_is_public():
    response = client.get("/corporate", follow_redirects=False)
    assert response.status_code == 200
    assert "Import compliance starts with reliable evidence." in response.text
    assert 'href="/company"' in response.text
    assert 'href="/security"' in response.text
    assert 'rel="canonical" href="https://lacey.litoraltrace.com/corporate"' in response.text
    assert response.headers["cache-control"] == "no-store, max-age=0"


def test_apex_host_will_show_corporate_without_touching_lacey_host():
    for hostname in ("litoraltrace.com", "www.litoraltrace.com"):
        response = client.get("/", headers={"host": hostname})
        assert response.status_code == 200
        assert "Import compliance starts with reliable evidence." in response.text
        assert 'rel="canonical" href="https://litoraltrace.com/"' in response.text
        assert 'href="https://lacey.litoraltrace.com/demo"' in response.text
    for hostname in ("lacey.litoraltrace.com", "testserver", "unknown.example"):
        response = client.get("/", headers={"host": hostname})
        assert response.status_code == 200
        assert "U.S. Lacey Act Compliance Infrastructure" in response.text
        assert "SYNTHETIC SAMPLE" in response.text


def test_public_company_and_security_remain_accessible_without_login():
    for path, phrase in (
        ("/company", "Independent by design."),
        ("/security", "Evidence belongs to the right customer."),
    ):
        response = client.get(path, follow_redirects=False)
        assert response.status_code == 200
        assert phrase in response.text
