"""Keep the official U.S. Lacey landing as the sole public marketing homepage."""
from __future__ import annotations

from fastapi.testclient import TestClient
from litoral_trace.web.us_lacey_unified_app import app

client = TestClient(app)


def test_duplicate_corporate_preview_removed():
    response = client.get("/corporate", follow_redirects=False)
    assert response.status_code == 301
    assert response.headers["location"] == "/"


def test_canonical_lacey_landing_unchanged_for_all_hosts():
    for hostname in ("lacey.litoraltrace.com", "litoraltrace.com", "www.litoraltrace.com", "testserver"):
        response = client.get("/", headers={"host": hostname}, follow_redirects=False)
        assert response.status_code == 200
        assert "U.S. Lacey Act Compliance Infrastructure" in response.text
        assert "SYNTHETIC SAMPLE" in response.text
        assert "Import compliance starts with reliable evidence." not in response.text


def test_official_footer_has_only_honest_country_reference():
    response = client.get("/")
    assert response.status_code == 200
    assert "Based in Argentina" in response.text
    assert "U.S. Lacey compliance infrastructure" in response.text
    assert "/demo" in response.text
    assert "/sandbox/start" in response.text
    assert "comercial@litoraltrace.com" in response.text


def test_public_company_and_security_still_open_without_login():
    for path, expected in (
        ("/company", "Independent by design."),
        ("/security", "Evidence belongs to the right customer."),
    ):
        response = client.get(path, follow_redirects=False)
        assert response.status_code == 200
        assert expected in response.text
