from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from fastapi.testclient import TestClient

from litoral_trace.us_lacey.portal_auth import US_LACEY_SESSION_COOKIE
import litoral_trace.web.us_lacey_pilot_app as pilot_app
from litoral_trace.web.us_lacey_unified_app import app


client = TestClient(app)


def test_anonymous_root_is_professional_us_lacey_landing():
    response = client.get("/")
    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store, max-age=0"

    html = response.text
    assert '<html lang="en-US"' in html
    assert "U.S. Lacey Act Compliance Infrastructure" in html
    assert "From supplier evidence and product composition to declaration-ready Lacey data." in html
    assert 'href="/demo"' in html
    assert 'href="/login"' in html
    assert 'href="/sandbox/start"' in html
    assert "Run a sample shipment" in html
    assert "Analyze my documents" in html
    assert "Professional Plan — USD 149/month" not in html
    assert "Includes up to 100 Lacey operations/month." not in html
    assert "USD 99/month" not in html
    assert "USD 199" not in html
    assert "25 operations" not in html
    assert "does not file declarations" in html
    assert "Private Beta" not in html
    assert "Early Access" not in html
    assert "Phase VII" not in html

    prohibited_regional_copy = (
        "Trazabilidad de origen",
        "Debida diligencia",
        "Contexto regional",
        "Auditabilidad",
        "Acceso de clientes",
        "Solicitar demostración",
        "Argentina · Cadenas forestales · Comercio exterior",
    )
    for text in prohibited_regional_copy:
        assert text not in html


def test_signed_in_root_keeps_original_portal_redirect(monkeypatch):
    monkeypatch.setattr(
        pilot_app,
        "resolve_us_lacey_session",
        lambda token: SimpleNamespace(account_status="PILOT"),
    )
    client.cookies.set(US_LACEY_SESSION_COOKIE, "opaque-test-session")
    try:
        response = client.get("/", follow_redirects=False)
    finally:
        client.cookies.delete(US_LACEY_SESSION_COOKIE)
    assert response.status_code == 303
    assert response.headers["location"] == "/operations"


def test_canonical_and_legacy_marketing_routes_coexist():
    for path in ("/demo", "/lacey/demo"):
        response = client.get(path)
        assert response.status_code == 200
        assert "Illustrative sample" in response.text
        assert 'href="/signup"' in response.text

    legacy_landing = client.get("/lacey")
    assert legacy_landing.status_code == 200
    assert "Run a sample shipment" in legacy_landing.text
    assert "Analyze my documents" in legacy_landing.text

    for path in ("/event", "/lacey/event"):
        response = client.post(path, data={"event": "lacey_visit"})
        assert response.status_code == 204


def test_free_render_blueprint_runs_unified_entrypoint():
    blueprint = Path("deploy/render-us-lacey-pilot-free.yaml").read_text(encoding="utf-8")
    assert "litoral_trace.web.us_lacey_unified_app:app" in blueprint
    assert "litoral_trace.web.us_lacey_free_app:app" not in blueprint
