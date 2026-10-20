"""Public Argentina-founded / U.S. market site does not imply customer traction."""
from __future__ import annotations
import asyncio
from pathlib import Path

from fastapi.responses import HTMLResponse
from litoral_trace.web import router as router_module
from litoral_trace.web.templates import templates

ROOT = Path(__file__).resolve().parents[1] / "src/litoral_trace/templates"


def test_international_templates_compile_and_have_clear_paths():
    for name in (
        "public/international_base.html",
        "public/international_home.html",
        "public/international_company.html",
        "public/international_trust.html",
        "public/lacey.html",
        "us_lacey/marketing_base.html",
    ):
        assert templates.env.get_template(name)
    base = (ROOT / "public/international_base.html").read_text(encoding="utf8")
    home = (ROOT / "public/international_home.html").read_text(encoding="utf8")
    company = (ROOT / "public/international_company.html").read_text(encoding="utf8")
    trust = (ROOT / "public/international_trust.html").read_text(encoding="utf8")
    lacey = (ROOT / "public/lacey.html").read_text(encoding="utf8")
    lbase = (ROOT / "us_lacey/marketing_base.html").read_text(encoding="utf8")
    assert '<html lang="en"' in base
    assert "Argentina-founded" in home and "Argentina" in company
    assert "/company" in base and "/security" in base and "/trust" in home
    assert "https://lacey.litoraltrace.com/demo" in home
    assert "https://lacey.litoraltrace.com/sandbox/start" in home
    assert "comercial@litoraltrace.com" in base and "support@litoraltrace.com" in company
    assert "does not submit declarations" in home
    assert "does not claim SOC 2" in trust
    assert "SYNTHETIC SAMPLE" in lacey
    assert "LIVE WORKFLOW" not in lacey
    assert "not real customer activity" in lacey
    assert 'href="/company"' in lbase
    assert 'href="/security"' in lbase


def test_company_and_trust_routes_render_public_no_auth(monkeypatch):
    seen = []
    def fake_render(request, name, *, user, context=None, status_code=200):
        seen.append((name, user))
        return HTMLResponse("ok", status_code=200)
    monkeypatch.setattr(router_module, "render_web_template", fake_render)
    assert asyncio.run(router_module.render_home_view(object())).status_code == 200
    assert asyncio.run(router_module.render_company_view(object())).status_code == 200
    assert asyncio.run(router_module.render_corporate_trust_view(object())).status_code == 200
    assert seen == [
        ("public/international_home.html", None),
        ("public/international_company.html", None),
        ("public/international_trust.html", None),
    ]


def test_sample_makes_no_false_security_or_revenue_claims():
    home = (ROOT / "public/international_home.html").read_text(encoding="utf8").lower()
    trust = (ROOT / "public/international_trust.html").read_text(encoding="utf8").lower()
    for phrase in ("soc 2 certified", "iso 27001 certified", "trusted by 100", "filings automatically submitted", "us-registered", "fully compliant"):
        assert phrase not in home
        assert phrase not in trust
    assert "illustrative sample" in home
    assert "not real customer activity" in (ROOT / "public/lacey.html").read_text(encoding="utf8")


def test_public_routes_render_full_html_with_static_urls():
    """Exercise the real Jinja renderer, not only route mocks."""
    from fastapi import FastAPI
    from fastapi.staticfiles import StaticFiles
    from fastapi.testclient import TestClient
    from litoral_trace.web.templates import STATIC_DIR
    app = FastAPI()
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
    app.include_router(router_module.router)
    client = TestClient(app)
    for path, phrase in (
        ("/", "Import compliance starts with reliable evidence."),
        ("/company", "Independent by design."),
        ("/trust", "Evidence belongs to the right customer."),
    ):
        response = client.get(path)
        assert response.status_code == 200, response.text[:300]
        assert phrase in response.text
        assert '<html lang="en"' in response.text
        assert 'href="https://lacey.litoraltrace.com/' in response.text
        assert 'href="/company"' in response.text
        assert 'href="/security"' in response.text


def test_public_company_and_security_on_lacey_domain():
    """The live marketing navigation must not land on 404 or require a login."""
    from fastapi.testclient import TestClient
    from litoral_trace.web.lacey_experiment_app import app
    client = TestClient(app)
    for path, expected in (
        ("/company", "Independent by design."),
        ("/security", "Evidence belongs to the right customer."),
    ):
        response = client.get(path, follow_redirects=False)
        assert response.status_code == 200, (path, response.status_code)
        assert expected in response.text
        assert '<html lang="en"' in response.text
        assert 'href="/company"' in response.text
        assert 'href="/security"' in response.text
        assert "https://lacey.litoraltrace.com" in response.text
        assert response.headers["cache-control"] == "no-store, max-age=0"
