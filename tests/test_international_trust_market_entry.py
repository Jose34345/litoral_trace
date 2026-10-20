"""International trust pages complement but never replace the Lacey homepage."""
from __future__ import annotations
import asyncio
from pathlib import Path
from fastapi.responses import HTMLResponse
from fastapi.testclient import TestClient
from litoral_trace.web import router as router_module
from litoral_trace.web.templates import templates
from litoral_trace.web.lacey_experiment_app import app as marketing_app

ROOT = Path(__file__).resolve().parents[1] / "src/litoral_trace/templates"


def test_international_company_and_security_templates_compile():
    for name in (
        "public/international_base.html",
        "public/international_company.html",
        "public/international_trust.html",
        "public/lacey.html",
        "us_lacey/marketing_base.html",
    ):
        assert templates.env.get_template(name)
    base = (ROOT / "public/international_base.html").read_text(encoding="utf8")
    company = (ROOT / "public/international_company.html").read_text(encoding="utf8")
    trust = (ROOT / "public/international_trust.html").read_text(encoding="utf8")
    lacey = (ROOT / "public/lacey.html").read_text(encoding="utf8")
    marketing = (ROOT / "us_lacey/marketing_base.html").read_text(encoding="utf8")
    assert '<html lang="en"' in base
    assert "Argentina" in company
    assert "does not claim SOC 2" in trust
    assert "SYNTHETIC SAMPLE" in lacey and "LIVE WORKFLOW" not in lacey
    assert "not real customer activity" in lacey
    assert 'href="/company"' in marketing and 'href="/security"' in marketing
    assert "Based in Argentina" in marketing
    assert not (ROOT / "public/international_home.html").exists()


def test_legacy_corporate_root_is_not_overridden(monkeypatch):
    seen = []
    def fake_render(request, name, *, user, context=None, status_code=200):
        seen.append((name, user))
        return HTMLResponse("ok")
    monkeypatch.setattr(router_module, "render_web_template", fake_render)
    for method in (
        router_module.render_home_view,
        router_module.render_company_view,
        router_module.render_corporate_trust_view,
    ):
        assert asyncio.run(method(object())).status_code == 200
    assert seen == [
        ("public/home.html", None),
        ("public/international_company.html", None),
        ("public/international_trust.html", None),
    ]


def test_company_and_security_marketing_routes_are_public():
    client = TestClient(marketing_app)
    for path, expected in (
        ("/company", "Independent by design."),
        ("/security", "Evidence belongs to the right customer."),
    ):
        response = client.get(path, follow_redirects=False)
        assert response.status_code == 200
        assert expected in response.text
        assert '<html lang="en"' in response.text
        assert 'href="/company"' in response.text
        assert 'href="/security"' in response.text
        assert response.headers["cache-control"] == "no-store, max-age=0"
