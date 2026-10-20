"""Corporate Argentina–U.S. pages: public access and truthful product claims."""
from __future__ import annotations

import asyncio
from pathlib import Path

from fastapi.responses import HTMLResponse
from litoral_trace.web import router as router_module
from litoral_trace.web.templates import templates

ROOT = Path(__file__).resolve().parents[1] / "src/litoral_trace/templates"


def test_corporate_templates_compile_and_expose_correct_jurisdiction():
    for name in (
        "public/international_base.html",
        "public/international_home.html",
        "public/international_company.html",
        "public/international_trust.html",
    ):
        templates.env.get_template(name)
    base = (ROOT / "public/international_base.html").read_text(encoding="utf-8")
    main_base = (ROOT / "base.html").read_text(encoding="utf-8")
    home = (ROOT / "public/international_home.html").read_text(encoding="utf-8")
    company = (ROOT / "public/international_company.html").read_text(encoding="utf-8")
    trust = (ROOT / "public/international_trust.html").read_text(encoding="utf-8")
    assert "{% block document_lang %}en{% endblock %}" in base
    assert "{% block document_lang %}es{% endblock %}" in main_base
    assert "Argentina-founded" in home and "Argentina" in company
    assert "https://lacey.litoraltrace.com/demo" in home
    assert "https://lacey.litoraltrace.com/sandbox/start" in home
    assert 'href="/company"' in base and 'href="/trust"' in base
    assert "does not submit declarations" in home
    assert "does not claim SOC 2" in trust
    assert "comercial@litoraltrace.com" in base
    assert "support@litoraltrace.com" in company


def test_corporate_routes_do_not_require_auth(monkeypatch):
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
        ("public/international_home.html", None),
        ("public/international_company.html", None),
        ("public/international_trust.html", None),
    ]


def test_corporate_real_jinja_render_and_no_false_claims():
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
        response = client.get(path, follow_redirects=False)
        assert response.status_code == 200, (path, response.status_code)
        assert phrase in response.text
        assert '<html lang="en"' in response.text
        assert 'href="/company"' in response.text
        assert 'href="/trust"' in response.text
    for filename in ("international_home.html", "international_company.html", "international_trust.html"):
        content = (ROOT / "public" / filename).read_text(encoding="utf8").lower()
        for phrase in ("soc 2 certified", "iso 27001 certified", "trusted by 100", "fully compliant", "u.s.-registered"):
            assert phrase not in content
