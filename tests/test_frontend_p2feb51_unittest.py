from __future__ import annotations

import asyncio
from pathlib import Path

from fastapi.responses import HTMLResponse

from litoral_trace.web import router as router_module
from litoral_trace.web.router import router as web_router
from litoral_trace.web.runtime import redirect_to_login


ROOT = Path(__file__).resolve().parents[1]

TEMPLATES = (
    ROOT
    / "src"
    / "litoral_trace"
    / "templates"
)


def _route_methods(path: str) -> set[str]:
    methods: set[str] = set()

    for route in web_router.routes:
        if route.path == path:
            methods.update(
                route.methods or set()
            )

    return methods


def test_public_root_and_login_are_distinct_routes():
    assert "GET" in _route_methods("/")
    assert "GET" in _route_methods("/login")
    assert "POST" in _route_methods("/login")


def test_root_renders_public_home_template(monkeypatch):
    captured: dict[str, object] = {}

    def fake_render(
        request,
        name,
        *,
        user,
        context=None,
        status_code=200,
    ):
        captured["name"] = name
        captured["user"] = user

        return HTMLResponse(
            "ok",
            status_code=status_code,
        )

    monkeypatch.setattr(
        router_module,
        "render_web_template",
        fake_render,
    )

    response = asyncio.run(
        router_module.render_home_view(
            object()
        )
    )

    assert response.status_code == 200
    assert captured == {
        "name": "public/home.html",
        "user": None,
    }


def test_login_get_remains_dedicated_login_template(
    monkeypatch,
):
    captured: dict[str, object] = {}

    def fake_render(
        request,
        name,
        *,
        user,
        context=None,
        status_code=200,
    ):
        captured["name"] = name
        captured["user"] = user

        return HTMLResponse(
            "ok",
            status_code=status_code,
        )

    monkeypatch.setattr(
        router_module,
        "render_web_template",
        fake_render,
    )

    response = asyncio.run(
        router_module.render_login_view(
            object()
        )
    )

    assert response.status_code == 200
    assert captured == {
        "name": "login.html",
        "user": None,
    }


def test_authenticated_route_failures_redirect_to_login():
    response = redirect_to_login(
        clear_cookies=False
    )

    assert response.status_code == 303
    assert response.headers["location"] == "/login"


def test_public_shell_has_only_live_public_transitions():
    shell = (
        TEMPLATES
        / "public"
        / "base_public.html"
    ).read_text(
        encoding="utf-8"
    )

    assert 'href="/"' in shell
    assert 'href="/login"' in shell

    assert 'href="/#platform"' in shell
    assert 'href="/#eudr"' in shell
    assert (
        'href="/#regional-intelligence"'
        in shell
    )
    assert 'href="/#security"' in shell

    assert 'href="/platform"' not in shell
    assert 'href="/solutions/eudr"' not in shell

    assert "user.organization_name" not in shell
    assert 'action="/logout"' not in shell

    # Protect structural shell ownership without pinning an obsolete
    # Tailwind class string that blocks visual refinement.
    assert '<div class="min-h-full' in shell
    assert "bg-white" in shell
    assert "flexflex-col" not in shell


def test_homepage_anchors_match_public_navigation():
    home = (
        TEMPLATES
        / "public"
        / "home.html"
    ).read_text(
        encoding="utf-8"
    )

    for section_id in (
        "platform",
        "eudr",
        "regional-intelligence",
        "security",
    ):
        assert (
            f'id="{section_id}"'
            in home
        )

    assert (
        '{% extends "public/base_public.html" %}'
        in home
    )


def test_public_home_avoids_unsubstantiated_metrics():
    home = (
        TEMPLATES
        / "public"
        / "home.html"
    ).read_text(
        encoding="utf-8"
    )

    forbidden = (
        "500 companies",
        "99.9%",
        "10M",
        "EU approved",
        "ISO certified",
    )

    for claim in forbidden:
        assert claim not in home


def test_public_home_has_encoding_safe_separators():
    home = (
        TEMPLATES
        / "public"
        / "home.html"
    ).read_text(
        encoding="utf-8"
    )

    assert "·" in home
    assert "→" in home

    assert (
        "EUDR ? Argentina ? South America"
        not in home
    )

    assert (
        "Origin ? Risk ? Evidence"
        not in home
    )
