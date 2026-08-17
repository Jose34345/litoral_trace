from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from litoral_trace.web.navigation import (
    build_navigation,
)


ROOT = Path(__file__).resolve().parents[1]

TEMPLATES = (
    ROOT
    / "src"
    / "litoral_trace"
    / "templates"
)

APP_JS = (
    ROOT
    / "src"
    / "litoral_trace"
    / "static"
    / "src"
    / "js"
    / "app.js"
)


def _template(path: str) -> str:
    return (
        TEMPLATES
        / path
    ).read_text(
        encoding="utf-8"
    )


def test_app_shell_has_enterprise_layout_contract():
    app = _template(
        "app/base_app.html"
    )

    assert 'id="app-navigation"' in app
    assert "lg:pl-72" in app
    assert 'id="main-content"' in app
    assert 'tabindex="-1"' in app

    assert "fa-solid fa-tree" in app

    assert (
        "Litoral Trace Compliance "
        "Intelligence v2.4"
        not in app
    )


def test_app_navigation_is_server_driven():
    app = _template(
        "app/base_app.html"
    )

    assert "{% for item in section_items %}" in app
    assert 'href="{{ item.href }}"' in app
    assert "{{ item.label }}" in app
    assert 'aria-current="page"' in app


def test_app_shell_has_no_fake_future_navigation():
    app = _template(
        "app/base_app.html"
    )

    forbidden_routes = (
        'href="/origins"',
        'href="/satellite"',
        'href="/imports"',
        'href="/suppliers"',
    )

    for route in forbidden_routes:
        assert route not in app


def test_tenant_identity_is_read_only():
    app = _template(
        "app/base_app.html"
    )

    assert "user.organization_name" in app
    assert "user.username" in app
    assert "user.role|upper" in app

    assert 'contenteditable="true"' not in app
    assert 'name="organization"' not in app
    assert 'name="organization_id"' not in app


def test_logout_is_csrf_protected_and_utf8_clean():
    app = _template(
        "app/base_app.html"
    )

    assert 'action="/logout"' in app
    assert (
        'name="{{ csrf_form_field }}"'
        in app
    )
    assert "{{ csrf_token }}" in app

    assert "Cerrar sesi\u00f3n" in app
    assert "sesi?n" not in app
    assert "\ufffd" not in app


def test_mobile_drawer_markup_and_runtime_are_accessible():
    app = _template(
        "app/base_app.html"
    )

    js = APP_JS.read_text(
        encoding="utf-8"
    )

    assert "data-app-drawer" in app
    assert "data-app-drawer-overlay" in app
    assert "data-app-drawer-open" in app
    assert "data-app-drawer-close" in app

    assert 'aria-controls="app-navigation"' in app
    assert 'aria-expanded="false"' in app

    assert "previouslyFocused" in js
    assert 'event.key === "Escape"' in js
    assert 'event.key !== "Tab"' in js
    assert "getDrawerFocusableElements" in js
    assert (
        'matchMedia(\n      "(min-width: 1024px)",'
        in js
    )


def test_public_shell_does_not_gain_app_chrome():
    public = _template(
        "public/base_public.html"
    )

    assert "data-app-drawer" not in public
    assert "user.organization_name" not in public
    assert 'action="/logout"' not in public


def test_superadmin_navigation_contains_only_live_routes():
    user = SimpleNamespace(
        username="admin",
        organization_id=1,
        session_id=1,
        role="superadmin",
    )

    navigation = build_navigation(
        user,
        current_path="/dashboard",
    )

    assert [
        (item.key, item.href)
        for item in navigation
    ] == [
        ("dashboard", "/dashboard"),
        ("imports", "/imports"),
        ("vault", "/vault"),
        ("settings", "/settings"),
        ("platform", "/admin"),
    ]