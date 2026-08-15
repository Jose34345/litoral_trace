from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

PUBLIC_BASE = (
    ROOT
    / "src"
    / "litoral_trace"
    / "templates"
    / "public"
    / "base_public.html"
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


def test_public_shell_has_mobile_navigation_contract():
    shell = PUBLIC_BASE.read_text(
        encoding="utf-8"
    )

    assert "data-public-nav-trigger" in shell
    assert "data-public-nav-panel" in shell

    assert (
        'aria-controls="public-mobile-navigation"'
        in shell
    )

    assert 'aria-expanded="false"' in shell


def test_public_mobile_navigation_contains_live_links():
    shell = PUBLIC_BASE.read_text(
        encoding="utf-8"
    )

    for href in (
        "/#platform",
        "/#eudr",
        "/#regional-intelligence",
        "/#security",
        "/login",
    ):
        assert f'href="{href}"' in shell


def test_public_navigation_runtime_supports_close_behaviors():
    js = APP_JS.read_text(
        encoding="utf-8"
    )

    assert "installPublicNavigation" in js
    assert 'event.key === "Escape"' in js

    assert (
        'trigger.focus({'
        in js
    )

    assert (
        '"(min-width: 640px)"'
        in js
    )


def test_public_shell_footer_is_encoding_safe():
    shell = PUBLIC_BASE.read_text(
        encoding="utf-8"
    )

    assert (
        "Argentina &middot; "
        "South America &middot; EUDR"
        in shell
    )

    assert (
        "Argentina ? South America ? EUDR"
        not in shell
    )

    assert "\ufffd" not in shell


def test_public_shell_has_skip_link_and_focus_target():
    shell = PUBLIC_BASE.read_text(
        encoding="utf-8"
    )

    assert 'href="#main-content"' in shell

    assert (
        'id="main-content"'
        in shell
    )

    assert (
        'tabindex="-1"'
        in shell
    )


def test_public_shell_does_not_gain_authenticated_context():
    shell = PUBLIC_BASE.read_text(
        encoding="utf-8"
    )

    assert "user.organization_name" not in shell
    assert "user.username" not in shell
    assert 'action="/logout"' not in shell