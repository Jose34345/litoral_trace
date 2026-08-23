from pathlib import Path

from jinja2 import Environment


ROOT = Path(__file__).resolve().parents[1]
TEMPLATES = ROOT / "src" / "litoral_trace" / "templates"
STATIC_SRC = ROOT / "src" / "litoral_trace" / "static" / "src"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_p17_app_shell_template_parses_and_loads_semantic_layer() -> None:
    shell = _read(TEMPLATES / "app" / "base_app.html")

    Environment().parse(shell)

    assert "path='/src/app-shell.css'" in shell
    for class_name in (
        "lt-app-shell",
        "lt-sidebar",
        "lt-nav-link",
        "lt-workspace",
        "lt-topbar",
        "lt-main",
    ):
        assert class_name in shell


def test_p17_app_shell_preserves_drawer_accessibility_contract() -> None:
    shell = _read(TEMPLATES / "app" / "base_app.html")

    assert 'id="app-navigation"' in shell
    assert "data-app-drawer" in shell
    assert "data-app-drawer-overlay" in shell
    assert "data-app-drawer-open" in shell
    assert "data-app-drawer-close" in shell
    assert 'aria-controls="app-navigation"' in shell
    assert 'aria-expanded="false"' in shell
    assert 'href="#main-content"' in shell
    assert 'id="main-content"' in shell
    assert 'tabindex="-1"' in shell


def test_p17_app_shell_preserves_navigation_and_session_contracts() -> None:
    shell = _read(TEMPLATES / "app" / "base_app.html")

    assert '("operacion", "Operación")' in shell
    assert '("compliance", "Trazabilidad y evidencia")' in shell
    assert '("administracion", "Administración")' in shell
    assert 'navigation | selectattr("section", "equalto", section_key)' in shell
    assert 'aria-current="page"' in shell

    assert 'method="post" action="/logout"' in shell
    assert 'name="{{ csrf_form_field }}"' in shell
    assert 'value="{{ csrf_token }}"' in shell


def test_p17_app_shell_keeps_existing_responsive_tailwind_candidates() -> None:
    shell = _read(TEMPLATES / "app" / "base_app.html")

    # B layers semantic CSS on top of the existing responsive mechanics.
    # These candidates are intentionally retained so drawer behavior and the
    # tracked Tailwind artifact cannot drift as a side effect of the restyle.
    for candidate in (
        "w-72",
        "-translate-x-full",
        "lg:translate-x-0",
        "lg:pl-72",
        "lg:hidden",
        "sticky top-0",
        "max-w-[1536px]",
    ):
        assert candidate in shell


def test_p17_app_shell_css_defines_primary_shell_states() -> None:
    css = _read(STATIC_SRC / "app-shell.css")

    for selector in (
        ".lt-app-shell",
        ".lt-sidebar",
        ".lt-nav-link[aria-current=\"page\"]",
        ".lt-org-card",
        ".lt-topbar",
        ".lt-audit-pill",
        ".lt-user-avatar",
        ".lt-main",
    ):
        assert selector in css

    assert "@media (max-width: 1023px)" in css
    assert "@media (min-width: 1024px)" in css
    assert "@media (prefers-reduced-motion: reduce)" in css
