from pathlib import Path

from jinja2 import Environment


ROOT = Path(__file__).resolve().parents[1]
TEMPLATES = ROOT / "src" / "litoral_trace" / "templates"
STATIC_SRC = ROOT / "src" / "litoral_trace" / "static" / "src"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_p17_design_system_is_loaded_globally_after_generated_css() -> None:
    base = _read(TEMPLATES / "base.html")

    generated = "path='/dist/app.css'"
    design_system = "path='/src/design-system.css'"

    assert generated in base
    assert design_system in base
    assert base.index(generated) < base.index(design_system)


def test_p17_shared_ui_macro_template_parses() -> None:
    source = _read(TEMPLATES / "components" / "ui.html")

    Environment().parse(source)

    for macro_name in (
        "button",
        "badge",
        "status_badge",
        "risk_badge",
        "page_header",
        "alert",
        "empty_state",
    ):
        assert f"macro {macro_name}(" in source


def test_p17_semantic_css_exposes_required_tokens_and_primitives() -> None:
    css = _read(STATIC_SRC / "design-system.css")

    for token in (
        "--lt-canvas:",
        "--lt-surface:",
        "--lt-text-primary:",
        "--lt-accent:",
        "--lt-positive:",
        "--lt-warning:",
        "--lt-danger:",
        "--lt-info:",
    ):
        assert token in css

    for primitive in (
        ".lt-card",
        ".lt-btn",
        ".lt-badge",
        ".lt-alert",
        ".lt-empty-state",
        ".lt-control",
    ):
        assert primitive in css


def test_p17_logout_canary_uses_shared_primitives_without_contract_drift() -> None:
    logout = _read(TEMPLATES / "logout.html")

    Environment().parse(logout)

    assert 'from "components/ui.html" import button, page_header' in logout
    assert 'method="post" action="/logout"' in logout
    assert 'name="{{ csrf_form_field }}"' in logout
    assert 'value="{{ csrf_token }}"' in logout
    assert 'href="/dashboard"' in logout
    # Keep the legacy candidate that was unique to this surface so the
    # tracked Tailwind artifact remains reproducible while P1.7 migrates UI.
    assert "max-w-lg" in logout


def test_p17_ui_catalog_is_server_rendered_and_parses() -> None:
    catalog = _read(TEMPLATES / "dev" / "ui_catalog.html")

    Environment().parse(catalog)

    assert '{% extends "app/base_app.html" %}' in catalog
    assert 'components/ui.html' in catalog
    assert "path='/src/ui-catalog.css'" in catalog
    assert 'UI Catalog' in catalog
    assert 'status_badge("READY")' in catalog
    assert 'risk_badge("CRITICAL")' in catalog


def test_p17_ui_catalog_does_not_expand_tailwind_candidate_surface() -> None:
    catalog = _read(TEMPLATES / "dev" / "ui_catalog.html")

    # The catalog is intentionally styled by semantic CSS. Keeping Tailwind
    # utilities out of this dev-only template prevents a documentation page
    # from mutating the production tracked bundle during `npm run build`.
    forbidden_candidates = (
        "mt-7",
        "max-w-2xl",
        "sm:grid-cols-2",
        "p-6",
    )

    for candidate in forbidden_candidates:
        assert candidate not in catalog
