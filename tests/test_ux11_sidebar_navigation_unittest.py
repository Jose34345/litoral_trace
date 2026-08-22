"""UX11 sidebar/navigation regression contracts."""
from __future__ import annotations

from pathlib import Path

from litoral_trace.web.navigation import build_navigation


def _by_key(role: str, path: str):
    return {
        item.key: item
        for item in build_navigation(role, current_path=path)
    }


def test_admin_navigation_exposes_operational_workflow_and_contextual_evidence() -> None:
    navigation = _by_key("admin", "/operations")

    assert navigation["operations"].href == "/operations"
    assert navigation["operations"].active is True
    assert navigation["evidence"].href == "/evidence"
    assert navigation["evidence"].label == "Evidencias"
    assert "platform" not in navigation


def test_auditor_keeps_read_only_traceability_and_evidence_without_operations() -> None:
    navigation = _by_key("auditor", "/evidence")

    assert navigation["traceability"].href == "/traceability"
    assert navigation["evidence"].active is True
    assert "operations" not in navigation
    assert "imports" not in navigation
    assert "settings" not in navigation


def test_legacy_vault_path_keeps_evidence_navigation_active() -> None:
    navigation = _by_key("admin", "/vault")

    assert navigation["evidence"].active is True


def test_superadmin_platform_label_is_compact() -> None:
    navigation = _by_key("superadmin", "/admin")

    assert navigation["platform"].label == "Plataforma"
    assert navigation["platform"].active is True


def test_sidebar_template_prioritizes_navigation_over_redundant_promo_card() -> None:
    root = Path(__file__).resolve().parents[1]
    template = (
        root / "src/litoral_trace/templates/app/base_app.html"
    ).read_text(encoding="utf-8")

    assert 'class="min-h-0 flex-1 overflow-y-auto' in template
    assert "Trazabilidad y evidencia" in template
    assert 'item.key == "operations"' in template
    assert 'item.key == "evidence"' in template
    assert "Cadena de custodia auditable" not in template
    assert 'aria-label="Organización y sesión"' in template
    assert 'aria-label="Cerrar sesión"' in template


def test_tracked_tailwind_asset_contains_sidebar_utilities() -> None:
    root = Path(__file__).resolve().parents[1]
    css = (
        root / "src/litoral_trace/static/dist/app.css"
    ).read_text(encoding="utf-8")

    assert ".min-h-0{" in css
    assert ".p-2\\.5{" in css
    assert ".ring-slate-800{" in css
