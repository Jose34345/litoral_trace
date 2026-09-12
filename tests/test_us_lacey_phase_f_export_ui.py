from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from jinja2 import Environment, FileSystemLoader, select_autoescape


ROOT = Path(__file__).resolve().parents[1]
TEMPLATE_ROOT = ROOT / "src" / "litoral_trace" / "templates"
STATIC_ROOT = ROOT / "src" / "litoral_trace" / "static" / "src"
OPERATION_ID = "11111111-2222-3333-4444-555555555555"


def _render_export_card(*, status: str, arithmetic_blocked: bool = False, open_items: int = 0) -> str:
    environment = Environment(
        loader=FileSystemLoader(str(TEMPLATE_ROOT)),
        autoescape=select_autoescape(("html", "xml")),
    )
    template = environment.get_template("us_lacey/fragments/export_declaration_package.html")
    return template.render(
        detail=SimpleNamespace(public_id=OPERATION_ID, status=status),
        arithmetic_blocked=arithmetic_blocked,
        exception_fields=tuple(object() for _ in range(open_items)),
    )


def test_completed_operation_renders_native_phase_e_download_links():
    html = _render_export_card(status="COMPLETED")

    assert "Export Declaration Package" in html
    assert f'href="/operations/{OPERATION_ID}/export/lawgs-xml"' in html
    assert f'href="/operations/{OPERATION_ID}/export/excel"' in html
    assert html.count("data-export-download") == 2
    assert "Download LAWGS XML" in html
    assert "Download Excel Summary" in html
    assert "fa-code" in html
    assert "fa-file-excel" in html
    assert 'data-export-ready="true"' in html
    assert 'aria-busy="true"' not in html


def test_incomplete_operation_keeps_exports_visible_but_safely_disabled():
    html = _render_export_card(status="READY_FOR_REVIEW", open_items=2)

    assert "Download LAWGS XML" in html
    assert "Download Excel Summary" in html
    assert 'data-export-ready="false"' in html
    assert html.count('aria-disabled="true"') == 2
    assert "cursor" not in html  # styling belongs to the design-system extension, not inline CSS
    assert f'/operations/{OPERATION_ID}/export/lawgs-xml' not in html
    assert f'/operations/{OPERATION_ID}/export/excel' not in html
    assert "Resolve all missing or conflicting review items" in html


def test_reconciliation_block_explains_specific_unlock_requirement():
    html = _render_export_card(
        status="READY_FOR_REVIEW",
        arithmetic_blocked=True,
        open_items=1,
    )

    assert "Entered Value reconciliation inconsistency" in html
    assert html.count("lt-export-tooltip") == 2
    assert html.count('tabindex="0"') == 2


def test_export_microinteraction_preserves_first_native_download_and_blocks_duplicates():
    script = (STATIC_ROOT / "js" / "us-lacey-workspace.js").read_text(encoding="utf-8")

    assert 'a[data-export-download]' in script
    assert 'link.dataset.exportBusy === "true"' in script
    assert "event.preventDefault();" in script
    assert 'link.classList.add("is-loading")' in script
    assert 'link.setAttribute("aria-busy", "true")' in script
    assert 'link.dataset.loadingLabel || "Generating..."' in script
    assert "window.setTimeout(() => resetExportDownload(link), 1200);" in script
    # There is intentionally no preventDefault on the first activation: the anchor
    # reaches the authenticated Phase E Content-Disposition endpoint natively.
    busy_guard = script.index('if (link.dataset.exportBusy === "true")')
    prevent_default = script.index("event.preventDefault();")
    loading_state = script.index('link.classList.add("is-loading")')
    assert busy_guard < prevent_default < loading_state


def test_export_styles_use_shared_tokens_and_premium_safeguards():
    styles = (STATIC_ROOT / "us-lacey-export.css").read_text(encoding="utf-8")

    assert "var(--lt-shadow-sm)" in styles
    assert "var(--lt-shadow-md)" in styles
    assert "var(--lt-radius-md)" in styles
    assert "background-color 200ms ease" in styles
    assert "transform 200ms ease" in styles
    assert ".lt-export-action.is-disabled" in styles
    assert "opacity: 0.5" in styles
    assert "cursor: not-allowed" in styles
    assert "@keyframes lt-export-spin" in styles
    assert "prefers-reduced-motion" in styles


def test_workspace_replaces_legacy_exports_with_phase_f_component():
    workspace = (
        TEMPLATE_ROOT / "us_lacey" / "fragments" / "operation_workspace.html"
    ).read_text(encoding="utf-8")
    base = (TEMPLATE_ROOT / "base.html").read_text(encoding="utf-8")

    assert '{% include "us_lacey/fragments/export_declaration_package.html" %}' in workspace
    assert "/export.xlsx" not in workspace
    assert "/export.csv" not in workspace
    assert "/src/us-lacey-export.css" in base
