"""Evidence preview must behave like a stable enterprise review surface."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
JS = (ROOT / "src/litoral_trace/static/js/us_lacey_source_inspector.js").read_text(encoding="utf-8")
CSS = (ROOT / "src/litoral_trace/static/css/us_lacey_source_inspector.css").read_text(encoding="utf-8")
HTML = (ROOT / "src/litoral_trace/templates/us_lacey/fragments/source_inspector.html").read_text(encoding="utf-8")
DETAIL = (ROOT / "src/litoral_trace/templates/us_lacey/operation_detail.html").read_text(encoding="utf-8")


def test_reading_panel_is_portaled_and_fixed_above_sticky_navigation() -> None:
    assert "document.body.appendChild(root)" in JS
    assert "document.body.appendChild(previewPortal)" in JS
    assert "position:fixed" in CSS
    assert "z-index: 90" in CSS
    assert "height:calc(100dvh - 112px)" in CSS
    assert ".lt-source-inspector:not([open]) { display:none!important; }" in CSS


def test_unrelated_htmx_swaps_do_not_discard_the_document_session() -> None:
    assert 'document.body.addEventListener("htmx:beforeSwap", (event) =>' in JS
    assert "if (target.matches(sourceScope) || target.querySelector(sourceScope)) discardSourceState();" in JS
    assert 'document.body.addEventListener("htmx:beforeSwap", discardSourceState);' not in JS
    assert 'window.addEventListener("pagehide", discardSourceState)' in JS


def test_hover_is_once_per_card_with_graceful_close_and_explicit_pin() -> None:
    assert "hoverSurface" in JS and "surface.contains(event.relatedTarget)" in JS
    assert "button === previewButton" in JS
    assert "if (!root.open) closePreview()" in JS
    assert "data-preview-pin" in HTML and "data-preview-close" in HTML
    assert "previewRenderer?.destroy()" in JS
    assert "if (mobileViewport() || root.open" in JS


def test_pdf_loading_experience_and_raster_budget() -> None:
    assert 'data-inspector-loading' in HTML and 'data-preview-loading' in HTML
    assert 'data-viewer-state="loading"' in HTML
    assert 'role="status"' in HTML
    assert ".lt-pdf-loading" in CSS
    assert 'root.dataset.viewerState = "ready"' in JS
    assert 'preview.dataset.viewerState = "ready"' in JS
    assert "12_000_000" in JS and "devicePixelRatio" in JS
    assert "this.viewportContainer?.clientWidth" in JS
    assert "Math.min(1.65" in JS
    assert "Promise.all([" in JS and "loadPdfModule()" in JS
    assert "cache:\"no-store\"" in JS
    assert "URL.revokeObjectURL" in JS


def test_cache_busted_assets_and_reduced_motion_contract() -> None:
    assert "?v=20261031-workbench1" in DETAIL
    assert ".lt-pdf-loading__spinner { animation:none; }" in CSS
