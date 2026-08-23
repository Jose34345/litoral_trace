from pathlib import Path

from jinja2 import Environment


ROOT = Path(__file__).resolve().parents[1]
TEMPLATES = ROOT / "src" / "litoral_trace" / "templates"
STATIC_SRC = ROOT / "src" / "litoral_trace" / "static" / "src"
JS_SRC = STATIC_SRC / "js"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_p17_dialog_templates_parse_and_assets_are_loaded() -> None:
    base = _read(TEMPLATES / "base.html")
    component = _read(TEMPLATES / "components" / "dialog.html")
    catalog = _read(TEMPLATES / "dev" / "ui_catalog.html")

    env = Environment()
    for template in (base, component, catalog):
        env.parse(template)

    assert "path='/src/dialog.css'" in base
    assert "path='/src/js/dialog.js'" in base
    assert "components/dialog.html" in catalog


def test_p17_dialog_uses_native_accessible_contract() -> None:
    component = _read(TEMPLATES / "components" / "dialog.html")

    assert "<dialog" in component
    assert "data-lt-dialog" in component
    assert 'aria-labelledby="{{ dialog_id }}-title"' in component
    assert "aria-describedby=" in component
    assert "data-lt-dialog-open" in component
    assert 'aria-haspopup="dialog"' in component
    assert "data-lt-dialog-close" in component
    assert 'aria-label="{{ close_label }}"' in component


def test_p17_dialog_controller_preserves_native_focus_and_escape_behavior() -> None:
    js = _read(JS_SRC / "dialog.js")

    for contract in (
        "HTMLDialogElement",
        "showModal()",
        ".close()",
        "previousFocus",
        "data-lt-dialog-open",
        "data-lt-dialog-close",
        'dialog.addEventListener("close"',
        "trigger.focus({ preventScroll: true })",
    ):
        assert contract in js

    # Native dialog supplies modal focus containment and Escape/cancel semantics.
    # The controller must not recreate a second manual focus trap.
    assert 'event.key === "Tab"' not in js
    assert 'event.key === "Escape"' not in js


def test_p17_dialog_catalog_is_non_domain_mutating_canary() -> None:
    catalog = _read(TEMPLATES / "dev" / "ui_catalog.html")

    assert "catalog-release-dialog" in catalog
    assert "Revisar liberación" in catalog
    assert "Confirmar liberación del despacho" in catalog
    assert "data-lt-dialog-close" in catalog
    assert "no modifica por sí mismo el estado de dominio" in catalog
    assert "hx-post=" not in catalog


def test_p17_dialog_css_defines_modal_backdrop_and_responsive_states() -> None:
    css = _read(STATIC_SRC / "dialog.css")

    for selector in (
        ".lt-dialog",
        ".lt-dialog--sm",
        ".lt-dialog--lg",
        ".lt-dialog::backdrop",
        ".lt-dialog[open]",
        ".lt-dialog__header",
        ".lt-dialog__close:focus-visible",
        ".lt-dialog__body",
        ".lt-dialog__footer",
        ".lt-dialog__notice",
    ):
        assert selector in css

    assert "100dvh" in css
    assert "@media (max-width: 639px)" in css
    assert "@media (prefers-reduced-motion: reduce)" in css
