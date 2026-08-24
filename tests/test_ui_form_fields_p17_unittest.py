from pathlib import Path

from jinja2 import Environment


ROOT = Path(__file__).resolve().parents[1]
TEMPLATES = ROOT / "src" / "litoral_trace" / "templates"
STATIC_SRC = ROOT / "src" / "litoral_trace" / "static" / "src"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_p17_form_field_templates_parse_and_layer_is_loaded() -> None:
    base = _read(TEMPLATES / "base.html")
    component = _read(TEMPLATES / "components" / "form_fields.html")
    settings = _read(TEMPLATES / "settings.html")
    catalog = _read(TEMPLATES / "dev" / "ui_catalog.html")

    env = Environment()
    for template in (base, component, settings, catalog):
        env.parse(template)

    assert "path='/src/form-controls.css'" in base
    assert "components/form_fields.html" in settings
    assert "components/form_fields.html" in catalog


def test_p17_form_field_component_exposes_accessible_primitives() -> None:
    component = _read(TEMPLATES / "components" / "form_fields.html")

    for macro in (
        "macro text_field",
        "macro select_field",
        "macro textarea_field",
        "macro checkbox_field",
    ):
        assert macro in component

    assert 'aria-invalid="true"' in component
    assert "aria-describedby=" in component
    assert 'role="alert"' in component
    assert "lt-field__required" in component
    assert "lt-field__optional" in component
    assert "lt-field__help" in component
    assert "lt-field__error" in component


def test_p17_settings_canary_preserves_htmx_and_payload_contract() -> None:
    settings = _read(TEMPLATES / "settings.html")

    assert 'hx-post="/api/v1/settings/invite_demo_user"' in settings
    assert 'hx-target="#inviteFeedback"' in settings
    assert 'id="inviteFeedback"' in settings

    for field_name in (
        "cuit_empresa",
        "nombre_contacto",
        "email_contacto",
        "especie_principal",
    ):
        assert field_name in settings

    for option_value in (
        "Madera Aserrada (Pino)",
        "Madera Aserrada (Eucalipto)",
        "Extracto de Quebracho (Tanino)",
        "Carbón Vegetal",
    ):
        assert option_value in settings

    assert "Generar acceso temporal" in settings
    assert "lt-form" in settings


def test_p17_form_macros_keep_historical_tailwind_candidates() -> None:
    component = _read(TEMPLATES / "components" / "form_fields.html")

    # The canary removes repeated control markup from settings.html, so the
    # historical utilities intentionally live in the shared macro instead.
    # This keeps Tailwind's tracked candidate graph stable during migration.
    for candidate in (
        "w-full rounded-lg border border-slate-300 px-3 py-2.5 text-sm outline-none focus:ring-2 focus:ring-forest-600",
        "w-full rounded-lg border border-slate-300 bg-white px-3 py-2.5 text-sm outline-none",
    ):
        assert candidate in component


def test_p17_form_css_defines_required_error_and_responsive_states() -> None:
    css = _read(STATIC_SRC / "form-controls.css")

    for selector in (
        ".lt-form__grid",
        ".lt-form__actions",
        ".lt-field__label-row",
        ".lt-field__required",
        ".lt-field__optional",
        ".lt-field__error",
        ".lt-field[data-state=\"error\"] .lt-field__label",
        ".lt-field[data-state=\"success\"] .lt-control",
        "textarea.lt-control",
        ".lt-check-field",
        ".lt-fieldset",
    ):
        assert selector in css

    assert "@media (max-width: 639px)" in css
    assert "@media (prefers-reduced-motion: reduce)" in css
