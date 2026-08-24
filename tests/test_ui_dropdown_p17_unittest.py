from pathlib import Path

from jinja2 import Environment


ROOT = Path(__file__).resolve().parents[1]
TEMPLATES = ROOT / "src" / "litoral_trace" / "templates"
STATIC_SRC = ROOT / "src" / "litoral_trace" / "static" / "src"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_p17_dropdown_templates_parse_and_layer_is_loaded() -> None:
    base = _read(TEMPLATES / "base.html")
    component = _read(TEMPLATES / "components" / "dropdown.html")
    shell = _read(TEMPLATES / "app" / "base_app.html")

    env = Environment()
    for template in (base, component, shell):
        env.parse(template)

    assert "path='/src/dropdown.css'" in base
    assert "components/dropdown.html" in shell


def test_p17_dropdown_uses_native_popover_contract_without_new_js() -> None:
    component = _read(TEMPLATES / "components" / "dropdown.html")
    base = _read(TEMPLATES / "base.html")

    assert "popovertarget=" in component
    assert 'popover="auto"' in component
    assert "macro dropdown_trigger" in component
    assert "macro dropdown" in component
    assert "macro dropdown_item" in component
    assert "macro dropdown_separator" in component
    assert 'role="separator"' in component

    # F deliberately relies on the native Popover API. It must not add a
    # second dropdown controller or mutate app.js for open/close behavior.
    assert "dropdown.js" not in base


def test_p17_app_shell_user_dropdown_is_additive_and_keeps_secure_logout() -> None:
    shell = _read(TEMPLATES / "app" / "base_app.html")

    assert 'popovertarget="app-user-dropdown"' in shell
    assert 'aria-label="Abrir menú de cuenta"' in shell
    assert 'dropdown("app-user-dropdown", "Cuenta y sesión")' in shell
    assert 'dropdown_item("Configuración", href="/settings"' in shell
    assert 'dropdown_item("Cerrar sesión", href="/logout"' in shell

    # The original CSRF-protected POST logout in the sidebar remains present.
    assert 'method="post" action="/logout"' in shell
    assert 'name="{{ csrf_form_field }}"' in shell
    assert 'value="{{ csrf_token }}"' in shell

    # B drawer and focus contracts remain untouched by the additive menu.
    assert "data-app-drawer" in shell
    assert 'href="#main-content"' in shell
    assert 'id="main-content"' in shell


def test_p17_dropdown_css_defines_popover_position_and_interaction_states() -> None:
    css = _read(STATIC_SRC / "dropdown.css")

    for selector in (
        ".lt-dropdown-trigger",
        ".lt-dropdown",
        ".lt-dropdown[popover]",
        ".lt-dropdown::backdrop",
        ".lt-dropdown__item",
        ".lt-dropdown__item:focus-visible",
        ".lt-dropdown__item--danger",
        ".lt-dropdown__secondary",
        ".lt-dropdown__separator",
    ):
        assert selector in css

    assert "position-area: block-end span-inline-end" in css
    assert "position-try-fallbacks" in css
    assert "@supports not (position-area: block-end)" in css
    assert "@media (max-width: 639px)" in css
