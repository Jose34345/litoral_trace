from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

TEMPLATES = (
    ROOT
    / "src"
    / "litoral_trace"
    / "templates"
)


def _read(relative_path: str) -> str:
    return (
        TEMPLATES
        / relative_path
    ).read_text(
        encoding="utf-8"
    )


def test_common_base_owns_runtime_not_application_chrome():
    base = _read("base.html")

    assert "/dist/app.css" in base
    assert "/vendor/htmx/htmx.min.js" in base
    assert "/vendor/leaflet/leaflet.css" in base
    assert "/vendor/leaflet/leaflet.js" in base
    assert (
        "/vendor/fontawesome/css/all.min.css"
        in base
    )
    assert "/src/js/app.js" in base

    assert 'action="/logout"' not in base
    assert "user.organization_name" not in base


def test_public_and_app_bases_inherit_common_runtime():
    public_base = _read(
        "public/base_public.html"
    )
    app_base = _read(
        "app/base_app.html"
    )

    assert (
        '{% extends "base.html" %}'
        in public_base
    )

    assert (
        '{% extends "base.html" %}'
        in app_base
    )


def test_public_shell_does_not_require_authenticated_user():
    public_base = _read(
        "public/base_public.html"
    )

    assert "user.organization_name" not in public_base
    assert "user.username" not in public_base
    assert 'action="/logout"' not in public_base


def test_authenticated_shell_owns_tenant_and_logout_context():
    app_base = _read(
        "app/base_app.html"
    )

    assert "user.organization_name" in app_base
    assert "user.username" in app_base
    assert "user.role|upper" in app_base

    assert 'action="/logout"' in app_base
    assert (
        'name="{{ csrf_form_field }}"'
        in app_base
    )


def test_login_uses_public_shell_only():
    login = _read("login.html")

    assert login.startswith(
        '{% extends "public/base_public.html" %}'
    )


def test_authenticated_views_use_app_shell():
    authenticated_templates = (
        "dashboard.html",
        "vault.html",
        "settings.html",
        "admin_organizations.html",
        "logout.html",
    )

    expected = (
        '{% extends "app/base_app.html" %}'
    )

    for template_name in authenticated_templates:
        template = _read(template_name)

        assert template.startswith(
            expected
        ), template_name


def test_no_leaf_template_inherits_common_base_directly():
    leaf_templates = (
        "login.html",
        "dashboard.html",
        "vault.html",
        "settings.html",
        "admin_organizations.html",
        "logout.html",
    )

    for template_name in leaf_templates:
        template = _read(template_name)

        assert not template.startswith(
            '{% extends "base.html" %}'
        ), template_name

def test_shell_copy_preserves_utf8_content():
    public_shell = _read(
        "public/base_public.html"
    )

    app_shell = _read(
        "app/base_app.html"
    )

    public_copy = (
        "\U0001F332",
        "Exportaci\u00f3n",
        "Uni\u00f3n Europea",
        "Carb\u00f3n Vegetal",
        "\U0001F4CD Resistencia",
        "Deforestaci\u00f3n",
    )

    for expected in public_copy:
        assert expected in public_shell

    app_copy = (
        "Operaci\u00f3n",
        "Administraci\u00f3n",
        "Organizaci\u00f3n",
        "Navegaci\u00f3n",
        "Cerrar sesi\u00f3n",
    )

    for expected in app_copy:
        assert expected in app_shell

    for template in (
        public_shell,
        app_shell,
    ):
        assert "\ufffd" not in template
        assert "sesi?n" not in template
