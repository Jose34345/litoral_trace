from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

TEMPLATES = (
    ROOT
    / "src"
    / "litoral_trace"
    / "templates"
)


def _read(path: str) -> str:
    return (
        TEMPLATES
        / path
    ).read_text(
        encoding="utf-8"
    )


def test_homepage_contains_commercial_polish_sections():
    home = _read(
        "public/home.html"
    )

    for section_id in (
        "platform",
        "eudr",
        "evidence-first",
        "regional-intelligence",
        "security",
        "commercial-cta",
    ):
        assert f'id="{section_id}"' in home


def test_platform_cards_only_target_live_home_sections():
    home = _read(
        "public/home.html"
    )

    allowed_targets = (
        "#eudr",
        "#evidence-first",
        "#security",
        "#regional-intelligence",
    )

    for target in allowed_targets:
        assert f'"{target}"' in home

    forbidden = (
        'href="/origins"',
        'href="/satellite"',
        'href="/imports"',
        'href="/platform"',
    )

    for target in forbidden:
        assert target not in home


def test_homepage_has_evidence_first_positioning():
    home = _read(
        "public/home.html"
    )

    assert (
        "Diseñado para explicar una operación, no para esconderla detrás de un puntaje."
        in home
    )

    assert "Trazabilidad basada en evidencia" in home
    assert "Genealogía trazable" in home
    assert "Expediente verificable" in home


def test_homepage_routes_regional_context_to_dedicated_view():
    home = _read(
        "public/home.html"
    )

    for region in (
        "Chaco",
        "Corrientes",
        "Misiones",
        "NEA",
        "Argentina",
    ):
        assert region in home

    assert 'href="/regional-intelligence"' in home
    assert "Contexto regional de origen" in home
    assert "No reemplaza la evaluación específica" in home


def test_homepage_has_final_commercial_cta():
    home = _read(
        "public/home.html"
    )

    assert (
        "Probá Litoral Trace con una muestra real de tu operación"
        in home
    )

    assert (
        'href="mailto:comercial@litoraltrace.com"'
        in home
    )

    assert 'href="/login"' in home


def test_login_matches_current_public_brand():
    login = _read(
        "login.html"
    )

    assert (
        '{% extends "public/base_public.html" %}'
        in login
    )

    assert "Espacio corporativo seguro" in login
    assert "Acceso seguro a Litoral Trace" in login
    assert "Ingresar de forma segura" in login

    assert "🪵" not in login
    assert (
        "Exportación Forestal a Europa"
        not in login
    )


def test_login_back_link_is_encoding_safe():
    login = _read(
        "login.html"
    )

    assert (
        "&larr; Volver al sitio público"
        in login
    )

    assert (
        "? Volver al sitio público"
        not in login
    )


def test_login_preserves_security_contract():
    login = _read(
        "login.html"
    )

    assert 'action="/login"' in login
    assert (
        'name="{{ csrf_form_field }}"'
        in login
    )
    assert "{{ csrf_token }}" in login

    assert 'autocomplete="username"' in login
    assert (
        'autocomplete="current-password"'
        in login
    )

    assert 'role="alert"' in login
