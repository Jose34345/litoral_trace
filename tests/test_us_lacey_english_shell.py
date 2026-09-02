from pathlib import Path


US_LACEY_TEMPLATE_DIR = Path("src/litoral_trace/templates/us_lacey")


def test_us_lacey_shell_is_english_and_isolated_from_regional_public_layout():
    text = (US_LACEY_TEMPLATE_DIR / "base.html").read_text(encoding="utf-8")

    assert '{% extends "base.html" %}' in text
    assert 'lang="en"' in text
    assert "U.S. Lacey Act workspace" in text
    assert "PPQ 505 preparation" in text
    assert "Human review required" in text
    assert "public/base_public.html" not in text


def test_us_lacey_templates_do_not_contain_regional_spanish_navigation_copy():
    banned = {
        "Trazabilidad de origen",
        "Plataforma",
        "Debida diligencia",
        "Contexto regional",
        "Auditabilidad",
        "Acceso de clientes",
        "Solicitar demostración",
        "Solicitar demo",
        "Ir al contenido principal",
        "Navegación pública",
        "Argentina · Cadenas forestales · Comercio exterior",
    }

    for template in sorted(US_LACEY_TEMPLATE_DIR.glob("*.html")):
        text = template.read_text(encoding="utf-8")
        for phrase in banned:
            assert phrase not in text, f"{template} contains regional Spanish copy: {phrase!r}"
