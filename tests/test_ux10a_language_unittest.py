from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TEMPLATES = ROOT / "src" / "litoral_trace" / "templates"


def _visible_sources() -> dict[str, str]:
    sources = {
        str(path.relative_to(ROOT)): path.read_text(encoding="utf-8")
        for path in sorted(TEMPLATES.rglob("*.html"))
    }
    for relative in (
        "src/litoral_trace/web/regional_intelligence.py",
        "src/litoral_trace/static/src/js/regional_intelligence_map.js",
    ):
        path = ROOT / relative
        sources[relative] = path.read_text(encoding="utf-8")
    return sources


def test_spanish_document_contract_and_canonical_product_language() -> None:
    base = (TEMPLATES / "base.html").read_text(encoding="utf-8")
    assert '<html lang="es"' in base

    corpus = "\n".join(_visible_sources().values())

    required = (
        "Trazabilidad de origen",
        "Contexto regional de origen",
        "Documentos y evidencias",
        "Expediente del despacho para el comprador",
        "No informado en esta vista",
    )
    for phrase in required:
        assert phrase in corpus, f"Falta el lenguaje canónico: {phrase!r}"


def test_legacy_english_product_phrases_are_not_visible() -> None:
    sources = _visible_sources()
    banned = (
        "Regional Intelligence",
        "Compliance Intelligence",
        "Compliance Workspace",
        "Tenant DB",
        "Registros visibles del tenant",
        "Satellite Engine",
        "Import public ID",
        "buyer-facing",
        "Request a demo",
        "Corporate access",
        "Forest Compliance OS",
    )

    violations: list[str] = []
    for relative, content in sources.items():
        for phrase in banned:
            if phrase in content:
                violations.append(f"{relative}: {phrase}")

    assert not violations, "Frases legacy visibles:\n" + "\n".join(violations)


def test_settings_do_not_present_old_demo_values_as_real_account_data() -> None:
    settings = (TEMPLATES / "settings.html").read_text(encoding="utf-8")
    forbidden_demo_values = (
        "12 / 100",
        "1,450 / 10,000",
        "31 de Diciembre de 2027",
        "Mario Darío Benítez",
        "30-71234567-8",
        "mario.benitez@despachantes.com",
    )

    for value in forbidden_demo_values:
        assert value not in settings, f"Dato ficticio legacy aún visible: {value!r}"


def test_regional_context_keeps_fail_closed_interpretation_language() -> None:
    model = (
        ROOT / "src" / "litoral_trace" / "web" / "regional_intelligence.py"
    ).read_text(encoding="utf-8")
    index = (TEMPLATES / "public" / "regional_index.html").read_text(
        encoding="utf-8"
    )
    detail = (TEMPLATES / "public" / "regional_detail.html").read_text(
        encoding="utf-8"
    )

    assert "no evaluación de riesgo" in model
    assert "Geografía, no riesgo" in index
    assert "no es una conclusión de cumplimiento" in detail


def test_traceability_keeps_accounting_and_read_only_boundaries() -> None:
    traceability = (TEMPLATES / "traceability.html").read_text(encoding="utf-8")

    assert "convención contable de trazabilidad" in traceability
    assert "no modifica inventario ni eventos" in traceability
    assert "no constituye por sí solo una declaración regulatoria" in traceability
    assert "Atribución proporcional según entradas documentadas" in traceability
