from __future__ import annotations

import json
from pathlib import Path

from litoral_trace.services.compliance import (
    LEGACY_NON_REGULATORY_PROFILE,
    evaluar_compliance_lote,
    generar_dds_json_traces_nt,
)


ROOT = Path(__file__).resolve().parents[1]
COMPLIANCE_PATH = ROOT / "src" / "litoral_trace" / "services" / "compliance.py"


def _sample_lote() -> dict[str, object]:
    return {
        "identificador": "P1D-LEGACY-01",
        "productor_id": "PROV-LEGACY",
        "producto_forestal": "Madera Aserrada (Pino)",
        "latitud": -28.05,
        "longitud": -56.03,
        "polygon_wkt": (
            "POLYGON((-56.04 -28.06,-56.02 -28.06,-56.02 -28.04,"
            "-56.04 -28.04,-56.04 -28.06))"
        ),
    }


def test_p1d_legacy_preview_cannot_claim_regulatory_compliance() -> None:
    result = evaluar_compliance_lote(
        _sample_lote(),
        volumen_ingresado_ton=100.0,
        volumen_exportar_ton=40.0,
    )

    assert result["profile"] == LEGACY_NON_REGULATORY_PROFILE
    assert result["regulatory_conclusion"] is None
    assert result["satelital"]["source"] == "SIMULATED_LEGACY_SERIES"
    assert "PREVIEW NO REGULATORIO" in result["observacion"]
    assert "APROBADO / COMPLIANT" not in result["observacion"].upper()


def test_p1d_retired_traces_generator_never_emits_legal_assertions() -> None:
    payload = json.loads(
        generar_dds_json_traces_nt(
            _sample_lote(),
            volumen_exportar_ton=40.0,
            operador_username="legacy@example.test",
        )
    )

    assert payload["profile"] == LEGACY_NON_REGULATORY_PROFILE
    assert payload["not_a_due_diligence_statement"] is True
    assert payload["submit_ready"] is False
    assert payload["regulatory_conclusion"] is None
    assert "compliance" not in payload

    serialized = json.dumps(payload, sort_keys=True).lower()
    for forbidden in (
        '"status": "compliant"',
        "deforestation_free",
        "legal_harvest_verified",
        "due_diligence_statement_reference_number",
    ):
        assert forbidden not in serialized


def test_p1d_legacy_module_contains_no_hardcoded_positive_legal_claims() -> None:
    source = COMPLIANCE_PATH.read_text(encoding="utf-8")
    forbidden_source_tokens = (
        '"deforestation_free": True',
        '"legal_harvest_verified": True',
        '"status": "COMPLIANT"',
        "APROBADO / COMPLIANT",
    )
    for token in forbidden_source_tokens:
        assert token not in source
