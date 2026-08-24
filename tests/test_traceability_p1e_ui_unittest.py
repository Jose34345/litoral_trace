"""P1E browser presentation contracts for origin dossier downloads."""
from __future__ import annotations

from pathlib import Path

from litoral_trace.web.traceability import build_traceability_view


ROOT = Path(__file__).resolve().parents[1]
TEMPLATE = (
    ROOT
    / "src"
    / "litoral_trace"
    / "templates"
    / "traceability.html"
)


def _payload() -> dict:
    return {
        "shipment": {
            "public_id": "8d4de42a-2283-48e4-996d-e47b19ae1001",
            "shipment_code": "EXP UE/2026 001",
            "sale_reference": "FACTURA-E-001",
            "buyer_reference": "BUYER-EU-001",
            "destination_country": "DE",
            "shipped_at": "2026-08-20T20:00:00+00:00",
            "status": "DISPATCHED",
            "lineage_state": "FINAL",
        },
        "allocation_method": "PROPORTIONAL_INPUT_ALLOCATION",
        "complete": True,
        "issues": [],
        "unit_totals": [
            {
                "unit": "M3",
                "shipped_quantity": "60.000000",
                "attributed_quantity": "60.000000",
                "unresolved_quantity": "0.000000",
            }
        ],
        "items": [],
        "events": [],
        "source_lotes": [],
    }


def test_traceability_view_exposes_query_safe_dossier_downloads() -> None:
    view = build_traceability_view(
        query="EXP UE/2026 001",
        payload=_payload(),
    )
    dossier = view["result"]["dossier"]
    base = "/api/v1/traceability/shipments/dossier"
    query = "shipment_code=EXP+UE%2F2026+001"

    assert dossier == {
        "bundle_href": f"{base}/bundle?{query}",
        "pdf_href": f"{base}/pdf?{query}",
        "geojson_href": f"{base}/geojson?{query}",
        "manifest_href": f"{base}/manifest?{query}",
    }


def test_traceability_template_surfaces_origin_dossier_without_compliance_claim() -> None:
    template = TEMPLATE.read_text(encoding="utf-8")

    assert "Expediente de origen" in template
    assert "Expediente del despacho para el comprador" in template
    assert "{{ result.dossier.bundle_href }}" in template
    assert "{{ result.dossier.pdf_href }}" in template
    assert "{{ result.dossier.geojson_href }}" in template
    assert "{{ result.dossier.manifest_href }}" in template
    assert "Descargar expediente" in template
    assert "no constituye por sí solo una declaración regulatoria" in template
