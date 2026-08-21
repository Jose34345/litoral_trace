"""UX10-C acceptance contracts for the visual shipment-lineage graph."""
from __future__ import annotations

from pathlib import Path

from litoral_trace.web.traceability import build_traceability_view


ROOT = Path(__file__).resolve().parents[1]
GRAPH_TEMPLATE = (
    ROOT
    / "src"
    / "litoral_trace"
    / "templates"
    / "app"
    / "_traceability_lineage_graph.html"
)
TRACEABILITY_TEMPLATE = (
    ROOT
    / "src"
    / "litoral_trace"
    / "templates"
    / "traceability.html"
)


def _payload(*, complete: bool = True) -> dict:
    unresolved = "0.000000" if complete else "25.000000"
    attributed = "60.000000" if complete else "35.000000"
    return {
        "organization_id": 17,
        "shipment": {
            "shipment_code": "EXP-UE-2026-001",
            "sale_reference": "FACTURA-E-001",
            "buyer_reference": "BUYER-EU-001",
            "destination_country": "DE",
            "shipped_at": "2026-08-20T18:00:00+00:00",
            "status": "DISPATCHED",
            "lineage_state": "FINAL" if complete else "PREVIEW",
        },
        "allocation_method": "PROPORTIONAL_INPUT_ALLOCATION",
        "complete": complete,
        "issues": (
            []
            if complete
            else [
                {
                    "code": "MISSING_PROVENANCE",
                    "message": "Falta un origen documentado.",
                }
            ]
        ),
        "unit_totals": [
            {
                "unit": "M3",
                "shipped_quantity": "60.000000",
                "attributed_quantity": attributed,
                "unresolved_quantity": unresolved,
            }
        ],
        "source_lotes": [
            {
                "lote": {
                    "identificador": "RODAL-A",
                    "productor_id": "CUIT-PROVEEDOR-A",
                    "producto_forestal": "Pino resinoso",
                    "hectareas": 50.0,
                    "latitud": -28.05,
                    "longitud": -56.03,
                    "polygon_wkt": "POLYGON((-56 -28,-55 -28,-55 -27,-56 -28))",
                    "estatus": "Verde",
                },
                "attributed_shipment_quantity": "42.000000",
                "unit": "M3",
                "share_of_shipped_unit": "0.700000",
            },
            {
                "lote": {
                    "identificador": "RODAL-B",
                    "productor_id": "CUIT-PROVEEDOR-B",
                    "producto_forestal": "Pino resinoso",
                    "hectareas": 30.0,
                    "latitud": -28.15,
                    "longitud": -56.13,
                    "polygon_wkt": None,
                    "estatus": "Verde",
                },
                "attributed_shipment_quantity": "18.000000",
                "unit": "M3",
                "share_of_shipped_unit": "0.300000",
            },
        ],
        "events": [
            {
                "event_code": "ASERRADO-TURNO-001",
                "event_type": "TRANSFORMATION",
                "status": "POSTED",
                "occurred_at": "2026-08-20T17:00:00+00:00",
                "facility_reference": "Planta Virasoro",
                "inputs": [
                    {"batch_code": "REC-A-001", "quantity": "70.000000", "unit": "M3"},
                    {"batch_code": "REC-B-001", "quantity": "30.000000", "unit": "M3"},
                ],
                "outputs": [
                    {"batch_code": "ASERRADO-001", "quantity": "65.000000", "unit": "M3"}
                ],
                "reconciliation": {
                    "unit": "M3",
                    "input_quantity": "100.000000",
                    "output_quantity": "65.000000",
                    "loss_quantity": "35.000000",
                    "yield_ratio": "0.650000",
                },
            }
        ],
        "items": [
            {
                "batch": {
                    "code": "ASERRADO-001",
                    "product_name": "Madera aserrada de pino",
                    "stage": "FINISHED_GOOD",
                    "status": "ACTIVE",
                },
                "shipped_quantity": "60.000000",
                "unit": "M3",
                "attributed_quantity": attributed,
                "unresolved_quantity": unresolved,
                "complete": complete,
                "source_contributions": [
                    {
                        "lote": {"identificador": "RODAL-A", "productor_id": "CUIT-PROVEEDOR-A"},
                        "attributed_shipment_quantity": "42.000000",
                        "unit": "M3",
                        "share_of_shipment_item": "0.700000",
                    },
                    {
                        "lote": {"identificador": "RODAL-B", "productor_id": "CUIT-PROVEEDOR-B"},
                        "attributed_shipment_quantity": "18.000000",
                        "unit": "M3",
                        "share_of_shipment_item": "0.300000",
                    },
                ],
            }
        ],
    }


def test_graph_uses_same_presenter_for_corrientes_70_30_case() -> None:
    view = build_traceability_view(query="EXP-UE-2026-001", payload=_payload())
    result = view["result"]

    assert result is not None
    assert result["complete"] is True
    assert result["sources"][0]["identifier"] == "RODAL-A"
    assert result["sources"][0]["quantity"] == "42 m³"
    assert result["sources"][0]["share"] == "70%"
    assert result["sources"][1]["quantity"] == "18 m³"
    assert result["sources"][1]["share"] == "30%"
    assert result["events"][0]["input_quantity"] == "100 m³"
    assert result["events"][0]["output_quantity"] == "65 m³"
    assert result["events"][0]["loss_quantity"] == "35 m³"
    assert result["events"][0]["yield"] == "65%"
    assert result["items"][0]["shipped_quantity"] == "60 m³"
    assert result["items"][0]["contributions"][0]["quantity"] == "42 m³"
    assert result["items"][0]["contributions"][1]["quantity"] == "18 m³"


def test_incomplete_graph_fails_closed_with_unresolved_volume() -> None:
    view = build_traceability_view(
        query="EXP-INCOMPLETO-001",
        payload=_payload(complete=False),
    )
    result = view["result"]

    assert result is not None
    assert result["complete"] is False
    assert result["state_label"] == "Cadena incompleta"
    assert result["primary_total"]["unresolved"] == "25 m³"
    assert result["items"][0]["complete"] is False
    assert result["items"][0]["unresolved_quantity"] == "25 m³"
    assert result["issues"][0]["code"] == "MISSING_PROVENANCE"


def test_graph_template_is_server_rendered_accessible_and_fail_closed() -> None:
    template = GRAPH_TEMPLATE.read_text(encoding="utf-8")

    assert 'data-lineage-graph' in template
    assert 'id="lineage-graph-heading"' in template
    assert 'aria-label="Flujo de genealogía desde origen hasta despacho"' in template
    assert "Origen atribuido" in template
    assert "Cadena industrial documentada" in template
    assert "Lotes, despacho y expediente" in template
    assert "data-lineage-gap" in template
    assert "La cadena visual contiene una brecha documentada" in template
    assert "no se dibuja una conexión cerrada" in template.lower()
    assert "fetch(" not in template
    assert "XMLHttpRequest" not in template


def test_graph_copy_keeps_proportional_allocation_scope_explicit() -> None:
    template = GRAPH_TEMPLATE.read_text(encoding="utf-8")

    assert "PROPORTIONAL_INPUT_ALLOCATION" in template
    assert "convención contable de trazabilidad" in template
    assert "no identifica físicamente fibras individuales" in template
    assert "EUDR compliant" not in template
    assert "cumplimiento garantizado" not in template.lower()


def test_traceability_page_mounts_visual_graph_before_detail_tables() -> None:
    template = TRACEABILITY_TEMPLATE.read_text(encoding="utf-8")
    include = '{% include "app/_traceability_lineage_graph.html" %}'

    assert include in template
    assert template.index(include) < template.index("Origen consolidado")
    assert template.index(include) < template.index("Reconciliación")
