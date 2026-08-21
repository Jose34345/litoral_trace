"""P1D UI acceptance contracts for the traceability workspace."""
from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI

from litoral_trace.api.traceability import router as traceability_domain_router
from litoral_trace.web.traceability import (
    build_traceability_view,
    router as traceability_web_router,
)


API_PATH = "/api/v1/traceability/shipments/{shipment_code}/origin"


def _complete_payload() -> dict:
    return {
        "organization_id": 17,
        "shipment": {
            "id": 1,
            "public_id": "00000000-0000-0000-0000-000000000001",
            "shipment_code": "EXP-UE-2026-001",
            "sale_reference": "FACTURA-E-001",
            "buyer_reference": "BUYER-EU-001",
            "destination_country": "DE",
            "shipped_at": "2026-08-20T18:00:00+00:00",
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
        "items": [
            {
                "shipment_item_id": 1,
                "batch": {
                    "id": 3,
                    "public_id": "00000000-0000-0000-0000-000000000003",
                    "code": "ASERRADO-001",
                    "product_name": "Madera aserrada de pino",
                    "stage": "FINISHED_GOOD",
                    "unit": "M3",
                    "status": "ACTIVE",
                    "source_lote_id": None,
                },
                "shipped_quantity": "60.000000",
                "unit": "M3",
                "attributed_quantity": "60.000000",
                "unresolved_quantity": "0.000000",
                "complete": True,
                "issues": [],
                "source_contributions": [
                    {
                        "lote": {
                            "identificador": "RODAL-A",
                            "productor_id": "CUIT-PROVEEDOR-A",
                        },
                        "attributed_shipment_quantity": "42.000000",
                        "unit": "M3",
                        "share_of_shipment_item": "0.700000",
                    },
                    {
                        "lote": {
                            "identificador": "RODAL-B",
                            "productor_id": "CUIT-PROVEEDOR-B",
                        },
                        "attributed_shipment_quantity": "18.000000",
                        "unit": "M3",
                        "share_of_shipment_item": "0.300000",
                    },
                ],
            }
        ],
        "events": [
            {
                "id": 3,
                "public_id": "00000000-0000-0000-0000-000000000013",
                "event_code": "ASERRADO-TURNO-001",
                "event_type": "TRANSFORMATION",
                "status": "POSTED",
                "occurred_at": "2026-08-20T18:00:00+00:00",
                "facility_reference": "Planta Virasoro",
                "inputs": [
                    {
                        "batch_id": 1,
                        "batch_code": "REC-A-001",
                        "quantity": "70.000000",
                        "unit": "M3",
                    },
                    {
                        "batch_id": 2,
                        "batch_code": "REC-B-001",
                        "quantity": "30.000000",
                        "unit": "M3",
                    },
                ],
                "outputs": [
                    {
                        "batch_id": 3,
                        "batch_code": "ASERRADO-001",
                        "quantity": "65.000000",
                        "unit": "M3",
                    }
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
        "source_lotes": [
            {
                "lote": {
                    "id": 1,
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
                    "id": 2,
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
    }


def test_p1d_presenter_formats_corrientes_70_30_genealogy() -> None:
    view = build_traceability_view(
        query="EXP-UE-2026-001",
        payload=_complete_payload(),
    )

    result = view["result"]
    assert result is not None
    assert result["complete"] is True
    assert result["state_label"] == "Genealogía cerrada"
    assert result["primary_total"] == {
        "shipped": "60 m³",
        "attributed": "60 m³",
        "unresolved": "0 m³",
    }
    assert result["sources"][0]["identifier"] == "RODAL-A"
    assert result["sources"][0]["quantity"] == "42 m³"
    assert result["sources"][0]["share"] == "70%"
    assert result["sources"][1]["quantity"] == "18 m³"
    assert result["sources"][1]["share"] == "30%"
    assert result["events"][0]["type_label"] == "Transformación"
    assert result["events"][0]["yield"] == "65%"
    assert result["events"][0]["loss_quantity"] == "35 m³"


def test_p1d_incomplete_lineage_is_never_presented_as_closed() -> None:
    payload = _complete_payload()
    payload["complete"] = False
    payload["issues"] = [
        {
            "code": "MISSING_PROVENANCE",
            "message": "Falta un origen documentado.",
        }
    ]
    payload["unit_totals"][0]["attributed_quantity"] = "35.000000"
    payload["unit_totals"][0]["unresolved_quantity"] = "25.000000"

    view = build_traceability_view(
        query="EXP-INCOMPLETO-001",
        payload=payload,
    )
    result = view["result"]

    assert result is not None
    assert result["complete"] is False
    assert result["state_label"] == "Cadena incompleta"
    assert result["primary_total"]["unresolved"] == "25 m³"
    assert result["issues"] == [
        {
            "code": "MISSING_PROVENANCE",
            "message": "Falta un origen documentado.",
        }
    ]


def test_p1d_web_router_and_p1c_api_are_both_registered() -> None:
    ui_app = FastAPI()
    ui_app.include_router(traceability_web_router)
    assert str(ui_app.url_path_for("render_traceability_view")) == "/traceability"

    domain_app = FastAPI()
    domain_app.include_router(traceability_domain_router)
    openapi = domain_app.openapi()
    assert API_PATH in openapi["paths"]
    assert "get" in openapi["paths"][API_PATH]
    assert "/traceability" not in openapi["paths"]


def test_p1d_template_is_read_only_and_fail_closed_in_copy() -> None:
    root = Path(__file__).resolve().parents[1]
    template = (
        root / "src/litoral_trace/templates/traceability.html"
    ).read_text(encoding="utf-8")

    assert 'method="get"' in template
    assert 'action="/traceability"' in template
    assert "Genealogía cerrada" not in template
    assert "Cadena incompleta" not in template
    assert "convención contable de trazabilidad" in template
    assert "no modifica inventario ni eventos" in template
    assert "fetch(" not in template
