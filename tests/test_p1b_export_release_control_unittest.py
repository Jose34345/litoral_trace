from __future__ import annotations

from uuid import uuid4

from litoral_trace.services.shipment_export_case import (
    ExportRequirementView,
    ShipmentExportReadinessView,
)
from litoral_trace.services.shipment_export_release_control import (
    apply_export_case_release_control,
    is_international_shipment,
)


def _base_control() -> dict:
    return {
        "overall": {
            "state": "READY",
            "state_label": "Listo",
            "title": "Listo para compartir",
            "message": "ok",
            "ready": 2,
            "attention": 0,
            "blocked": 0,
            "total": 2,
            "progress": 100,
        },
        "checks": [
            {
                "key": "dispatch",
                "title": "Estado del despacho",
                "state": "READY",
                "state_label": "Listo",
                "summary": "ok",
                "detail": "ok",
                "action_label": "Abrir",
                "action_href": "/operations",
                "metric": "Final",
                "category": "Despacho",
            },
            {
                "key": "dossier",
                "title": "Expediente verificable",
                "state": "READY",
                "state_label": "Listo",
                "summary": "ok",
                "detail": "ok",
                "action_label": "Abrir",
                "action_href": "/traceability",
                "metric": "ok",
                "category": "Expediente",
            },
        ],
        "next_actions": [],
        "links": {"operations": "/operations"},
        "stages": [
            {"label": "Origen", "state": "READY", "caption": "ok"},
            {"label": "Salida", "state": "READY", "caption": "Expediente verificable"},
        ],
    }


def _readiness(*, ready: bool) -> ShipmentExportReadinessView:
    requirement = ExportRequirementView(
        key="SIM_DESTINATION",
        label="Identificador de destinación aduanera SIM",
        satisfied=ready,
        source="SIM",
    )
    return ShipmentExportReadinessView(
        shipment_public_id=uuid4(),
        shipment_code="EXP-001",
        state="READY" if ready else "BLOCKED",
        origin_profile="CULTIVATED",
        requirements=(requirement,),
        missing=() if ready else ("SIM_DESTINATION",),
        evidence_types=("REMITO", "FRUIT_GUIDE"),
        export_case=None,
    )


def test_p1b_domestic_release_control_remains_unchanged() -> None:
    control = _base_control()
    payload = {
        "shipment": {
            "shipment_code": "AR-001",
            "destination_country": "AR",
        }
    }

    assert is_international_shipment(payload) is False
    result = apply_export_case_release_control(
        control,
        lineage_payload=payload,
        readiness=None,
    )

    assert result is control
    assert all(row["key"] != "export_case" for row in result["checks"])
    assert "export_case" not in result["links"]


def test_p1b_international_blocked_case_blocks_release_control() -> None:
    payload = {
        "shipment": {
            "shipment_code": "EXP-001",
            "destination_country": "DE",
        }
    }
    result = apply_export_case_release_control(
        _base_control(),
        lineage_payload=payload,
        readiness=_readiness(ready=False),
    )

    export_check = next(row for row in result["checks"] if row["key"] == "export_case")
    assert export_check["state"] == "BLOCKED"
    assert "destinación aduanera SIM" in export_check["detail"]
    assert export_check["action_href"] == "/export-case?shipment_code=EXP-001"
    assert result["overall"]["state"] == "BLOCKED"
    assert result["overall"]["blocked"] == 1
    assert result["links"]["export_case"] == "/export-case?shipment_code=EXP-001"
    assert next(row for row in result["stages"] if row["label"] == "Salida")["state"] == "BLOCKED"


def test_p1b_international_ready_case_keeps_release_ready() -> None:
    payload = {
        "shipment": {
            "shipment_code": "EXP-001",
            "destination_country": "NL",
        }
    }
    result = apply_export_case_release_control(
        _base_control(),
        lineage_payload=payload,
        readiness=_readiness(ready=True),
    )

    export_check = next(row for row in result["checks"] if row["key"] == "export_case")
    assert export_check["state"] == "READY"
    assert result["overall"] == {
        "state": "READY",
        "state_label": "Listo",
        "title": "Listo para compartir",
        "message": "Los controles operativos de Litoral Trace están cerrados y el expediente es verificable.",
        "ready": 3,
        "attention": 0,
        "blocked": 0,
        "total": 3,
        "progress": 100,
    }
