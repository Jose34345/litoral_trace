from __future__ import annotations

from uuid import uuid4

from litoral_trace.services.shipment_phytosanitary_case import (
    PhytosanitaryRequirementView,
    ShipmentPhytosanitaryReadinessView,
)
from litoral_trace.services.shipment_phytosanitary_release_control import (
    apply_phytosanitary_release_control,
)
from litoral_trace.web.traceability_evidence import _EVIDENCE_LABELS


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
                "key": "export_case",
                "title": "Expediente exportador Corrientes / ARCA",
                "state": "READY",
                "state_label": "Listo",
                "summary": "ok",
                "detail": "ok",
                "action_label": "Abrir",
                "action_href": "/export-case?shipment_code=EXP-001",
                "metric": "READY",
                "category": "Exportación",
            },
        ],
        "next_actions": [],
        "links": {"export_case": "/export-case?shipment_code=EXP-001"},
        "stages": [
            {"label": "Origen", "state": "READY", "caption": "ok"},
            {"label": "Salida", "state": "READY", "caption": "Expediente verificable"},
        ],
    }


def _readiness(*, ready: bool) -> ShipmentPhytosanitaryReadinessView:
    requirement = PhytosanitaryRequirementView(
        key="EPHYTO_XML_EVIDENCE",
        label="XML ePhyto disponible en Vault",
        satisfied=ready,
        source="Vault",
    )
    return ShipmentPhytosanitaryReadinessView(
        shipment_public_id=uuid4(),
        shipment_code="EXP-001",
        state="READY" if ready else "BLOCKED",
        certification_mode="EPHYTO",
        requirements=(requirement,),
        missing=() if ready else ("EPHYTO_XML_EVIDENCE",),
        evidence_types=("EPHYTO_XML",) if ready else (),
        phytosanitary_case=None,
    )


def test_p1c_domestic_release_control_remains_unchanged() -> None:
    control = _base_control()
    payload = {"shipment": {"shipment_code": "AR-001", "destination_country": "AR"}}

    result = apply_phytosanitary_release_control(
        control,
        lineage_payload=payload,
        readiness=None,
    )

    assert result is control
    assert all(row["key"] != "phytosanitary_case" for row in result["checks"])
    assert "phytosanitary_case" not in result["links"]


def test_p1c_international_blocked_case_blocks_release_control() -> None:
    payload = {"shipment": {"shipment_code": "EXP-001", "destination_country": "DE"}}

    result = apply_phytosanitary_release_control(
        _base_control(),
        lineage_payload=payload,
        readiness=_readiness(ready=False),
    )

    check = next(row for row in result["checks"] if row["key"] == "phytosanitary_case")
    assert check["state"] == "BLOCKED"
    assert "XML ePhyto" in check["detail"]
    assert check["action_href"] == "/phytosanitary-case?shipment_code=EXP-001"
    assert result["overall"]["state"] == "BLOCKED"
    assert result["overall"]["blocked"] == 1
    assert result["links"]["phytosanitary_case"] == "/phytosanitary-case?shipment_code=EXP-001"
    assert next(row for row in result["stages"] if row["label"] == "Salida")["state"] == "BLOCKED"


def test_p1c_international_ready_case_composes_with_p1b_ready() -> None:
    payload = {"shipment": {"shipment_code": "EXP-001", "destination_country": "NL"}}

    result = apply_phytosanitary_release_control(
        _base_control(),
        lineage_payload=payload,
        readiness=_readiness(ready=True),
    )

    keys = [row["key"] for row in result["checks"]]
    assert "export_case" in keys
    assert "phytosanitary_case" in keys
    assert next(row for row in result["checks"] if row["key"] == "phytosanitary_case")["state"] == "READY"
    assert result["overall"]["state"] == "READY"
    assert result["overall"]["ready"] == 3
    assert result["overall"]["total"] == 3
    assert result["overall"]["progress"] == 100


def test_p1c_browser_evidence_picker_exposes_required_senasa_types() -> None:
    assert _EVIDENCE_LABELS["PHYTOSANITARY_CERTIFICATE"] == "Certificado fitosanitario SENASA"
    assert _EVIDENCE_LABELS["EPHYTO_XML"] == "ePhyto XML"
