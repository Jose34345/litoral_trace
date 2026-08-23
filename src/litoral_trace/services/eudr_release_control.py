"""Compose P1-D local EUDR conformance into international release control."""
from __future__ import annotations

from copy import deepcopy
from typing import Any
from urllib.parse import urlencode

from litoral_trace.services.eudr_dds_candidate import EudrDdsConformanceView
from litoral_trace.services.shipment_export_release_control import (
    ATTENTION,
    BLOCKED,
    READY,
    _STATE_LABELS,
    _STATE_PRIORITY,
    is_international_shipment,
)


def apply_eudr_conformance_release_control(
    control: dict[str, Any],
    *,
    lineage_payload: dict[str, Any],
    conformance: EudrDdsConformanceView | None,
) -> dict[str, Any]:
    """Add a non-legal, fail-closed EUDR conformance gate for export shipments."""
    if not is_international_shipment(lineage_payload):
        return control

    result = deepcopy(control)
    shipment = lineage_payload.get("shipment") or {}
    shipment_code = str(shipment.get("shipment_code") or "").strip()
    query = urlencode({"shipment_code": shipment_code}) if shipment_code else ""
    href = f"/eudr-acceptance?{query}" if query else "/eudr-acceptance"

    if conformance is None:
        state = BLOCKED
        summary = "Falta preparar el candidato DDS EUDR."
        detail = (
            "El despacho internacional no tiene un candidato local conformance-ready. "
            "Esto no implica una presentación legal ni un resultado ACCEPTANCE."
        )
        metric = "Sin candidato"
    elif conformance.ready:
        state = READY
        summary = "El candidato DDS cumple el perfil local de conformance API V3."
        detail = (
            f"Fingerprint {conformance.payload_sha256 or '—'} · "
            "sin envío a ACCEPTANCE/LIVE."
        )
        metric = "CONFORMANCE_READY"
    else:
        state = BLOCKED
        missing_labels = [
            row.label for row in conformance.requirements if not row.satisfied
        ]
        summary = "El candidato DDS tiene requisitos de conformance pendientes."
        detail = (
            "Falta completar: " + ", ".join(missing_labels) + "."
            if missing_labels
            else "El candidato DDS no está listo para conformance."
        )
        metric = f"{len(conformance.missing)} pendiente(s)"

    result.setdefault("checks", []).append(
        {
            "key": "eudr_conformance",
            "title": "EUDR API V3 · conformance local",
            "state": state,
            "state_label": _STATE_LABELS[state],
            "summary": summary,
            "detail": detail,
            "action_label": "Abrir candidato EUDR",
            "action_href": href,
            "metric": metric,
            "category": "Exportación",
        }
    )
    result.setdefault("links", {})["eudr_conformance"] = href

    checks = list(result.get("checks") or [])
    ready = sum(item.get("state") == READY for item in checks)
    attention = sum(item.get("state") == ATTENTION for item in checks)
    blocked = sum(item.get("state") == BLOCKED for item in checks)
    total = len(checks)
    progress = int(round((ready / total) * 100)) if total else 0

    if blocked:
        overall_state = BLOCKED
        overall_title = "Bloqueado para compartir"
        overall_message = (
            "Hay controles operativos que impiden presentar este despacho como expediente cerrado."
        )
    elif attention:
        overall_state = ATTENTION
        overall_title = "Requiere atención antes de compartir"
        overall_message = (
            "La trazabilidad esencial está cerrada, pero conviene resolver las brechas visibles."
        )
    else:
        overall_state = READY
        overall_title = "Listo para compartir"
        overall_message = (
            "Los controles operativos de Litoral Trace están cerrados y el expediente es verificable."
        )

    result["overall"] = {
        "state": overall_state,
        "state_label": _STATE_LABELS[overall_state],
        "title": overall_title,
        "message": overall_message,
        "ready": ready,
        "attention": attention,
        "blocked": blocked,
        "total": total,
        "progress": progress,
    }

    indexed_checks = list(enumerate(checks))
    ordered_actions = sorted(
        (pair for pair in indexed_checks if pair[1].get("state") != READY),
        key=lambda pair: (_STATE_PRIORITY[pair[1].get("state", BLOCKED)], pair[0]),
    )
    result["next_actions"] = [
        {
            "title": item["title"],
            "state": item["state"],
            "state_label": item["state_label"],
            "message": item["summary"],
            "href": item["action_href"],
            "label": item["action_label"],
        }
        for _, item in ordered_actions[:4]
    ]

    stages = list(result.get("stages") or [])
    for stage in stages:
        if stage.get("label") == "Salida" and state == BLOCKED:
            stage["state"] = BLOCKED
            stage["caption"] = "Conformance EUDR pendiente"
            break
    result["stages"] = stages
    return result
