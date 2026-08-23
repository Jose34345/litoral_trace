"""Compose P1-B export readiness into the existing release-control view."""
from __future__ import annotations

from copy import deepcopy
from typing import Any
from urllib.parse import urlencode

from litoral_trace.services.shipment_export_case import ShipmentExportReadinessView

READY = "READY"
ATTENTION = "ATTENTION"
BLOCKED = "BLOCKED"

_STATE_LABELS = {
    READY: "Listo",
    ATTENTION: "Requiere atención",
    BLOCKED: "Bloqueado",
}
_STATE_PRIORITY = {BLOCKED: 0, ATTENTION: 1, READY: 2}


def is_international_shipment(lineage_payload: dict[str, Any]) -> bool:
    shipment = lineage_payload.get("shipment") or {}
    country = str(shipment.get("destination_country") or "").strip().upper()
    return bool(country and country != "AR")


def apply_export_case_release_control(
    control: dict[str, Any],
    *,
    lineage_payload: dict[str, Any],
    readiness: ShipmentExportReadinessView | None,
) -> dict[str, Any]:
    """Return release control with a P1-B gate for international shipments.

    Domestic or destination-less shipments remain byte-for-byte equivalent at
    the semantic level: no export check is added. International shipments fail
    closed if readiness is missing or BLOCKED.
    """
    if not is_international_shipment(lineage_payload):
        return control

    result = deepcopy(control)
    shipment = lineage_payload.get("shipment") or {}
    shipment_code = str(shipment.get("shipment_code") or "").strip()
    query = urlencode({"shipment_code": shipment_code}) if shipment_code else ""
    href = f"/export-case?{query}" if query else "/export-case"

    if readiness is None:
        state = BLOCKED
        summary = "Falta evaluar el expediente exportador Corrientes/ARCA."
        detail = "El despacho internacional no puede cerrarse sin una evaluación documental exportadora."
        metric = "Sin evaluar"
    elif readiness.ready:
        state = READY
        summary = "El expediente Corrientes/ARCA tiene completos sus requisitos configurados."
        detail = (
            f"Perfil {readiness.origin_profile or '—'} · "
            f"{len(readiness.requirements)} requisito(s) documental(es) verificados."
        )
        metric = "READY"
    else:
        state = BLOCKED
        missing_labels = [
            row.label for row in readiness.requirements if not row.satisfied
        ]
        summary = "El expediente exportador tiene requisitos pendientes."
        detail = (
            "Falta completar: " + ", ".join(missing_labels) + "."
            if missing_labels
            else "El expediente exportador no está listo."
        )
        metric = f"{len(readiness.missing)} pendiente(s)"

    export_check = {
        "key": "export_case",
        "title": "Expediente Corrientes + ARCA",
        "state": state,
        "state_label": _STATE_LABELS[state],
        "summary": summary,
        "detail": detail,
        "action_label": "Abrir expediente exportador",
        "action_href": href,
        "metric": metric,
        "category": "Exportación",
    }
    result.setdefault("checks", []).append(export_check)
    result.setdefault("links", {})["export_case"] = href

    checks = list(result.get("checks") or [])
    ready = sum(item.get("state") == READY for item in checks)
    attention = sum(item.get("state") == ATTENTION for item in checks)
    blocked = sum(item.get("state") == BLOCKED for item in checks)
    total = len(checks)
    progress = int(round((ready / total) * 100)) if total else 0

    if blocked:
        overall_state = BLOCKED
        overall_title = "Bloqueado para compartir"
        overall_message = "Hay controles operativos que impiden presentar este despacho como expediente cerrado."
    elif attention:
        overall_state = ATTENTION
        overall_title = "Requiere atención antes de compartir"
        overall_message = "La trazabilidad esencial está cerrada, pero conviene resolver las brechas visibles."
    else:
        overall_state = READY
        overall_title = "Listo para compartir"
        overall_message = "Los controles operativos de Litoral Trace están cerrados y el expediente es verificable."

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
            stage["caption"] = "Expediente exportador pendiente"
            break
    result["stages"] = stages
    return result
