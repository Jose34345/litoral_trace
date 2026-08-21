"""Operational release-control projection for one traced shipment.

The release control is deliberately not a regulatory score. It composes factual
signals already produced by P1C/P1E/UX10-E into a decision-oriented view:
what is closed, what needs attention, what blocks sharing, and where to act.
"""
from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any
from urllib.parse import urlencode


READY = "READY"
ATTENTION = "ATTENTION"
BLOCKED = "BLOCKED"

_STATE_LABELS = {
    READY: "Listo",
    ATTENTION: "Requiere atención",
    BLOCKED: "Bloqueado",
}

_STATE_PRIORITY = {
    BLOCKED: 0,
    ATTENTION: 1,
    READY: 2,
}

_SUBJECT_LABELS = {
    "SOURCE_LOTE": "Origen",
    "TRACEABILITY_EVENT": "Movimiento",
    "TRACEABILITY_BATCH": "Lote industrial",
    "SHIPMENT": "Despacho",
}


def _decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal("0")


def _text(value: Any, fallback: str = "—") -> str:
    normalized = str(value or "").strip()
    return normalized or fallback


def _check(
    *,
    key: str,
    title: str,
    state: str,
    summary: str,
    detail: str,
    action_label: str,
    action_href: str,
    metric: str | None = None,
    category: str,
) -> dict[str, Any]:
    return {
        "key": key,
        "title": title,
        "state": state,
        "state_label": _STATE_LABELS[state],
        "summary": summary,
        "detail": detail,
        "action_label": action_label,
        "action_href": action_href,
        "metric": metric,
        "category": category,
    }


def _lineage_subjects(payload: dict[str, Any]) -> set[tuple[str, str]]:
    subjects: set[tuple[str, str]] = set()
    shipment = payload.get("shipment") or {}
    shipment_code = _text(shipment.get("shipment_code"), "")
    if shipment_code:
        subjects.add(("SHIPMENT", shipment_code))

    for source in payload.get("source_lotes") or []:
        lote = source.get("lote") or {}
        identifier = _text(lote.get("identificador"), "")
        if identifier:
            subjects.add(("SOURCE_LOTE", identifier))

    for event in payload.get("events") or []:
        event_code = _text(event.get("event_code"), "")
        if event_code:
            subjects.add(("TRACEABILITY_EVENT", event_code))
        for edge in [*(event.get("inputs") or []), *(event.get("outputs") or [])]:
            batch_code = _text(edge.get("batch_code"), "")
            if batch_code:
                subjects.add(("TRACEABILITY_BATCH", batch_code))

    for item in payload.get("items") or []:
        batch = item.get("batch") or {}
        batch_code = _text(batch.get("code"), "")
        if batch_code:
            subjects.add(("TRACEABILITY_BATCH", batch_code))
    return subjects


def _documentary_coverage(
    payload: dict[str, Any],
    documentary_evidence: Any,
) -> dict[str, Any]:
    subjects = _lineage_subjects(payload)
    rows = list(documentary_evidence or [])
    covered = {
        (str(item.get("subject_type") or ""), str(item.get("subject_reference") or ""))
        for item in rows
        if item.get("subject_type") and item.get("subject_reference")
    }
    covered &= subjects
    total = len(subjects)
    covered_count = len(covered)
    percentage = int(round((covered_count / total) * 100)) if total else 0

    by_type: list[dict[str, Any]] = []
    for subject_type, label in _SUBJECT_LABELS.items():
        typed = {item for item in subjects if item[0] == subject_type}
        typed_covered = typed & covered
        by_type.append(
            {
                "key": subject_type,
                "label": label,
                "covered": len(typed_covered),
                "total": len(typed),
            }
        )
    return {
        "total_subjects": total,
        "covered_subjects": covered_count,
        "percentage": percentage,
        "evidence_count": len(rows),
        "by_type": by_type,
    }


def build_release_control_view(
    payload: dict[str, Any],
    *,
    documentary_evidence: Any = None,
    manifest_sha256: str | None = None,
    dossier_available: bool = True,
    dossier_error: str | None = None,
) -> dict[str, Any]:
    """Return a deterministic, buyer-safe operational release-control view."""

    shipment = payload.get("shipment") or {}
    shipment_code = _text(shipment.get("shipment_code"), "")
    query = urlencode({"shipment_code": shipment_code}) if shipment_code else ""
    traceability_href = f"/traceability?{query}" if query else "/traceability"
    release_href = f"/release-control?{query}" if query else "/release-control"
    evidence_href = "/evidence"
    operations_href = "/operations"
    dossier_base = "/api/v1/traceability/shipments/dossier"

    checks: list[dict[str, Any]] = []

    status = str(shipment.get("status") or "").upper()
    lineage_state = str(shipment.get("lineage_state") or "").upper()
    dispatch_ready = status == "DISPATCHED" and lineage_state == "FINAL"
    checks.append(
        _check(
            key="dispatch",
            title="Estado del despacho",
            state=READY if dispatch_ready else BLOCKED,
            summary=(
                "El despacho está cerrado y la genealogía se consulta en estado final."
                if dispatch_ready
                else "El despacho todavía no está en estado final para compartir."
            ),
            detail=f"Estado: {_text(shipment.get('status'))} · Genealogía: {_text(shipment.get('lineage_state'))}.",
            action_label="Revisar operación",
            action_href=operations_href,
            metric="Final" if dispatch_ready else "Pendiente",
            category="Despacho",
        )
    )

    complete = bool(payload.get("complete"))
    issues = payload.get("issues") or []
    checks.append(
        _check(
            key="lineage",
            title="Genealogía de origen",
            state=READY if complete else BLOCKED,
            summary=(
                "Todo el volumen despachado quedó atribuido a orígenes recorribles."
                if complete
                else "La genealogía tiene volumen sin resolver o incidencias abiertas."
            ),
            detail=(
                f"{len(issues)} incidencia(s) reportada(s) por el motor de trazabilidad."
                if issues
                else "El motor no reporta incidencias de genealogía."
            ),
            action_label="Ver genealogía",
            action_href=traceability_href,
            metric="Cerrada" if complete else f"{len(issues)} incidencia(s)",
            category="Trazabilidad",
        )
    )

    totals = payload.get("unit_totals") or []
    unresolved = sum((_decimal(item.get("unresolved_quantity")) for item in totals), Decimal("0"))
    volume_ready = bool(totals) and unresolved == 0
    if not totals:
        volume_detail = "No hay reconciliación de volumen disponible para el despacho."
    elif unresolved == 0:
        volume_detail = f"{len(totals)} unidad(es) reconciliada(s), sin volumen pendiente."
    else:
        volume_detail = f"Quedan {unresolved.normalize()} unidades sin atribuir en la reconciliación."
    checks.append(
        _check(
            key="volume",
            title="Reconciliación de volumen",
            state=READY if volume_ready else BLOCKED,
            summary=(
                "El volumen despachado está totalmente reconciliado."
                if volume_ready
                else "Existe volumen no reconciliado o falta el balance del despacho."
            ),
            detail=volume_detail,
            action_label="Abrir trazabilidad",
            action_href=traceability_href,
            metric="0 sin resolver" if volume_ready else "Revisar balance",
            category="Volumen",
        )
    )

    sources = payload.get("source_lotes") or []
    polygon_count = 0
    point_fallback_count = 0
    missing_geo_count = 0
    for source in sources:
        lote = source.get("lote") or {}
        if lote.get("polygon_wkt"):
            polygon_count += 1
        elif lote.get("latitud") is not None and lote.get("longitud") is not None:
            point_fallback_count += 1
        else:
            missing_geo_count += 1
    if not sources or missing_geo_count:
        geometry_state = BLOCKED
        geometry_summary = "Falta una ubicación utilizable para al menos un origen."
    elif point_fallback_count:
        geometry_state = ATTENTION
        geometry_summary = "Hay orígenes representados sólo por punto; conviene completar su polígono."
    else:
        geometry_state = READY
        geometry_summary = "Todos los orígenes atribuidos tienen polígono registrado."
    checks.append(
        _check(
            key="geometry",
            title="Geometría de origen",
            state=geometry_state,
            summary=geometry_summary,
            detail=(
                f"Polígonos: {polygon_count} · Puntos de respaldo: {point_fallback_count} · Sin ubicación: {missing_geo_count}."
            ),
            action_label="Revisar orígenes",
            action_href=traceability_href,
            metric=f"{polygon_count}/{len(sources)} polígonos" if sources else "Sin orígenes",
            category="Origen",
        )
    )

    coverage = _documentary_coverage(payload, documentary_evidence)
    if coverage["total_subjects"] == 0:
        evidence_state = BLOCKED
        evidence_summary = "No hay eslabones recorribles sobre los que evaluar respaldo documental."
    elif coverage["covered_subjects"] == coverage["total_subjects"]:
        evidence_state = READY
        evidence_summary = "Todos los eslabones de esta genealogía tienen al menos una evidencia vinculada."
    else:
        evidence_state = ATTENTION
        evidence_summary = "La cadena es recorrible, pero todavía tiene brechas documentales visibles."
    checks.append(
        _check(
            key="evidence",
            title="Huella Documental Litoral Trace",
            state=evidence_state,
            summary=evidence_summary,
            detail=(
                f"{coverage['covered_subjects']} de {coverage['total_subjects']} eslabones con evidencia · "
                f"{coverage['evidence_count']} referencia(s) documental(es)."
            ),
            action_label="Completar evidencia",
            action_href=evidence_href,
            metric=f"{coverage['covered_subjects']}/{coverage['total_subjects']} eslabones",
            category="Evidencia",
        )
    )

    commercial_fields = {
        "Referencia de venta": shipment.get("sale_reference"),
        "Comprador": shipment.get("buyer_reference"),
        "País destino": shipment.get("destination_country"),
        "Fecha de despacho": shipment.get("shipped_at"),
    }
    missing_commercial = [label for label, value in commercial_fields.items() if not _text(value, "")]
    commercial_state = READY if not missing_commercial else ATTENTION
    checks.append(
        _check(
            key="commercial",
            title="Contexto comercial",
            state=commercial_state,
            summary=(
                "Las referencias comerciales principales están completas."
                if commercial_state == READY
                else "La trazabilidad está disponible, pero faltan datos útiles para el comprador."
            ),
            detail=(
                "Venta, comprador, destino y fecha están informados."
                if not missing_commercial
                else "Falta completar: " + ", ".join(missing_commercial) + "."
            ),
            action_label="Revisar despacho",
            action_href=operations_href,
            metric="Completo" if commercial_state == READY else f"{len(missing_commercial)} pendiente(s)",
            category="Comercial",
        )
    )

    dossier_state = READY if dossier_available and manifest_sha256 else BLOCKED
    checks.append(
        _check(
            key="dossier",
            title="Expediente verificable",
            state=dossier_state,
            summary=(
                "El expediente puede compartirse con una huella SHA-256 verificable."
                if dossier_state == READY
                else "El expediente no pudo generarse de forma verificable."
            ),
            detail=(
                f"SHA-256 del manifest: {manifest_sha256}"
                if dossier_state == READY
                else (dossier_error or "El generador del expediente no está disponible.")
            ),
            action_label="Descargar expediente" if dossier_state == READY else "Revisar trazabilidad",
            action_href=(
                f"{dossier_base}/bundle?{query}" if dossier_state == READY and query else traceability_href
            ),
            metric=(manifest_sha256[:12] + "…") if dossier_state == READY else "No disponible",
            category="Expediente",
        )
    )

    blocked = sum(item["state"] == BLOCKED for item in checks)
    attention = sum(item["state"] == ATTENTION for item in checks)
    ready = sum(item["state"] == READY for item in checks)
    if blocked:
        overall = BLOCKED
        overall_title = "Bloqueado para compartir"
        overall_message = "Hay controles operativos que impiden presentar este despacho como expediente cerrado."
    elif attention:
        overall = ATTENTION
        overall_title = "Requiere atención antes de compartir"
        overall_message = "La trazabilidad esencial está cerrada, pero conviene resolver las brechas visibles."
    else:
        overall = READY
        overall_title = "Listo para compartir"
        overall_message = "Los controles operativos de Litoral Trace están cerrados y el expediente es verificable."

    ordered_actions = sorted(
        (item for item in checks if item["state"] != READY),
        key=lambda item: (_STATE_PRIORITY[item["state"]], checks.index(item)),
    )
    next_actions = [
        {
            "title": item["title"],
            "state": item["state"],
            "state_label": item["state_label"],
            "message": item["summary"],
            "href": item["action_href"],
            "label": item["action_label"],
        }
        for item in ordered_actions[:4]
    ]

    progress = int(round((ready / len(checks)) * 100)) if checks else 0
    return {
        "shipment": {
            "code": shipment_code or "—",
            "sale_reference": _text(shipment.get("sale_reference")),
            "buyer_reference": _text(shipment.get("buyer_reference")),
            "destination_country": _text(shipment.get("destination_country")),
            "status": _text(shipment.get("status")),
        },
        "overall": {
            "state": overall,
            "state_label": _STATE_LABELS[overall],
            "title": overall_title,
            "message": overall_message,
            "ready": ready,
            "attention": attention,
            "blocked": blocked,
            "total": len(checks),
            "progress": progress,
        },
        "checks": checks,
        "next_actions": next_actions,
        "documentary_coverage": coverage,
        "manifest_sha256": manifest_sha256,
        "links": {
            "self": release_href,
            "traceability": traceability_href,
            "evidence": evidence_href,
            "operations": operations_href,
            "dossier_bundle": f"{dossier_base}/bundle?{query}" if query else None,
            "dossier_pdf": f"{dossier_base}/pdf?{query}" if query else None,
        },
        "stages": [
            {
                "label": "Origen",
                "state": geometry_state,
                "caption": f"{len(sources)} origen(es)",
            },
            {
                "label": "Cadena",
                "state": READY if complete and volume_ready else BLOCKED,
                "caption": f"{len(payload.get('events') or [])} movimiento(s)",
            },
            {
                "label": "Evidencia",
                "state": evidence_state,
                "caption": f"{coverage['covered_subjects']}/{coverage['total_subjects']} eslabones",
            },
            {
                "label": "Salida",
                "state": dossier_state if dispatch_ready else BLOCKED,
                "caption": "Expediente verificable" if dossier_state == READY else "Revisar cierre",
            },
        ],
        "disclaimer": (
            "El Control de Salida Litoral Trace resume controles operativos y evidencia registrada. "
            "No constituye una certificación, una declaración regulatoria ni una conclusión automática de cumplimiento EUDR."
        ),
    }
