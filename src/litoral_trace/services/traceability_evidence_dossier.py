"""Project contextual evidence for one P1C shipment lineage into buyer-safe rows.

This module never reconstructs genealogy. It consumes the internal identifiers
already present in one P1C payload only to select evidence links belonging to
that exact graph, then emits no internal database identifiers.
"""
from __future__ import annotations

from typing import Any

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from litoral_trace.db.models import TraceabilityEvidenceLink, VaultDocument


def _iso(value: Any) -> str | None:
    return value.isoformat() if value is not None else None


def _lineage_subject_maps(payload: dict[str, Any]) -> dict[str, dict[int, str]]:
    source_map: dict[int, str] = {}
    event_map: dict[int, str] = {}
    batch_map: dict[int, str] = {}
    shipment_map: dict[int, str] = {}

    shipment = payload.get("shipment") or {}
    if shipment.get("id") is not None:
        shipment_map[int(shipment["id"])] = str(
            shipment.get("shipment_code") or "Despacho"
        )

    for source in payload.get("source_lotes") or []:
        lote = source.get("lote") or {}
        if lote.get("id") is not None:
            source_map[int(lote["id"])] = str(
                lote.get("identificador") or "Origen"
            )

    for event in payload.get("events") or []:
        if event.get("id") is not None:
            event_map[int(event["id"])] = str(
                event.get("event_code") or "Movimiento"
            )
        for edge in [*(event.get("inputs") or []), *(event.get("outputs") or [])]:
            if edge.get("batch_id") is not None:
                batch_map[int(edge["batch_id"])] = str(
                    edge.get("batch_code") or "Lote"
                )

    for item in payload.get("items") or []:
        batch = item.get("batch") or {}
        if batch.get("id") is not None:
            batch_map[int(batch["id"])] = str(batch.get("code") or "Lote")

    return {
        "SOURCE_LOTE": source_map,
        "TRACEABILITY_EVENT": event_map,
        "TRACEABILITY_BATCH": batch_map,
        "SHIPMENT": shipment_map,
    }


def project_documentary_evidence(
    *,
    session: Session,
    organization_id: int,
    lineage_payload: dict[str, Any],
) -> tuple[dict[str, Any], ...]:
    """Return deterministic buyer-safe evidence rows for one lineage graph."""

    maps = _lineage_subject_maps(lineage_payload)
    filters = []
    if maps["SOURCE_LOTE"]:
        filters.append(
            TraceabilityEvidenceLink.source_lote_id.in_(maps["SOURCE_LOTE"])
        )
    if maps["TRACEABILITY_EVENT"]:
        filters.append(
            TraceabilityEvidenceLink.traceability_event_id.in_(
                maps["TRACEABILITY_EVENT"]
            )
        )
    if maps["TRACEABILITY_BATCH"]:
        filters.append(
            TraceabilityEvidenceLink.traceability_batch_id.in_(
                maps["TRACEABILITY_BATCH"]
            )
        )
    if maps["SHIPMENT"]:
        filters.append(
            TraceabilityEvidenceLink.shipment_id.in_(maps["SHIPMENT"])
        )
    if not filters:
        return ()

    rows = session.execute(
        select(TraceabilityEvidenceLink, VaultDocument)
        .join(
            VaultDocument,
            VaultDocument.id == TraceabilityEvidenceLink.vault_document_id,
        )
        .where(
            TraceabilityEvidenceLink.organization_id == int(organization_id),
            TraceabilityEvidenceLink.unlinked_at.is_(None),
            VaultDocument.organization_id == int(organization_id),
            VaultDocument.status == "available",
            or_(*filters),
        )
    ).all()

    projected: list[dict[str, Any]] = []
    for link, document in rows:
        if link.source_lote_id is not None:
            subject_type = "SOURCE_LOTE"
            subject_reference = maps[subject_type].get(int(link.source_lote_id))
        elif link.traceability_event_id is not None:
            subject_type = "TRACEABILITY_EVENT"
            subject_reference = maps[subject_type].get(
                int(link.traceability_event_id)
            )
        elif link.traceability_batch_id is not None:
            subject_type = "TRACEABILITY_BATCH"
            subject_reference = maps[subject_type].get(
                int(link.traceability_batch_id)
            )
        else:
            subject_type = "SHIPMENT"
            subject_reference = maps[subject_type].get(int(link.shipment_id))

        # A link can be valid for the tenant yet not belong to this exact P1C
        # graph when the OR predicate matched another subject column. Skip any
        # row whose selected subject is not represented in the payload map.
        if not subject_reference:
            continue

        projected.append(
            {
                "subject_type": subject_type,
                "subject_reference": subject_reference,
                "evidence_type": link.evidence_type,
                "reference_number": link.reference_number,
                "issuer": link.issuer,
                "document_date": _iso(link.document_date),
                "valid_from": _iso(link.valid_from),
                "valid_until": _iso(link.valid_until),
                "notes": link.notes,
                "document": {
                    "public_id": str(document.public_id),
                    "filename": document.original_filename,
                    "content_type": document.content_type,
                    "size_bytes": int(document.size_bytes),
                    "sha256": document.sha256,
                },
            }
        )

    projected.sort(
        key=lambda item: (
            item["subject_type"],
            item["subject_reference"],
            item["evidence_type"],
            item["document"]["sha256"],
            item["document"]["filename"],
        )
    )
    return tuple(projected)
