"""Tenant-scoped persistence boundary for deterministic regulatory assessments.

Assessments produced here are rule-scoped, reproducible work products. They are
not canonical shipment truth and they do not mutate PPQ 505, LAWGS or ACE data.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Mapping
from uuid import UUID

from sqlalchemy import and_, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from litoral_trace.db.models import (
    UsLaceyOperation,
    UsLaceyProductIntelligenceSnapshot,
    UsLaceyRegulatoryAssessmentSnapshot,
    UsLaceySourceSetRevision,
)
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.us_lacey.db import get_us_lacey_db_session
from litoral_trace.us_lacey.product_intelligence_snapshot import (
    enrich_product_intelligence_taxonomy,
    snapshot_matches_claim,
)
from litoral_trace.us_lacey.regulatory.rules import (
    RULESET_VERSION,
    DeMinimisInput,
    EvidenceRef,
    ProtectedPlantStatus,
    RuleAssessment,
    RuleStatus,
    SpecialCompositeInput,
    TriState,
    classify_composite_material_name,
    evaluate_de_minimis,
    evaluate_special_composite,
)


SNAPSHOT_SCHEMA_VERSION = "regulatory-assessment-snapshot-v1"
SessionFactory = Callable[[], Session]


@dataclass(frozen=True, slots=True)
class RegulatoryAssessmentView:
    status: str
    generation: int
    source_set_fingerprint: str
    ruleset_version: str
    input_fingerprint: str
    assessment_count: int
    indeterminate_count: int
    payload: dict[str, Any]


def fingerprint_rule_inputs(payload: Mapping[str, Any]) -> str:
    """Return the stable SHA-256 identity of exact rule-relevant inputs."""
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _evidence_ref(source: object) -> EvidenceRef:
    source_map = source if isinstance(source, Mapping) else {}
    document_id = source_map.get("document_id")
    sheet = source_map.get("sheet")
    row = source_map.get("row")
    locator = source_map.get("locator")
    if not locator:
        parts = []
        if sheet:
            parts.append(str(sheet))
        if row is not None:
            parts.append(f"row:{row}")
        locator = ":".join(parts) or None
    return EvidenceRef(
        source_type="BOM_MATERIAL",
        source_id=None if document_id is None else str(document_id),
        locator=None if locator is None else str(locator),
    )


def _serialize_assessment(assessment: RuleAssessment, *, subject_ref: str) -> dict[str, Any]:
    return {
        "rule_id": assessment.rule_id,
        "ruleset_version": assessment.ruleset_version,
        "subject_ref": subject_ref,
        "status": assessment.status.value,
        "reason_codes": list(assessment.reason_codes),
        "explanation": assessment.explanation,
        "calculation_trace": dict(assessment.calculation_trace),
        "evidence_refs": [
            {
                "source_type": ref.source_type,
                "source_id": ref.source_id,
                "locator": ref.locator,
            }
            for ref in assessment.evidence_refs
        ],
        "review_required": bool(assessment.review_required),
    }


def _iter_compositions(payload: Mapping[str, Any]):
    for source in payload.get("sources", ()):
        if not isinstance(source, Mapping):
            continue
        for table in source.get("tables", ()):
            if not isinstance(table, Mapping):
                continue
            for composition in table.get("compositions", ()):
                if isinstance(composition, Mapping):
                    yield composition


def build_regulatory_assessment_payload(
    *,
    product_intelligence_payload: Mapping[str, Any],
    source_set: Mapping[str, Any],
) -> dict[str, Any]:
    """Build conservative rule assessments from supported Product Intelligence facts.

    Hito 8 intentionally does not guess entry-level facts that Product Intelligence
    does not yet model (10-digit HTS grouping, total unit weight, protected status).
    De minimis therefore remains INDETERMINATE until those exact inputs exist.
    SPECIAL/COMPOSITE can still deterministically reject disqualifying construction
    such as plywood while other missing facts remain reviewable.
    """
    assessments: list[dict[str, Any]] = []

    for composition in _iter_compositions(product_intelligence_payload):
        sku = str(composition.get("sku") or "").strip()
        if not sku:
            continue

        de_minimis = evaluate_de_minimis(
            DeMinimisInput(
                subject_ref=sku,
                hts10=None,
                plant_mass_per_unit_kg=None,
                total_unit_mass_kg=None,
                entry_same_hts_plant_mass_kg=None,
                protected_status=ProtectedPlantStatus.UNKNOWN,
                evidence_refs=(),
            )
        )
        assessments.append(_serialize_assessment(de_minimis, subject_ref=sku))

        for component in composition.get("components", ()):
            if not isinstance(component, Mapping):
                continue
            material = component.get("material")
            if not isinstance(material, Mapping):
                continue
            subject_ref = str(component.get("component_key") or f"{sku}:component").strip()
            material_name = material.get("name_raw") or material.get("name_normalized") or ""
            facts = classify_composite_material_name(material_name)
            evidence = (_evidence_ref(material.get("source") or component.get("source")),)
            special = evaluate_special_composite(
                SpecialCompositeInput(
                    subject_ref=subject_ref,
                    small_fibers_more_than_one_plant_kind=facts.small_fibers_more_than_one_plant_kind,
                    mechanically_processed_mixed_chemically_bonded=facts.mechanically_processed_mixed_chemically_bonded,
                    thin_solid_plies_or_layers=facts.thin_solid_plies_or_layers,
                    # Material names/taxonomy candidates do not prove the due-care step.
                    species_determinable_after_due_care=TriState.UNKNOWN,
                    evidence_refs=evidence,
                )
            )
            assessments.append(_serialize_assessment(special, subject_ref=subject_ref))

    indeterminate_count = sum(item["status"] == RuleStatus.INDETERMINATE.value for item in assessments)
    return {
        "schema_version": SNAPSHOT_SCHEMA_VERSION,
        "ruleset_version": RULESET_VERSION,
        "source_set": {
            "revision_id": source_set.get("revision_id"),
            "generation": source_set.get("generation"),
            "fingerprint": source_set.get("fingerprint"),
        },
        "summary": {
            "assessment_count": len(assessments),
            "indeterminate_count": int(indeterminate_count),
        },
        "assessments": assessments,
    }


def mark_regulatory_assessment_snapshots_stale(
    session: Session,
    *,
    organization_id: int,
    operation_id: int,
) -> int:
    result = session.execute(
        update(UsLaceyRegulatoryAssessmentSnapshot)
        .where(
            UsLaceyRegulatoryAssessmentSnapshot.organization_id == int(organization_id),
            UsLaceyRegulatoryAssessmentSnapshot.operation_id == int(operation_id),
            UsLaceyRegulatoryAssessmentSnapshot.status != "STALE",
        )
        .values(status="STALE")
    )
    return int(result.rowcount or 0)


def build_regulatory_assessment_snapshot(
    *,
    organization_id: int,
    operation_id: int,
    claim: Any,
    session_factory: SessionFactory | None = None,
) -> UsLaceyRegulatoryAssessmentSnapshot | None:
    """Persist one idempotent assessment for the exact claimed source-set/ruleset."""
    if not getattr(claim, "claimed", False) or getattr(claim, "revision_id", None) is None:
        return None
    factory = session_factory or get_us_lacey_db_session
    organization_id = int(organization_id)
    operation_id = int(operation_id)
    revision_id = int(claim.revision_id)

    session = factory()
    try:
        set_tenant_db_context(session, organization_id)
        revision = session.scalar(
            select(UsLaceySourceSetRevision).where(
                UsLaceySourceSetRevision.organization_id == organization_id,
                UsLaceySourceSetRevision.operation_id == operation_id,
                UsLaceySourceSetRevision.id == revision_id,
            )
        )
        if revision is None or not snapshot_matches_claim(revision, claim):
            return None

        existing = session.scalar(
            select(UsLaceyRegulatoryAssessmentSnapshot).where(
                UsLaceyRegulatoryAssessmentSnapshot.organization_id == organization_id,
                UsLaceyRegulatoryAssessmentSnapshot.source_set_revision_id == revision_id,
                UsLaceyRegulatoryAssessmentSnapshot.ruleset_version == RULESET_VERSION,
            )
        )
        if existing is not None:
            return existing

        product_snapshot = session.scalar(
            select(UsLaceyProductIntelligenceSnapshot).where(
                UsLaceyProductIntelligenceSnapshot.organization_id == organization_id,
                UsLaceyProductIntelligenceSnapshot.operation_id == operation_id,
                UsLaceyProductIntelligenceSnapshot.source_set_revision_id == revision_id,
                UsLaceyProductIntelligenceSnapshot.status != "STALE",
            )
        )
        if product_snapshot is None:
            return None

        product_payload = enrich_product_intelligence_taxonomy(dict(product_snapshot.payload_json or {}))
        source_set = {
            "revision_id": revision_id,
            "generation": int(claim.generation),
            "fingerprint": str(claim.fingerprint),
        }
        payload = build_regulatory_assessment_payload(
            product_intelligence_payload=product_payload,
            source_set=source_set,
        )
        exact_inputs = {
            "ruleset_version": RULESET_VERSION,
            "source_set": source_set,
            "product_intelligence": product_payload,
        }
        input_fingerprint = fingerprint_rule_inputs(exact_inputs)

        # Re-check claim in the same write transaction so a stale generation cannot win.
        revision = session.scalar(
            select(UsLaceySourceSetRevision).where(
                UsLaceySourceSetRevision.organization_id == organization_id,
                UsLaceySourceSetRevision.operation_id == operation_id,
                UsLaceySourceSetRevision.id == revision_id,
            )
        )
        if revision is None or not snapshot_matches_claim(revision, claim):
            session.rollback()
            return None

        summary = payload["summary"]
        snapshot = UsLaceyRegulatoryAssessmentSnapshot(
            organization_id=organization_id,
            operation_id=operation_id,
            source_set_revision_id=revision_id,
            generation=int(claim.generation),
            source_set_fingerprint=str(claim.fingerprint),
            ruleset_version=RULESET_VERSION,
            input_fingerprint=input_fingerprint,
            status="CURRENT",
            assessment_count=int(summary["assessment_count"]),
            indeterminate_count=int(summary["indeterminate_count"]),
            payload_json=payload,
            finalized_at=datetime.now(timezone.utc),
        )
        session.add(snapshot)
        try:
            session.commit()
        except IntegrityError:
            session.rollback()
            existing = session.scalar(
                select(UsLaceyRegulatoryAssessmentSnapshot).where(
                    UsLaceyRegulatoryAssessmentSnapshot.organization_id == organization_id,
                    UsLaceyRegulatoryAssessmentSnapshot.source_set_revision_id == revision_id,
                    UsLaceyRegulatoryAssessmentSnapshot.ruleset_version == RULESET_VERSION,
                )
            )
            if existing is None:
                raise
            return existing
        session.refresh(snapshot)
        return snapshot
    finally:
        session.close()


def get_current_regulatory_assessment_view(
    *,
    organization_id: int,
    operation_public_id: UUID | str,
    session_factory: SessionFactory | None = None,
) -> RegulatoryAssessmentView | None:
    """Read only a CURRENT assessment attached to the operation's current source set."""
    factory = session_factory or get_us_lacey_db_session
    organization_id = int(organization_id)
    try:
        public_id = operation_public_id if isinstance(operation_public_id, UUID) else UUID(str(operation_public_id))
    except (TypeError, ValueError, AttributeError):
        return None

    session = factory()
    try:
        set_tenant_db_context(session, organization_id)
        row = session.execute(
            select(UsLaceyRegulatoryAssessmentSnapshot)
            .join(
                UsLaceyOperation,
                and_(
                    UsLaceyOperation.id == UsLaceyRegulatoryAssessmentSnapshot.operation_id,
                    UsLaceyOperation.organization_id == UsLaceyRegulatoryAssessmentSnapshot.organization_id,
                ),
            )
            .join(
                UsLaceySourceSetRevision,
                and_(
                    UsLaceySourceSetRevision.id == UsLaceyRegulatoryAssessmentSnapshot.source_set_revision_id,
                    UsLaceySourceSetRevision.organization_id == UsLaceyRegulatoryAssessmentSnapshot.organization_id,
                ),
            )
            .where(
                UsLaceyRegulatoryAssessmentSnapshot.organization_id == organization_id,
                UsLaceyOperation.public_id == public_id,
                UsLaceySourceSetRevision.is_current.is_(True),
                UsLaceyRegulatoryAssessmentSnapshot.status == "CURRENT",
                UsLaceyRegulatoryAssessmentSnapshot.ruleset_version == RULESET_VERSION,
            )
            .order_by(UsLaceyRegulatoryAssessmentSnapshot.id.desc())
            .limit(1)
        ).scalar_one_or_none()
        if row is None:
            return None
        return RegulatoryAssessmentView(
            status=row.status,
            generation=row.generation,
            source_set_fingerprint=row.source_set_fingerprint,
            ruleset_version=row.ruleset_version,
            input_fingerprint=row.input_fingerprint,
            assessment_count=row.assessment_count,
            indeterminate_count=row.indeterminate_count,
            payload=dict(row.payload_json or {}),
        )
    finally:
        session.close()


__all__ = [
    "RULESET_VERSION",
    "SNAPSHOT_SCHEMA_VERSION",
    "RegulatoryAssessmentView",
    "build_regulatory_assessment_payload",
    "build_regulatory_assessment_snapshot",
    "fingerprint_rule_inputs",
    "get_current_regulatory_assessment_view",
    "mark_regulatory_assessment_snapshots_stale",
]
