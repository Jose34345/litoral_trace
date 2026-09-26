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
    UsLaceyOperationField,
    UsLaceyPpqPlantLine,
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
from litoral_trace.us_lacey.regulatory_input_contract import (
    InputStatus,
    build_regulatory_input_contract,
)
from litoral_trace.us_lacey.regulatory.engine import (
    RegulatoryContext,
    RegulatorySubject,
    evaluate_regulatory_rules,
)
from litoral_trace.us_lacey.regulatory.rules import (
    RULESET_VERSION,
    DeMinimisRule,
    EvidenceRef,
    RuleAssessment,
    RuleStatus,
    SpecialCompositeRule,
    SpecialRecycledRule,
)
from litoral_trace.us_lacey.regulatory.rules.hts_applicability import (
    HtsApplicabilityRule,
)


SNAPSHOT_SCHEMA_VERSION = "regulatory-assessment-snapshot-v3"
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


def _serialize_assessment(
    assessment: RuleAssessment,
    *,
    subject_ref: str,
) -> dict[str, Any]:
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


def _plant_line_references(
    operation_fields: tuple[object, ...],
    explicit_references: tuple[str, ...],
) -> tuple[str, ...]:
    ordered: list[str] = []
    seen: set[str] = set()

    for value in explicit_references:
        reference = str(value or "").strip()
        if reference and reference not in seen:
            seen.add(reference)
            ordered.append(reference)

    for field in operation_fields:
        if str(getattr(field, "field_scope", "") or "").upper() != "PLANT_LINE":
            continue
        reference = str(
            getattr(field, "merchandise_line_reference", "") or ""
        ).strip()
        if reference and reference not in seen:
            seen.add(reference)
            ordered.append(reference)

    return tuple(ordered)


def _field_value(field: object | None) -> str | None:
    if field is None:
        return None
    human = str(getattr(field, "human_value", None) or "").strip()
    if human:
        return human
    if str(getattr(field, "validation_status", "") or "").upper() != "VALID":
        return None
    value = str(
        getattr(field, "normalized_value", None)
        or getattr(field, "original_value", None)
        or ""
    ).strip()
    return value or None


def _fields_by_line(
    operation_fields: tuple[object, ...],
) -> dict[str, dict[str, object]]:
    grouped: dict[str, dict[str, object]] = {}
    for field in operation_fields:
        if str(getattr(field, "field_scope", "") or "").upper() != "PLANT_LINE":
            continue
        line_reference = str(
            getattr(field, "merchandise_line_reference", "") or ""
        ).strip()
        field_name = str(getattr(field, "field_name", "") or "").strip()
        if not line_reference or not field_name:
            continue
        grouped.setdefault(line_reference, {})[field_name] = field
    return grouped


def _contract_by_line(
    regulatory_input_contract: Mapping[str, Any],
) -> dict[str, Mapping[str, Any]]:
    result: dict[str, Mapping[str, Any]] = {}
    for item in regulatory_input_contract.get("subjects", ()):
        if not isinstance(item, Mapping):
            continue
        line_reference = str(
            item.get("shipment_line_reference")
            or item.get("subject_ref")
            or ""
        ).strip()
        if line_reference:
            result[line_reference] = item
    return result


def _supported_contract_value(
    contract_inputs: Mapping[str, Any],
    key: str,
) -> object | None:
    item = contract_inputs.get(key)
    if not isinstance(item, Mapping):
        return None
    if item.get("status") != InputStatus.SUPPORTED.value:
        return None
    return item.get("value")


def _hts_evidence_refs(
    contract_inputs: Mapping[str, Any],
) -> tuple[EvidenceRef, ...]:
    item = contract_inputs.get("hts10")
    if not isinstance(item, Mapping):
        return ()
    if item.get("status") != InputStatus.SUPPORTED.value:
        return ()
    evidence = item.get("evidence")
    if not isinstance(evidence, Mapping):
        return ()
    return (
        EvidenceRef(
            source_type="REVIEWED_HTS10",
            source_id=(
                None
                if evidence.get("source_assurance_document_id") is None
                else str(evidence.get("source_assurance_document_id"))
            ),
            locator=(
                None
                if evidence.get("source_locator") is None
                else str(evidence.get("source_locator"))
            ),
        ),
    )


def _linked_product_enrichment(
    *,
    product_intelligence_payload: Mapping[str, Any],
    line_reference: str,
    article_component: str | None,
) -> dict[str, Any]:
    bridge = product_intelligence_payload.get("shipment_product_bridge")
    links = bridge.get("links", ()) if isinstance(bridge, Mapping) else ()
    linked = [
        item
        for item in links
        if isinstance(item, Mapping)
        and str(item.get("status") or "") == "LINKED"
        and str(item.get("shipment_line_reference") or "").strip()
        == line_reference
        and isinstance(item.get("product"), Mapping)
    ]
    if len(linked) != 1:
        return {}

    link = linked[0]
    product = link["product"]
    enrichment: dict[str, Any] = {
        "bridge_status": "LINKED",
        "line_item_key": link.get("line_item_key"),
        "product": dict(product),
    }

    components = tuple(
        component
        for component in product.get("components", ())
        if isinstance(component, Mapping)
    )
    selected: Mapping[str, Any] | None = None
    normalized_article = str(article_component or "").strip().casefold()
    if normalized_article:
        matches = tuple(
            component
            for component in components
            if str(component.get("description_raw") or "").strip().casefold()
            == normalized_article
        )
        if len(matches) == 1:
            selected = matches[0]
    elif len(components) == 1:
        selected = components[0]

    if selected is not None:
        material = selected.get("material")
        if isinstance(material, Mapping):
            material_name = str(
                material.get("name_raw")
                or material.get("name_normalized")
                or ""
            ).strip()
            if material_name:
                enrichment["material"] = material_name
                enrichment["material_description"] = material_name
            enrichment["component"] = dict(selected)

    return enrichment


def _operation_field_fingerprint_payload(
    operation_fields: tuple[object, ...],
) -> list[dict[str, Any]]:
    relevant = []
    for field in operation_fields:
        if str(getattr(field, "field_scope", "") or "").upper() != "PLANT_LINE":
            continue
        relevant.append(
            {
                "line_reference": str(
                    getattr(field, "merchandise_line_reference", "") or ""
                ),
                "field_name": str(getattr(field, "field_name", "") or ""),
                "original_value": getattr(field, "original_value", None),
                "normalized_value": getattr(field, "normalized_value", None),
                "human_value": getattr(field, "human_value", None),
                "validation_status": getattr(field, "validation_status", None),
                "field_status": getattr(field, "field_status", None),
                "source_assurance_document_id": getattr(
                    field, "source_assurance_document_id", None
                ),
                "source_page": getattr(field, "source_page", None),
                "source_locator": getattr(field, "source_locator", None),
            }
        )
    return sorted(
        relevant,
        key=lambda item: (
            item["line_reference"],
            item["field_name"],
        ),
    )


def _build_regulatory_subjects(
    *,
    operation_fields: tuple[object, ...],
    plant_line_references: tuple[str, ...],
    product_intelligence_payload: Mapping[str, Any],
    regulatory_input_contract: Mapping[str, Any],
) -> tuple[RegulatorySubject, ...]:
    refs = _plant_line_references(operation_fields, plant_line_references)
    fields_by_line = _fields_by_line(operation_fields)
    contract_by_line = _contract_by_line(regulatory_input_contract)
    subjects: list[RegulatorySubject] = []

    for line_reference in refs:
        line_fields = fields_by_line.get(line_reference, {})
        contract_subject = contract_by_line.get(line_reference, {})
        contract_inputs = (
            contract_subject.get("inputs", {})
            if isinstance(contract_subject, Mapping)
            else {}
        )
        if not isinstance(contract_inputs, Mapping):
            contract_inputs = {}

        hts10_value = _supported_contract_value(contract_inputs, "hts10")
        hts10 = (
            str(hts10_value).strip()
            if hts10_value is not None and str(hts10_value).strip()
            else None
        )
        article_component = _field_value(line_fields.get("article_component"))
        genus = _field_value(line_fields.get("genus"))
        species = _field_value(line_fields.get("species"))
        country_of_harvest = _field_value(
            line_fields.get("country_of_harvest")
        )
        quantity = _field_value(line_fields.get("plant_quantity"))
        unit = _field_value(line_fields.get("metric_unit"))

        de_minimis_inputs = {
            "hts10": hts10,
            "plant_mass_per_unit_kg": _supported_contract_value(
                contract_inputs,
                "plant_mass_per_unit_kg",
            ),
            "total_unit_mass_kg": _supported_contract_value(
                contract_inputs,
                "total_unit_mass_kg",
            ),
            "entry_same_hts_plant_mass_kg": _supported_contract_value(
                contract_inputs,
                "entry_same_hts_plant_mass_kg",
            ),
            "protected_status": _supported_contract_value(
                contract_inputs,
                "protected_status",
            ),
        }

        subjects.append(
            RegulatorySubject(
                subject_ref=line_reference,
                line_reference=line_reference,
                hts10=hts10,
                article_component=article_component,
                genus=genus,
                species=species,
                country_of_harvest=country_of_harvest,
                quantity=quantity,
                unit=unit,
                evidence_refs=_hts_evidence_refs(contract_inputs),
                rule_inputs={"DE_MINIMIS": de_minimis_inputs},
                enrichment=_linked_product_enrichment(
                    product_intelligence_payload=product_intelligence_payload,
                    line_reference=line_reference,
                    article_component=article_component,
                ),
            )
        )

    return tuple(subjects)


def build_regulatory_assessment_payload(
    *,
    product_intelligence_payload: Mapping[str, Any],
    source_set: Mapping[str, Any],
    operation_fields: tuple[object, ...] = (),
    plant_line_references: tuple[str, ...] = (),
    regulatory_input_contract: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Evaluate every botanical shipment line, with Product Intelligence optional."""

    fields = tuple(operation_fields)
    line_refs = tuple(plant_line_references)
    input_contract = (
        regulatory_input_contract
        if isinstance(regulatory_input_contract, Mapping)
        else build_regulatory_input_contract(
            product_intelligence_payload=product_intelligence_payload,
            operation_fields=fields,
            plant_line_references=line_refs,
        )
    )

    subjects = _build_regulatory_subjects(
        operation_fields=fields,
        plant_line_references=line_refs,
        product_intelligence_payload=product_intelligence_payload,
        regulatory_input_contract=input_contract,
    )
    context = RegulatoryContext(subjects=subjects)
    evaluated = evaluate_regulatory_rules(
        context,
        rules=(
            HtsApplicabilityRule(),
            DeMinimisRule(),
            SpecialCompositeRule(),
            SpecialRecycledRule(),
        ),
    )
    assessments = [
        _serialize_assessment(
            item.assessment,
            subject_ref=item.subject_ref,
        )
        for item in evaluated
    ]

    indeterminate_count = sum(
        item["status"] == RuleStatus.INDETERMINATE.value
        for item in assessments
    )
    return {
        "schema_version": SNAPSHOT_SCHEMA_VERSION,
        "ruleset_version": RULESET_VERSION,
        "source_set": {
            "revision_id": source_set.get("revision_id"),
            "generation": source_set.get("generation"),
            "fingerprint": source_set.get("fingerprint"),
        },
        "summary": {
            "subject_count": len(subjects),
            "assessment_count": len(assessments),
            "indeterminate_count": int(indeterminate_count),
        },
        "regulatory_subjects": [
            {
                "subject_ref": subject.subject_ref,
                "line_reference": subject.line_reference,
                "hts10": subject.hts10,
                "article_component": subject.article_component,
                "genus": subject.genus,
                "species": subject.species,
                "country_of_harvest": subject.country_of_harvest,
                "quantity": subject.quantity,
                "unit": subject.unit,
                "has_product_enrichment": bool(subject.enrichment),
            }
            for subject in subjects
        ],
        "regulatory_input_contract": dict(input_contract),
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
        product_payload = (
            enrich_product_intelligence_taxonomy(
                dict(product_snapshot.payload_json or {})
            )
            if product_snapshot is not None
            else {}
        )
        operation_fields = tuple(
            session.scalars(
                select(UsLaceyOperationField).where(
                    UsLaceyOperationField.organization_id == organization_id,
                    UsLaceyOperationField.operation_id == operation_id,
                )
            ).all()
        )
        plant_line_references = tuple(
            str(value)
            for value in session.scalars(
                select(UsLaceyPpqPlantLine.line_reference)
                .where(
                    UsLaceyPpqPlantLine.organization_id == organization_id,
                    UsLaceyPpqPlantLine.operation_id == operation_id,
                )
                .order_by(
                    UsLaceyPpqPlantLine.ordinal.asc(),
                    UsLaceyPpqPlantLine.id.asc(),
                )
            ).all()
        )
        regulatory_input_contract = build_regulatory_input_contract(
            product_intelligence_payload=product_payload,
            operation_fields=operation_fields,
            plant_line_references=plant_line_references,
        )
        source_set = {
            "revision_id": revision_id,
            "generation": int(claim.generation),
            "fingerprint": str(claim.fingerprint),
        }
        payload = build_regulatory_assessment_payload(
            product_intelligence_payload=product_payload,
            source_set=source_set,
            operation_fields=operation_fields,
            plant_line_references=plant_line_references,
            regulatory_input_contract=regulatory_input_contract,
        )
        exact_inputs = {
            "ruleset_version": RULESET_VERSION,
            "source_set": source_set,
            "plant_line_references": list(plant_line_references),
            "operation_fields": _operation_field_fingerprint_payload(
                operation_fields
            ),
            "product_intelligence": product_payload,
            "regulatory_input_contract": regulatory_input_contract,
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
            set_tenant_db_context(session, organization_id)
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
        set_tenant_db_context(session, organization_id)
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