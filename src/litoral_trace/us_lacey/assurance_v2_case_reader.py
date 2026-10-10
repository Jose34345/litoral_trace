"""Tenant-scoped, fail-closed Assurance V2 shipment-case read projection.

Read-only shadow integration: never translates legacy 'MATCHED' to human
authority, never issues export authorization, never copies historical evidence
into a new shipment's original-document observations.
"""
from __future__ import annotations

from uuid import UUID

from sqlalchemy import and_, select
from sqlalchemy.orm import Session

from litoral_trace.db.models import (
    AssuranceDocument, AssuranceV2Decision, AssuranceV2DecisionSource,
    UsLaceyOperation, UsLaceyOperationDocument, UsLaceyOperationField,
    UsLaceyPpqPlantLine, UsLaceySourceSetMember, UsLaceySourceSetRevision,
    UsLaceySupplier, UsLaceySupplierProduct, UsLaceyOperationProductLink,
    VaultDocument,
)
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.us_lacey.assurance_v2_memory import evaluate_reuse
from litoral_trace.us_lacey.db import get_us_lacey_db_session
from litoral_trace.us_lacey.exporters.assurance_v2 import AssuranceCase
from litoral_trace.us_lacey.ppq505 import PPQ505_SHIPMENT_REFERENCE

_REUSABLE_FIELDS = frozenset({"genus", "species", "country_of_harvest"})


def read_tenant_assurance_case(
    *, organization_id: int, operation_public_id: str,
    session_factory=get_us_lacey_db_session,
) -> AssuranceCase | None:
    """Resolve the public ID only under the authenticated organization's RLS."""
    try:
        public_id = UUID(str(operation_public_id))
    except (ValueError, TypeError, AttributeError):
        return None
    db = session_factory()
    try:
        set_tenant_db_context(db, int(organization_id))
        return _case(db, int(organization_id), public_id)
    finally:
        db.close()


def _case(db: Session, org_id: int, public_id: UUID) -> AssuranceCase | None:
    op = db.scalar(select(UsLaceyOperation).where(
        UsLaceyOperation.organization_id == org_id,
        UsLaceyOperation.public_id == public_id,
    ))
    if op is None:
        return None
    revision = db.scalar(select(UsLaceySourceSetRevision).where(
        UsLaceySourceSetRevision.organization_id == org_id,
        UsLaceySourceSetRevision.operation_id == op.id,
        UsLaceySourceSetRevision.is_current.is_(True),
    ).order_by(UsLaceySourceSetRevision.generation.desc()))
    if revision is None:
        return None

    documents: list[dict] = []
    valid_document_ids: set[int] = set()
    for link, assurance, vault in db.execute(
        select(UsLaceyOperationDocument, AssuranceDocument, VaultDocument)
        .join(UsLaceySourceSetMember, and_(
            UsLaceySourceSetMember.organization_id == UsLaceyOperationDocument.organization_id,
            UsLaceySourceSetMember.operation_document_id == UsLaceyOperationDocument.id,
            UsLaceySourceSetMember.source_set_revision_id == revision.id,
        ))
        .join(AssuranceDocument, and_(
            AssuranceDocument.organization_id == UsLaceyOperationDocument.organization_id,
            AssuranceDocument.id == UsLaceyOperationDocument.assurance_document_id,
        ))
        .join(VaultDocument, and_(
            VaultDocument.organization_id == AssuranceDocument.organization_id,
            VaultDocument.id == AssuranceDocument.vault_document_id,
        ))
        .where(UsLaceyOperationDocument.organization_id == org_id)
        .order_by(UsLaceyOperationDocument.id)
    ):
        valid_document_ids.add(assurance.id)
        documents.append({
            "id": str(assurance.id), "version": str(link.version_number),
            "sha256": vault.sha256, "role": link.document_role,
            "name": getattr(vault, "original_filename", None) or getattr(vault, "filename", None) or str(assurance.public_id),
            "document_public_id": str(assurance.public_id),
            "raw_available": vault.status == "available",
        })

    decisions = db.scalars(select(AssuranceV2Decision).where(
        AssuranceV2Decision.organization_id == org_id,
        AssuranceV2Decision.operation_id == op.id,
        AssuranceV2Decision.source_set_revision_id == revision.id,
    ).order_by(AssuranceV2Decision.id)).all()
    decision_sources = db.scalars(select(AssuranceV2DecisionSource).where(
        AssuranceV2DecisionSource.organization_id == org_id,
        AssuranceV2DecisionSource.decision_id.in_([d.id for d in decisions] or [-1]),
    )).all()
    docs_by_decision: dict[int, list[AssuranceV2DecisionSource]] = {}
    for row in decision_sources:
        docs_by_decision.setdefault(row.decision_id, []).append(row)
    superseded = {d.supersedes_decision_id for d in decisions if d.supersedes_decision_id is not None}
    authoritative = {
        (d.line_reference or PPQ505_SHIPMENT_REFERENCE, d.field_name): d
        for d in decisions if d.id not in superseded and d.action in {"ACCEPT", "CORRECT"}
        and docs_by_decision.get(d.id)
        and all(source.assurance_document_id in valid_document_ids
                for source in docs_by_decision[d.id])
    }

    lines = tuple(db.scalars(select(UsLaceyPpqPlantLine.line_reference).where(
        UsLaceyPpqPlantLine.organization_id == org_id,
        UsLaceyPpqPlantLine.operation_id == op.id,
    ).order_by(UsLaceyPpqPlantLine.ordinal)).all())
    line_set = set(lines)
    bindings = db.execute(
        select(UsLaceyOperationProductLink, UsLaceySupplierProduct, UsLaceySupplier)
        .join(UsLaceySupplierProduct, and_(
            UsLaceySupplierProduct.id == UsLaceyOperationProductLink.supplier_product_id,
            UsLaceySupplierProduct.organization_id == UsLaceyOperationProductLink.organization_id,
        ))
        .join(UsLaceySupplier, and_(
            UsLaceySupplier.id == UsLaceySupplierProduct.supplier_id,
            UsLaceySupplier.organization_id == UsLaceySupplierProduct.organization_id,
        ))
        .where(
            UsLaceyOperationProductLink.organization_id == org_id,
            UsLaceyOperationProductLink.operation_id == op.id,
            UsLaceyOperationProductLink.source_set_revision_id == revision.id,
        )
    ).all()
    identities = tuple({
        "line_reference": link.line_reference,
        "supplier_id": supplier.id,
        "product_id": product.id,
        "sku": product.sku,
        "status": (
            "HUMAN_CONFIRMED" if link.link_method == "HUMAN_CONFIRMED"
            and supplier.status in {"ACTIVE", "VERIFIED"}
            and product.status in {"ACTIVE", "VERIFIED"}
            else "REVIEW_REQUIRED"
        ),
    } for link, product, supplier in bindings if link.line_reference in line_set)

    fields: list[dict] = []
    exceptions: list[dict] = []
    reuses: list[dict] = []
    for field in db.scalars(select(UsLaceyOperationField).where(
        UsLaceyOperationField.organization_id == org_id,
        UsLaceyOperationField.operation_id == op.id,
    ).order_by(UsLaceyOperationField.id)):
        line = field.merchandise_line_reference or PPQ505_SHIPMENT_REFERENCE
        key = (line, field.field_name)
        decision = authoritative.get(key)
        is_current_document = field.source_assurance_document_id in valid_document_ids
        proven_decision = (
            decision is not None and field.human_value
            and field.human_value == decision.selected_value
            and is_current_document
            and field.source_assurance_document_id in {
                row.assurance_document_id for row in docs_by_decision[decision.id]
            }
        )
        proposed = (field.normalized_value or field.original_value or "").strip()
        chosen = str(field.human_value or "").strip() if proven_decision else proposed
        authority = "HUMAN_CONFIRMED" if proven_decision else "CANDIDATE"
        evidence_ids = [
            f"v2decision-source:{row.id}" for row in docs_by_decision.get(decision.id, ())
        ] if proven_decision else []
        item = {
            "id": str(field.id), "name": field.field_name,
            "line_reference": line, "value": chosen,
            "authority": authority,
            "status": field.field_status,
            "source_locator": field.source_locator if is_current_document else None,
            "document_ids": (
                [str(field.source_assurance_document_id)] if is_current_document else []
            ),
            "evidence_ids": evidence_ids,
            "decision_id": str(decision.public_id) if proven_decision else None,
        }
        fields.append(item)
        if not proven_decision and field.field_status != "NOT_REQUIRED":
            exceptions.append({
                "id": f"field:{field.id}",
                "line_reference": line,
                "field_name": field.field_name,
                "reason": "Human V2 authority or source evidence is not yet verified.",
                "status": "OPEN", "blocking": True,
                "candidates": [{
                    "id": f"legacy:{field.id}", "value": proposed,
                    "document_id": str(field.source_assurance_document_id)
                    if is_current_document else None,
                    "locator": field.source_locator,
                    "page": field.source_page,
                }] if proposed else [],
            })
        if (
            field.field_name in _REUSABLE_FIELDS and line in line_set
            and not field.human_value and revision.status == "FINALIZED"
        ):
            reuse = evaluate_reuse(
                db, organization_id=org_id, operation_id=op.id,
                source_set_revision_id=revision.id, line_reference=line,
                field_name=field.field_name,
            )
            if reuse.eligible:
                reuses.append({
                    "id": str(reuse.memory_public_id), "status": "ELIGIBLE",
                    "field_name": field.field_name, "value": reuse.value,
                    "line_reference": line, "evidence_id": str(reuse.evidence_claim_id),
                    "origin_document_id": str(reuse.origin_document_id),
                    "origin_decision_id": str(reuse.origin_decision_public_id),
                    "provenance": "REUSE_PROPOSED_NOT_CANONICAL",
                })
    output_decisions = tuple({
        "id": str(d.public_id), "actor_id": d.actor_user_id,
        "field_name": d.field_name,
        "line_reference": d.line_reference or PPQ505_SHIPMENT_REFERENCE,
        "value": d.selected_value, "reason": d.reason, "action": d.action,
        "decided_at": d.decided_at.isoformat(),
    } for d in decisions)
    return AssuranceCase(
        organization_id=org_id,
        operation_id=str(op.public_id),
        source_set_fingerprint=revision.source_set_fingerprint,
        source_set_revision=revision.id,
        current_source_set_fingerprint=revision.source_set_fingerprint,
        current_source_set_revision=revision.id,
        ruleset_version="US_LACEY_ASSESSMENT_UNVERIFIED",
        documents=tuple(documents),
        lines=lines,
        fields=tuple(fields),
        exceptions=tuple(exceptions),
        decisions=output_decisions,
        reuses=tuple(reuses),
        identities=identities,
        rules=(),
        # Intentional: no automatic filing authorization from a read model.
        export_authorization={},
    )
