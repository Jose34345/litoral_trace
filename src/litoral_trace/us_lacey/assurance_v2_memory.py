"""Assurance V2 decision journal, source-linked memory and conservative reuse.

Opt-in service boundary: existing production review/worker paths are intentionally
unchanged. Integrating callers must supply an already-authenticated user ID and
one tenant-scoped SQLAlchemy transaction. The service never commits for them.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum
from hashlib import sha256
import json
from collections.abc import Mapping
from uuid import UUID, NAMESPACE_URL, uuid5

from sqlalchemy import and_, select, text
from sqlalchemy.orm import Session

from litoral_trace.db.models import (
    AssuranceDocument, AssuranceV2Decision, AssuranceV2DecisionSource,
    AssuranceV2IdentityEvent, AssuranceV2MemoryLink, SemanticEvidenceNode,
    UsLaceyAssuranceV2ClaimSnapshot, UsLaceyEngineShipmentRun,
    UsLaceyEvidenceClaim, UsLaceyOperation,
    UsLaceyOperationDocument, UsLaceyOperationEvent, UsLaceyOperationField,
    UsLaceyOperationProductLink, UsLaceySourceSetMember, UsLaceySourceSetRevision,
    UsLaceySupplier, UsLaceySupplierEvidence, UsLaceySupplierProduct,
    User, VaultDocument,
)
from litoral_trace.us_lacey.audit_trail import (
    OperationActorType, OperationEventType, append_operation_event,
)
from litoral_trace.us_lacey.reusable_evidence import (
    REUSABLE_EVIDENCE_EXTRACTOR, REUSABLE_FIELD_NAMES,
)
from litoral_trace.us_lacey.reusable_evidence_promotion import _add_one_calendar_year
from litoral_trace.us_lacey.assurance_v2_reuse_guard import current_claim_values_for_reuse

CONTRACT_VERSION = "assurance.v2/1.0.0"
MEMORY_EVIDENCE_TYPE = "HUMAN_VERIFIED_ASSURANCE_V2"
VALID_IDENTITY_STATES = frozenset({"ACTIVE", "VERIFIED"})


class EvidenceRelation(StrEnum):
    CORROBORATION = "CORROBORATION"
    CONTRADICTION = "CONTRADICTION"
    OTHER_ENTITY = "OTHER_ENTITY"
    HISTORICAL = "HISTORICAL"
    INSUFFICIENT_IDENTITY = "INSUFFICIENT_IDENTITY"
    ABSENT = "ABSENT"
    REVOKED = "REVOKED"
    OBSOLETE = "OBSOLETE"


@dataclass(frozen=True, slots=True)
class ReuseEligibility:
    eligible: bool
    status: str
    reason_codes: tuple[str, ...]
    field_name: str
    value: str | None = None
    memory_public_id: UUID | None = None
    evidence_claim_id: int | None = None
    origin_document_id: int | None = None
    origin_decision_public_id: UUID | None = None
    provenance: str | None = None


@dataclass(frozen=True, slots=True)
class ReuseApplication:
    """Human-authorized historical value; never a current-document assertion."""
    status: str
    field_name: str
    line_reference: str
    value: str
    memory_public_id: UUID
    evidence_claim_id: int
    origin_document_id: int
    origin_decision_public_id: UUID
    actor_user_id: int
    audit_event_id: int
    source_claim_snapshot_digest: str


@dataclass(frozen=True, slots=True)
class AssuranceCaseSnapshot:
    snapshot_id: UUID
    contract_version: str
    organization_id: int
    operation_id: int
    source_set_revision_id: int
    source_set_fingerprint: str
    generation: int
    state: str
    decision_ids: tuple[UUID, ...]
    verified_memory_ids: tuple[UUID, ...]


def classify_evidence(
    *, same_entity: bool | None, current_value: str | None,
    historical_value: str | None, valid: bool = True,
    revoked: bool = False, obsolete: bool = False,
    context_compatible: bool = True,
) -> EvidenceRelation:
    """No model confidence, HTS, or fuzzy score enters authority classification."""
    if revoked:
        return EvidenceRelation.REVOKED
    if obsolete:
        return EvidenceRelation.OBSOLETE
    if same_entity is None:
        return EvidenceRelation.INSUFFICIENT_IDENTITY
    if not same_entity:
        return EvidenceRelation.OTHER_ENTITY
    if not valid or not context_compatible:
        return EvidenceRelation.HISTORICAL
    if not (historical_value or "").strip():
        return EvidenceRelation.ABSENT
    if not (current_value or "").strip():
        return EvidenceRelation.HISTORICAL
    if current_value.strip().casefold() == historical_value.strip().casefold():
        return EvidenceRelation.CORROBORATION
    return EvidenceRelation.CONTRADICTION


def _actor(session: Session, organization_id: int, authenticated_user_id: int) -> None:
    """Verify tenant membership without widening runtime SELECT on users."""
    if int(authenticated_user_id) <= 0:
        raise ValueError("authenticated reviewer required")
    dialect = session.get_bind().dialect.name
    if dialect == "postgresql":
        user_id = session.execute(
            text("SELECT user_id FROM public.us_lacey_audit_user_identity(:user_id)"),
            {"user_id": int(authenticated_user_id)},
        ).scalar_one_or_none()
        if user_id != int(authenticated_user_id):
            raise ValueError("reviewer does not belong to tenant")
    else:
        user = session.scalar(
            select(User).where(
                User.organization_id == int(organization_id),
                User.id == int(authenticated_user_id),
                User.is_active.is_(True),
            )
        )
        if user is None:
            raise ValueError("reviewer does not belong to tenant")


def _revision(
    session: Session, *, organization_id: int, operation_id: int,
    source_set_revision_id: int,
) -> UsLaceySourceSetRevision:
    revision = session.scalar(
        select(UsLaceySourceSetRevision)
        .where(
            UsLaceySourceSetRevision.id == int(source_set_revision_id),
            UsLaceySourceSetRevision.organization_id == int(organization_id),
            UsLaceySourceSetRevision.operation_id == int(operation_id),
            UsLaceySourceSetRevision.is_current.is_(True),
            UsLaceySourceSetRevision.status == "FINALIZED",
        )
        .with_for_update()
    )
    if revision is None:
        raise ValueError("STALE_SOURCE_SET")
    return revision


def _source_document_version(
    session: Session, *, organization_id: int,
    source_set_revision_id: int, assurance_document_id: int,
) -> UsLaceyOperationDocument | None:
    return session.scalar(
        select(UsLaceyOperationDocument)
        .join(
            UsLaceySourceSetMember,
            and_(
                UsLaceyOperationDocument.id == UsLaceySourceSetMember.operation_document_id,
                UsLaceyOperationDocument.organization_id == UsLaceySourceSetMember.organization_id,
            ),
        )
        .where(
            UsLaceySourceSetMember.organization_id == int(organization_id),
            UsLaceySourceSetMember.source_set_revision_id == int(source_set_revision_id),
            UsLaceySourceSetMember.assurance_document_id == int(assurance_document_id),
            UsLaceyOperationDocument.is_current.is_(True),
        )
    )


def _current_source_document(
    session: Session, *, organization_id: int,
    source_set_revision_id: int, assurance_document_id: int,
) -> bool:
    return _source_document_version(
        session, organization_id=organization_id,
        source_set_revision_id=source_set_revision_id,
        assurance_document_id=assurance_document_id,
    ) is not None


def record_human_decision(
    session: Session, *, organization_id: int, operation_id: int,
    source_set_revision_id: int, authenticated_user_id: int,
    action: str, field_name: str, line_reference: str | None,
    selected_value: str | None, reason: str, idempotency_key: str,
    assurance_document_ids: tuple[int, ...] = (),
    semantic_evidence_node_ids: tuple[int, ...] = (),
    supersedes_decision_id: int | None = None,
    context: dict | None = None,
) -> AssuranceV2Decision:
    """One immutable decision and its existing audit event, in one transaction."""
    action = action.upper().strip()
    field_name = field_name.strip()
    key = idempotency_key.strip()
    if action not in {"ACCEPT", "REJECT", "CORRECT", "SUPERSEDE"}:
        raise ValueError("invalid decision action")
    if not field_name or len(field_name) > 100 or not reason.strip():
        raise ValueError("field and reason required")
    if not key or len(key) > 160:
        raise ValueError("idempotency key required (max 160)")
    if action in {"ACCEPT", "CORRECT"} and not (selected_value or "").strip():
        raise ValueError("selected value required")
    if action == "SUPERSEDE" and supersedes_decision_id is None:
        raise ValueError("supersession requires an original decision")

    org_id, op_id = int(organization_id), int(operation_id)
    existing = session.scalar(
        select(AssuranceV2Decision).where(
            AssuranceV2Decision.organization_id == org_id,
            AssuranceV2Decision.operation_id == op_id,
            AssuranceV2Decision.idempotency_key == key,
        )
    )
    if existing is not None:
        if (
            existing.action != action or existing.field_name != field_name
            or existing.selected_value != selected_value
            or existing.source_set_revision_id != int(source_set_revision_id)
            or existing.actor_user_id != int(authenticated_user_id)
            or existing.line_reference != line_reference
            or existing.reason != reason.strip()
            or dict(existing.context_json or {}) != dict(context or {})
        ):
            raise ValueError("idempotency collision with different decision")
        return existing

    _actor(session, org_id, authenticated_user_id)
    revision = _revision(
        session, organization_id=org_id, operation_id=op_id,
        source_set_revision_id=source_set_revision_id,
    )
    if supersedes_decision_id is not None:
        predecessor = session.scalar(
            select(AssuranceV2Decision).where(
                AssuranceV2Decision.organization_id == org_id,
                AssuranceV2Decision.operation_id == op_id,
                AssuranceV2Decision.id == int(supersedes_decision_id),
                AssuranceV2Decision.field_name == field_name,
                AssuranceV2Decision.line_reference == line_reference,
            )
        )
        if predecessor is None:
            raise ValueError("supersession target not found")
    documents = tuple(sorted(set(int(i) for i in assurance_document_ids)))
    nodes = tuple(sorted(set(int(i) for i in semantic_evidence_node_ids)))
    if action in {"ACCEPT", "CORRECT"} and not documents:
        raise ValueError("supported or corrected decision requires original document")
    for doc_id in documents:
        if not _current_source_document(
            session, organization_id=org_id, source_set_revision_id=revision.id,
            assurance_document_id=doc_id,
        ):
            raise ValueError("document superseded or outside current source set")
    # A document reference alone is insufficient for supported authority:
    # bind to an existing semantic node or a source-linked operation field.
    source_fields_by_document: dict[int, int] = {}
    if action in {"ACCEPT", "CORRECT"} and not nodes:
        for doc_id in documents:
            source_field = session.scalar(
                select(UsLaceyOperationField).where(
                    UsLaceyOperationField.organization_id == org_id,
                    UsLaceyOperationField.operation_id == op_id,
                    UsLaceyOperationField.source_assurance_document_id == doc_id,
                    UsLaceyOperationField.field_name == field_name,
                    UsLaceyOperationField.merchandise_line_reference == line_reference,
                )
            )
            if source_field is None or not (source_field.source_locator or "").strip():
                raise ValueError("no source-linked claim for the selected field")
            source_fields_by_document[doc_id] = source_field.id
    evidence_nodes = []
    for node_id in nodes:
        node = session.scalar(
            select(SemanticEvidenceNode).where(
                SemanticEvidenceNode.organization_id == org_id,
                SemanticEvidenceNode.id == node_id,
                SemanticEvidenceNode.assurance_document_id.in_(documents),
                SemanticEvidenceNode.target_field == field_name,
            )
        )
        if node is None:
            raise ValueError("semantic node/document/field mismatch")
        evidence_nodes.append(node)

    event = append_operation_event(
        session, organization_id=org_id, operation_id=op_id,
        actor_type=OperationActorType.USER,
        actor_identity=f"user:{int(authenticated_user_id)}",
        event_type=OperationEventType.HUMAN_REVIEW,
        event_key=f"assurance_v2:decision:{key}",
        details={
            "action": action.lower(), "field_name": field_name,
            "line_reference": line_reference, "source_set_revision_id": revision.id,
            "document_ids": list(documents),
            "semantic_node_ids": list(nodes),
        },
    )
    decision = AssuranceV2Decision(
        organization_id=org_id, operation_id=op_id,
        source_set_revision_id=revision.id,
        source_set_fingerprint=revision.source_set_fingerprint,
        generation=revision.generation, actor_user_id=int(authenticated_user_id),
        action=action, field_name=field_name, line_reference=line_reference,
        selected_value=selected_value, reason=reason.strip(),
        context_json=dict(context or {}), supersedes_decision_id=supersedes_decision_id,
        idempotency_key=key, audit_event_id=event.id,
    )
    session.add(decision)
    session.flush()
    for doc_id in documents:
        document_version = _source_document_version(
            session, organization_id=org_id,
            source_set_revision_id=revision.id, assurance_document_id=doc_id,
        )
        if document_version is None:
            raise ValueError("document superseded during review")
        matching_nodes = [node for node in evidence_nodes if node.assurance_document_id == doc_id]
        if not matching_nodes:
            session.add(AssuranceV2DecisionSource(
                organization_id=org_id, decision_id=decision.id,
                assurance_document_id=doc_id,
                operation_document_id=document_version.id,
                document_version_number=document_version.version_number,
                source_operation_field_id=source_fields_by_document.get(doc_id),
            ))
        for node in matching_nodes:
            session.add(AssuranceV2DecisionSource(
                organization_id=org_id, decision_id=decision.id,
                assurance_document_id=doc_id,
                operation_document_id=document_version.id,
                document_version_number=document_version.version_number,
                semantic_evidence_node_id=node.id, source_span_id=node.source_span_id,
            ))
    session.flush()
    return decision


def _exact_product_for_line(
    session: Session, *, organization_id: int, operation_id: int,
    revision_id: int, line_reference: str | None,
) -> tuple[UsLaceySupplier, UsLaceySupplierProduct] | None:
    if not (line_reference or "").strip():
        return None
    rows = session.execute(
        select(UsLaceySupplier, UsLaceySupplierProduct)
        .join(
            UsLaceySupplierProduct,
            and_(UsLaceySupplierProduct.supplier_id == UsLaceySupplier.id,
                 UsLaceySupplierProduct.organization_id == UsLaceySupplier.organization_id),
        )
        .join(
            UsLaceyOperationProductLink,
            and_(
                UsLaceyOperationProductLink.supplier_product_id == UsLaceySupplierProduct.id,
                UsLaceyOperationProductLink.organization_id == UsLaceySupplier.organization_id,
            ),
        )
        .where(
            UsLaceyOperationProductLink.organization_id == organization_id,
            UsLaceyOperationProductLink.operation_id == operation_id,
            UsLaceyOperationProductLink.source_set_revision_id == revision_id,
            UsLaceyOperationProductLink.line_reference == line_reference,
            UsLaceyOperationProductLink.link_method.in_(("EXACT_SKU", "HUMAN_CONFIRMED")),
            UsLaceySupplier.status.in_(VALID_IDENTITY_STATES),
            UsLaceySupplierProduct.status.in_(VALID_IDENTITY_STATES),
            UsLaceySupplierProduct.sku.is_not(None),
        )
    ).all()
    return rows[0] if len(rows) == 1 and (rows[0][1].sku or "").strip() else None


def promote_decision_to_memory(
    session: Session, *, organization_id: int, decision_id: int,
    as_of: datetime | None = None,
) -> AssuranceV2MemoryLink | None:
    """Only an explicit, current human decision may create durable verified memory."""
    org_id = int(organization_id)
    decision = session.scalar(
        select(AssuranceV2Decision).where(
            AssuranceV2Decision.organization_id == org_id,
            AssuranceV2Decision.id == int(decision_id),
        )
    )
    if decision is None or decision.action not in {"ACCEPT", "CORRECT"}:
        return None
    if decision.field_name not in REUSABLE_FIELD_NAMES:
        return None
    _revision(
        session, organization_id=org_id, operation_id=decision.operation_id,
        source_set_revision_id=decision.source_set_revision_id,
    )
    # A later decision replacing/rejecting this authority closes promotion.
    successor = session.scalar(
        select(AssuranceV2Decision.id).where(
            AssuranceV2Decision.organization_id == org_id,
            AssuranceV2Decision.supersedes_decision_id == decision.id,
        )
    )
    if successor is not None:
        return None
    exact = _exact_product_for_line(
        session, organization_id=org_id, operation_id=decision.operation_id,
        revision_id=decision.source_set_revision_id,
        line_reference=decision.line_reference,
    )
    if exact is None:
        return None
    _, product = exact
    sources = session.scalars(
        select(AssuranceV2DecisionSource).where(
            AssuranceV2DecisionSource.organization_id == org_id,
            AssuranceV2DecisionSource.decision_id == decision.id,
        )
    ).all()
    if not sources or len({s.assurance_document_id for s in sources}) != 1:
        return None
    doc_id = sources[0].assurance_document_id
    if not _current_source_document(
        session, organization_id=org_id,
        source_set_revision_id=decision.source_set_revision_id,
        assurance_document_id=doc_id,
    ):
        return None

    from litoral_trace.us_lacey.ppq505 import validate_ppq_value
    value = (decision.selected_value or "").strip()
    validation = validate_ppq_value(decision.field_name, value)
    if validation.status.value != "VALID" or not validation.normalized_value:
        return None

    row = session.execute(
        select(AssuranceDocument, VaultDocument)
        .join(
            VaultDocument,
            and_(VaultDocument.id == AssuranceDocument.vault_document_id,
                 VaultDocument.organization_id == AssuranceDocument.organization_id),
        )
        .where(
            AssuranceDocument.organization_id == org_id,
            AssuranceDocument.id == doc_id,
            VaultDocument.status == "available",
        )
    ).one_or_none()
    if row is None:
        return None
    source, vault = row
    now = as_of or datetime.now(timezone.utc)
    until = _add_one_calendar_year(now)
    if source.valid_until is not None:
        expires = source.valid_until
        if expires.tzinfo is None:
            expires = expires.replace(tzinfo=timezone.utc)
        until = min(until, expires)
    if until <= now:
        return None

    evidence = session.scalar(
        select(UsLaceySupplierEvidence).where(
            UsLaceySupplierEvidence.organization_id == org_id,
            UsLaceySupplierEvidence.supplier_product_id == product.id,
            UsLaceySupplierEvidence.document_hash == vault.sha256,
            UsLaceySupplierEvidence.evidence_type == MEMORY_EVIDENCE_TYPE,
        )
    )
    if evidence is None:
        evidence = UsLaceySupplierEvidence(
            organization_id=org_id, supplier_product_id=product.id,
            evidence_type=MEMORY_EVIDENCE_TYPE,
            document_hash=vault.sha256, source_reference=f"assurance:{source.public_id}",
            valid_from=now, valid_until=until,
            verified_by_user_id=decision.actor_user_id, verified_at=now,
            status="VERIFIED",
        )
        session.add(evidence)
        session.flush()
    elif evidence.status != "VERIFIED" or not (
        evidence.valid_from.replace(tzinfo=evidence.valid_from.tzinfo or timezone.utc)
        <= now.replace(tzinfo=now.tzinfo or timezone.utc)
        < evidence.valid_until.replace(tzinfo=evidence.valid_until.tzinfo or timezone.utc)
    ):
        return None

    claim = session.scalar(
        select(UsLaceyEvidenceClaim).where(
            UsLaceyEvidenceClaim.organization_id == org_id,
            UsLaceyEvidenceClaim.evidence_id == evidence.id,
            UsLaceyEvidenceClaim.field_name == decision.field_name,
        )
    )
    if claim is None:
        claim = UsLaceyEvidenceClaim(
            organization_id=org_id, evidence_id=evidence.id,
            field_name=decision.field_name, field_value=value,
            normalized_value=validation.normalized_value,
        )
        session.add(claim)
        session.flush()
    elif claim.normalized_value != validation.normalized_value:
        # NEVER overwrite a previously reviewed assertion of the same source.
        return None

    existing = session.scalar(
        select(AssuranceV2MemoryLink).where(
            AssuranceV2MemoryLink.organization_id == org_id,
            AssuranceV2MemoryLink.decision_id == decision.id,
            AssuranceV2MemoryLink.evidence_claim_id == claim.id,
        )
    )
    if existing is not None:
        return existing

    link = AssuranceV2MemoryLink(
        organization_id=org_id, decision_id=decision.id,
        evidence_claim_id=claim.id, supplier_product_id=product.id,
        assurance_document_id=doc_id,
        source_set_revision_id=decision.source_set_revision_id,
        context_json=dict(decision.context_json or {}),
    )
    session.add(link)
    session.flush()
    return link


def _blocked(field_name: str, reason: str) -> ReuseEligibility:
    return ReuseEligibility(False, "BLOCKED", (reason,), field_name)


def evaluate_reuse(
    session: Session, *, organization_id: int, operation_id: int,
    source_set_revision_id: int, line_reference: str, field_name: str,
    regulatory_context: dict | None = None,
    as_of: datetime | None = None,
    current_claim_values: tuple[str, ...] = (),
) -> ReuseEligibility:
    """Read-only eligibility: never inject memory into a current document claim."""
    org_id, op_id = int(organization_id), int(operation_id)
    if field_name not in REUSABLE_FIELD_NAMES:
        return _blocked(field_name, "FIELD_NOT_REUSABLE")
    try:
        _revision(
            session, organization_id=org_id, operation_id=op_id,
            source_set_revision_id=source_set_revision_id,
        )
    except ValueError:
        return _blocked(field_name, "STALE_SOURCE_SET")
    exact = _exact_product_for_line(
        session, organization_id=org_id, operation_id=op_id,
        revision_id=source_set_revision_id, line_reference=line_reference,
    )
    if exact is None:
        return _blocked(field_name, "EXACT_IDENTITY_UNCONFIRMED")
    _, product = exact
    current_fields = session.scalars(
        select(UsLaceyOperationField).where(
            UsLaceyOperationField.organization_id == org_id,
            UsLaceyOperationField.operation_id == op_id,
            UsLaceyOperationField.merchandise_line_reference == line_reference,
            UsLaceyOperationField.field_name == field_name,
        )
    ).all()
    values = set(v.strip().casefold() for v in current_claim_values if v and v.strip())
    for field in current_fields:
        if field.field_status in {"CONFLICT", "CONFLICTED", "REVIEW_REQUIRED"}:
            return _blocked(field_name, "CURRENT_SOURCE_CONFLICT")
        if field.human_value:
            return _blocked(field_name, "CURRENT_HUMAN_DECISION_WINS")
        if field.extractor == REUSABLE_EVIDENCE_EXTRACTOR:
            continue
        value = (field.normalized_value or field.original_value or "").strip()
        if value:
            values.add(value.casefold())
    if values:
        return _blocked(field_name, "CURRENT_SHIPMENT_EVIDENCE_WINS")

    now = as_of or datetime.now(timezone.utc)
    rows = session.execute(
        select(AssuranceV2MemoryLink, AssuranceV2Decision,
               UsLaceyEvidenceClaim, UsLaceySupplierEvidence)
        .join(
            AssuranceV2Decision,
            and_(AssuranceV2Decision.id == AssuranceV2MemoryLink.decision_id,
                 AssuranceV2Decision.organization_id == AssuranceV2MemoryLink.organization_id),
        )
        .join(
            UsLaceyEvidenceClaim,
            and_(UsLaceyEvidenceClaim.id == AssuranceV2MemoryLink.evidence_claim_id,
                 UsLaceyEvidenceClaim.organization_id == AssuranceV2MemoryLink.organization_id),
        )
        .join(
            UsLaceySupplierEvidence,
            and_(UsLaceySupplierEvidence.id == UsLaceyEvidenceClaim.evidence_id,
                 UsLaceySupplierEvidence.organization_id == UsLaceyEvidenceClaim.organization_id),
        )
        .where(
            AssuranceV2MemoryLink.organization_id == org_id,
            AssuranceV2MemoryLink.supplier_product_id == product.id,
            UsLaceyEvidenceClaim.field_name == field_name,
            UsLaceySupplierEvidence.status == "VERIFIED",
            UsLaceySupplierEvidence.valid_from <= now,
            UsLaceySupplierEvidence.valid_until > now,
        )
    ).all()
    if not rows:
        return _blocked(field_name, "NO_ACTIVE_VERIFIED_MEMORY")

    eligible_rows = []
    for link, decision, claim, evidence in rows:
        if decision.action not in {"ACCEPT", "CORRECT"}:
            continue
        from litoral_trace.us_lacey.ppq505 import validate_ppq_value
        authorized = validate_ppq_value(decision.field_name, decision.selected_value or "")
        if (
            authorized.status.value != "VALID"
            or (claim.normalized_value or claim.field_value).strip().casefold()
            != (authorized.normalized_value or "").strip().casefold()
        ):
            # The legacy mutable claim may have changed; never rewrite V2 authority.
            continue
        superseded = session.scalar(
            select(AssuranceV2Decision.id).where(
                AssuranceV2Decision.organization_id == org_id,
                AssuranceV2Decision.supersedes_decision_id == decision.id,
            )
        )
        if superseded is not None:
            continue
        origin = session.scalar(
            select(UsLaceySourceSetRevision).where(
                UsLaceySourceSetRevision.organization_id == org_id,
                UsLaceySourceSetRevision.id == link.source_set_revision_id,
                UsLaceySourceSetRevision.is_current.is_(True),
                UsLaceySourceSetRevision.source_set_fingerprint == decision.source_set_fingerprint,
                UsLaceySourceSetRevision.generation == decision.generation,
            )
        )
        if origin is None or not _current_source_document(
            session, organization_id=org_id,
            source_set_revision_id=origin.id,
            assurance_document_id=link.assurance_document_id,
        ):
            continue
        # Strict context compatibility (explicit keys only, no inferred defaults).
        if dict(link.context_json or {}) != dict(regulatory_context or {}):
            continue
        eligible_rows.append((link, decision, claim, evidence))
    if not eligible_rows:
        return _blocked(field_name, "ORIGIN_SUPERSEDED_OR_CONTEXT_MISMATCH")
    distinct = {
        (claim.normalized_value or claim.field_value).strip().casefold()
        for _, _, claim, _ in eligible_rows
    }
    # Also inspect older verified evidence for this exact supplier/SKU. An older
    # accepted fact that contradicts V2 memory must not be silently ignored
    # merely because it predates the V2 authority journal.
    verified_history = session.execute(
        select(UsLaceyEvidenceClaim, UsLaceySupplierEvidence)
        .join(
            UsLaceySupplierEvidence,
            and_(
                UsLaceySupplierEvidence.id == UsLaceyEvidenceClaim.evidence_id,
                UsLaceySupplierEvidence.organization_id == UsLaceyEvidenceClaim.organization_id,
            ),
        )
        .where(
            UsLaceyEvidenceClaim.organization_id == org_id,
            UsLaceyEvidenceClaim.field_name == field_name,
            UsLaceySupplierEvidence.supplier_product_id == product.id,
            UsLaceySupplierEvidence.status == "VERIFIED",
            UsLaceySupplierEvidence.valid_from <= now,
            UsLaceySupplierEvidence.valid_until > now,
        )
    ).all()
    from litoral_trace.us_lacey.ppq505 import validate_ppq_value
    for older_claim, _ in verified_history:
        validated = validate_ppq_value(
            field_name, older_claim.normalized_value or older_claim.field_value
        )
        if validated.status.value != "VALID" or not validated.normalized_value:
            return _blocked(field_name, "HISTORICAL_EVIDENCE_INVALID")
        distinct.add(validated.normalized_value.strip().casefold())
    if len(distinct) != 1:
        return _blocked(field_name, "HISTORICAL_EVIDENCE_CONTRADICTION")
    eligible_rows.sort(key=lambda row: (row[3].verified_at, row[0].id), reverse=True)
    link, decision, claim, _ = eligible_rows[0]
    return ReuseEligibility(
        True, "ELIGIBLE", (), field_name,
        value=claim.normalized_value or claim.field_value,
        memory_public_id=link.public_id,
        evidence_claim_id=claim.id,
        origin_document_id=link.assurance_document_id,
        origin_decision_public_id=decision.public_id,
        provenance="REUSED_HISTORICAL",
    )


def canonical_claim_snapshot_digest(snapshot: Mapping[str, object]) -> str:
    """SHA-256 of the *entire* JSON snapshot (sorted keys, compact ASCII JSON)."""
    if not isinstance(snapshot, Mapping):
        raise ValueError("complete source claim snapshot required")
    try:
        payload = json.dumps(snapshot, sort_keys=True, separators=(",", ":"),
                             ensure_ascii=True, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise ValueError("source claim snapshot is not canonical JSON") from exc
    return sha256(payload.encode("ascii")).hexdigest()


def _current_claim_snapshot(
    session: Session, *, organization_id: int, operation_id: int,
    revision: UsLaceySourceSetRevision,
) -> Mapping[str, object] | None:
    """Require the persisted full packet to match every current document byte hash."""
    sidecars = session.scalars(select(UsLaceyAssuranceV2ClaimSnapshot).where(
        UsLaceyAssuranceV2ClaimSnapshot.organization_id == organization_id,
        UsLaceyAssuranceV2ClaimSnapshot.operation_id == operation_id,
        UsLaceyAssuranceV2ClaimSnapshot.source_set_revision_id == revision.id,
    )).all()
    if sidecars:
        if len(sidecars) != 1:
            return None
        sidecar = sidecars[0]
        snapshot = sidecar.snapshot_json
        if (sidecar.source_set_fingerprint != revision.source_set_fingerprint
                or sidecar.status != "CANDIDATE_ONLY"
                or sidecar.document_count != revision.document_count
                or not isinstance(snapshot, dict)
                or snapshot.get("status") != sidecar.status
                or snapshot.get("document_count", len(snapshot.get("documents") or ())) != sidecar.document_count
                or snapshot.get("claim_count") != sidecar.claim_count):
            return None
    else:
        run = session.scalar(select(UsLaceyEngineShipmentRun).where(
            UsLaceyEngineShipmentRun.organization_id == organization_id,
            UsLaceyEngineShipmentRun.operation_id == operation_id,
            UsLaceyEngineShipmentRun.source_set_fingerprint == revision.source_set_fingerprint,
        ))
        raw = run.resolution_json if run is not None and isinstance(run.resolution_json, dict) else {}
        snapshot = raw.get("assurance_v2_claim_snapshot")
    if not isinstance(snapshot, dict) or any((
        snapshot.get("schema_version") != "assurance.claim.snapshot.v1",
        snapshot.get("status") != "CANDIDATE_ONLY",
        snapshot.get("source_set_revision_id") != revision.id,
        snapshot.get("source_set_fingerprint") != revision.source_set_fingerprint,
        not isinstance(snapshot.get("documents"), list),
    )):
        return None
    rows = session.execute(
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
        .where(UsLaceyOperationDocument.organization_id == organization_id,
               UsLaceyOperationDocument.operation_id == operation_id,
               UsLaceyOperationDocument.is_current.is_(True))
    ).all()
    current = {(document.id, assurance.id): (document, vault)
               for document, assurance, vault in rows}
    documents = snapshot["documents"]
    if len(current) != revision.document_count or len(documents) != len(current):
        return None
    seen: set[tuple[int, int]] = set()
    total_claims = 0
    for entry in documents:
        if not isinstance(entry, dict):
            return None
        key = (entry.get("operation_document_id"), entry.get("assurance_document_id"))
        if key in seen or key not in current:
            return None
        seen.add(key)
        document, vault = current[key]
        claims = entry.get("claims")
        if not isinstance(claims, list) or any(not isinstance(c, dict) for c in claims):
            return None
        if any((
            entry.get("schema_version") != "assurance.claim.snapshot.v1",
            entry.get("source_claim_contract_version") != "assurance.claim.v1",
            entry.get("organization_id") != organization_id,
            entry.get("operation_id") != operation_id,
            entry.get("source_set_revision_id") != revision.id,
            entry.get("source_set_fingerprint") != revision.source_set_fingerprint,
            entry.get("document_version_number") != document.version_number,
            entry.get("original_sha256") != vault.sha256,
            entry.get("status") == "FAILED",
            entry.get("claim_count") != len(claims),
        )):
            return None
        total_claims += len(claims)
    if len(seen) != len(current) or snapshot.get("claim_count") != total_claims:
        return None
    return snapshot


def _reuse_application_key(revision_id: int, line_reference: str, field_name: str) -> str:
    slot = json.dumps([revision_id, line_reference, field_name], separators=(",", ":"),
                      ensure_ascii=True)
    return f"assurance_v2:reuse_application:{sha256(slot.encode('ascii')).hexdigest()}"


def _application_details(
    *, organization_id: int, operation_id: int,
    revision: UsLaceySourceSetRevision, line_reference: str,
    field_name: str, reuse: ReuseEligibility,
    current_claim_snapshot_digest: str, authenticated_user_id: int,
    reason: str, request_digest: str,
) -> dict:
    return {
        "action": "authorize_historical_reuse",
        "organization_id": organization_id,
        "operation_id": operation_id,
        "source_set_revision_id": revision.id,
        "source_set_fingerprint": revision.source_set_fingerprint,
        "generation": revision.generation,
        "line_reference": line_reference,
        "field_name": field_name,
        "memory_public_id": str(reuse.memory_public_id),
        "evidence_claim_id": reuse.evidence_claim_id,
        "origin_document_id": reuse.origin_document_id,
        "origin_decision_public_id": str(reuse.origin_decision_public_id),
        "value": reuse.value,
        "source_claim_snapshot_digest": current_claim_snapshot_digest,
        "actor_user_id": authenticated_user_id,
        "reason": reason,
        "request_digest": request_digest,
        "provenance": "REUSED_HISTORICAL",
    }


def _application_view(event: UsLaceyOperationEvent) -> ReuseApplication:
    details = event.details
    return ReuseApplication(
        status="VERIFIED_REUSE", field_name=details["field_name"],
        line_reference=details["line_reference"], value=details["value"],
        memory_public_id=UUID(details["memory_public_id"]),
        evidence_claim_id=details["evidence_claim_id"],
        origin_document_id=details["origin_document_id"],
        origin_decision_public_id=UUID(details["origin_decision_public_id"]),
        actor_user_id=details["actor_user_id"], audit_event_id=event.id,
        source_claim_snapshot_digest=details["source_claim_snapshot_digest"],
    )


def _eligible_for_application(
    session: Session, *, organization_id: int, operation_id: int,
    source_set_revision_id: int, line_reference: str, field_name: str,
    current_claim_values: tuple[str, ...], current_claim_snapshot_digest: str,
    regulatory_context: dict | None,
) -> tuple[UsLaceySourceSetRevision, ReuseEligibility] | None:
    try:
        revision = _revision(session, organization_id=organization_id,
                             operation_id=operation_id,
                             source_set_revision_id=source_set_revision_id)
    except ValueError:
        return None
    snapshot = _current_claim_snapshot(session, organization_id=organization_id,
                                        operation_id=operation_id, revision=revision)
    if snapshot is None or canonical_claim_snapshot_digest(snapshot) != current_claim_snapshot_digest:
        return None
    claims = tuple(claim for document in snapshot["documents"]
                   for claim in document["claims"])
    actual_values = current_claim_values_for_reuse(
        claims, field_name=field_name, line_reference=line_reference)
    if actual_values is None or actual_values != current_claim_values:
        return None
    reuse = evaluate_reuse(
        session, organization_id=organization_id, operation_id=operation_id,
        source_set_revision_id=source_set_revision_id,
        line_reference=line_reference, field_name=field_name,
        regulatory_context=regulatory_context,
        current_claim_values=current_claim_values,
    )
    return (revision, reuse) if reuse.eligible else None


def authorize_reuse_application(
    session: Session, *, organization_id: int, operation_id: int,
    source_set_revision_id: int, authenticated_user_id: int,
    line_reference: str, field_name: str, memory_public_id: UUID,
    current_claim_values: tuple[str, ...], current_claim_snapshot_digest: str,
    reason: str, idempotency_key: str, regulatory_context: dict | None = None,
) -> ReuseApplication:
    """Append one human consent event; caller commits the whole transaction.

    This never writes a current-document field. A unique revision/line/field
    event key makes retries idempotent and contradictory authorizations collide.
    """
    org_id, op_id = int(organization_id), int(operation_id)
    line, field = str(line_reference or "").strip(), str(field_name or "").strip()
    reason, key = str(reason or "").strip(), str(idempotency_key or "").strip()
    if not line or not field or len(line) > 100 or len(field) > 100:
        raise ValueError("line and field required")
    if not reason or len(reason) > 180 or not key or len(key) > 160:
        raise ValueError("reason and idempotency key required")
    if not isinstance(current_claim_values, tuple) or any(not isinstance(v, str) for v in current_claim_values):
        raise ValueError("complete current claim values required")
    if not isinstance(current_claim_snapshot_digest, str) or len(current_claim_snapshot_digest) != 64:
        raise ValueError("complete current claim snapshot digest required")
    _actor(session, org_id, authenticated_user_id)
    session.scalar(select(UsLaceyOperation.id).where(
        UsLaceyOperation.organization_id == org_id,
        UsLaceyOperation.id == op_id,
    ).with_for_update())
    eligible = _eligible_for_application(
        session, organization_id=org_id, operation_id=op_id,
        source_set_revision_id=source_set_revision_id, line_reference=line,
        field_name=field, current_claim_values=current_claim_values,
        current_claim_snapshot_digest=current_claim_snapshot_digest,
        regulatory_context=regulatory_context,
    )
    if eligible is None or eligible[1].memory_public_id != memory_public_id:
        raise ValueError("historical memory is not eligible for this source set")
    revision, reuse = eligible
    details = _application_details(
        organization_id=org_id, operation_id=op_id, revision=revision,
        line_reference=line, field_name=field, reuse=reuse,
        current_claim_snapshot_digest=current_claim_snapshot_digest,
        authenticated_user_id=int(authenticated_user_id), reason=reason,
        request_digest=sha256(key.encode("utf-8")).hexdigest(),
    )
    event = append_operation_event(
        session, organization_id=org_id, operation_id=op_id,
        actor_type=OperationActorType.USER,
        actor_identity=f"user:{int(authenticated_user_id)}",
        event_type=OperationEventType.HUMAN_REVIEW,
        event_key=_reuse_application_key(revision.id, line, field),
        details=details,
    )
    if event.details != details or event.actor_identity != f"user:{int(authenticated_user_id)}":
        raise ValueError("idempotency collision with different reuse application")
    return _application_view(event)


def verified_reuse_application(
    session: Session, *, organization_id: int, operation_id: int,
    source_set_revision_id: int, line_reference: str, field_name: str,
    current_claim_values: tuple[str, ...], current_claim_snapshot_digest: str,
    regulatory_context: dict | None = None,
) -> ReuseApplication | None:
    """Return authority only while original evidence and current packet still pass."""
    org_id, op_id = int(organization_id), int(operation_id)
    line, field = str(line_reference or "").strip(), str(field_name or "").strip()
    if not line or not field or not isinstance(current_claim_values, tuple):
        return None
    eligible = _eligible_for_application(
        session, organization_id=org_id, operation_id=op_id,
        source_set_revision_id=source_set_revision_id, line_reference=line,
        field_name=field, current_claim_values=current_claim_values,
        current_claim_snapshot_digest=current_claim_snapshot_digest,
        regulatory_context=regulatory_context,
    )
    if eligible is None:
        return None
    revision, reuse = eligible
    event = session.scalar(select(UsLaceyOperationEvent).where(
        UsLaceyOperationEvent.organization_id == org_id,
        UsLaceyOperationEvent.operation_id == op_id,
        UsLaceyOperationEvent.event_key == _reuse_application_key(revision.id, line, field),
        UsLaceyOperationEvent.event_type == OperationEventType.HUMAN_REVIEW.value,
        UsLaceyOperationEvent.actor_type == OperationActorType.USER.value,
    ))
    if event is None or not event.actor_identity.startswith("user:"):
        return None
    try:
        actor_user_id = int(event.actor_identity.removeprefix("user:"))
    except ValueError:
        return None
    details = dict(event.details or {})
    expected = _application_details(
        organization_id=org_id, operation_id=op_id, revision=revision,
        line_reference=line, field_name=field, reuse=reuse,
        current_claim_snapshot_digest=current_claim_snapshot_digest,
        authenticated_user_id=actor_user_id,
        reason=str(details.get("reason") or ""),
        request_digest=str(details.get("request_digest") or ""),
    )
    digest = details.get("request_digest")
    if (details != expected or not details["reason"]
            or not isinstance(digest, str) or len(digest) != 64
            or any(char not in "0123456789abcdef" for char in digest)):
        return None
    return _application_view(event)


def record_identity_event(
    session: Session, *, organization_id: int, operation_id: int,
    authenticated_user_id: int, entity_type: str, source_id: int,
    action: str, reason: str, idempotency_key: str,
    target_id: int | None = None, alias_value: str | None = None,
    reverses_event_id: int | None = None,
) -> AssuranceV2IdentityEvent:
    """Human-only logical alias/merge journal; no automatic entity-key rewrites."""
    org_id = int(organization_id)
    action, entity_type = action.upper().strip(), entity_type.upper().strip()
    if action not in {"ALIAS_ADD", "ALIAS_REMOVE", "MERGE", "UNMERGE"}:
        raise ValueError("invalid identity action")
    if entity_type not in {"SUPPLIER", "SUPPLIER_PRODUCT"}:
        raise ValueError("invalid identity entity")
    if not reason.strip() or not idempotency_key.strip() or len(idempotency_key) > 160:
        raise ValueError("reason and idempotency key required")
    if action in {"MERGE", "UNMERGE"} and (target_id is None or target_id == source_id):
        raise ValueError("distinct target identity required")
    if action in {"ALIAS_ADD", "ALIAS_REMOVE"} and not (alias_value or "").strip():
        raise ValueError("exact alias required")
    if action in {"ALIAS_REMOVE", "UNMERGE"} and reverses_event_id is None:
        raise ValueError("reversal target required")

    existing = session.scalar(
        select(AssuranceV2IdentityEvent).where(
            AssuranceV2IdentityEvent.organization_id == org_id,
            AssuranceV2IdentityEvent.operation_id == operation_id,
            AssuranceV2IdentityEvent.idempotency_key == idempotency_key,
        )
    )
    if existing is not None:
        if existing.action != action or existing.entity_type != entity_type:
            raise ValueError("identity idempotency collision")
        return existing
    _actor(session, org_id, authenticated_user_id)
    model = UsLaceySupplier if entity_type == "SUPPLIER" else UsLaceySupplierProduct
    for entity_id in (source_id, target_id):
        if entity_id is not None and session.scalar(
            select(model.id).where(model.organization_id == org_id, model.id == entity_id)
        ) is None:
            raise ValueError("identity missing or belongs to different tenant")
    if reverses_event_id is not None:
        previous = session.scalar(
            select(AssuranceV2IdentityEvent).where(
                AssuranceV2IdentityEvent.organization_id == org_id,
                AssuranceV2IdentityEvent.id == reverses_event_id,
                AssuranceV2IdentityEvent.entity_type == entity_type,
                AssuranceV2IdentityEvent.source_supplier_id == (source_id if entity_type == "SUPPLIER" else None)
                if entity_type == "SUPPLIER" else
                AssuranceV2IdentityEvent.source_product_id == source_id,
            )
        )
        if previous is None or (
            previous.action != ("ALIAS_ADD" if action == "ALIAS_REMOVE" else "MERGE")
        ):
            raise ValueError("reversal must reference an original matching identity event")
        if session.scalar(select(AssuranceV2IdentityEvent.id).where(
            AssuranceV2IdentityEvent.organization_id == org_id,
            AssuranceV2IdentityEvent.reverses_event_id == previous.id,
        )) is not None:
            raise ValueError("identity event already reversed")
        if previous.target_supplier_id != (target_id if entity_type == "SUPPLIER" else None) or previous.target_product_id != (target_id if entity_type == "SUPPLIER_PRODUCT" else None) or previous.alias_value != alias_value:
            raise ValueError("reversal does not match original identity binding")
    event = append_operation_event(
        session, organization_id=org_id, operation_id=operation_id,
        actor_type=OperationActorType.USER,
        actor_identity=f"user:{authenticated_user_id}",
        event_type=OperationEventType.HUMAN_REVIEW,
        event_key=f"assurance_v2:identity:{idempotency_key}",
        details={"action": action.lower(), "entity_type": entity_type,
                 "source_entity_id": source_id, "target_entity_id": target_id},
    )
    record = AssuranceV2IdentityEvent(
        organization_id=org_id, operation_id=operation_id,
        actor_user_id=authenticated_user_id, entity_type=entity_type,
        source_supplier_id=source_id if entity_type == "SUPPLIER" else None,
        target_supplier_id=target_id if entity_type == "SUPPLIER" else None,
        source_product_id=source_id if entity_type == "SUPPLIER_PRODUCT" else None,
        target_product_id=target_id if entity_type == "SUPPLIER_PRODUCT" else None,
        alias_value=(alias_value or "").strip() or None, action=action,
        reverses_event_id=reverses_event_id, reason=reason.strip(),
        audit_event_id=event.id, idempotency_key=idempotency_key,
    )
    session.add(record)
    session.flush()
    return record


def build_case_snapshot(
    session: Session, *, organization_id: int, operation_id: int,
    source_set_revision_id: int,
) -> AssuranceCaseSnapshot:
    org_id, op_id = int(organization_id), int(operation_id)
    revision = session.scalar(
        select(UsLaceySourceSetRevision).where(
            UsLaceySourceSetRevision.organization_id == org_id,
            UsLaceySourceSetRevision.operation_id == op_id,
            UsLaceySourceSetRevision.id == source_set_revision_id,
        )
    )
    if revision is None:
        raise ValueError("unknown tenant-owned case revision")
    decisions = session.scalars(
        select(AssuranceV2Decision).where(
            AssuranceV2Decision.organization_id == org_id,
            AssuranceV2Decision.source_set_revision_id == revision.id,
        ).order_by(AssuranceV2Decision.id)
    ).all()
    links = session.scalars(
        select(AssuranceV2MemoryLink)
        .join(AssuranceV2Decision,
              and_(AssuranceV2Decision.id == AssuranceV2MemoryLink.decision_id,
                   AssuranceV2Decision.organization_id == AssuranceV2MemoryLink.organization_id))
        .where(
            AssuranceV2MemoryLink.organization_id == org_id,
            AssuranceV2Decision.source_set_revision_id == revision.id,
        )
    ).all()
    return AssuranceCaseSnapshot(
        snapshot_id=uuid5(NAMESPACE_URL, f"assurance-v2:{org_id}:{op_id}:{revision.id}:{revision.source_set_fingerprint}"),
        contract_version=CONTRACT_VERSION,
        organization_id=org_id, operation_id=op_id,
        source_set_revision_id=revision.id,
        source_set_fingerprint=revision.source_set_fingerprint,
        generation=revision.generation,
        state="CURRENT" if revision.is_current and revision.status == "FINALIZED" else "STALE",
        decision_ids=tuple(d.public_id for d in decisions),
        verified_memory_ids=tuple(m.public_id for m in links),
    )


@dataclass(frozen=True, slots=True)
class LogicalIdentityBinding:
    """Human event projection; not permission for automatic cross-identity reuse."""

    event_id: int
    entity_type: str
    action: str
    source_id: int
    target_id: int | None
    alias_value: str | None


def current_identity_bindings(
    session: Session, *, organization_id: int, entity_type: str,
) -> tuple[LogicalIdentityBinding, ...]:
    """Reversible logical alias/merge view; never modifies physical entity IDs.

    Duplicate sources and cycles are left visible, not arbitrarily resolved.
    Consumers must treat ambiguity as review-required, never pick first.
    """
    kind = entity_type.strip().upper()
    if kind not in {"SUPPLIER", "SUPPLIER_PRODUCT"}:
        raise ValueError("invalid entity kind")
    rows = session.scalars(
        select(AssuranceV2IdentityEvent)
        .where(
            AssuranceV2IdentityEvent.organization_id == int(organization_id),
            AssuranceV2IdentityEvent.entity_type == kind,
        )
        .order_by(AssuranceV2IdentityEvent.id)
    ).all()
    reversed_ids = {row.reverses_event_id for row in rows if row.reverses_event_id is not None}
    entries: list[LogicalIdentityBinding] = []
    for row in rows:
        if row.action not in {"ALIAS_ADD", "MERGE"} or row.id in reversed_ids:
            continue
        source_id = row.source_supplier_id if kind == "SUPPLIER" else row.source_product_id
        target_id = row.target_supplier_id if kind == "SUPPLIER" else row.target_product_id
        if source_id is not None:
            entries.append(LogicalIdentityBinding(
                event_id=int(row.id), entity_type=kind, action=row.action,
                source_id=int(source_id),
                target_id=int(target_id) if target_id is not None else None,
                alias_value=row.alias_value,
            ))
    return tuple(entries)


def revoke_verified_memory(
    session: Session, *, organization_id: int, authenticated_user_id: int,
    memory_public_id: UUID, reason: str, idempotency_key: str,
) -> bool:
    """Human-only, audited revocation of an existing mutable VERIFIED evidence.

    The original evidence claim text, original decision, and memory link remain
    untouched. This updates only the legacy evidence *status* to REVOKED.
    """
    org_id = int(organization_id)
    key = idempotency_key.strip()
    if not reason.strip() or not key or len(key) > 160:
        raise ValueError("revoke requires a reason and idempotency key")
    _actor(session, org_id, authenticated_user_id)
    linked = session.execute(
        select(AssuranceV2MemoryLink, AssuranceV2Decision, UsLaceySupplierEvidence)
        .join(
            AssuranceV2Decision,
            and_(
                AssuranceV2Decision.id == AssuranceV2MemoryLink.decision_id,
                AssuranceV2Decision.organization_id == AssuranceV2MemoryLink.organization_id,
            ),
        )
        .join(
            UsLaceyEvidenceClaim,
            and_(
                UsLaceyEvidenceClaim.id == AssuranceV2MemoryLink.evidence_claim_id,
                UsLaceyEvidenceClaim.organization_id == AssuranceV2MemoryLink.organization_id,
            ),
        )
        .join(
            UsLaceySupplierEvidence,
            and_(
                UsLaceySupplierEvidence.id == UsLaceyEvidenceClaim.evidence_id,
                UsLaceySupplierEvidence.organization_id == UsLaceyEvidenceClaim.organization_id,
            ),
        )
        .where(
            AssuranceV2MemoryLink.organization_id == org_id,
            AssuranceV2MemoryLink.public_id == memory_public_id,
        )
        # The runtime role may update the mutable evidence status, but the
        # historical decision and memory link are deliberately append-only.
        .with_for_update(of=UsLaceySupplierEvidence)
    ).one_or_none()
    if linked is None:
        raise ValueError("unknown tenant-owned memory record")
    memory, decision, evidence = linked
    audit = append_operation_event(
        session,
        organization_id=org_id, operation_id=decision.operation_id,
        actor_type=OperationActorType.USER,
        actor_identity=f"user:{int(authenticated_user_id)}",
        event_type=OperationEventType.HUMAN_REVIEW,
        event_key=f"assurance_v2:revoke:{key}",
        details={
            "action": "revoke_memory",
            "memory_id": str(memory.public_id),
            "reason": reason.strip()[:180],
        },
    )
    # Cannot roll a revoked record back to VERIFIED, even on retry.
    if evidence.status != "VERIFIED":
        return False
    evidence.status = "REVOKED"
    session.flush()
    return True
