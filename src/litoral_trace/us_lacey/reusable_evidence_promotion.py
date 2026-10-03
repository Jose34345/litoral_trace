"""Promotion and deterministic identity discovery for reusable supplier evidence.

This module is the reviewed boundary between one shipment and cross-shipment
memory.  It never creates memory from fuzzy supplier names or product
descriptions.  Promotion requires:

1. a human-reviewed reusable field with an immutable source document;
2. exactly one deterministic Assurance supplier link for that source document;
3. exactly one Product Intelligence bridge link for the reviewed plant line;
4. an explicit SKU identity.

The same identity discovery can later be used by the worker to apply previously
verified evidence to a new shipment.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import UUID

from sqlalchemy import and_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from litoral_trace.db.models import (
    AssuranceDocument,
    AssuranceSupplier,
    DocumentEntityLink,
    UsLaceyEvidenceClaim,
    UsLaceyOperation,
    UsLaceyOperationDocument,
    UsLaceyOperationField,
    UsLaceyProductIntelligenceSnapshot,
    UsLaceySourceSetRevision,
    UsLaceySupplier,
    UsLaceySupplierEvidence,
    UsLaceySupplierProduct,
    VaultDocument,
)
from litoral_trace.us_lacey.reusable_evidence import REUSABLE_FIELD_NAMES


PROMOTED_EVIDENCE_TYPE = "HUMAN_VERIFIED_SUPPLIER_DOCUMENT"


@dataclass(frozen=True, slots=True)
class ReusableProductIdentity:
    supplier_key: str
    supplier_display_name: str
    supplier_normalized_name: str
    product_key: str
    sku: str
    product_display_name: str | None
    line_reference: str


@dataclass(frozen=True, slots=True)
class EvidencePromotionResult:
    promoted: bool
    reason: str
    supplier_public_id: UUID | None = None
    supplier_product_public_id: UUID | None = None
    evidence_public_id: UUID | None = None


def _clean(value: object | None) -> str:
    return str(value or "").strip()


def _canonical_sku_product_key(sku: object | None) -> str:
    token = _clean(sku)
    return f"SKU:{token.upper()}" if token else ""


def _add_one_calendar_year(value: datetime) -> datetime:
    """Return the same instant one calendar year later, including leap-day safety."""
    try:
        return value.replace(year=value.year + 1)
    except ValueError:
        # February 29 -> February 28 of the next calendar year.
        return value.replace(year=value.year + 1, day=28)


def _supplier_identity_for_document(
    session: Session,
    *,
    organization_id: int,
    assurance_document_id: int,
) -> AssuranceSupplier | None:
    links = session.scalars(
        select(DocumentEntityLink).where(
            DocumentEntityLink.organization_id == int(organization_id),
            DocumentEntityLink.assurance_document_id == int(assurance_document_id),
            DocumentEntityLink.entity_type == "SUPPLIER",
        )
    ).all()
    references: set[UUID] = set()
    for link in links:
        raw = _clean(link.entity_reference)
        if not raw.startswith("supplier:"):
            continue
        try:
            references.add(UUID(raw.split(":", 1)[1]))
        except (ValueError, TypeError, AttributeError):
            continue
    if len(references) != 1:
        return None

    supplier = session.scalar(
        select(AssuranceSupplier).where(
            AssuranceSupplier.organization_id == int(organization_id),
            AssuranceSupplier.public_id == next(iter(references)),
            AssuranceSupplier.status != "NEEDS_REVIEW",
        )
    )
    return supplier


def _current_product_bridge(
    session: Session,
    *,
    organization_id: int,
    operation_id: int,
) -> dict:
    snapshot = session.scalar(
        select(UsLaceyProductIntelligenceSnapshot)
        .join(
            UsLaceySourceSetRevision,
            and_(
                UsLaceySourceSetRevision.id
                == UsLaceyProductIntelligenceSnapshot.source_set_revision_id,
                UsLaceySourceSetRevision.organization_id
                == UsLaceyProductIntelligenceSnapshot.organization_id,
            ),
        )
        .where(
            UsLaceyProductIntelligenceSnapshot.organization_id == int(organization_id),
            UsLaceyProductIntelligenceSnapshot.operation_id == int(operation_id),
            UsLaceyProductIntelligenceSnapshot.status != "STALE",
            UsLaceySourceSetRevision.is_current.is_(True),
        )
        .order_by(UsLaceyProductIntelligenceSnapshot.id.desc())
        .limit(1)
    )
    if snapshot is None:
        return {}
    payload = dict(snapshot.payload_json or {})
    bridge = payload.get("shipment_product_bridge")
    return dict(bridge) if isinstance(bridge, dict) else {}


def _product_identity_for_line(
    session: Session,
    *,
    organization_id: int,
    operation_id: int,
    line_reference: str,
    supplier: AssuranceSupplier,
) -> ReusableProductIdentity | None:
    line = _clean(line_reference)
    if not line:
        return None
    bridge = _current_product_bridge(
        session,
        organization_id=organization_id,
        operation_id=operation_id,
    )
    matches = [
        item
        for item in bridge.get("links", ())
        if isinstance(item, dict)
        and item.get("status") == "LINKED"
        and _clean(item.get("shipment_line_reference")) == line
    ]
    if len(matches) != 1:
        return None

    link = matches[0]
    product = link.get("product") if isinstance(link.get("product"), dict) else {}
    sku = _clean(product.get("sku"))
    product_key = _canonical_sku_product_key(sku)
    line_item_key = _clean(link.get("line_item_key"))
    if not sku or not product_key:
        return None
    if _canonical_sku_product_key(line_item_key.split(":", 1)[1] if line_item_key.upper().startswith("SKU:") else "") != product_key:
        return None

    supplier_key = f"assurance_supplier:{supplier.public_id}"
    display_name = _clean(supplier.display_name) or _clean(supplier.cuit)
    normalized_name = _clean(supplier.normalized_name) or display_name.casefold()
    if not display_name or not normalized_name:
        return None

    return ReusableProductIdentity(
        supplier_key=supplier_key,
        supplier_display_name=display_name,
        supplier_normalized_name=normalized_name,
        product_key=product_key,
        sku=sku,
        product_display_name=_clean(product.get("product_name")) or None,
        line_reference=line,
    )


def _ensure_reusable_supplier(
    session: Session,
    *,
    organization_id: int,
    identity: ReusableProductIdentity,
) -> UsLaceySupplier:
    supplier = session.scalar(
        select(UsLaceySupplier).where(
            UsLaceySupplier.organization_id == int(organization_id),
            UsLaceySupplier.supplier_key == identity.supplier_key,
        )
    )
    if supplier is not None:
        if supplier.status != "ACTIVE":
            raise ValueError("reusable supplier is not active")
        return supplier

    candidate = UsLaceySupplier(
        organization_id=int(organization_id),
        supplier_key=identity.supplier_key,
        display_name=identity.supplier_display_name,
        normalized_name=identity.supplier_normalized_name,
        status="ACTIVE",
    )
    try:
        with session.begin_nested():
            session.add(candidate)
            session.flush()
        return candidate
    except IntegrityError:
        supplier = session.scalar(
            select(UsLaceySupplier).where(
                UsLaceySupplier.organization_id == int(organization_id),
                UsLaceySupplier.supplier_key == identity.supplier_key,
            )
        )
        if supplier is None:
            raise
        return supplier


def _ensure_reusable_product(
    session: Session,
    *,
    organization_id: int,
    supplier: UsLaceySupplier,
    identity: ReusableProductIdentity,
) -> UsLaceySupplierProduct:
    product = session.scalar(
        select(UsLaceySupplierProduct).where(
            UsLaceySupplierProduct.organization_id == int(organization_id),
            UsLaceySupplierProduct.supplier_id == supplier.id,
            UsLaceySupplierProduct.product_key == identity.product_key,
        )
    )
    if product is not None:
        if product.status != "ACTIVE":
            raise ValueError("reusable supplier product is not active")
        return product

    candidate = UsLaceySupplierProduct(
        organization_id=int(organization_id),
        supplier_id=supplier.id,
        product_key=identity.product_key,
        sku=identity.sku,
        display_name=identity.product_display_name,
        normalized_name=(
            identity.product_display_name.casefold()
            if identity.product_display_name
            else identity.sku.casefold()
        ),
        status="ACTIVE",
    )
    try:
        with session.begin_nested():
            session.add(candidate)
            session.flush()
        return candidate
    except IntegrityError:
        product = session.scalar(
            select(UsLaceySupplierProduct).where(
                UsLaceySupplierProduct.organization_id == int(organization_id),
                UsLaceySupplierProduct.supplier_id == supplier.id,
                UsLaceySupplierProduct.product_key == identity.product_key,
            )
        )
        if product is None:
            raise
        return product


def _upsert_evidence_and_claim(
    session: Session,
    *,
    organization_id: int,
    product: UsLaceySupplierProduct,
    source_document: AssuranceDocument,
    source_vault: VaultDocument,
    field: UsLaceyOperationField,
    user_id: int,
) -> UsLaceySupplierEvidence:
    verified_at = field.reviewed_at or datetime.now(timezone.utc)
    valid_until = _add_one_calendar_year(verified_at)
    if source_document.valid_until is not None:
        source_valid_until = source_document.valid_until
        if source_valid_until.tzinfo is None:
            source_valid_until = source_valid_until.replace(tzinfo=timezone.utc)
        if source_valid_until <= verified_at:
            raise ValueError("source document is already expired")
        valid_until = min(valid_until, source_valid_until)

    evidence = session.scalar(
        select(UsLaceySupplierEvidence).where(
            UsLaceySupplierEvidence.organization_id == int(organization_id),
            UsLaceySupplierEvidence.supplier_product_id == product.id,
            UsLaceySupplierEvidence.document_hash == source_vault.sha256,
            UsLaceySupplierEvidence.evidence_type == PROMOTED_EVIDENCE_TYPE,
        )
    )
    if evidence is None:
        candidate = UsLaceySupplierEvidence(
            organization_id=int(organization_id),
            supplier_product_id=product.id,
            evidence_type=PROMOTED_EVIDENCE_TYPE,
            document_hash=source_vault.sha256,
            source_reference=f"assurance:{source_document.public_id}",
            valid_from=verified_at,
            valid_until=valid_until,
            verified_by_user_id=int(user_id),
            verified_at=verified_at,
            status="VERIFIED",
        )
        try:
            with session.begin_nested():
                session.add(candidate)
                session.flush()
            evidence = candidate
        except IntegrityError:
            evidence = session.scalar(
                select(UsLaceySupplierEvidence).where(
                    UsLaceySupplierEvidence.organization_id == int(organization_id),
                    UsLaceySupplierEvidence.supplier_product_id == product.id,
                    UsLaceySupplierEvidence.document_hash == source_vault.sha256,
                    UsLaceySupplierEvidence.evidence_type == PROMOTED_EVIDENCE_TYPE,
                )
            )
            if evidence is None:
                raise
    elif evidence.status != "VERIFIED":
        raise ValueError("previous reusable evidence was revoked")
    else:
        # A fresh explicit human review renews the bounded verification window.
        evidence.valid_from = min(evidence.valid_from, verified_at)
        evidence.valid_until = valid_until
        evidence.verified_by_user_id = int(user_id)
        evidence.verified_at = verified_at
        evidence.source_reference = f"assurance:{source_document.public_id}"

    value = _clean(field.human_value or field.normalized_value or field.original_value)
    if not value:
        raise ValueError("reviewed field has no value")

    claim = session.scalar(
        select(UsLaceyEvidenceClaim).where(
            UsLaceyEvidenceClaim.organization_id == int(organization_id),
            UsLaceyEvidenceClaim.evidence_id == evidence.id,
            UsLaceyEvidenceClaim.field_name == field.field_name,
        )
    )
    if claim is None:
        candidate_claim = UsLaceyEvidenceClaim(
            organization_id=int(organization_id),
            evidence_id=evidence.id,
            field_name=field.field_name,
            field_value=value,
            normalized_value=value,
        )
        try:
            with session.begin_nested():
                session.add(candidate_claim)
                session.flush()
        except IntegrityError:
            claim = session.scalar(
                select(UsLaceyEvidenceClaim).where(
                    UsLaceyEvidenceClaim.organization_id == int(organization_id),
                    UsLaceyEvidenceClaim.evidence_id == evidence.id,
                    UsLaceyEvidenceClaim.field_name == field.field_name,
                )
            )
            if claim is None:
                raise
            claim.field_value = value
            claim.normalized_value = value
    else:
        claim.field_value = value
        claim.normalized_value = value

    return evidence


def promote_reviewed_field(
    session: Session,
    *,
    organization_id: int,
    operation: UsLaceyOperation,
    field: UsLaceyOperationField,
    user_id: int,
) -> EvidencePromotionResult:
    """Promote one reviewed field when supplier/product identity is deterministic."""
    org_id = int(organization_id)
    if field.field_name not in REUSABLE_FIELD_NAMES:
        return EvidencePromotionResult(False, "field_not_reusable")
    if field.field_status != "MATCHED" or field.reviewed_at is None:
        return EvidencePromotionResult(False, "field_not_human_confirmed")
    if field.source_assurance_document_id is None:
        return EvidencePromotionResult(False, "no_source_document")

    supplier = _supplier_identity_for_document(
        session,
        organization_id=org_id,
        assurance_document_id=field.source_assurance_document_id,
    )
    if supplier is None:
        return EvidencePromotionResult(False, "supplier_identity_not_deterministic")

    identity = _product_identity_for_line(
        session,
        organization_id=org_id,
        operation_id=operation.id,
        line_reference=field.merchandise_line_reference,
        supplier=supplier,
    )
    if identity is None:
        return EvidencePromotionResult(False, "product_identity_not_deterministic")

    source_row = session.execute(
        select(AssuranceDocument, VaultDocument)
        .join(
            VaultDocument,
            and_(
                VaultDocument.id == AssuranceDocument.vault_document_id,
                VaultDocument.organization_id == AssuranceDocument.organization_id,
            ),
        )
        .where(
            AssuranceDocument.organization_id == org_id,
            AssuranceDocument.id == int(field.source_assurance_document_id),
            VaultDocument.status == "available",
        )
    ).one_or_none()
    if source_row is None:
        return EvidencePromotionResult(False, "source_document_unavailable")
    source_document, source_vault = source_row

    reusable_supplier = _ensure_reusable_supplier(
        session,
        organization_id=org_id,
        identity=identity,
    )
    product = _ensure_reusable_product(
        session,
        organization_id=org_id,
        supplier=reusable_supplier,
        identity=identity,
    )
    evidence = _upsert_evidence_and_claim(
        session,
        organization_id=org_id,
        product=product,
        source_document=source_document,
        source_vault=source_vault,
        field=field,
        user_id=user_id,
    )
    return EvidencePromotionResult(
        True,
        "promoted",
        supplier_public_id=reusable_supplier.public_id,
        supplier_product_public_id=product.public_id,
        evidence_public_id=evidence.public_id,
    )


def discover_operation_reusable_products(
    session: Session,
    *,
    organization_id: int,
    operation_id: int,
) -> tuple[ReusableProductIdentity, ...]:
    """Return exact supplier/SKU identities eligible for automatic reuse.

    Multi-supplier shipments fail closed until a per-line supplier binding exists.
    """
    org_id = int(organization_id)
    document_ids = tuple(
        int(value)
        for value in session.scalars(
            select(UsLaceyOperationDocument.assurance_document_id).where(
                UsLaceyOperationDocument.organization_id == org_id,
                UsLaceyOperationDocument.operation_id == int(operation_id),
                UsLaceyOperationDocument.is_current.is_(True),
            )
        ).all()
    )
    suppliers: dict[UUID, AssuranceSupplier] = {}
    for document_id in document_ids:
        supplier = _supplier_identity_for_document(
            session,
            organization_id=org_id,
            assurance_document_id=document_id,
        )
        if supplier is not None:
            suppliers[supplier.public_id] = supplier
    if len(suppliers) != 1:
        return ()
    supplier = next(iter(suppliers.values()))

    bridge = _current_product_bridge(
        session,
        organization_id=org_id,
        operation_id=int(operation_id),
    )
    identities: list[ReusableProductIdentity] = []
    seen_lines: set[str] = set()
    for link in bridge.get("links", ()):
        if not isinstance(link, dict) or link.get("status") != "LINKED":
            continue
        line_reference = _clean(link.get("shipment_line_reference"))
        if not line_reference or line_reference in seen_lines:
            continue
        identity = _product_identity_for_line(
            session,
            organization_id=org_id,
            operation_id=int(operation_id),
            line_reference=line_reference,
            supplier=supplier,
        )
        if identity is None:
            continue
        seen_lines.add(line_reference)
        identities.append(identity)
    return tuple(identities)
