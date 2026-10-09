"""Promotion and discovery for exact reusable U.S. Lacey supplier evidence.

This is the reviewed authority boundary between one shipment and cross-shipment
memory. Supplier/product identity is owned by the U.S. Lacey identity graph, not
by the legacy AssuranceSupplier/CUIT model. Promotion remains fail-closed:
only explicitly human-reviewed stable fields, tied to an exact current
supplier-product relationship and immutable source document, become VERIFIED
reusable evidence.
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
    DocumentEntityLink,
    UsLaceyEvidenceClaim,
    UsLaceyOperation,
    UsLaceyOperationField,
    UsLaceyOperationProductLink,
    UsLaceySourceSetRevision,
    UsLaceySupplier,
    UsLaceySupplierEvidence,
    UsLaceySupplierProduct,
    VaultDocument,
)
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.us_lacey.db import get_us_lacey_db_session
from litoral_trace.us_lacey.reusable_evidence import REUSABLE_FIELD_NAMES


PROMOTED_EVIDENCE_TYPE = "HUMAN_VERIFIED_SUPPLIER_DOCUMENT"
_SUPPLIER_REFERENCE_PREFIX = "us_lacey_supplier:"


@dataclass(frozen=True, slots=True)
class ReusableProductIdentity:
    supplier_id: int
    supplier_product_id: int
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


def _add_one_calendar_year(value: datetime) -> datetime:
    """Return the same instant one calendar year later, including leap-day safety."""
    try:
        return value.replace(year=value.year + 1)
    except ValueError:
        return value.replace(year=value.year + 1, day=28)


def _current_source_revision(
    session: Session,
    *,
    organization_id: int,
    operation_id: int,
) -> UsLaceySourceSetRevision | None:
    return session.scalar(
        select(UsLaceySourceSetRevision)
        .where(
            UsLaceySourceSetRevision.organization_id == int(organization_id),
            UsLaceySourceSetRevision.operation_id == int(operation_id),
            UsLaceySourceSetRevision.is_current.is_(True),
        )
        .order_by(UsLaceySourceSetRevision.id.desc())
        .limit(1)
    )


def _supplier_identity_for_document(
    session: Session,
    *,
    organization_id: int,
    assurance_document_id: int,
) -> UsLaceySupplier | None:
    """Resolve one exact U.S. Lacey supplier linked to the source document."""
    links = session.scalars(
        select(DocumentEntityLink).where(
            DocumentEntityLink.organization_id == int(organization_id),
            DocumentEntityLink.assurance_document_id
            == int(assurance_document_id),
            DocumentEntityLink.entity_type == "SUPPLIER",
        )
    ).all()

    references: set[UUID] = set()
    for link in links:
        raw = _clean(link.entity_reference)
        if not raw.startswith(_SUPPLIER_REFERENCE_PREFIX):
            continue
        try:
            references.add(
                UUID(raw[len(_SUPPLIER_REFERENCE_PREFIX) :])
            )
        except (ValueError, TypeError, AttributeError):
            continue
    if len(references) != 1:
        return None

    return session.scalar(
        select(UsLaceySupplier).where(
            UsLaceySupplier.organization_id == int(organization_id),
            UsLaceySupplier.public_id == next(iter(references)),
            UsLaceySupplier.status.in_(("ACTIVE", "VERIFIED")),
        )
    )


def _identity_from_rows(
    *,
    link: UsLaceyOperationProductLink,
    product: UsLaceySupplierProduct,
    supplier: UsLaceySupplier,
) -> ReusableProductIdentity | None:
    sku = _clean(product.sku)
    if not sku or not _clean(product.product_key):
        return None
    return ReusableProductIdentity(
        supplier_id=int(supplier.id),
        supplier_product_id=int(product.id),
        supplier_key=str(supplier.supplier_key),
        supplier_display_name=str(supplier.display_name),
        supplier_normalized_name=str(supplier.normalized_name),
        product_key=str(product.product_key),
        sku=sku,
        product_display_name=(
            _clean(product.display_name) or None
        ),
        line_reference=str(link.line_reference),
    )


def _product_identity_for_line(
    session: Session,
    *,
    organization_id: int,
    operation_id: int,
    line_reference: str,
    supplier: UsLaceySupplier,
) -> ReusableProductIdentity | None:
    line = _clean(line_reference)
    if not line:
        return None
    revision = _current_source_revision(
        session,
        organization_id=organization_id,
        operation_id=operation_id,
    )
    if revision is None:
        return None

    rows = session.execute(
        select(
            UsLaceyOperationProductLink,
            UsLaceySupplierProduct,
        )
        .join(
            UsLaceySupplierProduct,
            and_(
                UsLaceySupplierProduct.id
                == UsLaceyOperationProductLink.supplier_product_id,
                UsLaceySupplierProduct.organization_id
                == UsLaceyOperationProductLink.organization_id,
            ),
        )
        .where(
            UsLaceyOperationProductLink.organization_id
            == int(organization_id),
            UsLaceyOperationProductLink.operation_id == int(operation_id),
            UsLaceyOperationProductLink.source_set_revision_id
            == int(revision.id),
            UsLaceyOperationProductLink.line_reference == line,
            UsLaceySupplierProduct.supplier_id == int(supplier.id),
            UsLaceySupplierProduct.status.in_(("ACTIVE", "VERIFIED")),
        )
    ).all()
    if len(rows) != 1:
        return None
    link, product = rows[0]
    return _identity_from_rows(
        link=link,
        product=product,
        supplier=supplier,
    )


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
            source_valid_until = source_valid_until.replace(
                tzinfo=timezone.utc
            )
        if source_valid_until <= verified_at:
            raise ValueError("source document is already expired")
        valid_until = min(valid_until, source_valid_until)

    evidence = session.scalar(
        select(UsLaceySupplierEvidence).where(
            UsLaceySupplierEvidence.organization_id
            == int(organization_id),
            UsLaceySupplierEvidence.supplier_product_id
            == int(product.id),
            UsLaceySupplierEvidence.document_hash == source_vault.sha256,
            UsLaceySupplierEvidence.evidence_type
            == PROMOTED_EVIDENCE_TYPE,
        )
    )
    if evidence is None:
        candidate = UsLaceySupplierEvidence(
            organization_id=int(organization_id),
            supplier_product_id=int(product.id),
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
                    UsLaceySupplierEvidence.organization_id
                    == int(organization_id),
                    UsLaceySupplierEvidence.supplier_product_id
                    == int(product.id),
                    UsLaceySupplierEvidence.document_hash
                    == source_vault.sha256,
                    UsLaceySupplierEvidence.evidence_type
                    == PROMOTED_EVIDENCE_TYPE,
                )
            )
            if evidence is None:
                raise
    elif evidence.status != "VERIFIED":
        raise ValueError("previous reusable evidence was revoked")
    else:
        evidence.valid_from = min(evidence.valid_from, verified_at)
        evidence.valid_until = valid_until
        evidence.verified_by_user_id = int(user_id)
        evidence.verified_at = verified_at
        evidence.source_reference = (
            f"assurance:{source_document.public_id}"
        )

    value = _clean(
        field.human_value
        or field.normalized_value
        or field.original_value
    )
    if not value:
        raise ValueError("reviewed field has no value")

    claim = session.scalar(
        select(UsLaceyEvidenceClaim).where(
            UsLaceyEvidenceClaim.organization_id
            == int(organization_id),
            UsLaceyEvidenceClaim.evidence_id == int(evidence.id),
            UsLaceyEvidenceClaim.field_name == field.field_name,
        )
    )
    if claim is None:
        candidate_claim = UsLaceyEvidenceClaim(
            organization_id=int(organization_id),
            evidence_id=int(evidence.id),
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
                    UsLaceyEvidenceClaim.organization_id
                    == int(organization_id),
                    UsLaceyEvidenceClaim.evidence_id == int(evidence.id),
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
    """Promote one human-reviewed stable field when identity is exact."""
    org_id = int(organization_id)
    if field.field_name not in REUSABLE_FIELD_NAMES:
        return EvidencePromotionResult(False, "field_not_reusable")
    if field.field_status != "MATCHED" or field.reviewed_at is None:
        return EvidencePromotionResult(
            False,
            "field_not_human_confirmed",
        )
    if field.source_assurance_document_id is None:
        return EvidencePromotionResult(False, "no_source_document")

    supplier = _supplier_identity_for_document(
        session,
        organization_id=org_id,
        assurance_document_id=int(
            field.source_assurance_document_id
        ),
    )
    if supplier is None:
        return EvidencePromotionResult(
            False,
            "supplier_identity_not_deterministic",
        )

    identity = _product_identity_for_line(
        session,
        organization_id=org_id,
        operation_id=int(operation.id),
        line_reference=field.merchandise_line_reference,
        supplier=supplier,
    )
    if identity is None:
        return EvidencePromotionResult(
            False,
            "product_identity_not_deterministic",
        )

    product = session.scalar(
        select(UsLaceySupplierProduct).where(
            UsLaceySupplierProduct.organization_id == org_id,
            UsLaceySupplierProduct.id
            == int(identity.supplier_product_id),
            UsLaceySupplierProduct.supplier_id == int(supplier.id),
            UsLaceySupplierProduct.status.in_(("ACTIVE", "VERIFIED")),
        )
    )
    if product is None:
        return EvidencePromotionResult(
            False,
            "product_identity_not_active",
        )

    source_row = session.execute(
        select(AssuranceDocument, VaultDocument)
        .join(
            VaultDocument,
            and_(
                VaultDocument.id == AssuranceDocument.vault_document_id,
                VaultDocument.organization_id
                == AssuranceDocument.organization_id,
            ),
        )
        .where(
            AssuranceDocument.organization_id == org_id,
            AssuranceDocument.id
            == int(field.source_assurance_document_id),
            VaultDocument.status == "available",
        )
    ).one_or_none()
    if source_row is None:
        return EvidencePromotionResult(
            False,
            "source_document_unavailable",
        )
    source_document, source_vault = source_row

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
        supplier_public_id=supplier.public_id,
        supplier_product_public_id=product.public_id,
        evidence_public_id=evidence.public_id,
    )


def discover_operation_reusable_products(
    session: Session,
    *,
    organization_id: int,
    operation_id: int,
) -> tuple[ReusableProductIdentity, ...]:
    """Return exact current line/product identities eligible for reuse."""
    org_id = int(organization_id)
    revision = _current_source_revision(
        session,
        organization_id=org_id,
        operation_id=int(operation_id),
    )
    if revision is None:
        return ()

    rows = session.execute(
        select(
            UsLaceyOperationProductLink,
            UsLaceySupplierProduct,
            UsLaceySupplier,
        )
        .join(
            UsLaceySupplierProduct,
            and_(
                UsLaceySupplierProduct.id
                == UsLaceyOperationProductLink.supplier_product_id,
                UsLaceySupplierProduct.organization_id
                == UsLaceyOperationProductLink.organization_id,
            ),
        )
        .join(
            UsLaceySupplier,
            and_(
                UsLaceySupplier.id
                == UsLaceySupplierProduct.supplier_id,
                UsLaceySupplier.organization_id
                == UsLaceySupplierProduct.organization_id,
            ),
        )
        .where(
            UsLaceyOperationProductLink.organization_id == org_id,
            UsLaceyOperationProductLink.operation_id
            == int(operation_id),
            UsLaceyOperationProductLink.source_set_revision_id
            == int(revision.id),
            UsLaceySupplier.status.in_(("ACTIVE", "VERIFIED")),
            UsLaceySupplierProduct.status.in_(("ACTIVE", "VERIFIED")),
        )
        .order_by(
            UsLaceyOperationProductLink.line_reference.asc(),
            UsLaceyOperationProductLink.id.asc(),
        )
    ).all()

    identities: list[ReusableProductIdentity] = []
    seen_lines: set[str] = set()
    for link, product, supplier in rows:
        line = _clean(link.line_reference)
        if not line or line in seen_lines:
            continue
        identity = _identity_from_rows(
            link=link,
            product=product,
            supplier=supplier,
        )
        if identity is None:
            continue
        seen_lines.add(line)
        identities.append(identity)
    return tuple(identities)


def apply_reusable_evidence_for_operation(
    *,
    organization_id: int,
    operation_id: int,
    session_factory=None,
) -> int:
    """Inject valid verified historical claims into exact current line gaps."""
    factory = session_factory or get_us_lacey_db_session
    org_id = int(organization_id)
    session = factory()
    try:
        set_tenant_db_context(session, org_id)
        identities = discover_operation_reusable_products(
            session,
            organization_id=org_id,
            operation_id=int(operation_id),
        )
    finally:
        session.close()

    from litoral_trace.us_lacey.reusable_evidence import (
        ReusableEvidenceService,
    )

    service = ReusableEvidenceService(session_factory=factory)
    reused_count = 0
    for identity in identities:
        result = service.apply_to_operation_line(
            organization_id=org_id,
            operation_id=int(operation_id),
            supplier_key=identity.supplier_key,
            product_key=identity.product_key,
            line_reference=identity.line_reference,
        )
        reused_count += int(result.reused_evidence_count)
    return reused_count


__all__ = [
    "EvidencePromotionResult",
    "PROMOTED_EVIDENCE_TYPE",
    "ReusableProductIdentity",
    "apply_reusable_evidence_for_operation",
    "discover_operation_reusable_products",
    "promote_reviewed_field",
]
