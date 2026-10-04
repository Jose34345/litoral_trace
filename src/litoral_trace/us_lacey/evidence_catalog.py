"""Read-only customer views over verified reusable supplier evidence."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

from litoral_trace.db.models import (
    UsLaceyEvidenceClaim,
    UsLaceySupplier,
    UsLaceySupplierEvidence,
    UsLaceySupplierProduct,
)
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.us_lacey.db import get_us_lacey_db_session


@dataclass(frozen=True, slots=True)
class EvidenceClaimView:
    field_name: str
    value: str


@dataclass(frozen=True, slots=True)
class EvidenceRecordView:
    public_id: UUID
    evidence_type: str
    source_reference: str | None
    valid_from: datetime
    valid_until: datetime
    verified_at: datetime
    verified_by: str
    status: str
    claims: tuple[EvidenceClaimView, ...]


@dataclass(frozen=True, slots=True)
class SupplierProductEvidenceView:
    public_id: UUID
    product_key: str
    sku: str | None
    display_name: str
    status: str
    evidence: tuple[EvidenceRecordView, ...]


@dataclass(frozen=True, slots=True)
class SupplierEvidenceView:
    public_id: UUID
    supplier_key: str
    display_name: str
    status: str
    products: tuple[SupplierProductEvidenceView, ...]


@dataclass(frozen=True, slots=True)
class EvidenceExpiryView:
    next_30_days: int
    days_31_60: int
    days_61_90: int
    expired: int


@dataclass(frozen=True, slots=True)
class EvidenceCatalogView:
    suppliers: tuple[SupplierEvidenceView, ...]
    supplier_count: int
    product_count: int
    evidence_count: int
    claim_count: int
    expiry: EvidenceExpiryView


@dataclass(frozen=True, slots=True)
class ReusedEvidenceSourceView:
    public_id: UUID
    supplier_name: str
    product_name: str
    verified_at: datetime
    valid_until: datetime
    source_reference: str | None


@dataclass(frozen=True, slots=True)
class ReusedEvidenceSummaryView:
    claim_count: int
    sources: tuple[ReusedEvidenceSourceView, ...]


class UsLaceyEvidenceCatalogService:
    """Tenant-scoped evidence catalog for customer-facing read surfaces."""

    def __init__(self, *, session_factory=None) -> None:
        self._session_factory = session_factory or get_us_lacey_db_session

    def _session(self, organization_id: int) -> Session:
        org_id = int(organization_id)
        if org_id <= 0:
            raise ValueError("organization_id must be positive")
        session = self._session_factory()
        set_tenant_db_context(session, org_id)
        return session

    def catalog(
        self,
        *,
        organization_id: int,
        as_of: datetime | None = None,
    ) -> EvidenceCatalogView:
        org_id = int(organization_id)
        now = as_of or datetime.now(timezone.utc)
        session = self._session(org_id)
        try:
            suppliers = session.scalars(
                select(UsLaceySupplier)
                .where(UsLaceySupplier.organization_id == org_id)
                .order_by(UsLaceySupplier.display_name.asc(), UsLaceySupplier.id.asc())
            ).all()
            products = session.scalars(
                select(UsLaceySupplierProduct)
                .where(UsLaceySupplierProduct.organization_id == org_id)
                .order_by(UsLaceySupplierProduct.supplier_id.asc(), UsLaceySupplierProduct.id.asc())
            ).all()
            evidence = session.scalars(
                select(UsLaceySupplierEvidence)
                .where(UsLaceySupplierEvidence.organization_id == org_id)
                .order_by(
                    UsLaceySupplierEvidence.supplier_product_id.asc(),
                    UsLaceySupplierEvidence.verified_at.desc(),
                    UsLaceySupplierEvidence.id.desc(),
                )
            ).all()
            claims = session.scalars(
                select(UsLaceyEvidenceClaim)
                .where(UsLaceyEvidenceClaim.organization_id == org_id)
                .order_by(UsLaceyEvidenceClaim.evidence_id.asc(), UsLaceyEvidenceClaim.id.asc())
            ).all()

            claims_by_evidence: dict[int, list[EvidenceClaimView]] = {}
            for claim in claims:
                claims_by_evidence.setdefault(int(claim.evidence_id), []).append(
                    EvidenceClaimView(
                        field_name=str(claim.field_name),
                        value=str(claim.normalized_value or claim.field_value),
                    )
                )

            evidence_by_product: dict[int, list[EvidenceRecordView]] = {}
            for row in evidence:
                evidence_by_product.setdefault(int(row.supplier_product_id), []).append(
                    EvidenceRecordView(
                        public_id=row.public_id,
                        evidence_type=str(row.evidence_type),
                        source_reference=row.source_reference,
                        valid_from=row.valid_from,
                        valid_until=row.valid_until,
                        verified_at=row.verified_at,
                        verified_by="Verified reviewer",
                        status=str(row.status),
                        claims=tuple(claims_by_evidence.get(int(row.id), ())),
                    )
                )

            products_by_supplier: dict[int, list[SupplierProductEvidenceView]] = {}
            for product in products:
                label = (
                    str(product.display_name).strip()
                    if product.display_name and str(product.display_name).strip()
                    else str(product.sku or product.product_key)
                )
                products_by_supplier.setdefault(int(product.supplier_id), []).append(
                    SupplierProductEvidenceView(
                        public_id=product.public_id,
                        product_key=str(product.product_key),
                        sku=product.sku,
                        display_name=label,
                        status=str(product.status),
                        evidence=tuple(evidence_by_product.get(int(product.id), ())),
                    )
                )

            supplier_views = tuple(
                SupplierEvidenceView(
                    public_id=supplier.public_id,
                    supplier_key=str(supplier.supplier_key),
                    display_name=str(supplier.display_name),
                    status=str(supplier.status),
                    products=tuple(products_by_supplier.get(int(supplier.id), ())),
                )
                for supplier in suppliers
            )

            active = [row for row in evidence if str(row.status) == "VERIFIED"]
            next_30 = now + timedelta(days=30)
            next_60 = now + timedelta(days=60)
            next_90 = now + timedelta(days=90)
            expiry = EvidenceExpiryView(
                next_30_days=sum(now <= row.valid_until <= next_30 for row in active),
                days_31_60=sum(next_30 < row.valid_until <= next_60 for row in active),
                days_61_90=sum(next_60 < row.valid_until <= next_90 for row in active),
                expired=sum(row.valid_until < now for row in active),
            )
            return EvidenceCatalogView(
                suppliers=supplier_views,
                supplier_count=len(suppliers),
                product_count=len(products),
                evidence_count=len(evidence),
                claim_count=len(claims),
                expiry=expiry,
            )
        finally:
            session.close()

    def reuse_summary(
        self,
        *,
        organization_id: int,
        fields,
    ) -> ReusedEvidenceSummaryView:
        evidence_ids: list[UUID] = []
        claim_count = 0
        for field in fields:
            if getattr(field, "provenance", "") != "reused_evidence":
                continue
            claim_count += 1
            locator = str(getattr(field, "source_locator", "") or "")
            if not locator.startswith("reused_evidence:"):
                continue
            raw = locator.split(":", 1)[1].strip()
            try:
                evidence_ids.append(UUID(raw))
            except (TypeError, ValueError):
                continue

        unique_ids = tuple(dict.fromkeys(evidence_ids))
        if not unique_ids:
            return ReusedEvidenceSummaryView(claim_count=claim_count, sources=())

        org_id = int(organization_id)
        session = self._session(org_id)
        try:
            rows = session.execute(
                select(
                    UsLaceySupplierEvidence,
                    UsLaceySupplierProduct,
                    UsLaceySupplier,
                )
                .join(
                    UsLaceySupplierProduct,
                    (UsLaceySupplierProduct.id == UsLaceySupplierEvidence.supplier_product_id)
                    & (UsLaceySupplierProduct.organization_id == UsLaceySupplierEvidence.organization_id),
                )
                .join(
                    UsLaceySupplier,
                    (UsLaceySupplier.id == UsLaceySupplierProduct.supplier_id)
                    & (UsLaceySupplier.organization_id == UsLaceySupplierProduct.organization_id),
                )
                .where(
                    UsLaceySupplierEvidence.organization_id == org_id,
                    UsLaceySupplierEvidence.public_id.in_(unique_ids),
                )
                .order_by(UsLaceySupplierEvidence.verified_at.desc())
            ).all()
            return ReusedEvidenceSummaryView(
                claim_count=claim_count,
                sources=tuple(
                    ReusedEvidenceSourceView(
                        public_id=evidence.public_id,
                        supplier_name=str(supplier.display_name),
                        product_name=(
                            str(product.display_name)
                            if product.display_name
                            else str(product.sku or product.product_key)
                        ),
                        verified_at=evidence.verified_at,
                        valid_until=evidence.valid_until,
                        source_reference=evidence.source_reference,
                    )
                    for evidence, product, supplier in rows
                ),
            )
        finally:
            session.close()
