"""Customer-facing supplier intelligence backed by reusable evidence."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

from litoral_trace.db.models import UsLaceyOperationField
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.us_lacey.db import get_us_lacey_db_session
from litoral_trace.us_lacey.evidence_catalog import (
    EvidenceClaimView,
    EvidenceRecordView,
    SupplierEvidenceView,
    UsLaceyEvidenceCatalogService,
)


class UsLaceySupplierNotFound(LookupError):
    """Raised when a supplier is unavailable inside the current tenant."""


@dataclass(frozen=True, slots=True)
class SupplierDirectoryRow:
    public_id: UUID
    display_name: str
    product_count: int
    active_claim_count: int
    valid_until: datetime | None
    status: str


@dataclass(frozen=True, slots=True)
class SupplierDirectoryView:
    suppliers: tuple[SupplierDirectoryRow, ...]
    supplier_count: int
    verified_count: int
    needs_review_count: int
    active_claim_count: int


@dataclass(frozen=True, slots=True)
class SupplierEvidenceDetailView:
    public_id: UUID
    product_name: str
    evidence_type: str
    source_reference: str | None
    source_label: str
    valid_from: datetime
    valid_until: datetime
    verified_at: datetime
    claims: tuple[EvidenceClaimView, ...]
    used_in_shipments: int
    active: bool


@dataclass(frozen=True, slots=True)
class SupplierDetailView:
    public_id: UUID
    display_name: str
    supplier_key: str
    status: str
    total_products: int
    verified_claims: int
    operations_supported: int
    evidence: tuple[SupplierEvidenceDetailView, ...]


def _active_evidence(record: EvidenceRecordView, *, as_of: datetime) -> bool:
    return (
        str(record.status).upper() == "VERIFIED"
        and record.valid_from <= as_of < record.valid_until
    )


def _supplier_evidence_records(
    supplier: SupplierEvidenceView,
) -> tuple[tuple[str, EvidenceRecordView], ...]:
    rows: list[tuple[str, EvidenceRecordView]] = []
    for product in supplier.products:
        for evidence in product.evidence:
            rows.append((product.display_name, evidence))
    return tuple(rows)


def _supplier_status(
    supplier: SupplierEvidenceView,
    *,
    active_claim_count: int,
) -> str:
    if str(supplier.status).upper() == "ACTIVE" and active_claim_count > 0:
        return "VERIFIED"
    return "NEEDS_REVIEW"


def _source_label(evidence_type: str) -> str:
    normalized = str(evidence_type or "").strip().upper()
    if normalized in {
        "SUPPLIER_DECLARATION",
        "HUMAN_VERIFIED_SUPPLIER_DOCUMENT",
    }:
        return "Supplier Declaration"
    return normalized.replace("_", " ").title() or "Verified supplier evidence"


class UsLaceySupplierIntelligenceService:
    """Tenant-scoped read model for the Suppliers workspace."""

    def __init__(self, *, session_factory=None) -> None:
        self._session_factory = session_factory or get_us_lacey_db_session
        self._catalog = UsLaceyEvidenceCatalogService(
            session_factory=self._session_factory
        )

    def _session(self, organization_id: int) -> Session:
        org_id = int(organization_id)
        if org_id <= 0:
            raise ValueError("organization_id must be positive")
        session = self._session_factory()
        set_tenant_db_context(session, org_id)
        return session

    def _usage_for_evidence(
        self,
        *,
        organization_id: int,
        evidence_ids: tuple[UUID, ...],
    ) -> dict[UUID, set[int]]:
        if not evidence_ids:
            return {}
        locators = tuple(f"reused_evidence:{public_id}" for public_id in evidence_ids)
        by_locator = {
            f"reused_evidence:{public_id}": public_id for public_id in evidence_ids
        }
        usage: dict[UUID, set[int]] = {public_id: set() for public_id in evidence_ids}
        session = self._session(organization_id)
        try:
            rows = session.execute(
                select(
                    UsLaceyOperationField.operation_id,
                    UsLaceyOperationField.source_locator,
                ).where(
                    UsLaceyOperationField.organization_id
                    == int(organization_id),
                    UsLaceyOperationField.source_locator.in_(locators),
                )
            ).all()
            for operation_id, source_locator in rows:
                evidence_id = by_locator.get(str(source_locator or ""))
                if evidence_id is not None:
                    usage[evidence_id].add(int(operation_id))
            return usage
        finally:
            session.close()

    @staticmethod
    def _valid_until(
        records: tuple[tuple[str, EvidenceRecordView], ...],
        *,
        as_of: datetime,
    ) -> datetime | None:
        verified = [
            evidence.valid_until
            for _, evidence in records
            if str(evidence.status).upper() == "VERIFIED"
        ]
        if not verified:
            return None
        future = [value for value in verified if value >= as_of]
        return min(future) if future else max(verified)

    def directory(
        self,
        *,
        organization_id: int,
        as_of: datetime | None = None,
    ) -> SupplierDirectoryView:
        now = as_of or datetime.now(timezone.utc)
        catalog = self._catalog.catalog(
            organization_id=organization_id,
            as_of=now,
        )
        rows: list[SupplierDirectoryRow] = []
        for supplier in catalog.suppliers:
            records = _supplier_evidence_records(supplier)
            active_claim_count = sum(
                len(evidence.claims)
                for _, evidence in records
                if _active_evidence(evidence, as_of=now)
            )
            rows.append(
                SupplierDirectoryRow(
                    public_id=supplier.public_id,
                    display_name=supplier.display_name,
                    product_count=len(supplier.products),
                    active_claim_count=active_claim_count,
                    valid_until=self._valid_until(records, as_of=now),
                    status=_supplier_status(
                        supplier,
                        active_claim_count=active_claim_count,
                    ),
                )
            )

        suppliers = tuple(rows)
        return SupplierDirectoryView(
            suppliers=suppliers,
            supplier_count=len(suppliers),
            verified_count=sum(row.status == "VERIFIED" for row in suppliers),
            needs_review_count=sum(
                row.status == "NEEDS_REVIEW" for row in suppliers
            ),
            active_claim_count=sum(
                row.active_claim_count for row in suppliers
            ),
        )


    def detail(
        self,
        *,
        organization_id: int,
        supplier_public_id: UUID,
        as_of: datetime | None = None,
    ) -> SupplierDetailView:
        now = as_of or datetime.now(timezone.utc)
        catalog = self._catalog.catalog(
            organization_id=organization_id,
            as_of=now,
        )
        supplier = next(
            (
                item
                for item in catalog.suppliers
                if item.public_id == supplier_public_id
            ),
            None,
        )
        if supplier is None:
            raise UsLaceySupplierNotFound("Supplier not found.")

        records = _supplier_evidence_records(supplier)
        verified_records = tuple(
            (product_name, evidence)
            for product_name, evidence in records
            if str(evidence.status).upper() == "VERIFIED"
        )
        evidence_ids = tuple(
            evidence.public_id for _, evidence in verified_records
        )
        usage = self._usage_for_evidence(
            organization_id=organization_id,
            evidence_ids=evidence_ids,
        )

        evidence_views = tuple(
            SupplierEvidenceDetailView(
                public_id=evidence.public_id,
                product_name=product_name,
                evidence_type=evidence.evidence_type,
                source_reference=evidence.source_reference,
                source_label=_source_label(evidence.evidence_type),
                valid_from=evidence.valid_from,
                valid_until=evidence.valid_until,
                verified_at=evidence.verified_at,
                claims=evidence.claims,
                used_in_shipments=len(usage.get(evidence.public_id, set())),
                active=_active_evidence(evidence, as_of=now),
            )
            for product_name, evidence in verified_records
        )
        active_claims = sum(
            len(evidence.claims)
            for _, evidence in verified_records
            if _active_evidence(evidence, as_of=now)
        )
        supported_operations = {
            operation_id
            for evidence_id in evidence_ids
            for operation_id in usage.get(evidence_id, set())
        }
        return SupplierDetailView(
            public_id=supplier.public_id,
            display_name=supplier.display_name,
            supplier_key=supplier.supplier_key,
            status=_supplier_status(
                supplier,
                active_claim_count=active_claims,
            ),
            total_products=len(supplier.products),
            verified_claims=active_claims,
            operations_supported=len(supported_operations),
            evidence=evidence_views,
        )
