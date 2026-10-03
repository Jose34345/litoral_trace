"""Conservative cross-shipment reuse of verified supplier evidence.

Current shipment evidence always wins.  Reusable evidence is considered only for
an exact tenant-scoped supplier/product identity and only for stable Lacey fields
whose verified evidence window is active.  Shipment-specific quantities are never
reused by this MVP.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import UUID

from sqlalchemy import and_, select
from sqlalchemy.orm import Session

from litoral_trace.db.models import (
    UsLaceyEvidenceClaim,
    UsLaceyOperation,
    UsLaceyOperationField,
    UsLaceySupplier,
    UsLaceySupplierEvidence,
    UsLaceySupplierProduct,
)
from litoral_trace.db.tenant import set_tenant_db_context
from litoral_trace.us_lacey.db import get_us_lacey_db_session
from litoral_trace.us_lacey.ppq505 import validate_ppq_value
from litoral_trace.us_lacey.projection import refresh_us_lacey_operation_status
from litoral_trace.us_lacey.reconciliation_schemas import (
    DetectedSupplierProductInput,
    ReconciliationFieldInput,
    ReconciliationFieldResponse,
    ReconciliationProvenance,
    ReusableEvidenceReconciliationResponse,
)


REUSABLE_FIELD_NAMES = frozenset({"genus", "species", "country_of_harvest"})
REUSABLE_EVIDENCE_EXTRACTOR = "reusable-supplier-evidence"
REUSABLE_EVIDENCE_EXTRACTOR_VERSION = "1"


@dataclass(frozen=True, slots=True)
class _HistoricalClaim:
    claim: UsLaceyEvidenceClaim
    evidence: UsLaceySupplierEvidence
    validated_value: str


def _clean(value: object | None) -> str:
    return str(value or "").strip()


def _comparison_key(value: str) -> str:
    return " ".join(value.split()).casefold()


def _current_field_response(field: ReconciliationFieldInput) -> ReconciliationFieldResponse:
    value = _clean(field.field_value)
    if value:
        validation = validate_ppq_value(field.field_name, value)
        if validation.status.value == "VALID" and validation.normalized_value:
            return ReconciliationFieldResponse(
                field_name=field.field_name,
                field_value=validation.normalized_value,
                provenance=ReconciliationProvenance.CURRENT_SHIPMENT,
                requires_review=False,
                reason="Current shipment evidence takes precedence.",
            )
        return ReconciliationFieldResponse(
            field_name=field.field_name,
            field_value=value,
            provenance=ReconciliationProvenance.REVIEW_REQUIRED,
            requires_review=True,
            reason="Current shipment value requires review; historical evidence is not allowed to override it.",
        )
    return ReconciliationFieldResponse(
        field_name=field.field_name,
        field_value=None,
        provenance=ReconciliationProvenance.REVIEW_REQUIRED,
        requires_review=True,
        reason="No current shipment value is available.",
    )


class ReusableEvidenceService:
    """Tenant-safe resolver for exact supplier/product historical evidence."""

    def __init__(self, *, session_factory=None) -> None:
        self._session_factory = session_factory or get_us_lacey_db_session

    def _session(self, organization_id: int) -> Session:
        org_id = int(organization_id)
        if org_id <= 0:
            raise ValueError("organization_id must be positive")
        session = self._session_factory()
        set_tenant_db_context(session, org_id)
        return session

    @staticmethod
    def _resolve_product(
        session: Session,
        *,
        organization_id: int,
        supplier_key: str,
        product_key: str,
    ) -> UsLaceySupplierProduct | None:
        normalized_supplier_key = _clean(supplier_key)
        normalized_product_key = _clean(product_key)
        if not normalized_supplier_key or not normalized_product_key:
            return None
        return session.scalar(
            select(UsLaceySupplierProduct)
            .join(
                UsLaceySupplier,
                and_(
                    UsLaceySupplier.id == UsLaceySupplierProduct.supplier_id,
                    UsLaceySupplier.organization_id
                    == UsLaceySupplierProduct.organization_id,
                ),
            )
            .where(
                UsLaceySupplierProduct.organization_id == int(organization_id),
                UsLaceySupplierProduct.product_key == normalized_product_key,
                UsLaceySupplierProduct.status == "ACTIVE",
                UsLaceySupplier.supplier_key == normalized_supplier_key,
                UsLaceySupplier.status == "ACTIVE",
            )
        )

    @staticmethod
    def _historical_claims(
        session: Session,
        *,
        organization_id: int,
        supplier_product_id: int,
        as_of: datetime,
    ) -> dict[str, list[_HistoricalClaim]]:
        rows = session.execute(
            select(UsLaceyEvidenceClaim, UsLaceySupplierEvidence)
            .join(
                UsLaceySupplierEvidence,
                and_(
                    UsLaceySupplierEvidence.id == UsLaceyEvidenceClaim.evidence_id,
                    UsLaceySupplierEvidence.organization_id
                    == UsLaceyEvidenceClaim.organization_id,
                ),
            )
            .where(
                UsLaceyEvidenceClaim.organization_id == int(organization_id),
                UsLaceyEvidenceClaim.field_name.in_(tuple(REUSABLE_FIELD_NAMES)),
                UsLaceySupplierEvidence.organization_id == int(organization_id),
                UsLaceySupplierEvidence.supplier_product_id
                == int(supplier_product_id),
                UsLaceySupplierEvidence.status == "VERIFIED",
                UsLaceySupplierEvidence.verified_at.is_not(None),
                UsLaceySupplierEvidence.valid_from <= as_of,
                UsLaceySupplierEvidence.valid_until > as_of,
            )
            .order_by(
                UsLaceySupplierEvidence.verified_at.desc(),
                UsLaceySupplierEvidence.id.desc(),
                UsLaceyEvidenceClaim.id.desc(),
            )
        ).all()

        grouped: dict[str, list[_HistoricalClaim]] = defaultdict(list)
        for claim, evidence in rows:
            candidate = _clean(claim.normalized_value or claim.field_value)
            validation = validate_ppq_value(claim.field_name, candidate)
            if validation.status.value != "VALID" or not validation.normalized_value:
                # Verified metadata that no longer validates against the current
                # regulatory field contract cannot be silently reused.
                continue
            grouped[claim.field_name].append(
                _HistoricalClaim(
                    claim=claim,
                    evidence=evidence,
                    validated_value=validation.normalized_value,
                )
            )
        return dict(grouped)

    @staticmethod
    def _reuse_field(
        field: ReconciliationFieldInput,
        historical: dict[str, list[_HistoricalClaim]],
    ) -> ReconciliationFieldResponse:
        current = _current_field_response(field)
        if _clean(field.field_value):
            return current

        if field.field_name not in REUSABLE_FIELD_NAMES:
            return ReconciliationFieldResponse(
                field_name=field.field_name,
                field_value=None,
                provenance=ReconciliationProvenance.REVIEW_REQUIRED,
                requires_review=True,
                reason="This field is shipment-specific or is not approved for historical reuse.",
            )

        candidates = historical.get(field.field_name, [])
        if not candidates:
            return ReconciliationFieldResponse(
                field_name=field.field_name,
                field_value=None,
                provenance=ReconciliationProvenance.REVIEW_REQUIRED,
                requires_review=True,
                reason="No currently valid verified supplier evidence is available.",
            )

        by_value: dict[str, list[_HistoricalClaim]] = defaultdict(list)
        for candidate in candidates:
            by_value[_comparison_key(candidate.validated_value)].append(candidate)
        if len(by_value) != 1:
            return ReconciliationFieldResponse(
                field_name=field.field_name,
                field_value=None,
                provenance=ReconciliationProvenance.REVIEW_REQUIRED,
                requires_review=True,
                reason="Multiple valid historical evidence records disagree; human review is required.",
            )

        selected = candidates[0]
        return ReconciliationFieldResponse(
            field_name=field.field_name,
            field_value=selected.validated_value,
            provenance=ReconciliationProvenance.REUSED_EVIDENCE,
            requires_review=False,
            reason="Reused from verified supplier evidence because the current shipment is missing this field.",
            evidence_public_id=selected.evidence.public_id,
            evidence_document_hash=selected.evidence.document_hash,
            evidence_valid_until=selected.evidence.valid_until,
            evidence_verified_at=selected.evidence.verified_at,
        )

    def reconcile_detected_product(
        self,
        *,
        organization_id: int,
        detected: DetectedSupplierProductInput,
        as_of: datetime | None = None,
    ) -> ReusableEvidenceReconciliationResponse:
        """Return a frontend-ready reconciliation without mutating shipment state."""
        org_id = int(organization_id)
        effective_time = as_of or datetime.now(timezone.utc)
        session = self._session(org_id)
        try:
            product = self._resolve_product(
                session,
                organization_id=org_id,
                supplier_key=detected.supplier_key,
                product_key=detected.product_key,
            )
            if product is None:
                fields = [
                    (
                        _current_field_response(field)
                        if _clean(field.field_value)
                        else ReconciliationFieldResponse(
                            field_name=field.field_name,
                            field_value=None,
                            provenance=ReconciliationProvenance.REVIEW_REQUIRED,
                            requires_review=True,
                            reason="Exact supplier/product identity is not registered for reusable evidence.",
                        )
                    )
                    for field in detected.fields
                ]
                return self._response(
                    product_public_id=None,
                    line_reference=detected.line_reference,
                    fields=fields,
                )

            historical = self._historical_claims(
                session,
                organization_id=org_id,
                supplier_product_id=product.id,
                as_of=effective_time,
            )
            fields = [
                self._reuse_field(field, historical)
                for field in detected.fields
            ]
            return self._response(
                product_public_id=product.public_id,
                line_reference=detected.line_reference,
                fields=fields,
            )
        finally:
            session.close()

    def apply_to_operation_line(
        self,
        *,
        organization_id: int,
        operation_id: int,
        supplier_key: str,
        product_key: str,
        line_reference: str,
        as_of: datetime | None = None,
    ) -> ReusableEvidenceReconciliationResponse:
        """Inject only missing stable fields for one exact detected product line.

        Existing shipment values, human-reviewed values, review conflicts, and
        shipment-specific fields are never overwritten.
        """
        org_id = int(organization_id)
        effective_time = as_of or datetime.now(timezone.utc)
        line = _clean(line_reference)
        if not line:
            raise ValueError("line_reference is required")

        session = self._session(org_id)
        try:
            operation = session.scalar(
                select(UsLaceyOperation).where(
                    UsLaceyOperation.organization_id == org_id,
                    UsLaceyOperation.id == int(operation_id),
                )
            )
            if operation is None:
                raise ValueError("operation not found")

            product = self._resolve_product(
                session,
                organization_id=org_id,
                supplier_key=supplier_key,
                product_key=product_key,
            )
            rows = session.scalars(
                select(UsLaceyOperationField)
                .where(
                    UsLaceyOperationField.organization_id == org_id,
                    UsLaceyOperationField.operation_id == operation.id,
                    UsLaceyOperationField.merchandise_line_reference == line,
                )
                .order_by(UsLaceyOperationField.id.asc())
            ).all()

            inputs = [
                ReconciliationFieldInput(
                    field_name=row.field_name,
                    field_value=row.human_value or row.normalized_value or row.original_value,
                    validation_status=row.validation_status,
                )
                for row in rows
            ]

            if product is None:
                fields = [
                    (
                        _current_field_response(field)
                        if _clean(field.field_value)
                        else ReconciliationFieldResponse(
                            field_name=field.field_name,
                            field_value=None,
                            provenance=ReconciliationProvenance.REVIEW_REQUIRED,
                            requires_review=True,
                            reason="Exact supplier/product identity is not registered for reusable evidence.",
                        )
                    )
                    for field in inputs
                ]
                return self._response(
                    product_public_id=None,
                    line_reference=line,
                    fields=fields,
                )

            historical = self._historical_claims(
                session,
                organization_id=org_id,
                supplier_product_id=product.id,
                as_of=effective_time,
            )
            fields = [self._reuse_field(field, historical) for field in inputs]
            by_name = {field.field_name: field for field in fields}

            changed = False
            for row in rows:
                resolution = by_name.get(row.field_name)
                if resolution is None:
                    continue
                if (
                    resolution.provenance
                    != ReconciliationProvenance.REUSED_EVIDENCE
                    or not resolution.field_value
                ):
                    continue
                if (
                    row.field_status != "MISSING"
                    or row.human_value
                    or row.normalized_value
                    or row.original_value
                    or row.reviewed_at is not None
                ):
                    continue

                validation = validate_ppq_value(row.field_name, resolution.field_value)
                if validation.status.value != "VALID" or not validation.normalized_value:
                    continue

                row.original_value = resolution.field_value
                row.normalized_value = validation.normalized_value
                row.field_status = "FOUND"
                row.validation_status = "VALID"
                row.validation_error = None
                row.confidence = 1.0
                row.source_assurance_document_id = None
                row.source_page = None
                row.source_locator = (
                    f"reused_evidence:{resolution.evidence_public_id}"
                    if resolution.evidence_public_id is not None
                    else "reused_evidence"
                )
                row.extractor = REUSABLE_EVIDENCE_EXTRACTOR
                row.extractor_version = REUSABLE_EVIDENCE_EXTRACTOR_VERSION
                changed = True

            if changed:
                refresh_us_lacey_operation_status(
                    session,
                    organization_id=org_id,
                    operation=operation,
                )
                session.commit()

            return self._response(
                product_public_id=product.public_id,
                line_reference=line,
                fields=fields,
            )
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    @staticmethod
    def _response(
        *,
        product_public_id: UUID | None,
        line_reference: str,
        fields: list[ReconciliationFieldResponse],
    ) -> ReusableEvidenceReconciliationResponse:
        return ReusableEvidenceReconciliationResponse(
            supplier_product_public_id=product_public_id,
            line_reference=line_reference,
            fields=fields,
            current_shipment_count=sum(
                field.provenance == ReconciliationProvenance.CURRENT_SHIPMENT
                for field in fields
            ),
            reused_evidence_count=sum(
                field.provenance == ReconciliationProvenance.REUSED_EVIDENCE
                for field in fields
            ),
            review_required_count=sum(
                field.provenance == ReconciliationProvenance.REVIEW_REQUIRED
                for field in fields
            ),
        )


def operation_field_provenance(field: UsLaceyOperationField) -> ReconciliationProvenance:
    """Map persisted operation fields to the three frontend provenance labels."""
    if field.extractor == REUSABLE_EVIDENCE_EXTRACTOR:
        return ReconciliationProvenance.REUSED_EVIDENCE
    if field.field_status in {"MISSING", "REVIEW", "CONFLICT"}:
        return ReconciliationProvenance.REVIEW_REQUIRED
    return ReconciliationProvenance.CURRENT_SHIPMENT
