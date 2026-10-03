"""Pydantic contracts for reusable U.S. Lacey evidence reconciliation."""
from __future__ import annotations

from datetime import datetime
from enum import Enum
from uuid import UUID

from pydantic import BaseModel, Field


class ReconciliationProvenance(str, Enum):
    """Customer-visible origin/state of one reconciled field."""

    CURRENT_SHIPMENT = "current_shipment"
    REUSED_EVIDENCE = "reused_evidence"
    REVIEW_REQUIRED = "review_required"


class ReconciliationFieldInput(BaseModel):
    """Current-shipment field presented to the conservative reuse layer."""

    field_name: str = Field(min_length=1, max_length=100)
    field_value: str | None = None
    validation_status: str = Field(default="MISSING", max_length=24)


class ReconciliationFieldResponse(BaseModel):
    """One field after current evidence and reusable evidence are reconciled."""

    field_name: str
    field_value: str | None
    provenance: ReconciliationProvenance
    requires_review: bool
    reason: str | None = None
    evidence_public_id: UUID | None = None
    evidence_document_hash: str | None = None
    evidence_valid_until: datetime | None = None
    evidence_verified_at: datetime | None = None


class DetectedSupplierProductInput(BaseModel):
    """Exact product identity produced by a deterministic upstream detector."""

    supplier_key: str = Field(min_length=1, max_length=128)
    product_key: str = Field(min_length=1, max_length=128)
    line_reference: str = Field(min_length=1, max_length=100)
    fields: list[ReconciliationFieldInput]


class ReusableEvidenceReconciliationResponse(BaseModel):
    """Frontend-ready summary for one exact supplier/product/line."""

    supplier_product_public_id: UUID | None
    line_reference: str
    fields: list[ReconciliationFieldResponse]
    current_shipment_count: int
    reused_evidence_count: int
    review_required_count: int
