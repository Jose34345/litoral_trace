"""Assurance V2 authority journal and provenance links.

The existing supplier/evidence/semantic graph remains the system of facts.
These append-only rows attach explicit human authority to those facts.
"""
from __future__ import annotations

from datetime import datetime
from uuid import UUID, uuid4

from sqlalchemy import (
    CheckConstraint, DateTime, ForeignKeyConstraint, Index, Integer, JSON,
    String, Text, UniqueConstraint, Uuid, func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from litoral_trace.db.base import Base


def _json_type():
    return JSON().with_variant(JSONB(), "postgresql")


class AssuranceV2Decision(Base):
    __tablename__ = "us_lacey_assurance_v2_decisions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    public_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), default=uuid4, nullable=False)
    organization_id: Mapped[int] = mapped_column(Integer, nullable=False)
    operation_id: Mapped[int] = mapped_column(Integer, nullable=False)
    source_set_revision_id: Mapped[int] = mapped_column(Integer, nullable=False)
    source_set_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    generation: Mapped[int] = mapped_column(Integer, nullable=False)
    actor_user_id: Mapped[int] = mapped_column(Integer, nullable=False)
    action: Mapped[str] = mapped_column(String(16), nullable=False)
    field_name: Mapped[str] = mapped_column(String(100), nullable=False)
    line_reference: Mapped[str | None] = mapped_column(String(100), nullable=True)
    selected_value: Mapped[str | None] = mapped_column(Text, nullable=True)
    reason: Mapped[str] = mapped_column(Text, nullable=False)
    context_json: Mapped[dict] = mapped_column(_json_type(), nullable=False, default=dict)
    supersedes_decision_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    audit_event_id: Mapped[int] = mapped_column(Integer, nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(160), nullable=False)
    decided_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())

    __table_args__ = (
        ForeignKeyConstraint(["operation_id", "organization_id"], ["us_lacey_operations.id", "us_lacey_operations.organization_id"], ondelete="CASCADE"),
        ForeignKeyConstraint(["source_set_revision_id", "organization_id"], ["us_lacey_source_set_revisions.id", "us_lacey_source_set_revisions.organization_id"], ondelete="CASCADE"),
        ForeignKeyConstraint(["audit_event_id", "organization_id"], ["us_lacey_operation_events.id", "us_lacey_operation_events.organization_id"], ondelete="CASCADE"),
        ForeignKeyConstraint(["actor_user_id", "organization_id"], ["users.id", "users.organization_id"], ondelete="CASCADE"),
        ForeignKeyConstraint(["supersedes_decision_id", "organization_id"], ["us_lacey_assurance_v2_decisions.id", "us_lacey_assurance_v2_decisions.organization_id"], ondelete="CASCADE"),
        UniqueConstraint("id", "organization_id", name="uq_assurance_v2_decision_tenant"),
        UniqueConstraint("public_id", name="uq_assurance_v2_decision_public"),
        UniqueConstraint("organization_id", "operation_id", "idempotency_key", name="uq_assurance_v2_decision_idempotent"),
        UniqueConstraint("organization_id", "supersedes_decision_id", name="uq_assurance_v2_decision_one_successor"),
        CheckConstraint("action IN ('ACCEPT','REJECT','CORRECT','SUPERSEDE')", name="ck_assurance_v2_decision_action"),
        CheckConstraint("generation > 0", name="ck_assurance_v2_decision_generation"),
        CheckConstraint("length(trim(reason)) > 0", name="ck_assurance_v2_decision_reason"),
        CheckConstraint("(action IN ('REJECT','SUPERSEDE')) OR (selected_value IS NOT NULL AND length(trim(selected_value)) > 0)", name="ck_assurance_v2_decision_value"),
        Index("ix_assurance_v2_decision_case", "organization_id", "operation_id", "source_set_revision_id"),
    )


class AssuranceV2DecisionSource(Base):
    __tablename__ = "us_lacey_assurance_v2_decision_sources"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    organization_id: Mapped[int] = mapped_column(Integer, nullable=False)
    decision_id: Mapped[int] = mapped_column(Integer, nullable=False)
    assurance_document_id: Mapped[int] = mapped_column(Integer, nullable=False)
    operation_document_id: Mapped[int] = mapped_column(Integer, nullable=False)
    document_version_number: Mapped[int] = mapped_column(Integer, nullable=False)
    semantic_evidence_node_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    source_operation_field_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    source_span_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())

    __table_args__ = (
        ForeignKeyConstraint(["decision_id", "organization_id"], ["us_lacey_assurance_v2_decisions.id", "us_lacey_assurance_v2_decisions.organization_id"], ondelete="CASCADE"),
        ForeignKeyConstraint(["assurance_document_id", "organization_id"], ["assurance_documents.id", "assurance_documents.organization_id"], ondelete="CASCADE"),
        ForeignKeyConstraint(["operation_document_id", "organization_id"], ["us_lacey_operation_documents.id", "us_lacey_operation_documents.organization_id"], ondelete="CASCADE"),
        ForeignKeyConstraint(["semantic_evidence_node_id", "organization_id"], ["semantic_evidence_nodes.id", "semantic_evidence_nodes.organization_id"], ondelete="CASCADE"),
        ForeignKeyConstraint(["source_operation_field_id", "organization_id"], ["us_lacey_operation_fields.id", "us_lacey_operation_fields.organization_id"], ondelete="CASCADE"),
        ForeignKeyConstraint(["source_span_id", "organization_id"], ["document_text_spans.id", "document_text_spans.organization_id"], ondelete="CASCADE"),
        UniqueConstraint("id", "organization_id", name="uq_assurance_v2_decision_source_tenant"),
        Index("ix_assurance_v2_decision_source_decision", "organization_id", "decision_id"),
    )


class AssuranceV2MemoryLink(Base):
    """Versioned authority/provenance overlay for existing verified evidence claims."""

    __tablename__ = "us_lacey_assurance_v2_memory_links"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    public_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), default=uuid4, nullable=False)
    organization_id: Mapped[int] = mapped_column(Integer, nullable=False)
    decision_id: Mapped[int] = mapped_column(Integer, nullable=False)
    evidence_claim_id: Mapped[int] = mapped_column(Integer, nullable=False)
    supplier_product_id: Mapped[int] = mapped_column(Integer, nullable=False)
    assurance_document_id: Mapped[int] = mapped_column(Integer, nullable=False)
    source_set_revision_id: Mapped[int] = mapped_column(Integer, nullable=False)
    context_json: Mapped[dict] = mapped_column(_json_type(), nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())

    __table_args__ = (
        ForeignKeyConstraint(["decision_id", "organization_id"], ["us_lacey_assurance_v2_decisions.id", "us_lacey_assurance_v2_decisions.organization_id"], ondelete="CASCADE"),
        ForeignKeyConstraint(["evidence_claim_id", "organization_id"], ["us_lacey_evidence_claim.id", "us_lacey_evidence_claim.organization_id"], ondelete="CASCADE"),
        ForeignKeyConstraint(["supplier_product_id", "organization_id"], ["us_lacey_supplier_product.id", "us_lacey_supplier_product.organization_id"], ondelete="CASCADE"),
        ForeignKeyConstraint(["assurance_document_id", "organization_id"], ["assurance_documents.id", "assurance_documents.organization_id"], ondelete="CASCADE"),
        ForeignKeyConstraint(["source_set_revision_id", "organization_id"], ["us_lacey_source_set_revisions.id", "us_lacey_source_set_revisions.organization_id"], ondelete="CASCADE"),
        UniqueConstraint("public_id", name="uq_assurance_v2_memory_public"),
        UniqueConstraint("id", "organization_id", name="uq_assurance_v2_memory_tenant"),
        UniqueConstraint("organization_id", "decision_id", "evidence_claim_id", name="uq_assurance_v2_memory_once"),
        Index("ix_assurance_v2_memory_product", "organization_id", "supplier_product_id"),
    )


class AssuranceV2IdentityEvent(Base):
    """Logical, reversible aliases/merges without physical key rewrites."""

    __tablename__ = "us_lacey_assurance_v2_identity_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    public_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), default=uuid4, nullable=False)
    organization_id: Mapped[int] = mapped_column(Integer, nullable=False)
    operation_id: Mapped[int] = mapped_column(Integer, nullable=False)
    actor_user_id: Mapped[int] = mapped_column(Integer, nullable=False)
    entity_type: Mapped[str] = mapped_column(String(24), nullable=False)
    source_supplier_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    target_supplier_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    source_product_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    target_product_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    alias_value: Mapped[str | None] = mapped_column(String(512), nullable=True)
    action: Mapped[str] = mapped_column(String(16), nullable=False)
    reverses_event_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    reason: Mapped[str] = mapped_column(Text, nullable=False)
    audit_event_id: Mapped[int] = mapped_column(Integer, nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(160), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())

    __table_args__ = (
        ForeignKeyConstraint(["operation_id", "organization_id"], ["us_lacey_operations.id", "us_lacey_operations.organization_id"], ondelete="CASCADE"),
        ForeignKeyConstraint(["audit_event_id", "organization_id"], ["us_lacey_operation_events.id", "us_lacey_operation_events.organization_id"], ondelete="CASCADE"),
        ForeignKeyConstraint(["actor_user_id", "organization_id"], ["users.id", "users.organization_id"], ondelete="CASCADE"),
        ForeignKeyConstraint(["source_supplier_id", "organization_id"], ["us_lacey_supplier.id", "us_lacey_supplier.organization_id"], ondelete="CASCADE"),
        ForeignKeyConstraint(["target_supplier_id", "organization_id"], ["us_lacey_supplier.id", "us_lacey_supplier.organization_id"], ondelete="CASCADE"),
        ForeignKeyConstraint(["source_product_id", "organization_id"], ["us_lacey_supplier_product.id", "us_lacey_supplier_product.organization_id"], ondelete="CASCADE"),
        ForeignKeyConstraint(["target_product_id", "organization_id"], ["us_lacey_supplier_product.id", "us_lacey_supplier_product.organization_id"], ondelete="CASCADE"),
        ForeignKeyConstraint(["reverses_event_id", "organization_id"], ["us_lacey_assurance_v2_identity_events.id", "us_lacey_assurance_v2_identity_events.organization_id"], ondelete="CASCADE"),
        UniqueConstraint("public_id", name="uq_assurance_v2_identity_public"),
        UniqueConstraint("id", "organization_id", name="uq_assurance_v2_identity_tenant"),
        UniqueConstraint("organization_id", "operation_id", "idempotency_key", name="uq_assurance_v2_identity_idempotent"),
        CheckConstraint("action IN ('ALIAS_ADD','ALIAS_REMOVE','MERGE','UNMERGE')", name="ck_assurance_v2_identity_action"),
        CheckConstraint("entity_type IN ('SUPPLIER','SUPPLIER_PRODUCT')", name="ck_assurance_v2_identity_type"),
        CheckConstraint("length(trim(reason)) > 0", name="ck_assurance_v2_identity_reason"),
        Index("ix_assurance_v2_identity_case", "organization_id", "entity_type", "source_supplier_id", "source_product_id"),
    )

\n