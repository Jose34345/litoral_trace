"""Schema-only U.S. Lacey evidence snapshot foundations (Phase A)."""
from __future__ import annotations

from datetime import datetime
from uuid import UUID, uuid4

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    Float,
    ForeignKeyConstraint,
    Index,
    Integer,
    JSON,
    String,
    Text,
    UniqueConstraint,
    Uuid,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from litoral_trace.db.base import Base
from litoral_trace.db.models.us_lacey import UsLaceyOperation


class UsLaceyEvidenceSnapshot(Base):
    """Immutable operation-wide source-set generation once promoted to CURRENT."""

    __tablename__ = "us_lacey_evidence_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    public_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), default=uuid4, nullable=False)
    organization_id: Mapped[int] = mapped_column(Integer, nullable=False)
    operation_id: Mapped[int] = mapped_column(Integer, nullable=False)
    generation: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="BUILDING")
    source_set_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    graph_version: Mapped[str] = mapped_column(String(64), nullable=False)
    ontology_version: Mapped[str] = mapped_column(String(64), nullable=False)
    translation_pipeline_version: Mapped[str] = mapped_column(String(64), nullable=False)
    document_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    node_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    conflict_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    finalized_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    documents: Mapped[list["UsLaceyEvidenceSnapshotDocument"]] = relationship(
        back_populates="snapshot", cascade="all, delete-orphan"
    )
    snapshot_nodes: Mapped[list["SemanticSnapshotNode"]] = relationship(
        back_populates="snapshot", cascade="all, delete-orphan", overlaps="snapshot_links"
    )
    edges: Mapped[list["SemanticEvidenceEdge"]] = relationship(
        back_populates="snapshot", cascade="all, delete-orphan"
    )

    __table_args__ = (
        ForeignKeyConstraint(
            ["operation_id", "organization_id"],
            ["us_lacey_operations.id", "us_lacey_operations.organization_id"],
            name="fk_lacey_evidence_snapshots_operation_tenant",
            ondelete="CASCADE",
        ),
        UniqueConstraint("id", "organization_id", name="uq_lacey_evidence_snapshots_id_org"),
        UniqueConstraint("public_id", name="uq_lacey_evidence_snapshots_public_id"),
        UniqueConstraint(
            "organization_id", "operation_id", "generation",
            name="uq_lacey_evidence_snapshots_generation",
        ),
        UniqueConstraint(
            "organization_id", "operation_id", "source_set_fingerprint",
            name="uq_lacey_evidence_snapshots_fingerprint",
        ),
        CheckConstraint("generation > 0", name="ck_lacey_evidence_snapshots_generation"),
        CheckConstraint(
            "status IN ('BUILDING','CURRENT','SUPERSEDED','FAILED')",
            name="ck_lacey_evidence_snapshots_status",
        ),
        CheckConstraint("document_count >= 0", name="ck_lacey_evidence_snapshots_document_count"),
        CheckConstraint("node_count >= 0", name="ck_lacey_evidence_snapshots_node_count"),
        CheckConstraint("conflict_count >= 0", name="ck_lacey_evidence_snapshots_conflict_count"),
        Index("ix_lacey_evidence_snapshots_org_operation", "organization_id", "operation_id"),
        Index("ix_lacey_evidence_snapshots_org_status", "organization_id", "status"),
    )


class UsLaceyEvidenceSnapshotDocument(Base):
    """Exact immutable document/extraction-run member of an evidence snapshot."""

    __tablename__ = "us_lacey_evidence_snapshot_documents"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    organization_id: Mapped[int] = mapped_column(Integer, nullable=False)
    snapshot_id: Mapped[int] = mapped_column(Integer, nullable=False)
    operation_document_id: Mapped[int] = mapped_column(Integer, nullable=False)
    assurance_document_id: Mapped[int] = mapped_column(Integer, nullable=False)
    extraction_run_id: Mapped[int] = mapped_column(Integer, nullable=False)
    source_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    document_role: Mapped[str] = mapped_column(String(64), nullable=False, default="UNKNOWN")
    processing_result: Mapped[str] = mapped_column(String(16), nullable=False, default="SUCCEEDED")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    snapshot: Mapped[UsLaceyEvidenceSnapshot] = relationship(back_populates="documents")

    __table_args__ = (
        ForeignKeyConstraint(
            ["snapshot_id", "organization_id"],
            ["us_lacey_evidence_snapshots.id", "us_lacey_evidence_snapshots.organization_id"],
            name="fk_lacey_snapshot_documents_snapshot_tenant",
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["operation_document_id", "organization_id"],
            ["us_lacey_operation_documents.id", "us_lacey_operation_documents.organization_id"],
            name="fk_lacey_snapshot_documents_operation_document_tenant",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["assurance_document_id", "organization_id"],
            ["assurance_documents.id", "assurance_documents.organization_id"],
            name="fk_lacey_snapshot_documents_assurance_tenant",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["extraction_run_id", "organization_id"],
            ["document_extraction_runs.id", "document_extraction_runs.organization_id"],
            name="fk_lacey_snapshot_documents_extraction_run_tenant",
            ondelete="RESTRICT",
        ),
        UniqueConstraint("id", "organization_id", name="uq_lacey_snapshot_documents_id_org"),
        UniqueConstraint(
            "organization_id", "snapshot_id", "operation_document_id",
            name="uq_lacey_snapshot_documents_member",
        ),
        CheckConstraint(
            "processing_result IN ('SUCCEEDED','FAILED','SKIPPED')",
            name="ck_lacey_snapshot_documents_result",
        ),
        Index("ix_lacey_snapshot_documents_org_snapshot", "organization_id", "snapshot_id"),
        Index("ix_lacey_snapshot_documents_org_assurance", "organization_id", "assurance_document_id"),
    )


# Add the nullable pointer to the legacy operation mapping without changing the
# legacy module or runtime behavior.  use_alter mirrors migration 045 and breaks
# the intentional operation <-> snapshot DDL cycle for fresh metadata creates.
if "current_evidence_snapshot_id" not in UsLaceyOperation.__table__.c:
    setattr(
        UsLaceyOperation,
        "current_evidence_snapshot_id",
        mapped_column(Integer, nullable=True),
    )

if not any(
    constraint.name == "fk_us_lacey_operations_current_snapshot_tenant"
    for constraint in UsLaceyOperation.__table__.foreign_key_constraints
):
    UsLaceyOperation.__table__.append_constraint(
        ForeignKeyConstraint(
            ["current_evidence_snapshot_id", "organization_id"],
            ["us_lacey_evidence_snapshots.id", "us_lacey_evidence_snapshots.organization_id"],
            name="fk_us_lacey_operations_current_snapshot_tenant",
            use_alter=True,
        )
    )
