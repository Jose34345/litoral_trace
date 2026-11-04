"""Persistent semantic evidence graph foundations (Phase A)."""
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
from litoral_trace.db.models.document_text import DocumentTextSpan
from litoral_trace.db.models.us_lacey_evidence_snapshot import UsLaceyEvidenceSnapshot


class SemanticEvidenceNode(Base):
    """Immutable semantic candidate rooted in one original document text span."""

    __tablename__ = "semantic_evidence_nodes"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    public_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), default=uuid4, nullable=False)
    organization_id: Mapped[int] = mapped_column(Integer, nullable=False)
    assurance_document_id: Mapped[int] = mapped_column(Integer, nullable=False)
    extraction_run_id: Mapped[int] = mapped_column(Integer, nullable=False)
    source_span_id: Mapped[int] = mapped_column(Integer, nullable=False)
    target_field: Mapped[str] = mapped_column(String(100), nullable=False)
    semantic_role: Mapped[str] = mapped_column(String(100), nullable=False)
    scope: Mapped[str] = mapped_column(String(64), nullable=False)
    local_entity_key: Mapped[str | None] = mapped_column(String(512), nullable=True)
    original_value: Mapped[str] = mapped_column(Text, nullable=False)
    normalized_value: Mapped[str | None] = mapped_column(Text, nullable=True)
    evidence_class: Mapped[str] = mapped_column(String(16), nullable=False)
    document_type: Mapped[str] = mapped_column(String(64), nullable=False)
    extraction_confidence: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    authority_score: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    candidate_score: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    source_span: Mapped[DocumentTextSpan] = relationship(back_populates="evidence_nodes")
    snapshot_links: Mapped[list["SemanticSnapshotNode"]] = relationship(
        back_populates="evidence_node", cascade="all, delete-orphan", overlaps="snapshot,snapshot_nodes"
    )

    __table_args__ = (
        ForeignKeyConstraint(
            ["assurance_document_id", "organization_id"],
            ["assurance_documents.id", "assurance_documents.organization_id"],
            name="fk_semantic_evidence_nodes_assurance_tenant",
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["extraction_run_id", "organization_id"],
            ["document_extraction_runs.id", "document_extraction_runs.organization_id"],
            name="fk_semantic_evidence_nodes_extraction_run_tenant",
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["source_span_id", "organization_id"],
            ["document_text_spans.id", "document_text_spans.organization_id"],
            name="fk_semantic_evidence_nodes_span_tenant",
            ondelete="CASCADE",
        ),
        UniqueConstraint("id", "organization_id", name="uq_semantic_evidence_nodes_id_org"),
        UniqueConstraint("public_id", name="uq_semantic_evidence_nodes_public_id"),
        UniqueConstraint("organization_id", "fingerprint", name="uq_semantic_evidence_nodes_fingerprint"),
        CheckConstraint(
            "evidence_class IN ('EXPLICIT','DERIVED','INFERRED')",
            name="ck_semantic_evidence_nodes_class",
        ),
        CheckConstraint(
            "extraction_confidence >= 0 AND extraction_confidence <= 1",
            name="ck_semantic_evidence_nodes_confidence",
        ),
        Index("ix_semantic_evidence_nodes_org_document", "organization_id", "assurance_document_id"),
        Index("ix_semantic_evidence_nodes_org_field", "organization_id", "target_field"),
        Index("ix_semantic_evidence_nodes_org_local_entity", "organization_id", "local_entity_key"),
    )


class SemanticSnapshotNode(Base):
    """Snapshot-scoped membership and canonical-entity resolution for a node."""

    __tablename__ = "semantic_snapshot_nodes"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    organization_id: Mapped[int] = mapped_column(Integer, nullable=False)
    snapshot_id: Mapped[int] = mapped_column(Integer, nullable=False)
    evidence_node_id: Mapped[int] = mapped_column(Integer, nullable=False)
    canonical_entity_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    snapshot: Mapped[UsLaceyEvidenceSnapshot] = relationship(back_populates="snapshot_nodes", overlaps="snapshot_links")
    evidence_node: Mapped[SemanticEvidenceNode] = relationship(back_populates="snapshot_links", overlaps="snapshot,snapshot_nodes")

    __table_args__ = (
        ForeignKeyConstraint(
            ["snapshot_id", "organization_id"],
            ["us_lacey_evidence_snapshots.id", "us_lacey_evidence_snapshots.organization_id"],
            name="fk_semantic_snapshot_nodes_snapshot_tenant",
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["evidence_node_id", "organization_id"],
            ["semantic_evidence_nodes.id", "semantic_evidence_nodes.organization_id"],
            name="fk_semantic_snapshot_nodes_node_tenant",
            ondelete="CASCADE",
        ),
        UniqueConstraint("id", "organization_id", name="uq_semantic_snapshot_nodes_id_org"),
        UniqueConstraint(
            "organization_id", "snapshot_id", "evidence_node_id",
            name="uq_semantic_snapshot_nodes_member",
        ),
        Index("ix_semantic_snapshot_nodes_org_snapshot", "organization_id", "snapshot_id"),
        Index("ix_semantic_snapshot_nodes_org_canonical", "organization_id", "snapshot_id", "canonical_entity_id"),
    )


class SemanticEvidenceEdge(Base):
    """Snapshot-scoped semantic relationship between two immutable evidence nodes."""

    __tablename__ = "semantic_evidence_edges"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    organization_id: Mapped[int] = mapped_column(Integer, nullable=False)
    snapshot_id: Mapped[int] = mapped_column(Integer, nullable=False)
    from_node_id: Mapped[int] = mapped_column(Integer, nullable=False)
    to_node_id: Mapped[int] = mapped_column(Integer, nullable=False)
    relation_type: Mapped[str] = mapped_column(String(32), nullable=False)
    confidence: Mapped[float] = mapped_column(Float, nullable=False, default=1.0)
    reason_code: Mapped[str | None] = mapped_column(String(100), nullable=True)
    metadata_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    snapshot: Mapped[UsLaceyEvidenceSnapshot] = relationship(back_populates="edges")

    __table_args__ = (
        ForeignKeyConstraint(
            ["snapshot_id", "organization_id"],
            ["us_lacey_evidence_snapshots.id", "us_lacey_evidence_snapshots.organization_id"],
            name="fk_semantic_evidence_edges_snapshot_tenant",
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["from_node_id", "organization_id"],
            ["semantic_evidence_nodes.id", "semantic_evidence_nodes.organization_id"],
            name="fk_semantic_evidence_edges_from_node_tenant",
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["to_node_id", "organization_id"],
            ["semantic_evidence_nodes.id", "semantic_evidence_nodes.organization_id"],
            name="fk_semantic_evidence_edges_to_node_tenant",
            ondelete="CASCADE",
        ),
        UniqueConstraint("id", "organization_id", name="uq_semantic_evidence_edges_id_org"),
        UniqueConstraint(
            "organization_id", "snapshot_id", "from_node_id", "to_node_id", "relation_type",
            name="uq_semantic_evidence_edges_relation",
        ),
        CheckConstraint(
            "relation_type IN ('CORROBORATES','CONTRADICTS','SAME_ENTITY_AS','SUPERSEDES','OUT_OF_SCOPE','DERIVED_FROM')",
            name="ck_semantic_evidence_edges_relation_type",
        ),
        CheckConstraint("confidence >= 0 AND confidence <= 1", name="ck_semantic_evidence_edges_confidence"),
        Index("ix_semantic_evidence_edges_org_snapshot", "organization_id", "snapshot_id"),
        Index("ix_semantic_evidence_edges_org_from", "organization_id", "from_node_id"),
        Index("ix_semantic_evidence_edges_org_to", "organization_id", "to_node_id"),
    )
