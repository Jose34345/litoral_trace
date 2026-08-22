"""Tenant-safe integration core models for external systems and ERP staging."""
from __future__ import annotations

from datetime import datetime
from typing import Any, Final
from uuid import UUID, uuid4

from sqlalchemy import (
    CheckConstraint,
    DateTime,
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
from sqlalchemy.orm import Mapped, mapped_column

from litoral_trace.db.base import Base, TimestampMixin

INTEGRATION_CONNECTOR_TYPES: Final[frozenset[str]] = frozenset({"GENERIC_ERP"})
INTEGRATION_CONNECTION_STATUSES: Final[frozenset[str]] = frozenset({"ACTIVE", "DISABLED"})
INTEGRATION_SYNC_STATUSES: Final[frozenset[str]] = frozenset(
    {"RUNNING", "SUCCEEDED", "PARTIAL", "FAILED"}
)
INTEGRATION_ENTITY_TYPES: Final[frozenset[str]] = frozenset(
    {"SUPPLIER", "PRODUCT", "RECEIPT", "SHIPMENT"}
)
INTEGRATION_ENTITY_STATUSES: Final[frozenset[str]] = frozenset(
    {"STAGED", "RECONCILED", "CONFLICT", "IGNORED"}
)
INTEGRATION_TARGET_TYPES: Final[frozenset[str]] = frozenset(
    {"LOTE", "TRACEABILITY_BATCH", "TRACEABILITY_EVENT", "SHIPMENT"}
)


class IntegrationConnection(Base, TimestampMixin):
    """Configuration envelope for one external system connection.

    Secrets are deliberately not stored here. ``secret_ref`` may contain only a
    provider-specific reference/name that resolves through the deployment secret
    manager.
    """

    __tablename__ = "integration_connections"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    public_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), default=uuid4, nullable=False)
    organization_id: Mapped[int] = mapped_column(Integer, nullable=False)
    name: Mapped[str] = mapped_column(String(160), nullable=False)
    connector_type: Mapped[str] = mapped_column(String(40), nullable=False)
    status: Mapped[str] = mapped_column(
        String(16), nullable=False, default="ACTIVE", server_default="ACTIVE"
    )
    secret_ref: Mapped[str | None] = mapped_column(String(255), nullable=True)
    config_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)

    __table_args__ = (
        UniqueConstraint("id", "organization_id", name="uq_integration_connections_id_org"),
        UniqueConstraint("public_id", name="uq_integration_connections_public_id"),
        ForeignKeyConstraint(
            ["organization_id"],
            ["organizations.id"],
            name="fk_integration_connections_organization_id",
            ondelete="RESTRICT",
        ),
        CheckConstraint(
            "connector_type IN ('GENERIC_ERP')",
            name="ck_integration_connections_connector_type",
        ),
        CheckConstraint(
            "status IN ('ACTIVE','DISABLED')",
            name="ck_integration_connections_status",
        ),
        Index(
            "uq_integration_connections_tenant_name_ci",
            "organization_id",
            func.lower(name),
            unique=True,
        ),
        Index(
            "ix_integration_connections_tenant_type_status",
            "organization_id",
            "connector_type",
            "status",
        ),
    )


class IntegrationSyncRun(Base):
    """One auditable pull/push attempt for an integration connection."""

    __tablename__ = "integration_sync_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    public_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), default=uuid4, nullable=False)
    organization_id: Mapped[int] = mapped_column(Integer, nullable=False)
    connection_id: Mapped[int] = mapped_column(Integer, nullable=False)
    idempotency_key_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(
        String(16), nullable=False, default="RUNNING", server_default="RUNNING"
    )
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    records_seen: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    records_created: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    records_updated: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    records_unchanged: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    records_conflict: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    error_summary: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (
        UniqueConstraint("id", "organization_id", name="uq_integration_sync_runs_id_org"),
        UniqueConstraint("public_id", name="uq_integration_sync_runs_public_id"),
        UniqueConstraint(
            "connection_id",
            "idempotency_key_hash",
            name="uq_integration_sync_runs_connection_idempotency",
        ),
        ForeignKeyConstraint(
            ["connection_id", "organization_id"],
            ["integration_connections.id", "integration_connections.organization_id"],
            name="fk_integration_sync_runs_connection_tenant",
            ondelete="RESTRICT",
        ),
        CheckConstraint(
            "status IN ('RUNNING','SUCCEEDED','PARTIAL','FAILED')",
            name="ck_integration_sync_runs_status",
        ),
        CheckConstraint(
            "records_seen >= 0 AND records_created >= 0 AND records_updated >= 0 "
            "AND records_unchanged >= 0 AND records_conflict >= 0",
            name="ck_integration_sync_runs_counts",
        ),
        Index(
            "ix_integration_sync_runs_tenant_started",
            "organization_id",
            "started_at",
        ),
        Index(
            "ix_integration_sync_runs_tenant_connection_status",
            "organization_id",
            "connection_id",
            "status",
        ),
    )


class ExternalEntity(Base, TimestampMixin):
    """Version-current staged representation of one external business object."""

    __tablename__ = "external_entities"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    public_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), default=uuid4, nullable=False)
    organization_id: Mapped[int] = mapped_column(Integer, nullable=False)
    connection_id: Mapped[int] = mapped_column(Integer, nullable=False)
    last_sync_run_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    entity_type: Mapped[str] = mapped_column(String(24), nullable=False)
    external_id: Mapped[str] = mapped_column(String(200), nullable=False)
    payload_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    payload_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    normalized_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    status: Mapped[str] = mapped_column(
        String(16), nullable=False, default="STAGED", server_default="STAGED"
    )
    source_updated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    reconciled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    conflict_reason: Mapped[str | None] = mapped_column(String(500), nullable=True)

    __table_args__ = (
        UniqueConstraint("id", "organization_id", name="uq_external_entities_id_org"),
        UniqueConstraint("public_id", name="uq_external_entities_public_id"),
        UniqueConstraint(
            "connection_id",
            "entity_type",
            "external_id",
            name="uq_external_entities_connection_type_external",
        ),
        ForeignKeyConstraint(
            ["connection_id", "organization_id"],
            ["integration_connections.id", "integration_connections.organization_id"],
            name="fk_external_entities_connection_tenant",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["last_sync_run_id", "organization_id"],
            ["integration_sync_runs.id", "integration_sync_runs.organization_id"],
            name="fk_external_entities_sync_run_tenant",
            ondelete="RESTRICT",
        ),
        CheckConstraint(
            "entity_type IN ('SUPPLIER','PRODUCT','RECEIPT','SHIPMENT')",
            name="ck_external_entities_type",
        ),
        CheckConstraint(
            "status IN ('STAGED','RECONCILED','CONFLICT','IGNORED')",
            name="ck_external_entities_status",
        ),
        CheckConstraint("length(payload_hash) = 64", name="ck_external_entities_payload_hash"),
        Index(
            "ix_external_entities_tenant_status_type",
            "organization_id",
            "status",
            "entity_type",
        ),
        Index(
            "ix_external_entities_tenant_connection_updated",
            "organization_id",
            "connection_id",
            "updated_at",
        ),
    )


class ExternalEntityVersion(Base):
    """Immutable payload snapshot for one distinct external-entity version."""

    __tablename__ = "external_entity_versions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    public_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), default=uuid4, nullable=False)
    organization_id: Mapped[int] = mapped_column(Integer, nullable=False)
    external_entity_id: Mapped[int] = mapped_column(Integer, nullable=False)
    sync_run_id: Mapped[int] = mapped_column(Integer, nullable=False)
    payload_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    payload_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    normalized_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    source_updated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        UniqueConstraint("id", "organization_id", name="uq_external_entity_versions_id_org"),
        UniqueConstraint("public_id", name="uq_external_entity_versions_public_id"),
        UniqueConstraint(
            "external_entity_id",
            "payload_hash",
            name="uq_external_entity_versions_entity_hash",
        ),
        ForeignKeyConstraint(
            ["external_entity_id", "organization_id"],
            ["external_entities.id", "external_entities.organization_id"],
            name="fk_external_entity_versions_entity_tenant",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["sync_run_id", "organization_id"],
            ["integration_sync_runs.id", "integration_sync_runs.organization_id"],
            name="fk_external_entity_versions_sync_run_tenant",
            ondelete="RESTRICT",
        ),
        CheckConstraint("length(payload_hash) = 64", name="ck_external_entity_versions_payload_hash"),
        Index(
            "ix_external_entity_versions_tenant_entity_created",
            "organization_id",
            "external_entity_id",
            "created_at",
        ),
    )


class ExternalReference(Base, TimestampMixin):
    """Explicit reconciliation link from an external entity to an LT subject."""

    __tablename__ = "external_references"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    public_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), default=uuid4, nullable=False)
    organization_id: Mapped[int] = mapped_column(Integer, nullable=False)
    external_entity_id: Mapped[int] = mapped_column(Integer, nullable=False)
    target_type: Mapped[str] = mapped_column(String(32), nullable=False)
    target_reference: Mapped[str] = mapped_column(String(200), nullable=False)
    reconciled_by_user_id: Mapped[int | None] = mapped_column(Integer, nullable=True)

    __table_args__ = (
        UniqueConstraint("id", "organization_id", name="uq_external_references_id_org"),
        UniqueConstraint("public_id", name="uq_external_references_public_id"),
        UniqueConstraint("external_entity_id", name="uq_external_references_entity"),
        ForeignKeyConstraint(
            ["external_entity_id", "organization_id"],
            ["external_entities.id", "external_entities.organization_id"],
            name="fk_external_references_entity_tenant",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["reconciled_by_user_id"],
            ["users.id"],
            name="fk_external_references_reconciled_by_user_id",
            ondelete="SET NULL",
        ),
        CheckConstraint(
            "target_type IN ('LOTE','TRACEABILITY_BATCH','TRACEABILITY_EVENT','SHIPMENT')",
            name="ck_external_references_target_type",
        ),
        Index(
            "ix_external_references_tenant_target",
            "organization_id",
            "target_type",
            "target_reference",
        ),
    )


class IntegrationDocument(Base, TimestampMixin):
    """External-system document linked to durable private Vault evidence."""

    __tablename__ = "integration_documents"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    public_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), default=uuid4, nullable=False)
    organization_id: Mapped[int] = mapped_column(Integer, nullable=False)
    external_entity_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    vault_document_id: Mapped[int] = mapped_column(Integer, nullable=False)
    document_type: Mapped[str] = mapped_column(String(64), nullable=False)
    source_reference: Mapped[str | None] = mapped_column(String(200), nullable=True)

    __table_args__ = (
        UniqueConstraint("id", "organization_id", name="uq_integration_documents_id_org"),
        UniqueConstraint("public_id", name="uq_integration_documents_public_id"),
        UniqueConstraint(
            "vault_document_id",
            "external_entity_id",
            name="uq_integration_documents_vault_entity",
        ),
        ForeignKeyConstraint(
            ["external_entity_id", "organization_id"],
            ["external_entities.id", "external_entities.organization_id"],
            name="fk_integration_documents_entity_tenant",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["vault_document_id", "organization_id"],
            ["vault_documents.id", "vault_documents.organization_id"],
            name="fk_integration_documents_vault_tenant",
            ondelete="RESTRICT",
        ),
        Index(
            "ix_integration_documents_tenant_entity",
            "organization_id",
            "external_entity_id",
        ),
    )


class IntegrationEvent(Base):
    """Append-only integration-domain event envelope for diagnostics/auditability."""

    __tablename__ = "integration_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    public_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), default=uuid4, nullable=False)
    organization_id: Mapped[int] = mapped_column(Integer, nullable=False)
    connection_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    sync_run_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    external_entity_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    actor_user_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    metadata_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        UniqueConstraint("id", "organization_id", name="uq_integration_events_id_org"),
        UniqueConstraint("public_id", name="uq_integration_events_public_id"),
        ForeignKeyConstraint(
            ["connection_id", "organization_id"],
            ["integration_connections.id", "integration_connections.organization_id"],
            name="fk_integration_events_connection_tenant",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["sync_run_id", "organization_id"],
            ["integration_sync_runs.id", "integration_sync_runs.organization_id"],
            name="fk_integration_events_sync_run_tenant",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["external_entity_id", "organization_id"],
            ["external_entities.id", "external_entities.organization_id"],
            name="fk_integration_events_entity_tenant",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["actor_user_id"],
            ["users.id"],
            name="fk_integration_events_actor_user_id",
            ondelete="SET NULL",
        ),
        Index(
            "ix_integration_events_tenant_created",
            "organization_id",
            "created_at",
        ),
    )
