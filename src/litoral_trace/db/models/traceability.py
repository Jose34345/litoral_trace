"""Industrial traceability graph models for end-to-end chain of custody."""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Final
from uuid import UUID, uuid4

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    Uuid,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from litoral_trace.db.base import Base, TimestampMixin

if TYPE_CHECKING:
    from litoral_trace.db.models.lote import Lote
    from litoral_trace.db.models.user import User

TRACEABILITY_BATCH_STAGES: Final[frozenset[str]] = frozenset(
    {"RECEIPT", "RAW_MATERIAL", "INTERMEDIATE", "FINISHED_GOOD"}
)
TRACEABILITY_BATCH_STATUSES: Final[frozenset[str]] = frozenset(
    {"ACTIVE", "CLOSED", "VOID"}
)
TRACEABILITY_EVENT_TYPES: Final[frozenset[str]] = frozenset(
    {"RECEIPT", "TRANSFORMATION", "MIX", "SPLIT", "REPACK", "ADJUSTMENT"}
)
TRACEABILITY_EVENT_STATUSES: Final[frozenset[str]] = frozenset(
    {"DRAFT", "POSTED", "VOID"}
)
TRACEABILITY_UNITS: Final[frozenset[str]] = frozenset({"TON", "KG", "M3"})


class TraceabilityBatch(Base, TimestampMixin):
    """Material lot node in the industrial genealogy graph."""

    __tablename__ = "traceability_batches"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    public_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), default=uuid4, nullable=False)
    organization_id: Mapped[int] = mapped_column(Integer, nullable=False)
    code: Mapped[str] = mapped_column(String(120), nullable=False)
    product_name: Mapped[str] = mapped_column(String(160), nullable=False)
    stage: Mapped[str] = mapped_column(String(32), nullable=False)
    unit: Mapped[str] = mapped_column(String(16), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="ACTIVE", server_default="ACTIVE")
    source_lote_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_by_user_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("users.id", ondelete="SET NULL", name="fk_traceability_batches_created_by_user_id"),
        nullable=True,
    )

    source_lote: Mapped[Lote | None] = relationship("Lote")
    created_by_user: Mapped[User | None] = relationship("User")
    event_inputs: Mapped[list[TraceabilityEventInput]] = relationship(
        "TraceabilityEventInput", back_populates="batch", cascade="save-update, merge"
    )
    event_outputs: Mapped[list[TraceabilityEventOutput]] = relationship(
        "TraceabilityEventOutput", back_populates="batch", cascade="save-update, merge"
    )
    shipment_items: Mapped[list[ShipmentItem]] = relationship(
        "ShipmentItem", back_populates="batch", cascade="save-update, merge"
    )

    __table_args__ = (
        UniqueConstraint("id", "organization_id", name="uq_traceability_batches_id_org"),
        UniqueConstraint("public_id", name="uq_traceability_batches_public_id"),
        ForeignKeyConstraint(
            ["organization_id"], ["organizations.id"],
            name="fk_traceability_batches_organization_id", ondelete="RESTRICT"
        ),
        ForeignKeyConstraint(
            ["source_lote_id", "organization_id"],
            ["lotes.id", "lotes.organization_id"],
            name="fk_traceability_batches_source_lote_tenant",
            ondelete="RESTRICT",
        ),
        CheckConstraint(
            "stage IN ('RECEIPT','RAW_MATERIAL','INTERMEDIATE','FINISHED_GOOD')",
            name="ck_traceability_batches_stage",
        ),
        CheckConstraint("unit IN ('TON','KG','M3')", name="ck_traceability_batches_unit"),
        CheckConstraint("status IN ('ACTIVE','CLOSED','VOID')", name="ck_traceability_batches_status"),
        Index("uq_traceability_batches_tenant_code_ci", "organization_id", func.lower(code), unique=True),
        Index("ix_traceability_batches_tenant_stage_status", "organization_id", "stage", "status"),
        Index("ix_traceability_batches_tenant_source_lote", "organization_id", "source_lote_id"),
    )


class TraceabilityEvent(Base, TimestampMixin):
    """Industrial event connecting one or more input batches to output batches."""

    __tablename__ = "traceability_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    public_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), default=uuid4, nullable=False)
    organization_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("organizations.id", ondelete="RESTRICT", name="fk_traceability_events_organization_id"),
        nullable=False,
    )
    event_code: Mapped[str] = mapped_column(String(120), nullable=False)
    event_type: Mapped[str] = mapped_column(String(32), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="DRAFT", server_default="DRAFT")
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    facility_reference: Mapped[str | None] = mapped_column(String(160), nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_by_user_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("users.id", ondelete="SET NULL", name="fk_traceability_events_created_by_user_id"),
        nullable=True,
    )

    # Association rows intentionally share organization_id across two tenant-safe
    # composite foreign keys. ``overlaps`` documents that shared write path while
    # the database constraints continue to enforce that both parents are same-tenant.
    inputs: Mapped[list[TraceabilityEventInput]] = relationship(
        "TraceabilityEventInput",
        back_populates="event",
        cascade="save-update, merge",
        overlaps="event_inputs",
    )
    outputs: Mapped[list[TraceabilityEventOutput]] = relationship(
        "TraceabilityEventOutput",
        back_populates="event",
        cascade="save-update, merge",
        overlaps="event_outputs",
    )
    created_by_user: Mapped[User | None] = relationship("User")

    __table_args__ = (
        UniqueConstraint("id", "organization_id", name="uq_traceability_events_id_org"),
        UniqueConstraint("public_id", name="uq_traceability_events_public_id"),
        CheckConstraint(
            "event_type IN ('RECEIPT','TRANSFORMATION','MIX','SPLIT','REPACK','ADJUSTMENT')",
            name="ck_traceability_events_type",
        ),
        CheckConstraint("status IN ('DRAFT','POSTED','VOID')", name="ck_traceability_events_status"),
        Index("uq_traceability_events_tenant_code_ci", "organization_id", func.lower(event_code), unique=True),
        Index("ix_traceability_events_tenant_occurred_at", "organization_id", "occurred_at"),
        Index("ix_traceability_events_tenant_type_status", "organization_id", "event_type", "status"),
    )


class TraceabilityEventInput(Base):
    """Quantity of one batch consumed by an industrial event."""

    __tablename__ = "traceability_event_inputs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    organization_id: Mapped[int] = mapped_column(Integer, nullable=False)
    event_id: Mapped[int] = mapped_column(Integer, nullable=False)
    batch_id: Mapped[int] = mapped_column(Integer, nullable=False)
    quantity: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)
    unit: Mapped[str] = mapped_column(String(16), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())

    event: Mapped[TraceabilityEvent] = relationship(
        "TraceabilityEvent",
        back_populates="inputs",
        overlaps="event_inputs",
    )
    batch: Mapped[TraceabilityBatch] = relationship(
        "TraceabilityBatch",
        back_populates="event_inputs",
        overlaps="event,inputs",
    )

    __table_args__ = (
        ForeignKeyConstraint(
            ["event_id", "organization_id"],
            ["traceability_events.id", "traceability_events.organization_id"],
            name="fk_traceability_event_inputs_event_tenant", ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["batch_id", "organization_id"],
            ["traceability_batches.id", "traceability_batches.organization_id"],
            name="fk_traceability_event_inputs_batch_tenant", ondelete="RESTRICT",
        ),
        UniqueConstraint("event_id", "batch_id", name="uq_traceability_event_inputs_event_batch"),
        CheckConstraint("quantity > 0", name="ck_traceability_event_inputs_quantity"),
        CheckConstraint("unit IN ('TON','KG','M3')", name="ck_traceability_event_inputs_unit"),
        Index("ix_traceability_event_inputs_tenant_batch", "organization_id", "batch_id"),
    )


class TraceabilityEventOutput(Base):
    """Quantity of one batch produced by an industrial event."""

    __tablename__ = "traceability_event_outputs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    organization_id: Mapped[int] = mapped_column(Integer, nullable=False)
    event_id: Mapped[int] = mapped_column(Integer, nullable=False)
    batch_id: Mapped[int] = mapped_column(Integer, nullable=False)
    quantity: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)
    unit: Mapped[str] = mapped_column(String(16), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())

    event: Mapped[TraceabilityEvent] = relationship(
        "TraceabilityEvent",
        back_populates="outputs",
        overlaps="event_outputs",
    )
    batch: Mapped[TraceabilityBatch] = relationship(
        "TraceabilityBatch",
        back_populates="event_outputs",
        overlaps="event,outputs",
    )

    __table_args__ = (
        ForeignKeyConstraint(
            ["event_id", "organization_id"],
            ["traceability_events.id", "traceability_events.organization_id"],
            name="fk_traceability_event_outputs_event_tenant", ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["batch_id", "organization_id"],
            ["traceability_batches.id", "traceability_batches.organization_id"],
            name="fk_traceability_event_outputs_batch_tenant", ondelete="RESTRICT",
        ),
        UniqueConstraint("event_id", "batch_id", name="uq_traceability_event_outputs_event_batch"),
        CheckConstraint("quantity > 0", name="ck_traceability_event_outputs_quantity"),
        CheckConstraint("unit IN ('TON','KG','M3')", name="ck_traceability_event_outputs_unit"),
        Index("ix_traceability_event_outputs_tenant_batch", "organization_id", "batch_id"),
    )


class Shipment(Base, TimestampMixin):
    """Commercial dispatch/sale that consumes one or more traceability batches."""

    __tablename__ = "shipments"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    public_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), default=uuid4, nullable=False)
    organization_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("organizations.id", ondelete="RESTRICT", name="fk_shipments_organization_id"),
        nullable=False,
    )
    shipment_code: Mapped[str] = mapped_column(String(120), nullable=False)
    sale_reference: Mapped[str | None] = mapped_column(String(160), nullable=True)
    buyer_reference: Mapped[str | None] = mapped_column(String(160), nullable=True)
    destination_country: Mapped[str | None] = mapped_column(String(2), nullable=True)
    shipped_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="DRAFT", server_default="DRAFT")
    created_by_user_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("users.id", ondelete="SET NULL", name="fk_shipments_created_by_user_id"),
        nullable=True,
    )

    items: Mapped[list[ShipmentItem]] = relationship(
        "ShipmentItem",
        back_populates="shipment",
        cascade="save-update, merge",
        overlaps="shipment_items",
    )
    created_by_user: Mapped[User | None] = relationship("User")

    __table_args__ = (
        UniqueConstraint("id", "organization_id", name="uq_shipments_id_org"),
        UniqueConstraint("public_id", name="uq_shipments_public_id"),
        CheckConstraint(
            "status IN ('DRAFT','CONFIRMED','DISPATCHED','CANCELLED')",
            name="ck_shipments_status",
        ),
        CheckConstraint(
            "destination_country IS NULL OR length(destination_country) = 2",
            name="ck_shipments_destination_country",
        ),
        Index("uq_shipments_tenant_code_ci", "organization_id", func.lower(shipment_code), unique=True),
        Index("ix_shipments_tenant_shipped_at", "organization_id", "shipped_at"),
        Index("ix_shipments_tenant_status", "organization_id", "status"),
    )


class ShipmentItem(Base):
    """Quantity of a traceability batch allocated to one shipment."""

    __tablename__ = "shipment_items"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    organization_id: Mapped[int] = mapped_column(Integer, nullable=False)
    shipment_id: Mapped[int] = mapped_column(Integer, nullable=False)
    batch_id: Mapped[int] = mapped_column(Integer, nullable=False)
    quantity: Mapped[Decimal] = mapped_column(Numeric(18, 6), nullable=False)
    unit: Mapped[str] = mapped_column(String(16), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())

    shipment: Mapped[Shipment] = relationship(
        "Shipment",
        back_populates="items",
        overlaps="shipment_items",
    )
    batch: Mapped[TraceabilityBatch] = relationship(
        "TraceabilityBatch",
        back_populates="shipment_items",
        overlaps="items,shipment",
    )

    __table_args__ = (
        ForeignKeyConstraint(
            ["shipment_id", "organization_id"],
            ["shipments.id", "shipments.organization_id"],
            name="fk_shipment_items_shipment_tenant", ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["batch_id", "organization_id"],
            ["traceability_batches.id", "traceability_batches.organization_id"],
            name="fk_shipment_items_batch_tenant", ondelete="RESTRICT",
        ),
        UniqueConstraint("shipment_id", "batch_id", name="uq_shipment_items_shipment_batch"),
        CheckConstraint("quantity > 0", name="ck_shipment_items_quantity"),
        CheckConstraint("unit IN ('TON','KG','M3')", name="ck_shipment_items_unit"),
        Index("ix_shipment_items_tenant_batch", "organization_id", "batch_id"),
    )
