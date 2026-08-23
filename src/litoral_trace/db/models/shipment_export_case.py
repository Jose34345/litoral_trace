"""Structured export dossier metadata attached to one traced shipment.

Binary documents remain in Vault and are linked through TraceabilityEvidenceLink.
This model stores only shipment-level Corrientes/ARCA/SIM references needed to
calculate a deterministic export-readiness result.
"""
from __future__ import annotations

from datetime import datetime
from typing import Final
from uuid import UUID, uuid4

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    Uuid,
)
from sqlalchemy.orm import Mapped, mapped_column

from litoral_trace.db.base import Base, TimestampMixin


EXPORT_ORIGIN_PROFILES: Final[frozenset[str]] = frozenset({"CULTIVATED", "NATIVE"})


class ShipmentExportCase(Base, TimestampMixin):
    """One structured Corrientes/ARCA export case for a tenant shipment."""

    __tablename__ = "shipment_export_cases"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    public_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), default=uuid4, nullable=False)
    organization_id: Mapped[int] = mapped_column(Integer, nullable=False)
    shipment_id: Mapped[int] = mapped_column(Integer, nullable=False)

    origin_profile: Mapped[str] = mapped_column(String(24), nullable=False)
    export_invoice_number: Mapped[str | None] = mapped_column(String(80), nullable=True)
    export_invoice_cae: Mapped[str | None] = mapped_column(String(32), nullable=True)
    customs_destination_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    customs_subregime: Mapped[str | None] = mapped_column(String(16), nullable=True)
    customs_officialized_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_by_user_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey(
            "users.id",
            ondelete="SET NULL",
            name="fk_shipment_export_cases_created_by_user_id",
        ),
        nullable=True,
    )
    updated_by_user_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey(
            "users.id",
            ondelete="SET NULL",
            name="fk_shipment_export_cases_updated_by_user_id",
        ),
        nullable=True,
    )

    __table_args__ = (
        UniqueConstraint("id", "organization_id", name="uq_shipment_export_cases_id_org"),
        UniqueConstraint("public_id", name="uq_shipment_export_cases_public_id"),
        UniqueConstraint("shipment_id", name="uq_shipment_export_cases_shipment_id"),
        ForeignKeyConstraint(
            ["organization_id"],
            ["organizations.id"],
            name="fk_shipment_export_cases_organization_id",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["shipment_id", "organization_id"],
            ["shipments.id", "shipments.organization_id"],
            name="fk_shipment_export_cases_shipment_tenant",
            ondelete="RESTRICT",
        ),
        CheckConstraint(
            "origin_profile IN ('CULTIVATED','NATIVE')",
            name="ck_shipment_export_cases_origin_profile",
        ),
        Index(
            "ix_shipment_export_cases_tenant_shipment",
            "organization_id",
            "shipment_id",
        ),
        Index(
            "ix_shipment_export_cases_tenant_customs_destination",
            "organization_id",
            "customs_destination_id",
        ),
    )
