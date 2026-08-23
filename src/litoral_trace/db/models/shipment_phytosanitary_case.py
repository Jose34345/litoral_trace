"""Structured SENASA/CERT-POV/ePhyto assessment attached to one shipment."""
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


PHYTOSANITARY_CERTIFICATION_MODES: Final[frozenset[str]] = frozenset(
    {"UNASSESSED", "NOT_REQUIRED", "PAPER", "EPHYTO"}
)


class ShipmentPhytosanitaryCase(Base, TimestampMixin):
    """One tenant-safe phytosanitary assessment for an export shipment."""

    __tablename__ = "shipment_phytosanitary_cases"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    public_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), default=uuid4, nullable=False)
    organization_id: Mapped[int] = mapped_column(Integer, nullable=False)
    shipment_id: Mapped[int] = mapped_column(Integer, nullable=False)

    certification_mode: Mapped[str] = mapped_column(String(24), nullable=False)
    requirements_reference: Mapped[str | None] = mapped_column(String(500), nullable=True)
    requirements_checked_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    cert_pov_reference: Mapped[str | None] = mapped_column(String(120), nullable=True)
    certificate_number: Mapped[str | None] = mapped_column(String(120), nullable=True)
    ephyto_reference: Mapped[str | None] = mapped_column(String(160), nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_by_user_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey(
            "users.id",
            ondelete="SET NULL",
            name="fk_shipment_phytosanitary_cases_created_by_user_id",
        ),
        nullable=True,
    )
    updated_by_user_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey(
            "users.id",
            ondelete="SET NULL",
            name="fk_shipment_phytosanitary_cases_updated_by_user_id",
        ),
        nullable=True,
    )

    __table_args__ = (
        UniqueConstraint(
            "id", "organization_id", name="uq_shipment_phytosanitary_cases_id_org"
        ),
        UniqueConstraint(
            "public_id", name="uq_shipment_phytosanitary_cases_public_id"
        ),
        UniqueConstraint(
            "shipment_id", name="uq_shipment_phytosanitary_cases_shipment_id"
        ),
        ForeignKeyConstraint(
            ["organization_id"],
            ["organizations.id"],
            name="fk_shipment_phytosanitary_cases_organization_id",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["shipment_id", "organization_id"],
            ["shipments.id", "shipments.organization_id"],
            name="fk_shipment_phytosanitary_cases_shipment_tenant",
            ondelete="RESTRICT",
        ),
        CheckConstraint(
            "certification_mode IN ('UNASSESSED','NOT_REQUIRED','PAPER','EPHYTO')",
            name="ck_shipment_phytosanitary_cases_mode",
        ),
        Index(
            "ix_shipment_phytosanitary_cases_tenant_shipment",
            "organization_id",
            "shipment_id",
        ),
        Index(
            "ix_shipment_phytosanitary_cases_tenant_mode",
            "organization_id",
            "certification_mode",
        ),
    )
