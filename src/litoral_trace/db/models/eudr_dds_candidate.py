"""Tenant-safe local EUDR API V3 DDS candidate attached to one Shipment."""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Final
from uuid import UUID, uuid4

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    Date,
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
)
from sqlalchemy.orm import Mapped, mapped_column

from litoral_trace.db.base import Base, TimestampMixin


EUDR_ACTIVITY_TYPES: Final[frozenset[str]] = frozenset(
    {"IMPORT", "DOMESTIC", "EXPORT"}
)
EUDR_COMMODITY_PROFILES: Final[frozenset[str]] = frozenset(
    {"WOOD", "OTHER_EUDR"}
)
EUDR_RISK_CONCLUSIONS: Final[frozenset[str]] = frozenset(
    {"UNASSESSED", "NO_OR_NEGLIGIBLE_RISK", "NON_NEGLIGIBLE_RISK"}
)


class EudrDdsCandidate(Base, TimestampMixin):
    """Local candidate metadata; readiness and payload are always recomputed.

    This row is not a legal Due Diligence Statement and stores no EUDR API
    credentials. Source plots are deliberately not copied here: they are
    reconstructed from the current tenant shipment genealogy at conformance
    time so stale provenance cannot silently survive upstream corrections.
    """

    __tablename__ = "eudr_dds_candidates"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    public_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), default=uuid4, nullable=False)
    organization_id: Mapped[int] = mapped_column(Integer, nullable=False)
    shipment_id: Mapped[int] = mapped_column(Integer, nullable=False)

    activity_type: Mapped[str] = mapped_column(String(16), nullable=False)
    commodity_profile: Mapped[str] = mapped_column(String(16), nullable=False)

    operator_name: Mapped[str | None] = mapped_column(String(240), nullable=True)
    operator_address: Mapped[str | None] = mapped_column(Text, nullable=True)
    operator_country_code: Mapped[str | None] = mapped_column(String(2), nullable=True)
    operator_eori: Mapped[str | None] = mapped_column(String(32), nullable=True)

    hs_code: Mapped[str | None] = mapped_column(String(16), nullable=True)
    trade_name: Mapped[str | None] = mapped_column(String(240), nullable=True)
    product_description: Mapped[str | None] = mapped_column(Text, nullable=True)
    common_species_name: Mapped[str | None] = mapped_column(String(240), nullable=True)
    scientific_species_name: Mapped[str | None] = mapped_column(String(240), nullable=True)
    net_mass_kg: Mapped[Decimal | None] = mapped_column(Numeric(18, 3), nullable=True)

    production_country_code: Mapped[str | None] = mapped_column(String(2), nullable=True)
    production_date_from: Mapped[date | None] = mapped_column(Date, nullable=True)
    production_date_to: Mapped[date | None] = mapped_column(Date, nullable=True)

    relies_on_previous_dds: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
    )
    previous_dds_reference: Mapped[str | None] = mapped_column(String(160), nullable=True)
    previous_dds_verification: Mapped[str | None] = mapped_column(String(160), nullable=True)

    risk_conclusion: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default="UNASSESSED",
    )
    risk_assessment_reference: Mapped[str | None] = mapped_column(String(240), nullable=True)
    risk_assessed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    spec_profile: Mapped[str] = mapped_column(String(120), nullable=False)
    spec_fingerprint_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_by_user_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey(
            "users.id",
            ondelete="SET NULL",
            name="fk_eudr_dds_candidates_created_by_user_id",
        ),
        nullable=True,
    )
    updated_by_user_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey(
            "users.id",
            ondelete="SET NULL",
            name="fk_eudr_dds_candidates_updated_by_user_id",
        ),
        nullable=True,
    )

    __table_args__ = (
        UniqueConstraint("id", "organization_id", name="uq_eudr_dds_candidates_id_org"),
        UniqueConstraint("public_id", name="uq_eudr_dds_candidates_public_id"),
        UniqueConstraint("shipment_id", name="uq_eudr_dds_candidates_shipment_id"),
        ForeignKeyConstraint(
            ["organization_id"],
            ["organizations.id"],
            name="fk_eudr_dds_candidates_organization_id",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["shipment_id", "organization_id"],
            ["shipments.id", "shipments.organization_id"],
            name="fk_eudr_dds_candidates_shipment_tenant",
            ondelete="RESTRICT",
        ),
        CheckConstraint(
            "activity_type IN ('IMPORT','DOMESTIC','EXPORT')",
            name="ck_eudr_dds_candidates_activity_type",
        ),
        CheckConstraint(
            "commodity_profile IN ('WOOD','OTHER_EUDR')",
            name="ck_eudr_dds_candidates_commodity_profile",
        ),
        CheckConstraint(
            "risk_conclusion IN ('UNASSESSED','NO_OR_NEGLIGIBLE_RISK','NON_NEGLIGIBLE_RISK')",
            name="ck_eudr_dds_candidates_risk_conclusion",
        ),
        CheckConstraint(
            "net_mass_kg IS NULL OR net_mass_kg > 0",
            name="ck_eudr_dds_candidates_net_mass_positive",
        ),
        CheckConstraint(
            "production_date_from IS NULL OR production_date_to IS NULL OR production_date_to >= production_date_from",
            name="ck_eudr_dds_candidates_production_date_range",
        ),
        Index(
            "ix_eudr_dds_candidates_tenant_shipment",
            "organization_id",
            "shipment_id",
        ),
        Index(
            "ix_eudr_dds_candidates_tenant_risk",
            "organization_id",
            "risk_conclusion",
        ),
    )
