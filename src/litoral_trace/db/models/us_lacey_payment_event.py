"""Provider payment event audit model for U.S. Lacey billing."""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKeyConstraint,
    Index,
    Integer,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column

from litoral_trace.db.base import Base


_SHA256_HEX_REMAINDER_SQL = "payload_sha256"
for _hex_char in "0123456789abcdef":
    _SHA256_HEX_REMAINDER_SQL = (
        f"replace({_SHA256_HEX_REMAINDER_SQL}, '{_hex_char}', '')"
    )
_SHA256_HEX_CHECK_SQL = (
    f"length(payload_sha256) = 64 AND {_SHA256_HEX_REMAINDER_SQL} = ''"
)


class UsLaceyPaymentEvent(Base):
    __tablename__ = "us_lacey_payment_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    organization_id: Mapped[int] = mapped_column(Integer, nullable=False)
    payment_id: Mapped[int] = mapped_column(Integer, nullable=False)
    provider: Mapped[str] = mapped_column(String(40), nullable=False)
    provider_order_id: Mapped[str] = mapped_column(String(128), nullable=False)
    event_name: Mapped[str] = mapped_column(String(64), nullable=False)
    payload_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    amount_cents: Mapped[int] = mapped_column(Integer, nullable=False)
    currency: Mapped[str] = mapped_column(String(3), nullable=False)
    store_id: Mapped[int] = mapped_column(Integer, nullable=False)
    variant_id: Mapped[int] = mapped_column(Integer, nullable=False)
    test_mode: Mapped[bool] = mapped_column(Boolean, nullable=False)
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    __table_args__ = (
        ForeignKeyConstraint(
            ["payment_id", "organization_id"],
            ["us_lacey_payments.id", "us_lacey_payments.organization_id"],
            name="fk_us_lacey_payment_events_payment_tenant",
            ondelete="CASCADE",
        ),
        UniqueConstraint(
            "provider", "provider_order_id", name="uq_us_lacey_payment_events_provider_order"
        ),
        CheckConstraint(
            "provider = 'LEMON_SQUEEZY'", name="ck_us_lacey_payment_events_provider"
        ),
        CheckConstraint(
            "event_name = 'order_created'", name="ck_us_lacey_payment_events_name"
        ),
        CheckConstraint(
            _SHA256_HEX_CHECK_SQL,
            name="ck_us_lacey_payment_events_payload_sha256",
        ),
        CheckConstraint(
            "amount_cents > 0", name="ck_us_lacey_payment_events_amount_positive"
        ),
        CheckConstraint(
            "currency = 'USD'", name="ck_us_lacey_payment_events_currency_usd"
        ),
        CheckConstraint(
            "store_id > 0", name="ck_us_lacey_payment_events_store_positive"
        ),
        CheckConstraint(
            "variant_id > 0", name="ck_us_lacey_payment_events_variant_positive"
        ),
        Index(
            "ix_us_lacey_payment_events_org_payment", "organization_id", "payment_id"
        ),
    )
