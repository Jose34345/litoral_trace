"""Canonical external ERP contract independent of any vendor-specific API."""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class _CanonicalBase(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    external_id: str = Field(min_length=1, max_length=200)
    source_updated_at: datetime | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class CanonicalSupplier(_CanonicalBase):
    name: str = Field(min_length=1, max_length=200)
    tax_id: str | None = Field(default=None, max_length=40)
    country: str | None = Field(default=None, min_length=2, max_length=2)

    @field_validator("country")
    @classmethod
    def normalize_country(cls, value: str | None) -> str | None:
        return value.upper() if value else None


class CanonicalProduct(_CanonicalBase):
    code: str = Field(min_length=1, max_length=120)
    name: str = Field(min_length=1, max_length=200)
    unit: Literal["M3", "KG", "TON", "UNIT"] | None = None
    hs_code: str | None = Field(default=None, max_length=20)


class CanonicalReceipt(_CanonicalBase):
    receipt_code: str = Field(min_length=1, max_length=120)
    supplier_external_id: str | None = Field(default=None, max_length=200)
    product_external_id: str | None = Field(default=None, max_length=200)
    document_reference: str | None = Field(default=None, max_length=160)
    quantity: Decimal = Field(gt=0)
    unit: Literal["M3", "KG", "TON"]
    occurred_at: datetime
    origin_reference: str | None = Field(default=None, max_length=200)


class CanonicalShipmentLine(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    product_external_id: str | None = Field(default=None, max_length=200)
    batch_reference: str | None = Field(default=None, max_length=200)
    quantity: Decimal = Field(gt=0)
    unit: Literal["M3", "KG", "TON"]


class CanonicalShipment(_CanonicalBase):
    shipment_code: str = Field(min_length=1, max_length=120)
    sale_reference: str | None = Field(default=None, max_length=160)
    buyer_reference: str | None = Field(default=None, max_length=160)
    destination_country: str | None = Field(default=None, min_length=2, max_length=2)
    shipped_at: datetime | None = None
    lines: list[CanonicalShipmentLine] = Field(default_factory=list, max_length=500)

    @field_validator("destination_country")
    @classmethod
    def normalize_destination_country(cls, value: str | None) -> str | None:
        return value.upper() if value else None


class GenericErpPayload(BaseModel):
    """Vendor-neutral batch accepted by the first ERP bridge.

    The source remains authoritative only for administrative/commercial facts.
    Ingestion never posts ledger events or dispatches stock automatically.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    source_system: str = Field(default="GENERIC_ERP", min_length=1, max_length=120)
    suppliers: list[CanonicalSupplier] = Field(default_factory=list, max_length=5000)
    products: list[CanonicalProduct] = Field(default_factory=list, max_length=5000)
    receipts: list[CanonicalReceipt] = Field(default_factory=list, max_length=10000)
    shipments: list[CanonicalShipment] = Field(default_factory=list, max_length=5000)

    def entity_count(self) -> int:
        return len(self.suppliers) + len(self.products) + len(self.receipts) + len(self.shipments)
