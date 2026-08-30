"""Domain constants for the U.S. Lacey document-preparation product.

The field catalog mirrors the structured review/export shape used by the public
synthetic demo. It is a preparation schema, not a legal determination of what a
particular shipment must file.
"""
from __future__ import annotations

from enum import StrEnum


class UsLaceyOperationStatus(StrEnum):
    NEW = "NEW"
    PROCESSING = "PROCESSING"
    REVIEW_REQUIRED = "REVIEW_REQUIRED"
    READY_FOR_REVIEW = "READY_FOR_REVIEW"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class UsLaceyFieldStatus(StrEnum):
    FOUND = "FOUND"
    MATCHED = "MATCHED"
    MISSING = "MISSING"
    REVIEW = "REVIEW"
    NOT_REQUIRED = "NOT_REQUIRED"


class UsLaceyBusinessType(StrEnum):
    IMPORTER = "IMPORTER"
    CUSTOMS_BROKER = "CUSTOMS_BROKER"
    OTHER = "OTHER"


class UsLaceyAccountStatus(StrEnum):
    PENDING_EMAIL = "PENDING_EMAIL"
    PAYMENT_PENDING = "PAYMENT_PENDING"
    PILOT = "PILOT"  # legacy/manual pilot accounts
    ACTIVE = "ACTIVE"
    SUSPENDED = "SUSPENDED"


class UsLaceySubscriptionStatus(StrEnum):
    PENDING = "PENDING"
    ACTIVE = "ACTIVE"
    PAST_DUE = "PAST_DUE"
    CANCELED = "CANCELED"


class UsLaceyPaymentStatus(StrEnum):
    PENDING = "PENDING"
    VERIFIED = "VERIFIED"
    REJECTED = "REJECTED"
    REFUNDED = "REFUNDED"


class UsLaceyProcessingJobStatus(StrEnum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    RETRY = "RETRY"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


# Stable internal keys -> user-facing labels. Keeping these names centralized
# prevents every parser/exporter from inventing a slightly different schema.
US_LACEY_REVIEW_FIELDS: tuple[tuple[str, str], ...] = (
    ("filing_entry_reference", "Filing / Entry Reference"),
    ("entry_type", "Entry Type"),
    ("importer_name", "Importer Name"),
    ("importer_identifier", "Importer Identifier"),
    ("importer_address", "Importer Address"),
    ("consignee_name", "Consignee Name"),
    ("consignee_address", "Consignee Address"),
    ("filer_name", "Filer Name"),
    ("filer_contact", "Filer Contact"),
    ("bill_of_lading", "Bill of Lading"),
    ("container_number", "Container Number"),
    ("manufacturer_id", "Manufacturer ID"),
    ("shipment_description", "Shipment Description"),
    ("source_line_id", "Source Line ID"),
    ("hts_code", "HTS Code"),
    ("article_component", "Article / Component"),
    ("merchandise_description", "Merchandise Description"),
    ("entered_value", "Entered Value"),
    ("genus", "Genus"),
    ("species", "Species"),
    ("country_of_harvest", "Country of Harvest"),
    ("plant_quantity", "Plant Quantity"),
    ("metric_unit", "Metric Unit"),
    ("percent_recycled", "Percent Recycled"),
    ("supporting_reference", "Supporting Reference"),
)

US_LACEY_REVIEW_FIELD_KEYS = frozenset(key for key, _label in US_LACEY_REVIEW_FIELDS)
