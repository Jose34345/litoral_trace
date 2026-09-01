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


from litoral_trace.us_lacey.ppq505 import PPQ505_FIELDS


# Backwards-compatible public catalog, now driven by the explicit PPQ contract.
US_LACEY_REVIEW_FIELDS: tuple[tuple[str, str], ...] = tuple(
    (field.key, field.label) for field in PPQ505_FIELDS
)

US_LACEY_REVIEW_FIELD_KEYS = frozenset(key for key, _label in US_LACEY_REVIEW_FIELDS)
