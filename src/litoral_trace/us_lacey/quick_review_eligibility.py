"""Display-only guard for direct human confirmation of Lacey document candidates.

The review writer remains the sole authority for any acceptance. Validation here
mirrors the PPQ value contract so REVIEW_REQUIRED candidate metadata (meaning
that a person must decide) is not mistaken for an invalid PPQ value.
"""
from __future__ import annotations

from litoral_trace.us_lacey.ppq505 import PpqValidationStatus, validate_ppq_value


def quick_accept_eligible(field, candidate) -> bool:
    """Whether the UI may OFFER a single-candidate explicit Accept & next action.

    This cannot authorize anything. The existing tenant-scoped review writer
    revalidates the candidate ID and its PPQ value on submission.
    """
    if field is None or candidate is None:
        return False
    if str(getattr(field, "status", "")).upper() == "CONFLICT":
        return False
    if not getattr(candidate, "id", None) or not getattr(candidate, "source_document_id", None):
        return False
    if str(getattr(candidate, "decision", "PENDING") or "PENDING").upper() in {"REJECTED", "SELECTED"}:
        return False
    if str(getattr(candidate, "validation_status", "") or "").upper() in {"INVALID", "MISSING"}:
        return False
    value = getattr(candidate, "normalized_value", None) or getattr(candidate, "original_value", None)
    validation = validate_ppq_value(str(getattr(field, "field_name", "") or ""), value)
    return validation.status is PpqValidationStatus.VALID and bool(
        str(validation.normalized_value or "").strip()
    )
