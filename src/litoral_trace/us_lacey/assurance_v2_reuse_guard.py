"""Fail-closed current-document evidence boundary for Assurance V2 reuse.

A historical claim cannot override a source-backed value from the current
shipment. A missing or partial source-claim snapshot is NOT an empty packet.
"""
from __future__ import annotations

from collections.abc import Mapping


def current_claim_values_for_reuse(
    observations: tuple[dict, ...] | None, *,
    field_name: str,
    line_reference: str,
) -> tuple[str, ...] | None:
    """Return competing current values, or None when the scan is not trustworthy."""
    if observations is None:
        return None
    values: list[str] = []
    for claim in observations:
        if not isinstance(claim, Mapping):
            return None
        if claim.get("field_name") != field_name:
            continue
        if "NONFACTUAL_PLACEHOLDER" in (claim.get("reason_codes") or ()):
            continue
        source_line = claim.get("line_reference")
        if source_line not in (None, "", line_reference):
            continue
        value = str(claim.get("normalized_value") or claim.get("original_value") or "").strip()
        if not value:
            return None
        values.append(value)
    return tuple(values)
