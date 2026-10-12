"""Required evidence completeness for the human Assurance V2 work queue.

This is a READ-ONLY checklist, not a regulatory verdict or a second gate.
The final gate alone decides whether a package may be issued.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from litoral_trace.us_lacey.exporters.assurance_v2 import AssuranceCase
from litoral_trace.us_lacey.regulatory.assurance_v2_adapter import LACEY_ADAPTER


@dataclass(frozen=True)
class RequiredEvidenceRow:
    line_reference: str
    field_name: str
    label: str
    value: str
    status: str
    guidance: str


def required_evidence_checklist(case: AssuranceCase) -> tuple[RequiredEvidenceRow, ...]:
    """Show exactly why each legally required output field is or is not ready."""
    fields = {(str(f.get("line_reference")), str(f.get("name"))): f
              for f in case.fields}
    exceptions = tuple(case.exceptions)
    result: list[RequiredEvidenceRow] = []
    for line, requirement in LACEY_ADAPTER.required_fields(case.lines):
        row = fields.get((line, requirement.key), {})
        value = str(row.get("value") or "").strip()
        authority = str(row.get("authority") or "CANDIDATE").upper()
        open_field = any(
            ex.get("field_name") == requirement.key
            and ex.get("line_reference") == line
            and ex.get("status") not in {"RESOLVED", "DISMISSED"}
            for ex in exceptions
        )
        source_choice = any(
            ex.get("field_name") == requirement.key
            and ex.get("line_reference") == line
            and any(str(c.get("id", "")).startswith("source:")
                    for c in ex.get("candidates") or ())
            for ex in exceptions
        )
        pending_identity = any(
            ex.get("field_name") == requirement.key
            and ex.get("line_reference") == line
            and bool(ex.get("pending_candidates"))
            for ex in exceptions
        )
        if value and authority in {"HUMAN_CONFIRMED", "VERIFIED_REUSE"} and not open_field:
            status, guidance = "VERIFIED", "Reviewed source or authorized historical evidence"
        elif source_choice:
            status, guidance = "REVIEW_SOURCE", "Select the supported PDF observation and confirm it"
        elif pending_identity:
            status, guidance = "REVIEW_IDENTITY", "Source located; confirm the exact supplier/SKU/line before reviewing"
        elif value:
            status, guidance = "REVIEW_VALUE", "Check the original source and record a human decision"
        else:
            status, guidance = "MISSING_EVIDENCE", "Provide a current source document or supported evidence"
        result.append(RequiredEvidenceRow(
            line_reference=str(line), field_name=requirement.key,
            label=requirement.label, value=value, status=status,
            guidance=guidance,
        ))
    return tuple(result)


def required_evidence_summary(case: AssuranceCase) -> dict[str, Any]:
    rows = required_evidence_checklist(case)
    return {
        "rows": rows, "total": len(rows),
        "verified": sum(r.status == "VERIFIED" for r in rows),
        "review": sum(r.status in {"REVIEW_SOURCE", "REVIEW_IDENTITY", "REVIEW_VALUE"} for r in rows),
        "missing": sum(r.status == "MISSING_EVIDENCE" for r in rows),
    }
