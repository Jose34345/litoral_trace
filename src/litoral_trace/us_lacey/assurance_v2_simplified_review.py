"""Exception-first customer presentation derived from existing authority state.

Read-model only: never promotes candidates, writes regulatory truth, or authorizes
historical evidence. Aggregation groups UI actions, not compliance checks.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, is_dataclass
from typing import Any

_BOTANICAL = frozenset({"genus", "species", "country_of_harvest"})
_INTERNAL_DIAGNOSTICS = frozenset({
    "UNSUPPORTED_CLAIM", "MISSING_SOURCE_LINK", "MISSING_EVIDENCE_REF",
    "UNDECIDED_CONFLICT", "BLOCKING_EXCEPTION",
})


@dataclass(frozen=True)
class ReviewTask:
    key: str
    title: str
    detail: str
    target: str
    reason: str
    priority: int


def build_review_workspace(case: dict[str, Any], required: dict[str, Any], gate: Any) -> dict[str, Any]:
    """Summarize next human ACTIONS without claiming the fields are approved."""
    identities = tuple(case.get("identities") or ())
    reuses = tuple(case.get("reuses") or ())
    rows = tuple(asdict(row) if is_dataclass(row) else row for row in (required.get("rows") or ()))
    tasks: list[ReviewTask] = []
    pending_identities = [i for i in identities if i.get("status") == "REVIEW_REQUIRED"]
    for item in pending_identities:
        line = str(item.get("line_reference") or "")
        supplier = str(item.get("supplier_name") or "the supplier")
        sku = str(item.get("sku") or "unidentified SKU")
        tasks.append(ReviewTask(
            "identity:" + line, "Verify supplier and product",
            f"Confirm {supplier} / {sku} belongs to shipment line {line}.",
            "#identities", "Current source identity must be confirmed once per line.", 10,
        ))
    botanical = [r for r in reuses if r.get("status") == "ELIGIBLE" and r.get("field_name") in _BOTANICAL]
    if botanical:
        tasks.append(ReviewTask(
            "botanical-memory", "Review supplier botanical evidence",
            f"{len(botanical)} reusable botanical claims need their original provenance and scope checked.",
            "#identities" if pending_identities else "#botanical-group-panel",
            "Confirm supplier/SKU identity before reusing history." if pending_identities
            else "Memory is eligible, not yet authorized for this shipment.", 20,
        ))
    missing = [r for r in rows if r.get("status") == "MISSING_EVIDENCE"]
    # Request supporting evidence once per shipment line, rather than asking
    # the same supplier to perform one task per missing declaration field.
    # This is presentation-only: retain all original checklist rows and gates.
    missing_by_line: dict[str, list[dict[str, Any]]] = {}
    for item in missing:
        line = str(item.get("line_reference") or "__shipment__")
        missing_by_line.setdefault(line, []).append(item)
    for line, items in missing_by_line.items():
        labels = [
            str(item.get("label") or item.get("field_name") or "Required field")
            for item in items
        ]
        visible = ", ".join(labels[:3])
        if len(labels) > 3:
            visible += f", and {len(labels) - 3} more"
        scope = "shipment" if line == "__shipment__" else f"line {line}"
        key = (
            "missing:" + line + ":" + str(items[0].get("field_name"))
            if len(items) == 1 else "missing:" + line + ":group"
        )
        tasks.append(ReviewTask(
            key, "Request missing evidence",
            f"{len(items)} missing input{'s' if len(items) != 1 else ''} for {scope}: "
            f"{visible}. Request supporting documentation; do not infer values.",
            "#required",
            "Grouped evidence request only; each underlying requirement stays open.",
            30,
        ))
    unresolved_review = [r for r in rows if r.get("status") not in ("VERIFIED", "MISSING_EVIDENCE")]
    tasks.append(ReviewTask(
        "batch-review", "Review document-backed values",
        f"{len(unresolved_review)} declaration inputs still require authorized review. "
        "Select the source-backed values you have checked. One approval can cover the eligible fields while each source remains auditable.",
        "#batch-review-panel", "Review the source-backed fields; do not treat candidates as verified.", 40,
    )) if unresolved_review else None
    tasks.sort(key=lambda t: t.priority)
    grouped: dict[str, int] = {}
    for blocker in gate.blockers:
        code = str(blocker.code)
        grouped[code] = grouped.get(code, 0) + 1
    return {
        "found_count": sum(bool(str(r.get("value") or "").strip()) for r in rows),
        "missing_count": len(missing),
        "verified_count": required.get("verified", 0),
        "review_count": required.get("review", 0),
        "source_count": len(case.get("documents") or ()),
        "tasks": tuple(t.__dict__ if hasattr(t, "__dict__") else {
            "key": t.key, "title": t.title, "detail": t.detail,
            "target": t.target, "reason": t.reason, "priority": t.priority
        } for t in tasks),
        "gate_findings": grouped,
        "technical_finding_count": sum(grouped.values()),
        "regulatory_blocks": {
            code: count for code, count in grouped.items() if code not in _INTERNAL_DIAGNOSTICS
        },
    }
