"""Read-only, tenant-scoped bridge for a single V2LAB shipment cockpit.

This module never promotes legacy MATCHED fields to V2 authority, mutates source
state, or authorizes a filing. All commands remain on existing V2 endpoints with
their original CSRF, current-source and immutable audit gates.
"""
from __future__ import annotations

import os

from litoral_trace.us_lacey.assurance_v2_batch_review import preview_batch_review
from litoral_trace.us_lacey.assurance_v2_case_reader import read_tenant_assurance_case
from litoral_trace.us_lacey.assurance_v2_evidence_checklist import required_evidence_summary
from litoral_trace.us_lacey.assurance_v2_review_delta import read_p1_review_insights
from litoral_trace.us_lacey.assurance_v2_simplified_review import build_review_workspace
from litoral_trace.us_lacey.csrf import us_lacey_csrf_token
from litoral_trace.us_lacey.exporters.assurance_v2 import evaluate_filing_gate
from litoral_trace.us_lacey.portal_auth import US_LACEY_SESSION_COOKIE


def is_private_v2_shipment(request, identity, business_reference: str) -> bool:
    """Apply only to this user's explicitly allowlisted laboratory operations."""
    app = request.scope.get("app") if hasattr(request, "scope") else None
    allowlist = getattr(getattr(app, "state", None), "assurance_v2_lab_org_ids", ())
    return (
        identity is not None
        and int(identity.organization_id) in allowlist
        and str(business_reference or "").upper().startswith("V2LAB-")
    )


def unified_v2_review_context(
    *, request, identity, operation_public_id: str,
) -> dict:
    """Project the same data as the V2 case from authenticated tenant storage.

    Missing/stale V2 data does not silently fall back to the legacy review:
    the presentation must fail closed and show an explicit unavailable state.
    """
    case = read_tenant_assurance_case(
        organization_id=int(identity.organization_id),
        operation_public_id=str(operation_public_id),
    )
    if case is None:
        return {"available": False, "reason": "Current Assurance V2 evidence is unavailable."}

    snapshot = case.snapshot()
    gate = evaluate_filing_gate(case)
    required = required_evidence_summary(case)
    operation = str(operation_public_id)
    current_document_ids = {str(doc.get("id")) for doc in snapshot["documents"]}

    def current_source_link(doc_id, page=None):
        doc = str(doc_id or "")
        if doc not in current_document_ids:
            return None
        target = f"/assurance-v2/operations/{operation}/documents/{doc}"
        if page is not None and str(page).isdigit() and 0 < int(page) <= 10000:
            target += f"#page={int(page)}"
        return target

    batch_choices = [dict(choice) for choice in preview_batch_review(case)]
    for choice in batch_choices:
        choice["url"] = current_source_link(choice.get("document_id"), choice.get("page"))
        choice["previously_reviewed"] = False

    pending_identities = tuple(
        identity_row for identity_row in snapshot.get("identities") or ()
        if identity_row.get("status") == "REVIEW_REQUIRED"
    )
    confirmed_lines = {
        str(row.get("line_reference"))
        for row in snapshot.get("identities") or ()
        if row.get("status") == "HUMAN_CONFIRMED"
    }
    botanical_choices = []
    for reuse in snapshot.get("reuses") or ():
        if (
            reuse.get("status") != "ELIGIBLE"
            or str(reuse.get("line_reference")) not in confirmed_lines
            or reuse.get("field_name") not in {"genus", "species", "country_of_harvest"}
        ):
            continue
        doc = str(reuse.get("origin_document_id") or "")
        memory_id = str(reuse.get("id") or "")
        if not doc.isdigit() or not memory_id:
            continue
        botanical_choices.append({
            **reuse,
            "url": f"/assurance-v2/operations/{operation}/historical-documents/{doc}/{memory_id}",
        })

    insights = {}
    if os.environ.get("US_LACEY_ASSURANCE_V2_REVIEW_JOURNAL", "OFF").upper() == "ON":
        insights = read_p1_review_insights(
            organization_id=int(identity.organization_id),
            operation_public_id=operation,
            current_case=case,
        )
    previous_fields = {
        (str(item["line_reference"]), str(item["field_name"]))
        for item in insights.get("previously_reviewed_fields", ())
        if item.get("status") == "UNCHANGED_RECONFIRM_REQUIRED"
    }
    for choice in batch_choices:
        choice["previously_reviewed"] = (
            str(choice["line_reference"]), str(choice["field_name"])
        ) in previous_fields

    token = request.cookies.get(US_LACEY_SESSION_COOKIE) or ""
    return {
        "available": True,
        "case": snapshot,
        "gate": gate,
        "review_workspace": build_review_workspace(snapshot, required, gate),
        "required_evidence": required,
        "pending_identities": pending_identities,
        "botanical_choices": botanical_choices,
        "batch_choices": batch_choices,
        "review_insights": insights,
        "csrf_token": us_lacey_csrf_token(
            session_token=token, purpose=f"assurance-v2:{operation}"
        ),
    }
