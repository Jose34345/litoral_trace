"""Exception-first review policy for the PRIVATE Assurance V2 laboratory.

Read-only classification: confidence scores and legacy MATCHED flags never
grant human authority. The current source-set and all source-claim/identity
fences are enforced by the command service again under PostgreSQL locks.
"""
from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
import json

from litoral_trace.us_lacey.assurance_v2_evidence_checklist import required_evidence_summary
from litoral_trace.us_lacey.assurance_v2_source_claim_review import select_reviewable_source_claim
from litoral_trace.us_lacey.ppq505 import PPQ505_SHIPMENT_REFERENCE, validate_ppq_value

_BOTANICAL = frozenset({"genus", "species", "country_of_harvest"})


@dataclass(frozen=True)
class BatchCandidate:
    field_id: int
    field_name: str
    line_reference: str
    value: str
    claim_id: str
    document_id: int


def _comparable_value(field_name: str, value: str) -> str | None:
    result = validate_ppq_value(field_name, value)
    if result.status.value != "VALID":
        return None
    return str(result.normalized_value or "").strip() or None


def batch_candidates(case) -> tuple[BatchCandidate, ...]:
    """One unambiguous, explicitly sourced, valid candidate per still-unreviewed slot.

    No OCR-only 'supported' value, historical memory, unknown product identity,
    inferred line binding, or ambiguous alternatives may enter a batch.
    """
    if not case.documents or not case.source_set_revision or not case.source_set_fingerprint:
        return ()
    eligible = []
    for field in case.fields:
        name = str(field.get("name") or "")
        line = str(field.get("line_reference") or "")
        if (not name or not line or not field.get("id")
                or field.get("authority") != "CANDIDATE"
                or name in _BOTANICAL
                or field.get("status") in {"CONFLICT", "NOT_REQUIRED", "MATCHED"}):
            continue
        proposed = _comparable_value(name, str(field.get("value") or ""))
        if proposed is None:
            continue
        suitable = []
        for claim in case.candidate_observations:
            if claim.get("field_name") != name:
                continue
            try:
                selected = select_reviewable_source_claim(
                    case, claim_id=str(claim.get("id") or ""),
                    field_name=name, line_reference=line,
                )
            except ValueError:
                continue
            value = _comparable_value(name, selected.normalized_value)
            if value:
                suitable.append((selected, value))
        # One confirmed value across ALL current eligible source spans.
        # If a supported source contradicts the chosen canonical value, abstain.
        if not suitable or len({value for _, value in suitable}) != 1:
            continue
        if suitable[0][1] != proposed:
            continue
        selected = sorted(suitable, key=lambda row: row[0].claim_id)[0][0]
        eligible.append(BatchCandidate(
            field_id=int(field["id"]), field_name=name, line_reference=line,
            value=proposed, claim_id=selected.claim_id,
            document_id=selected.assurance_document_id,
        ))
    if len({(x.line_reference, x.field_name) for x in eligible}) != len(eligible):
        return ()
    return tuple(sorted(eligible, key=lambda x: (x.line_reference, x.field_name)))


def batch_fingerprint(case, candidates: tuple[BatchCandidate, ...]) -> str:
    """Content-addressed approval of one source generation AND exact field set."""
    payload = {
        "source_revision": case.source_set_revision,
        "source_fingerprint": case.source_set_fingerprint,
        "authority_fingerprint": case.authority_fingerprint(),
        "fields": [
            {"field_id": x.field_id, "line": x.line_reference, "field": x.field_name,
             "value": x.value, "claim": x.claim_id, "doc": x.document_id}
            for x in candidates
        ],
    }
    return sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def simplified_review(case) -> dict:
    """Small, truthful customer-facing work queue, not 76 low-level gate findings."""
    checklist = required_evidence_summary(case)
    identities = [
        item for item in case.identities
        if item.get("status") not in {"HUMAN_CONFIRMED", "VERIFIED"}
    ]
    eligible_reuses = [item for item in case.reuses if item.get("status") == "ELIGIBLE"]
    missing = [
        {"field": row.field_name, "label": row.label,
         "line": row.line_reference, "guidance": row.guidance}
        for row in checklist.rows if row.status == "MISSING_EVIDENCE"
    ]
    conflicts = [
        item for item in case.exceptions
        if str(item.get("reason") or "").startswith(("CONFLICT", "REUSE_PROVENANCE_INVALID"))
        or str(item.get("id") or "").startswith(("source:", "reuse:provenance:"))
    ]
    candidates = batch_candidates(case)
    return {
        "total": checklist.total,
        "verified": checklist.verified,
        "found": checklist.total - checklist.missing,
        "missing_count": checklist.missing,
        "missing": missing,
        "identities": identities,
        "reuse": eligible_reuses,
        "conflicts": conflicts,
        "batch_candidates": candidates,
        "batch_count": len(candidates),
        "batch_fingerprint": batch_fingerprint(case, candidates) if candidates else None,
        "can_batch": bool(candidates) and not identities and not conflicts,
        "requires_review": bool(identities or eligible_reuses or missing or conflicts or candidates),
        "filing_ready": False,  # Lab issuance is always disabled.
    }
