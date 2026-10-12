"""Select a current, replay-verified source observation for explicit human review.

No model/source candidate is human authority. This function only returns
evidence that can be passed to the EXISTING authenticated review journal.
"""
from __future__ import annotations

from dataclasses import dataclass
import re

from litoral_trace.us_lacey.exporters.assurance_v2 import AssuranceCase
from litoral_trace.us_lacey.ppq505 import PPQ505_SHIPMENT_REFERENCE


@dataclass(frozen=True, slots=True)
class ReviewableSourceClaim:
    claim_id: str
    raw_value: str
    normalized_value: str
    assurance_document_id: int
    page: int | None
    locator: str


def select_reviewable_source_claim(
    case: AssuranceCase, *, claim_id: str, field_name: str,
    line_reference: str,
) -> ReviewableSourceClaim:
    """Fail closed unless a current source claim supports this exact subject."""
    if not re.fullmatch(r"[0-9a-f]{64}", str(claim_id or "")):
        raise ValueError("Invalid source claim reference")
    candidates = [c for c in case.candidate_observations if c.get("id") == claim_id]
    if len(candidates) != 1:
        raise ValueError("Current source claim does not exist uniquely")
    claim = candidates[0]
    if (claim.get("field_name") != field_name
            or claim.get("candidate_origin") != "DOCUMENT"
            or claim.get("state") != "CANDIDATE"
            or claim.get("candidate_state", "CANDIDATE") != "CANDIDATE"
            or claim.get("authority_status") != "CANDIDATE"
            or claim.get("support_status") != "SUPPORTED"):
        raise ValueError("Source candidate is not supported for this field")
    if (claim.get("organization_id") != case.organization_id
            or claim.get("operation_id") is None
            or claim.get("source_set_revision_id") != case.source_set_revision
            or claim.get("source_set_fingerprint") != case.source_set_fingerprint
            or claim.get("generation") != case.source_set_generation):
        raise ValueError("Source candidate belongs to another source set")
    if line_reference == PPQ505_SHIPMENT_REFERENCE:
        if claim.get("association_status") != "UNBOUND" or claim.get("entity_type") != "SHIPMENT":
            raise ValueError("Shipment field needs a shipment-scope source")
    else:
        identity = [i for i in case.identities
                    if i.get("line_reference") == line_reference
                    and i.get("status") in {"VERIFIED", "HUMAN_CONFIRMED"}]
        if (len(identity) != 1
                or claim.get("association_status") != "DOCUMENT_EXACT_SINGLE_SKU"
                or claim.get("line_reference") != line_reference
                or claim.get("sku") != identity[0].get("sku")):
            raise ValueError("Product source is not bound to the reviewed supplier/SKU/line")
    docs = [doc for doc in case.documents
            if str(doc.get("id")) == str(claim.get("assurance_document_id"))]
    if (len(docs) != 1 or docs[0].get("sha256") != claim.get("original_sha256")
            or str(docs[0].get("version")) != str(claim.get("document_version_number"))):
        raise ValueError("Source document was replaced or changed")
    loc = claim.get("source_locator")
    if not isinstance(loc, dict) or loc.get("kind") != "TEXT_SPAN":
        raise ValueError("Source review needs an exact PDF text-span")
    page, block, start, end = (loc.get("page"), loc.get("block_id"),
                                loc.get("start"), loc.get("end"))
    if (type(page) is not int or page < 1 or not isinstance(block, str)
            or not block or type(start) is not int or type(end) is not int
            or start < 0 or end <= start):
        raise ValueError("Invalid PDF source anchor")
    value = claim.get("normalized_value")
    raw = claim.get("raw_value")
    if not isinstance(raw, str) or not raw.strip() or not isinstance(value, str) or not value.strip():
        raise ValueError("The source claim has no usable supported value")
    document_id = claim["assurance_document_id"]
    if type(document_id) is not int or document_id < 1:
        raise ValueError("Invalid current document reference")
    return ReviewableSourceClaim(
        claim_id=claim_id, raw_value=raw, normalized_value=value,
        assurance_document_id=document_id, page=page,
        locator=f"assurance_v2_source_claim:{claim_id}:p{page}:{block}:{start}-{end}",
    )
