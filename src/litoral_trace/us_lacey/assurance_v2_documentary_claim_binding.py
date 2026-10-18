"""Verified documentary supplier/SKU/line context for immutable V2 claim snapshots.

This adapter does not infer field authority. It only binds source-supported,
line-scoped observations when the current set of parsed original PDFs proves
one invoice line, one exact supplier identity, and corroborated manufacturer SKU.
"""
from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
import json
from types import SimpleNamespace
from typing import Mapping, Sequence

from litoral_trace.lacey_engine.domain import BundleResolution
from litoral_trace.lacey_engine.packing_net_material_mass import (
    extract_packing_plant_net_mass,
)
from litoral_trace.us_lacey.pdf_documentary_identity import discover_single_product_pdf_identity
from litoral_trace.us_lacey.single_sku_recovery import _explicit_document_skus

PROOF_VERSION = "assurance.documentary.binding.v1"
ASSOCIATION_STATUS = "DOCUMENT_EXACT_SINGLE_SKU"
DOCUMENTARY_BINDING_STATUS = ASSOCIATION_STATUS
_LINE_FIELDS = frozenset({
    "article_component", "genus", "species", "country_of_harvest",
    "plant_quantity", "metric_unit", "percent_recycled", "hts_code",
    "description", "entered_value",
})


@dataclass(frozen=True, slots=True)
class ParsedIdentityDocument:
    assurance_document_id: int
    original_sha256: str
    bundle: BundleResolution


def _lines(bundle: BundleResolution) -> tuple[str, ...] | None:
    # A physical PDF containing several logical documents is ambiguous at
    # document scope; never carry context across those sections.
    if len(bundle.documents) != 1:
        return None
    return tuple(
        block.text for block in bundle.documents[0].resolution.layout.blocks
        if block.block_type in {"TEXT_LINE", "OCR_LINE"}
    )


def _witness(ids: set[int], hashes: Mapping[int, str]) -> list[dict]:
    return [{"document_id": i, "sha256": hashes[i]} for i in sorted(ids)]


def bind_documentary_claims(
    *, snapshots: list[dict], parsed_documents: Sequence[ParsedIdentityDocument],
    line_references: Sequence[str],
) -> int:
    """Enrich only supported, anchored product observations; never facts.

    The caller has already verified document bytes and fenced the source-set
    revision. This function changes candidate metadata before immutable
    sidecar persistence, and returns 0 without changes on uncertainty.
    """
    if not snapshots or len(snapshots) != len(parsed_documents):
        return 0
    docs: dict[int, tuple[tuple[str, ...], str]] = {}
    for item in parsed_documents:
        lines = _lines(item.bundle)
        if item.assurance_document_id in docs or not lines:
            return 0
        docs[item.assurance_document_id] = (lines, item.original_sha256)
    hashes = {i: digest for i, (_, digest) in docs.items()}
    if set(hashes) != {s.get("assurance_document_id") for s in snapshots}:
        return 0
    if any(s.get("status") == "FAILED" or
           hashes.get(s.get("assurance_document_id")) != s.get("original_sha256")
           for s in snapshots):
        return 0

    fields = [
        SimpleNamespace(
            assurance_document_id=doc_id, field_name="raw.document_text",
            original_value="\n".join(lines), normalized_value="",
        )
        for doc_id, (lines, _) in docs.items()
    ]
    identity = discover_single_product_pdf_identity(
        fields, line_references=line_references,
    )
    if identity is None:
        return 0
    sku_docs = {i for i, (lines, _) in docs.items()
                if _explicit_document_skus(lines) == frozenset({identity.sku})}
    # Carton tables encode SKU as a row value, not as a literal "SKU: ..."
    # phrase. Include them as corroboration only when the existing strict
    # packing parser independently proves a single SKU, solid bamboo,
    # exact carton arithmetic and an explicitly printed NET bamboo mass.
    for item in parsed_documents:
        if len(item.bundle.documents) != 1:
            continue
        doc = item.bundle.documents[0]
        mass = extract_packing_plant_net_mass(
            doc.resolution.layout, document_type=doc.document_type,
        )
        if mass is not None and mass.sku == identity.sku:
            sku_docs.add(item.assurance_document_id)
    supplier_docs = {
        f.assurance_document_id for f in identity.virtual_fields
        if f.field_name == "supplier_address"
    }
    invoice_docs = {
        f.assurance_document_id for f in identity.virtual_fields
        if f.field_name == "line" and f.normalized_value == identity.line_reference
    }
    if (len(invoice_docs) != 1 or len(supplier_docs) < 2
            or len(sku_docs - invoice_docs) < 2):
        return 0
    names = {" ".join(f.normalized_value.casefold().split()) for f in identity.virtual_fields
             if f.field_name == "supplier_name"}
    addresses = {" ".join(f.normalized_value.casefold().split()) for f in identity.virtual_fields
                 if f.field_name == "supplier_address"}
    if len(names) != 1 or len(addresses) != 1:
        return 0
    supplier_digest = sha256(json.dumps(
        [next(iter(names)), next(iter(addresses))],
        ensure_ascii=False, separators=(",", ":"),
    ).encode("utf-8")).hexdigest()

    updates: list[tuple[dict, dict]] = []
    for snap in snapshots:
        doc_id = snap["assurance_document_id"]
        if doc_id not in sku_docs:
            continue
        subject = (f"docproduct:{snap['organization_id']}:{snap['operation_id']}:"
                   f"{snap['source_set_revision_id']}:{identity.line_reference}:"
                   f"{identity.sku}:{supplier_digest}")
        for claim in snap["claims"]:
            if not (
                claim.get("field_name") in _LINE_FIELDS
                and claim.get("association_status") == "UNBOUND"
                and claim.get("support_status") == "SUPPORTED"
                and claim.get("source_locator") is not None
                and claim.get("candidate_state") == "CANDIDATE"
            ):
                continue
            proof = {
                "version": PROOF_VERSION,
                "method": "CORROBORATED_SINGLE_PRODUCT",
                "organization_id": snap["organization_id"],
                "operation_id": snap["operation_id"],
                "source_set_revision_id": snap["source_set_revision_id"],
                "source_set_fingerprint": snap["source_set_fingerprint"],
                "claim_document_id": doc_id,
                "claim_document_sha256": snap["original_sha256"],
                "invoice_documents": _witness(invoice_docs, hashes),
                "sku_documents": _witness(sku_docs, hashes),
                "supplier_documents": _witness(supplier_docs, hashes),
                "supplier_identity_digest": supplier_digest,
                "line_reference": identity.line_reference,
                "sku": identity.sku,
                "subject_id": subject,
            }
            updates.append((claim, proof))
    for claim, proof in updates:
        claim.update({
            "association_status": ASSOCIATION_STATUS,
            "line_reference": identity.line_reference,
            "sku": identity.sku,
            "subject_id": proof["subject_id"],
            "documentary_binding": proof,
        })
    return len(updates)


def validate_documentary_binding(
    source: Mapping, *, binding: object,
    current_document_hashes: Mapping[int, str] | None,
) -> None:
    """Reject forged, superseded, incomplete or cross-tenant document witnesses."""
    proof = source.get("documentary_binding")
    if not isinstance(proof, dict) or current_document_hashes is None:
        raise ValueError("Documentary association needs current verified witnesses")
    checks = {
        "version": PROOF_VERSION,
        "method": "CORROBORATED_SINGLE_PRODUCT",
        "organization_id": binding.organization_id,
        "operation_id": binding.operation_id,
        "source_set_revision_id": binding.source_set_revision_id,
        "source_set_fingerprint": binding.source_set_fingerprint,
        "claim_document_id": binding.assurance_document_id,
        "claim_document_sha256": binding.original_sha256,
        "line_reference": source.get("line_reference"),
        "sku": source.get("sku"),
        "subject_id": source.get("subject_id"),
    }
    if any(proof.get(key) != value for key, value in checks.items()):
        raise ValueError("Documentary proof does not match source-set or subject")
    digest = proof.get("supplier_identity_digest")
    if (not isinstance(digest, str) or len(digest) != 64
            or any(char not in "0123456789abcdef" for char in digest)):
        raise ValueError("Exact supplier witness is missing")
    subject = (f"docproduct:{binding.organization_id}:{binding.operation_id}:"
               f"{binding.source_set_revision_id}:{source['line_reference']}:"
               f"{source['sku']}:{digest}")
    if source.get("subject_id") != subject or source.get("field_name") not in _LINE_FIELDS:
        raise ValueError("Unsupported document/product field association")
    ids: dict[str, set[int]] = {}
    for key, minimum in (
        ("invoice_documents", 1), ("sku_documents", 2),
        ("supplier_documents", 2),
    ):
        entries = proof.get(key)
        if not isinstance(entries, list) or len(entries) < minimum:
            raise ValueError("Insufficient documentary corroboration")
        found: set[int] = set()
        for entry in entries:
            if not isinstance(entry, dict):
                raise ValueError("Invalid documentary witness")
            doc_id = entry.get("document_id")
            if (type(doc_id) is not int or doc_id in found or
                    current_document_hashes.get(doc_id) != entry.get("sha256")):
                raise ValueError("Stale or duplicate source document witness")
            found.add(doc_id)
        ids[key] = found
    if len(ids["invoice_documents"]) != 1:
        raise ValueError("Expected exactly one explicit invoice line source")
    if binding.assurance_document_id not in ids["sku_documents"]:
        raise ValueError("Claim document lacks corroborated SKU context")
    if len(ids["sku_documents"] - ids["invoice_documents"]) < 2:
        raise ValueError("Need two independent manufacturer SKU references")
