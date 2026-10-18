"""Exact PDF document-to-line identity remains candidate-only and fail-closed."""
from __future__ import annotations

from copy import deepcopy
from hashlib import sha256
from pathlib import Path

import pytest

from litoral_trace.lacey_engine.pipeline import process_bundle
from litoral_trace.us_lacey.assurance_v2_claim_bridge import (
    ResolvedDocumentBinding, to_authority_candidate_snapshot,
)
from litoral_trace.us_lacey.assurance_v2_claim_ingestion import collect_source_claims
from litoral_trace.us_lacey.assurance_v2_documentary_claim_binding import (
    ParsedIdentityDocument, bind_documentary_claims, ASSOCIATION_STATUS,
)

FIXTURES = Path(__file__).parent / "fixtures" / "assurance_v2_pdf_e2e"


def _packet(pack: str):
    snapshots = []
    parsed = []
    for doc_id, source in enumerate(sorted((FIXTURES / pack).glob("*.pdf")), 1):
        data = source.read_bytes()
        digest = sha256(data).hexdigest()
        bundle = process_bundle(filename=source.name, content=data)
        parsed.append(ParsedIdentityDocument(doc_id, digest, bundle))
        snapshots.append(collect_source_claims(
            organization_id=11, operation_id=22,
            source_set_revision_id=33, source_set_fingerprint="f" * 64,
            assurance_document_id=doc_id, operation_document_id=100 + doc_id,
            document_version_number=1, original_sha256=digest,
            original_bytes=data, bundle=bundle,
        ))
    return snapshots, parsed


@pytest.mark.parametrize("pack", ["pack1", "pack2"])
def test_exact_single_sku_packet_binds_only_product_candidates(pack):
    snapshots, parsed = _packet(pack)
    assert all(c["association_status"] == "UNBOUND"
               for s in snapshots for c in s["claims"])
    bound_count = bind_documentary_claims(
        snapshots=snapshots, parsed_documents=parsed,
        line_references=["1"],
    )
    assert bound_count > 0
    actual = [c for snap in snapshots for c in snap["claims"]
              if c["association_status"] == ASSOCIATION_STATUS]
    assert len(actual) == bound_count
    assert {c["sku"] for c in actual} == {"BAM-COAST-04"}
    assert {c["line_reference"] for c in actual} == {"1"}
    assert {c["field_name"] for c in actual} >= {"article_component"}
    assert all(c["candidate_state"] == "CANDIDATE" for c in actual)
    assert all(c["support_status"] == "SUPPORTED" for c in actual)
    assert all(c["association_status"] == "UNBOUND"
               for snap in snapshots for c in snap["claims"]
               if c["field_name"] in {"bill_of_lading", "importer_name", "supplier_name"})
    selected = actual[0]
    owner = next(s for s in snapshots if selected in s["claims"])
    binding = ResolvedDocumentBinding(
        organization_id=11, operation_id=22, source_set_revision_id=33,
        source_set_fingerprint="f" * 64, generation=1,
        assurance_document_id=owner["assurance_document_id"],
        operation_document_id=owner["operation_document_id"],
        document_version_number=1, original_sha256=owner["original_sha256"],
    )
    hashes = {snap["assurance_document_id"]: snap["original_sha256"]
              for snap in snapshots}
    output = to_authority_candidate_snapshot(
        selected, binding=binding, current_document_hashes=hashes,
    )
    assert output.association_status == ASSOCIATION_STATUS
    assert output.state == "CANDIDATE"
    with pytest.raises(ValueError):
        to_authority_candidate_snapshot(selected, binding=binding)
    for mutation in (
        {**selected, "source_set_revision_id": 99},
        {**selected, "sku": "OTHER-SKU-01"},
        {**selected, "documentary_binding": None},
        {**selected, "documentary_binding": {**selected["documentary_binding"],
                                            "supplier_identity_digest": "a" * 64}},
        {**selected, "documentary_binding": {**selected["documentary_binding"],
                                            "sku_documents": []}},
    ):
        with pytest.raises(ValueError):
            to_authority_candidate_snapshot(
                mutation, binding=binding, current_document_hashes=hashes,
            )
    with pytest.raises(ValueError):
        to_authority_candidate_snapshot(
            selected, binding=binding,
            current_document_hashes={**hashes,
                                     selected["documentary_binding"]["sku_documents"][0]["document_id"]: "a" * 64},
        )


def test_ambiguous_or_missing_line_never_binds_and_never_rewrites_source():
    snapshots, parsed = _packet("pack1")
    original = deepcopy(snapshots)
    assert bind_documentary_claims(
        snapshots=snapshots, parsed_documents=parsed,
        line_references=["1", "2"],
    ) == 0
    assert snapshots == original
    assert bind_documentary_claims(
        snapshots=snapshots, parsed_documents=parsed[:-1],
        line_references=["1"],
    ) == 0
    assert snapshots == original


def test_source_document_hash_mismatch_abstains():
    snapshots, parsed = _packet("pack2")
    snapshots[0]["original_sha256"] = "a" * 64
    original = deepcopy(snapshots)
    assert bind_documentary_claims(
        snapshots=snapshots, parsed_documents=parsed,
        line_references=["1"],
    ) == 0
    assert snapshots == original


def test_pack2_packing_net_mass_has_current_original_sku_bound_candidate():
    """Real PDFs: source locator, invoice-line and supplier witnesses must all agree."""
    snapshots, parsed = _packet("pack2")
    assert bind_documentary_claims(
        snapshots=snapshots, parsed_documents=parsed, line_references=["1"],
    ) >= 2
    packing = snapshots[1]
    exact = [
        c for c in packing["claims"]
        if c.get("extractor_name") == "packing_net_mass_exact"
    ]
    assert {(c["field_name"], c["original_value"]) for c in exact} == {
        ("plant_quantity", "288.00"), ("metric_unit", "KG"),
    }
    assert all(
        c["support_status"] == "SUPPORTED"
        and c["association_status"] == "DOCUMENT_EXACT_SINGLE_SKU"
        and c["sku"] == "BAM-COAST-04"
        and c["line_reference"] == "1"
        and c["candidate_state"] == "CANDIDATE"
        and c["source_locator"]["page"] == 1
        for c in exact
    )
    assert all(
        packing["assurance_document_id"]
        in {row["document_id"] for row in c["documentary_binding"]["sku_documents"]}
        for c in exact
    )
    hashes = {x["assurance_document_id"]: x["original_sha256"] for x in snapshots}
    binding = ResolvedDocumentBinding(
        organization_id=11, operation_id=22, source_set_revision_id=33,
        source_set_fingerprint="f" * 64, generation=1,
        assurance_document_id=packing["assurance_document_id"],
        operation_document_id=packing["operation_document_id"],
        document_version_number=1, original_sha256=packing["original_sha256"],
    )
    for claim in exact:
        adapted = to_authority_candidate_snapshot(
            claim, binding=binding, current_document_hashes=hashes,
        )
        assert adapted.state == "CANDIDATE"
        assert adapted.support_status == "SUPPORTED"
        assert adapted.association_status == "DOCUMENT_EXACT_SINGLE_SKU"
        with pytest.raises(ValueError):
            to_authority_candidate_snapshot(
                claim, binding=binding,
                current_document_hashes={**hashes, packing["assurance_document_id"]: "a"*64},
            )


def test_mass_binding_abstains_without_unique_current_line_and_keeps_snapshot_intact():
    snapshots, parsed = _packet("pack2")
    old = deepcopy(snapshots)
    assert bind_documentary_claims(
        snapshots=snapshots, parsed_documents=parsed, line_references=["1", "2"],
    ) == 0
    assert snapshots == old
