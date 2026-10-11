"""Assurance V2 end-to-end documentary gate on 13 ORIGINAL synthetic PDF bytes.

These are intentionally small, fictional golden test documents. A transcript
reconstruction or mocked engine result MUST NOT replace this fixture.
The controlled-memory PostgreSQL test is separate; this proves raw PDF input,
source-linked claims, reconciliation and true line identity across both packs.
"""
from __future__ import annotations

from functools import lru_cache
from hashlib import sha256
import json
from pathlib import Path

import pytest

from litoral_trace.lacey_engine.pipeline import process_bundle
from litoral_trace.lacey_engine.shipment import ShipmentDocumentInput, process_shipment
from litoral_trace.lacey_engine.serialization import serialize_shipment_resolution
from litoral_trace.us_lacey.assurance_v2_claim_ingestion import collect_source_claims
from litoral_trace.us_lacey.canonical_shipment_truth import build_canonical_shipment_truth

_ROOT = Path(__file__).resolve().parent / "fixtures" / "assurance_v2_pdf_e2e"
_MANIFEST = json.loads((_ROOT / "sha256.json").read_text(encoding="utf-8"))


@lru_cache(maxsize=2)
def _raw_pack(pack: str):
    files = sorted((_ROOT / pack).glob("*.pdf"))
    assert len(files) == len(_MANIFEST[pack]) == (7 if pack == "pack1" else 6)
    inputs = []
    observations = []
    for index, path in enumerate(files, 1):
        original = path.read_bytes()
        assert original.startswith(b"%PDF")
        assert sha256(original).hexdigest() == _MANIFEST[pack][path.name]
        bundle = process_bundle(filename=path.name, content=original)
        assert bundle.documents
        for doc in bundle.documents:
            inputs.append(ShipmentDocumentInput(
                document_id=f"{pack}-{index}-{doc.logical_document_id}",
                filename=path.name, resolution=doc.resolution,
            ))
        payload = collect_source_claims(
            organization_id=1, operation_id=1 if pack == "pack1" else 2,
            source_set_revision_id=1 if pack == "pack1" else 2,
            source_set_fingerprint="a" * 64, assurance_document_id=index,
            operation_document_id=index, document_version_number=1,
            original_sha256=sha256(original).hexdigest(),
            original_bytes=original, bundle=bundle,
        )
        assert payload["claim_count"] == len(payload["claims"])
        observations.extend(payload["claims"])
    shipment = process_shipment(documents=inputs)
    truth = build_canonical_shipment_truth(serialize_shipment_resolution(shipment))
    return truth, tuple(observations)


@pytest.mark.parametrize("pack,supported_min", [("pack1", 30), ("pack2", 25)])
def test_original_pdf_byte_provenance_cannot_invent_human_authority(pack, supported_min):
    truth, claims = _raw_pack(pack)
    assert sum(c["support_status"] == "SUPPORTED" for c in claims) >= supported_min
    assert len(truth.plant_lines) == 1
    assert truth.plant_lines[0].entity_key == "sku:bam-coast-04"
    assert not truth.unresolved_component_keys
    for claim in claims:
        assert claim["candidate_state"] == "CANDIDATE"
        assert claim["interpretation_status"] == "PROPOSED"
        assert claim["contract_version"] == "assurance.claim.v1"
        assert claim["organization_id"] == 1
        assert len(claim["original_sha256"]) == 64
        if claim["support_status"] == "SUPPORTED":
            assert claim["source_locator"]
            assert claim["normalized_value"] is not None


def test_pack1_contains_source_verified_botanicals():
    truth, _ = _raw_pack("pack1")
    fields = truth.plant_lines[0].fields
    assert fields["hts_code"].values == ("4419199010",)
    assert fields["genus"].values == ("Phyllostachys",)
    assert fields["species"].values == ("edulis",)
    assert fields["country_of_harvest"].values == ("CHINA",)
    assert all(fields[key].evidence for key in ("hts_code", "genus", "species", "country_of_harvest"))


def test_pack2_has_new_shipment_data_but_no_copied_botanicals():
    truth, claims = _raw_pack("pack2")
    fields = truth.plant_lines[0].fields
    assert fields["hts_code"].values == ("4419199010",)
    assert not any(key in fields and fields[key].values for key in ("genus", "species", "country_of_harvest"))
    assert truth.shipment_fields["estimated_arrival_date"].values == ("2026-12-08",)
    assert truth.shipment_fields["container_number"].values == ("TGHU6042314",)
    placeholder_claims = [c for c in claims if c["field_name"] in ("genus", "species")]
    assert len(placeholder_claims) >= 2
    assert all(c["support_status"] == "UNSUPPORTED" and c["normalized_value"] is None
               and "NONFACTUAL_PLACEHOLDER" in c["reason_codes"]
               for c in placeholder_claims)
    # Longitudinal genus/species can come only from reviewable verified memory,
    # not hallucinated by a new upload lacking botanical evidence.
