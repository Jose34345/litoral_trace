"""Executable Assurance claim.v1 contract and existing synthetic packet benchmark."""
from __future__ import annotations

from dataclasses import replace
import json
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

import pytest

from litoral_trace.lacey_engine.ai_shadow import AICandidate
from litoral_trace.lacey_engine.domain import (
    BoundingBox, EvidenceClass, LayoutBlock, ParsedLayout,
    RawCandidate, Provenance, AdmittedCandidate, DocumentType as EngineDocumentType,
    DocumentResolution, ResolvedField, FieldStatus,
)
from litoral_trace.lacey_engine.multi_agent.contracts import (
    CandidateEnvelope, DocumentType, SpecialistRole,
    MultiAgentExtractionResult, SpecialistResult,
)
from litoral_trace.lacey_engine.source_linked_claims import (
    AssociationStatus, ClaimContext, ClaimRelation, SheetCell,
    SourceEvidenceDocument, SourceLocator, SupportStatus,
    from_engine2_candidate, from_specialist_envelope, from_spreadsheet_cell,
    group_claims, serialize_claims, verify_claim_anchor,
    claims_from_engine2_resolution, claims_from_specialist_result,
)

ROOT = Path(__file__).resolve().parents[2]
CTX = ClaimContext("tenant-A", "shipment-A", "revision-7", "run-1")


def _document(text: str, *, id: str = "source-1", historical: bool = False,
              bbox: BoundingBox | None = None) -> SourceEvidenceDocument:
    blocks = tuple(LayoutBlock(f"p1-l{i}", 1, bbox, line, "TEXT_LINE")
                   for i, line in enumerate(text.splitlines(), 1))
    return SourceEvidenceDocument(id, text.encode("utf-8"), ParsedLayout(blocks, 1),
                                  historical=historical, document_version="fixture-v1")


def _envelope(
    doc: SourceEvidenceDocument, value: str, source_text: str,
    *, field_name: str = "species", sku: str | None = None,
    evidence_class: EvidenceClass = EvidenceClass.EXPLICIT,
    normalized_value: str | None = None, page: int = 1,
) -> CandidateEnvelope:
    return CandidateEnvelope(
        candidate=AICandidate(
            field_key=field_name, value=value,
            normalized_value=normalized_value or value,
            evidence_class=evidence_class, page=page, source_text=source_text,
            confidence=0.91, provider="benchmark", model="v1",
            # An old verifier's YES must never be trusted by the new contract.
            evidence_verified=True,
        ),
        document_id=uuid5(NAMESPACE_URL, doc.document_id),
        document_type=DocumentType.BOTANICAL_DECLARATION,
        specialist=SpecialistRole.BOTANICAL,
        agent_run_id=uuid5(NAMESPACE_URL, "agent-run"),
        line_item_key="SKU:" + sku if sku else None,
        source_span_id=None,
    )


def _claim(text: str, value: str, *, field_name: str = "species",
           sku: str | None = None, historical: bool = False, id: str = "file-A",
           quote: str | None = None, evidence_class: EvidenceClass = EvidenceClass.EXPLICIT):
    doc = _document(text, id=id, historical=historical)
    # Preserve one consistent UUID across the specialist sidecar and source.
    source_id = str(uuid5(NAMESPACE_URL, id))
    doc = replace(doc, document_id=source_id)
    env = replace(_envelope(doc, value, quote or text.splitlines()[0],
                             field_name=field_name, sku=sku, evidence_class=evidence_class),
                  document_id=uuid5(NAMESPACE_URL, id))
    return from_specialist_envelope(env, context=CTX, document=doc), doc


def test_supported_span_is_exact_serializable_and_still_only_candidate():
    bbox = BoundingBox(10, 20, 200, 30)
    doc = _document("SKU ACT-TRAY-18 Especie: Quercus alba", bbox=bbox)
    doc = replace(doc, document_id=str(uuid5(NAMESPACE_URL, doc.document_id)))
    env = replace(_envelope(doc, "Quercus alba", "Especie: Quercus alba",
                             sku="ACT-TRAY-18"), document_id=uuid5(NAMESPACE_URL, "source-1"))
    claim = from_specialist_envelope(env, context=CTX, document=doc)
    assert claim.support_status is SupportStatus.SUPPORTED
    assert claim.candidate_state == "CANDIDATE"
    assert claim.locator.bbox == bbox
    assert claim.original_value == "Quercus alba"
    assert claim.sku == "ACT-TRAY-18"
    assert claim.association_status is AssociationStatus.EXPLICIT_SKU
    assert claim.normalized_value == "quercus alba"
    assert verify_claim_anchor(claim, doc)
    wire = serialize_claims((claim,))
    assert wire["contract_version"] == "assurance.claim.v1"
    assert wire["claims"][0]["source_set_revision_id"] == "revision-7"
    assert wire["claims"][0]["candidate_state"] == "CANDIDATE"
    assert wire["claims"][0]["original_document_sha256"] == doc.sha256
    assert "original_bytes" not in claim.to_json()
    assert not hasattr(claim, "approved_by")


def test_absent_source_or_invented_value_cannot_become_supported():
    invented, doc = _claim("SKU ACT-TRAY-18 Species Quercus alba",
                           "Pinus radiata", sku="ACT-TRAY-18")
    assert invented.support_status is SupportStatus.UNSUPPORTED
    assert invented.locator is None
    assert not verify_claim_anchor(invented, doc)
    assert invented.original_value == "Pinus radiata"
    # Legacy evidence_verified=True on the model cannot override this failure.


def test_normalization_and_ai_interpretation_never_replace_raw():
    claim, _ = _claim("País de cosecha: Brasil", "Brasil",
                      field_name="country_of_harvest")
    assert claim.original_value == "Brasil"
    assert claim.normalized_value == "brazil"
    assert claim.interpretation_value == "Brasil"
    assert claim.support_status is SupportStatus.SUPPORTED


def test_no_unique_anchor_means_ambiguous():
    claim, _ = _claim("Species: alba\nSpecies: alba", "alba", quote="Species: alba")
    assert claim.support_status is SupportStatus.AMBIGUOUS
    assert claim.locator is None
    assert "AMBIGUOUS_SOURCE_ANCHOR" in claim.issues


@pytest.mark.parametrize("wrong_sku", ["RUB-CB-32", "TEK-SRV-04"])
def test_same_hts_position_and_similar_quantity_cannot_cross_bind(wrong_sku):
    line = "1 ACT-TRAY-18 HTS 4419.90.9000 420 PCS"
    claim, _ = _claim(line, "4419.90.9000", field_name="hts_code", sku=wrong_sku)
    assert claim.support_status is SupportStatus.SUPPORTED
    assert claim.sku is None
    assert claim.association_status is AssociationStatus.UNBOUND
    assert "UNPROVEN_LINE_ASSOCIATION" in claim.issues


def test_historical_document_remains_visible_but_out_of_scope():
    claim, doc = _claim("SKU ACT-TRAY-18 Species Pinus radiata", "Pinus radiata",
                        sku="ACT-TRAY-18", historical=True)
    assert verify_claim_anchor(claim, doc)
    assert claim.support_status is SupportStatus.OUT_OF_SCOPE
    assert "HISTORICAL_DOCUMENT" in claim.issues


def test_ai_inference_with_quote_is_not_explicit_evidence():
    claim, doc = _claim("SKU ACT-TRAY-18 Species Quercus alba", "Quercus alba",
                        sku="ACT-TRAY-18", evidence_class=EvidenceClass.INFERRED)
    assert verify_claim_anchor(claim, doc)
    assert claim.support_status is SupportStatus.UNSUPPORTED
    assert claim.candidate_state == "CANDIDATE"


def test_corroboration_conflicts_and_multiple_skus_preserve_every_observation():
    a, _ = _claim("SKU ACT-TRAY-18 Species Acacia mangium", "Acacia mangium",
                  sku="ACT-TRAY-18", id="invoice")
    b, _ = _claim("SKU ACT-TRAY-18 Species Acacia mangium", "Acacia mangium",
                  sku="ACT-TRAY-18", id="botanical")
    c, _ = _claim("SKU ACT-TRAY-18 Species Pinus radiata", "Pinus radiata",
                  sku="ACT-TRAY-18", id="supplier")
    d, _ = _claim("SKU RUB-CB-32 Species Acacia mangium", "Acacia mangium",
                  sku="RUB-CB-32", id="other-product")
    groups = group_claims((a, b, c, d))
    assert len(groups) == 2
    assert next(g for g in groups if g.subject_key == "SKU:ACT-TRAY-18").relation is ClaimRelation.CONFLICT
    assert sum(len(g.claims) for g in groups) == 4
    assert set(c.original_value for g in groups for c in g.claims) == {"Acacia mangium", "Pinus radiata"}
    assert a.observation_id != b.observation_id


def test_incompatible_quantities_and_units_do_not_convert_or_overwrite():
    a, _ = _claim("SKU ACT-TRAY-18 Qty: 500 KG", "KG",
                  field_name="metric_unit", sku="ACT-TRAY-18", id="botanical")
    b, _ = _claim("SKU ACT-TRAY-18 Qty: 500 PCS", "PCS",
                  field_name="metric_unit", sku="ACT-TRAY-18", id="packing")
    assert group_claims((a, b))[0].relation is ClaimRelation.CONFLICT
    assert {a.original_value, b.original_value} == {"KG", "PCS"}


def test_pdf_span_and_multilingual_sheet_cells():
    doc = SourceEvidenceDocument(
        document_id="sheet-1", original_bytes=b"source workbook bytes",
        cells=(SheetCell("Declaracion", 3, 2, "País: Perú"),
               SheetCell("Declaração", 5, 4, "Espécie: Tectona grandis"),
               SheetCell("Sheet 1", 9, 2, "Species: Acacia mangium")),
    )
    for sheet, row, col, raw in (
        ("Declaracion", 3, 2, "Perú"),
        ("Declaração", 5, 4, "Tectona grandis"),
        ("Sheet 1", 9, 2, "Acacia mangium"),
    ):
        claim = from_spreadsheet_cell(
            context=CTX, document=doc, sheet=sheet, row=row, column=col,
            field_name="species", original_value=raw,
        )
        assert claim.support_status is SupportStatus.SUPPORTED
        assert claim.locator.sheet == sheet
        assert claim.original_value == raw
        assert verify_claim_anchor(claim, doc)
        assert claim.association_status is AssociationStatus.SOURCE_ROW
    missing = from_spreadsheet_cell(
        context=CTX, document=doc, sheet="Sheet 1", row=9, column=2,
        field_name="species", original_value="Eucalyptus grandis",
    )
    assert missing.support_status is SupportStatus.UNSUPPORTED


def test_reextract_same_document_new_run_preserves_observation_id():
    a, doc = _claim("SKU ACT-TRAY-18 Species Acacia mangium", "Acacia mangium",
                    sku="ACT-TRAY-18")
    env = replace(_envelope(doc, "Acacia mangium", "SKU ACT-TRAY-18 Species Acacia mangium",
                            sku="ACT-TRAY-18"), document_id=uuid5(NAMESPACE_URL, "file-A"))
    b = from_specialist_envelope(env, context=replace(CTX, extraction_run_id="run-2"),
                                 document=doc)
    assert a.claim_id != b.claim_id
    assert a.observation_id == b.observation_id
    # Same anchor in a re-run is not a second independent corroboration.
    assert group_claims((a, b))[0].relation is ClaimRelation.UNRESOLVED
    tampered = replace(doc, original_bytes=b"different content")
    assert not verify_claim_anchor(a, tampered)


def test_engine2_adapter_keeps_raw_and_cannot_support_missing_block():
    doc = _document("Genus: Eucalyptus")
    block = doc.layout.blocks[0]
    raw = RawCandidate("genus", "Eucalyptus", "eucalyptus", block,
                       EvidenceClass.EXPLICIT, "engine2", "2.0")
    prov = Provenance("sample.pdf", 1, None, block.block_id, block.text,
                      "engine2", "2.0", EvidenceClass.EXPLICIT)
    admitted = AdmittedCandidate(raw, prov, .94, EngineDocumentType.SPECIES_DECLARATION)
    claim = from_engine2_candidate(replace(admitted, score=155), context=CTX, document=doc)
    assert claim.support_status is SupportStatus.SUPPORTED
    assert claim.extractor_score == 155
    assert claim.confidence == 0.0  # Ranking score is not calibrated probability.
    assert verify_claim_anchor(claim, doc)
    broken = replace(admitted, raw=replace(raw, raw_text="Tectona"))
    other = from_engine2_candidate(broken, context=CTX, document=doc)
    assert other.support_status is SupportStatus.UNSUPPORTED


def test_locator_and_context_reject_forged_authority():
    with pytest.raises(ValueError):
        SourceLocator("TEXT_SPAN", page=1, block_id="b", start=9, end=1)
    with pytest.raises(ValueError):
        ClaimContext("", "shipment", "revision", "run")
    claim, _ = _claim("SKU ACT-TRAY-18 Species Acacia mangium", "Acacia mangium")
    with pytest.raises(ValueError):
        replace(claim, candidate_state="HUMAN_CONFIRMED")
    with pytest.raises(ValueError):
        replace(claim, support_status=SupportStatus.SUPPORTED, locator=None)


def test_existing_lacey_v1_field_truth_benchmark_and_adversarial_abstention(capsys):
    """Field-level observed precision/coverage/abstention. Print failures, never conceal."""
    truth = json.loads((ROOT / "benchmarks/lacey/v1/field_truth.json").read_text(encoding="utf-8"))
    packet = json.loads((ROOT / truth["source_fixture"]).read_text(encoding="utf-8"))
    docs = {d["filename"]: d for d in packet["documents"]}
    # These are parsed-text fixture pages, NOT the original PDFs. This benchmark
    # exercises claim anchoring against fixture layout rather than OCR accuracy.
    cases: list[tuple[str, bool, bool, str]] = []
    for index, item in enumerate(truth["fields"]):
        document_data = docs[item["document_id"]]
        page = document_data["pages"][str(item["page"])]
        doc = _document(page, id=item["document_id"])
        source_id = str(uuid5(NAMESPACE_URL, doc.document_id))
        doc = replace(doc, document_id=source_id)
        env = replace(_envelope(doc, str(item["value"]), str(item["evidence_text"]),
                                field_name=item["field_key"],
                                sku=(item.get("line_item_key") or "")[4:]
                                if str(item.get("line_item_key") or "").startswith("SKU:") else None),
                      document_id=uuid5(NAMESPACE_URL, item["document_id"]))
        claim = from_specialist_envelope(env, context=CTX, document=doc)
        supported = claim.support_status is SupportStatus.SUPPORTED
        cases.append((item["field_key"], True, supported, f"v1:{index}:{claim.issues}"))
        if supported:
            assert verify_claim_anchor(claim, doc)

    novel = json.loads((ROOT / "benchmarks/lacey/assurance_v2/adversarial_cases.json").read_text(encoding="utf-8"))
    for case in novel["cases"]:
        claim, doc = _claim(
            case["source"], case["value"], field_name=case["field"],
            sku=case.get("sku"), quote=case.get("quote"),
            historical=case.get("historical", False), id=case["id"],
            evidence_class=EvidenceClass(case.get("evidence_class", "EXPLICIT")),
        )
        actual = claim.support_status is SupportStatus.SUPPORTED
        cases.append((case["field"], bool(case["expected_supported"]), actual, case["id"]))
        if actual:
            assert verify_claim_anchor(claim, doc)

    stats: dict[str, dict[str, object]] = {}
    for field in sorted({item[0] for item in cases}):
        rows = [item for item in cases if item[0] == field]
        true_positive = sum(expected and observed for _, expected, observed, _ in rows)
        false_positive = sum(not expected and observed for _, expected, observed, _ in rows)
        expected_count = sum(expected for _, expected, _, _ in rows)
        admitted = sum(observed for _, _, observed, _ in rows)
        stats[field] = {
            "total": len(rows),
            "precision": round(true_positive / admitted, 4) if admitted else None,
            "coverage": round(true_positive / expected_count, 4) if expected_count else None,
            "abstention": round((len(rows) - admitted) / len(rows), 4),
            "false_supported": false_positive,
            "misses": [why for _, expected, observed, why in rows if expected and not observed],
        }
    print("ASSURANCE_V2_BENCHMARK " + json.dumps(stats, sort_keys=True))
    assert sum(s["false_supported"] for s in stats.values()) == 0
    assert any(s["abstention"] > 0 for s in stats.values())


def test_batch_specialist_adapter_retains_all_candidates_not_just_fusion_winner():
    doc = _document("SKU ACT-TRAY-18 Species Acacia mangium\n"
                    "SKU ACT-TRAY-18 Species Pinus radiata", id="batch")
    doc = replace(doc, document_id=str(uuid5(NAMESPACE_URL, "batch")))
    base = replace(_envelope(doc, "Acacia mangium",
                             "SKU ACT-TRAY-18 Species Acacia mangium",
                             sku="ACT-TRAY-18"),
                   document_id=uuid5(NAMESPACE_URL, "batch"))
    conflicting = replace(
        base, candidate=replace(base.candidate, value="Pinus radiata",
                                normalized_value="Pinus radiata",
                                source_text="SKU ACT-TRAY-18 Species Pinus radiata"))
    result = MultiAgentExtractionResult(
        routing_plan=None, specialist_results=(
            SpecialistResult(SpecialistRole.BOTANICAL, (base, conflicting),
                             "benchmark", "v1", 2, ()),
        ),
        fused_candidates=(base,), partial_failures=(),
    )
    claims = claims_from_specialist_result(
        result, context=CTX, documents={doc.document_id: doc})
    assert len(claims) == 2
    assert all(c.support_status is SupportStatus.SUPPORTED for c in claims)
    assert all(c.entity_type == "PLANT_COMPONENT" for c in claims)
    assert group_claims(claims)[0].relation is ClaimRelation.CONFLICT
    with pytest.raises(ValueError, match="Missing original"):
        claims_from_specialist_result(result, context=CTX, documents={})


def test_batch_engine2_preserves_losing_candidates_and_checks_layout():
    doc = _document("Genus: Acacia\nGenus: Pinus", id="engine")
    blocks = doc.layout.blocks
    def admitted(block, raw):
        candidate = RawCandidate("genus", raw, raw.lower(), block,
                                 EvidenceClass.EXPLICIT, "engine2", "2.0")
        provenance = Provenance("engine.pdf", 1, None, block.block_id, block.text,
                                "engine2", "2.0", EvidenceClass.EXPLICIT)
        return AdmittedCandidate(candidate, provenance, 111.0,
                                 EngineDocumentType.SPECIES_DECLARATION)
    a = admitted(blocks[0], "Acacia")
    b = admitted(blocks[1], "Pinus")
    resolution = DocumentResolution(
        "engine.pdf", "2.0", EngineDocumentType.SPECIES_DECLARATION, .95,
        doc.layout, (), {"genus": ResolvedField(
            "genus", FieldStatus.CONFLICT, None, None, (a, b))})
    claims = claims_from_engine2_resolution(resolution, context=CTX, document=doc)
    assert len(claims) == 2
    assert {c.original_value for c in claims} == {"Acacia", "Pinus"}
    assert all(c.extractor_score == 111.0 and c.confidence == 0.0 for c in claims)
    with pytest.raises(ValueError, match="layout must match"):
        claims_from_engine2_resolution(
            resolution, context=CTX, document=_document("Genus: different", id="engine"))


@pytest.mark.parametrize(("text", "proposed"), [
    ("SKU ACT-TRAY-18 Qty: 500 KG", "18"),
    ("SKU ACT-TRAY-18 Qty: 18,900 KG", "18"),
    ("SKU ACT-TRAY-18 Qty: 1,500 KG", "500"),
])
def test_numeric_fragments_of_skus_or_larger_amounts_are_not_source_values(text, proposed):
    claim, _ = _claim(text, proposed, field_name="plant_quantity", sku="ACT-TRAY-18")
    assert claim.support_status is SupportStatus.UNSUPPORTED
    assert claim.locator is None
