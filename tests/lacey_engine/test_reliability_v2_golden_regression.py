from __future__ import annotations

from uuid import NAMESPACE_URL, uuid5

from litoral_trace.lacey_engine.ai_shadow import AICandidate, extraction_result_from_payload
from litoral_trace.lacey_engine.domain import EvidenceClass
from litoral_trace.lacey_engine.multi_agent.contracts import (
    CandidateEnvelope,
    DocumentType,
    SpecialistRole,
)
from litoral_trace.lacey_engine.multi_agent.fusion import fuse_candidates
from litoral_trace.lacey_engine.multi_agent.line_binding import bind_line_items
from litoral_trace.lacey_engine.multi_agent.router import classify_page
from litoral_trace.us_lacey.ppq505 import PPQ505_SHIPMENT_REFERENCE
from litoral_trace.us_lacey.specialized_projection import (
    SpecializedProjectionMode,
    project_specialized_candidates,
)
from tests.test_us_lacey_field_judge_regression_corpus import _corpus, _target


PACK3_SHIPMENT = {
    "bill_of_lading": "RPT-HOU-260913-42",
    "container_number": "CMAU8842110",
    "estimated_arrival_date": "2026-10-10",
}
PACK3_LINES = {
    "SKU:PT-38": {
        "hts_code": "4407110190",
        "genus": "Pinus",
        "species": "Pinus taeda",
        "country_of_harvest": "Brazil",
        "plant_quantity": "30",
        "metric_unit": "m3",
        "entered_value": "18300.00",
    },
    "SKU:EG-22": {
        "hts_code": "4407990190",
        "genus": "Eucalyptus",
        "species": "Eucalyptus grandis",
        "country_of_harvest": "Brazil",
        "plant_quantity": "16",
        "metric_unit": "m3",
        "entered_value": "12640.00",
    },
}


def _envelope(
    field_key: str,
    value: str,
    *,
    document_type: DocumentType,
    specialist: SpecialistRole,
    seed: str,
    line_key: str | None = None,
    confidence: float = 0.9,
) -> CandidateEnvelope:
    source_text = (
        f"SKU {line_key} {field_key} {value} verified source row"
        if line_key
        else f"{field_key} {value} verified source evidence"
    )
    candidate = AICandidate(
        field_key=field_key,
        value=value,
        normalized_value=value,
        evidence_class=EvidenceClass.EXPLICIT,
        page=1,
        source_text=source_text,
        confidence=confidence,
        provider="reliability-v2-golden",
        model="reliability-v2-golden",
        evidence_verified=True,
    )
    return CandidateEnvelope(
        candidate=candidate,
        document_id=uuid5(NAMESPACE_URL, f"reliability-v2:{seed}"),
        document_type=document_type,
        specialist=specialist,
        agent_run_id=uuid5(NAMESPACE_URL, f"reliability-v2-run:{seed}"),
        line_item_key=None,
        source_span_id=None,
        source_line_key=line_key,
    )


def _ai_payload(field_key: str, value: str, source_text: str) -> dict[str, object]:
    return {
        "field_key": field_key,
        "value": value,
        "evidence_class": "EXPLICIT",
        "page": 1,
        "source_text": source_text,
        "confidence": 0.99,
        "bbox": None,
        "reason": None,
    }


def test_pack1_clean_corroboration_does_not_fabricate_conflict() -> None:
    line = "PT-38"
    candidates = (
        _envelope(
            "entered_value",
            "18300.00",
            document_type=DocumentType.COMMERCIAL_INVOICE,
            specialist=SpecialistRole.COMMERCIAL_LINES,
            seed="pack1-invoice",
            line_key=line,
        ),
        _envelope(
            "entered_value",
            "18300.00",
            document_type=DocumentType.ENTRY_WORKSHEET,
            specialist=SpecialistRole.COMMERCIAL_LINES,
            seed="pack1-entry",
            line_key=line,
        ),
    )

    fusion = fuse_candidates(bind_line_items(candidates))

    assert fusion.conflicts == ()
    assert len(fusion.fused_candidates) == 1
    assert fusion.fused_candidates[0].line_item_key == "SKU:PT-38"
    assert fusion.fused_candidates[0].candidate.value == "18300.00"


def test_pack2_preserves_value_eta_and_harvest_conflicts_for_review() -> None:
    line = "PT-38"
    candidates = bind_line_items(
        (
            _envelope(
                "entered_value",
                "18300.00",
                document_type=DocumentType.COMMERCIAL_INVOICE,
                specialist=SpecialistRole.COMMERCIAL_LINES,
                seed="pack2-value-invoice",
                line_key=line,
                confidence=0.99,
            ),
            _envelope(
                "entered_value",
                "18100.00",
                document_type=DocumentType.ENTRY_WORKSHEET,
                specialist=SpecialistRole.COMMERCIAL_LINES,
                seed="pack2-value-entry",
                line_key=line,
                confidence=0.91,
            ),
            _envelope(
                "estimated_arrival_date",
                "2026-10-10",
                document_type=DocumentType.BILL_OF_LADING,
                specialist=SpecialistRole.LOGISTICS,
                seed="pack2-eta-bol",
                confidence=0.99,
            ),
            _envelope(
                "estimated_arrival_date",
                "2026-10-12",
                document_type=DocumentType.ARRIVAL_NOTICE,
                specialist=SpecialistRole.LOGISTICS,
                seed="pack2-eta-arrival",
                confidence=0.92,
            ),
            _envelope(
                "country_of_harvest",
                "Brazil",
                document_type=DocumentType.BOTANICAL_DECLARATION,
                specialist=SpecialistRole.BOTANICAL,
                seed="pack2-harvest-botanical",
                line_key=line,
                confidence=0.99,
            ),
            _envelope(
                "country_of_harvest",
                "Paraguay",
                document_type=DocumentType.SUPPLIER_ORIGIN,
                specialist=SpecialistRole.BOTANICAL,
                seed="pack2-harvest-origin",
                line_key=line,
                confidence=0.90,
            ),
        )
    )

    fusion = fuse_candidates(candidates)
    conflicts = {
        (item.key.field_key, item.key.line_item_key): item
        for item in fusion.conflicts
    }

    assert set(conflicts) == {
        ("entered_value", "SKU:PT-38"),
        ("estimated_arrival_date", None),
        ("country_of_harvest", "SKU:PT-38"),
    }
    assert all(item.requires_ai_resolution is True for item in conflicts.values())
    assert set(conflicts[("entered_value", "SKU:PT-38")].normalized_values) == {
        "18100.00",
        "18300.00",
    }
    assert set(conflicts[("estimated_arrival_date", None)].normalized_values) == {
        "2026-10-10",
        "2026-10-12",
    }
    assert set(conflicts[("country_of_harvest", "SKU:PT-38")].normalized_values) == {
        "brazil",
        "paraguay",
    }


def test_pack3_shipment_semantics_accept_only_exact_bol_container_eta() -> None:
    payload = {
        "candidates": [
            _ai_payload(
                "bill_of_lading",
                PACK3_SHIPMENT["bill_of_lading"],
                "Bill of Lading No: RPT-HOU-260913-42",
            ),
            _ai_payload(
                "container_number",
                PACK3_SHIPMENT["container_number"],
                "Container No: CMAU8842110",
            ),
            _ai_payload(
                "estimated_arrival_date",
                PACK3_SHIPMENT["estimated_arrival_date"],
                "ETA: 2026-10-10",
            ),
            _ai_payload("bill_of_lading", "MSC AMBAR", "Vessel: MSC AMBAR"),
            _ai_payload("bill_of_lading", "SANTOS", "POD: SANTOS"),
            _ai_payload("bill_of_lading", "SHANGHAI", "POL: SHANGHAI"),
            _ai_payload("bill_of_lading", "2026-10-10", "ETA: 2026-10-10"),
            _ai_payload("bill_of_lading", "32450 KG", "Gross Weight: 32450 KG"),
            _ai_payload("container_number", "SEAL778899", "Seal No: SEAL778899"),
            _ai_payload("estimated_arrival_date", "2026-09-30", "ETD: 2026-09-30"),
        ]
    }

    result = extraction_result_from_payload(
        payload=payload,
        provider="reliability-v2-golden",
        model="reliability-v2-golden",
    )

    accepted = {item.field_key: item.value for item in result.candidates}
    assert accepted == PACK3_SHIPMENT
    assert len(result.candidates) == 3


def test_pack3_two_plant_lines_keep_exact_values_without_cross_line_conflicts() -> None:
    document_types = {
        "hts_code": DocumentType.ENTRY_WORKSHEET,
        "entered_value": DocumentType.COMMERCIAL_INVOICE,
        "genus": DocumentType.BOTANICAL_DECLARATION,
        "species": DocumentType.BOTANICAL_DECLARATION,
        "country_of_harvest": DocumentType.BOTANICAL_DECLARATION,
        "plant_quantity": DocumentType.BOTANICAL_DECLARATION,
        "metric_unit": DocumentType.BOTANICAL_DECLARATION,
    }
    specialists = {
        "hts_code": SpecialistRole.COMMERCIAL_LINES,
        "entered_value": SpecialistRole.COMMERCIAL_LINES,
        "genus": SpecialistRole.BOTANICAL,
        "species": SpecialistRole.BOTANICAL,
        "country_of_harvest": SpecialistRole.BOTANICAL,
        "plant_quantity": SpecialistRole.BOTANICAL,
        "metric_unit": SpecialistRole.BOTANICAL,
    }
    raw: list[CandidateEnvelope] = []
    for line_key, fields in PACK3_LINES.items():
        sku = line_key.removeprefix("SKU:")
        for field_key, value in fields.items():
            raw.append(
                _envelope(
                    field_key,
                    value,
                    document_type=document_types[field_key],
                    specialist=specialists[field_key],
                    seed=f"pack3-{sku}-{field_key}",
                    line_key=sku,
                )
            )

    fusion = fuse_candidates(bind_line_items(tuple(raw)))
    actual: dict[str, dict[str, str]] = {}
    for envelope in fusion.fused_candidates:
        assert envelope.line_item_key is not None
        actual.setdefault(envelope.line_item_key, {})[
            envelope.candidate.field_key
        ] = envelope.candidate.value

    assert fusion.conflicts == ()
    assert actual == PACK3_LINES


def test_pack3_classifier_assigns_all_seven_required_document_roles() -> None:
    documents = (
        (
            DocumentType.COMMERCIAL_INVOICE,
            "COMMERCIAL INVOICE\nInvoice No. INV-260913\nCommercial Line Items\nEntered Value USD 30940",
        ),
        (
            DocumentType.ENTRY_WORKSHEET,
            "U.S. ENTRY WORKSHEET\nEntry / Filing Reference 123-4567890-1\nEntry Summary Lines\nImporter Number 9988",
        ),
        (
            DocumentType.BILL_OF_LADING,
            "OCEAN BILL OF LADING\nBill of Lading No RPT-HOU-260913-42\nPort of Loading Santos\nPort of Discharge Houston",
        ),
        (
            DocumentType.PACKING_LIST,
            "PACKING LIST\nPackage Detail\nCartons Pieces\nNet Wt. Gross Wt.",
        ),
        (
            DocumentType.BOTANICAL_DECLARATION,
            "BOTANICAL / LACEY SUPPORTING DECLARATION\nGenus Species\nCountry of Harvest\nPlant Quantity",
        ),
        (
            DocumentType.SUPPLIER_ORIGIN,
            "SUPPLIER MATERIAL ORIGIN STATEMENT\nStatement of Material Origin\nScientific Name Pinus taeda\nHarvest Country Brazil",
        ),
        (
            DocumentType.ARRIVAL_NOTICE,
            "ARRIVAL NOTICE\nCarrier Reference CR-260913\nETA 2026-10-10\nAvailability subject to customs release",
        ),
    )

    classified = tuple(
        classify_page(text, filename="shipment_document.pdf").document_type
        for _, text in documents
    )

    assert classified == tuple(expected for expected, _ in documents)
    assert len(classified) == 7
    assert len(set(classified)) == 7


def test_reliability_v2_false_safe_gate_is_zero() -> None:
    false_safe = 0
    safe_projected = 0
    cases = _corpus()["bol_cases"]

    for index, case in enumerate(cases):
        candidate = _envelope(
            "bill_of_lading",
            case["value"],
            document_type=DocumentType.BILL_OF_LADING,
            specialist=SpecialistRole.LOGISTICS,
            seed=f"pack3-false-safe-{index}",
        )
        candidate = CandidateEnvelope(
            candidate=AICandidate(
                field_key="bill_of_lading",
                value=case["value"],
                normalized_value=case["value"],
                evidence_class=EvidenceClass.EXPLICIT,
                page=1,
                source_text=case["source_text"],
                confidence=0.97,
                provider="reliability-v2-golden",
                model="reliability-v2-golden",
                evidence_verified=True,
            ),
            document_id=candidate.document_id,
            document_type=candidate.document_type,
            specialist=candidate.specialist,
            agent_run_id=candidate.agent_run_id,
            line_item_key=None,
            source_span_id=None,
        )
        target = _target(
            field_name="bill_of_lading",
            line_reference=PPQ505_SHIPMENT_REFERENCE,
            field_scope="SHIPMENT",
        )
        projection = project_specialized_candidates(
            candidates=(candidate,),
            targets=(target,),
            mode=SpecializedProjectionMode.ENFORCE,
        )
        projected = projection.projected_count == 1
        if case["expected_safe"]:
            safe_projected += int(projected)
        else:
            false_safe += int(projected)

    assert false_safe == 0
    assert safe_projected == sum(int(case["expected_safe"]) for case in cases)
