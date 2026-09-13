from litoral_trace.lacey_engine.domain import AdmittedCandidate, DocumentResolution, DocumentType, EvidenceClass, FieldStatus, LayoutBlock, ParsedLayout, Provenance, RawCandidate, ResolvedField
from litoral_trace.lacey_engine.shipment import LaceyRuleset, ReconciliationState, ShipmentDocumentInput, ShipmentPreparationContext, ShipmentReadiness, normalize_mass, normalize_money, normalize_quantity, process_shipment
from litoral_trace.lacey_engine.pipeline import process_document
from litoral_trace.lacey_engine.serialization import deserialize_document_resolution, deserialize_shipment_resolution, serialize_document_resolution, serialize_shipment_resolution


def _document(identifier, values, document_type=DocumentType.BILL_OF_LADING):
    fields = {}
    for index, (key, item) in enumerate(values.items()):
        value, label = item if isinstance(item, tuple) else (item, key)
        block = LayoutBlock(f"{identifier}-{index}", 1, None, value, "TEXT_LINE")
        raw = RawCandidate(key, value, value, block, EvidenceClass.EXPLICIT, "test", "1", label=label)
        candidate = AdmittedCandidate(raw, Provenance(f"{identifier}.pdf", 1, None, block.block_id, value, "test", "1", EvidenceClass.EXPLICIT), 90, document_type)
        fields[key] = ResolvedField(key, FieldStatus.MATCHED, value, candidate, (candidate,))
    return DocumentResolution(f"{identifier}.pdf", "test", document_type, 1, ParsedLayout((), 1), (), fields)


def _shipment(*docs, ruleset=LaceyRuleset()):
    return process_shipment(documents=[ShipmentDocumentInput(str(i), f"{i}.pdf", resolution=doc) for i, doc in enumerate(docs)], ruleset=ruleset)


def _text_pdf(text):
    stream = f"BT /F1 12 Tf 72 720 Td ({text.replace(chr(10), ') Tj 0 -18 Td (')}) Tj ET".encode()
    objects = [b"<< /Type /Catalog /Pages 2 0 R >>", b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>", b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >>", b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>", b"<< /Length " + str(len(stream)).encode() + b" >>\nstream\n" + stream + b"\nendstream"]
    body = b"%PDF-1.4\n"; offsets = [0]
    for number, item in enumerate(objects, 1):
        offsets.append(len(body)); body += f"{number} 0 obj\n".encode() + item + b"\nendobj\n"
    start = len(body); xref = b"xref\n0 6\n0000000000 65535 f \n" + b"".join(f"{offset:010d} 00000 n \n".encode() for offset in offsets[1:])
    return body + xref + f"trailer << /Size 6 /Root 1 0 R >>\nstartxref\n{start}\n%%EOF".encode()


def test_dossier_1_clean_agreement_and_containers():
    result = _shipment(_document("bl", {"bill_of_lading": "MAEU274342495", "container_number": "MSKU9228574"}), _document("packing", {"container_number": "MSKU9228574"}), _document("more", {"container_number": "MSCU1234567"}))
    assert result.readiness is ShipmentReadiness.READY
    assert result.canonical_fields["container_number"].state is ReconciliationState.SUPPORTED_MULTIPLE


def test_dossier_2_same_component_disagreement_is_conflict():
    result = _shipment(_document("a", {"species": ("radiata", "Component A")}), _document("b", {"species": ("taeda", "Component A")}))
    assert result.canonical_fields["species"].state is ReconciliationState.CONFLICT


def test_dossier_3_origin_never_populates_harvest_country():
    result = _shipment(_document("origin", {"country_of_origin": "NEW ZEALAND"}), _document("harvest", {"country_of_harvest": "CHILE"}, DocumentType.HARVEST_DECLARATION))
    harvest = result.canonical_fields["country_of_harvest"]
    assert harvest.values[0].value == "CHILE" and {e.document_id for e in harvest.supporting_evidence} == {"1"}


def test_dossier_4_decimal_quantity_normalization():
    assert normalize_quantity("20350 KG") == normalize_quantity("20.35 metric tons") == normalize_mass("20350", "KG")


def test_dossier_5_containers_are_a_set():
    result = _shipment(_document("a", {"container_number": "MSKU9228574"}), _document("b", {"container_number": "MSCU1234567"}))
    assert result.canonical_fields["container_number"].state is ReconciliationState.SUPPORTED_MULTIPLE


def test_dossier_6_party_role_separation():
    result = _shipment(_document("a", {"party_name": ("WOOD BROKERAGE INTERNATIONAL", "Consignee")}), _document("b", {"party_name": ("PAGE JONES INC", "Notify Party")}))
    assert result.canonical_fields["consignee_name"].state is ReconciliationState.SUPPORTED
    assert result.canonical_fields["notify_party_name"].state is ReconciliationState.SUPPORTED


def test_dossier_7_master_and_house_bol_are_distinct():
    result = _shipment(_document("a", {"bill_of_lading": ("MAEU274342495", "Master B/L")}), _document("b", {"bill_of_lading": ("GPXGG10013119", "House Bill of Lading")}))
    assert result.canonical_fields["master_bill_of_lading"].values[0].value == "MAEU274342495"
    assert result.canonical_fields["house_bill_of_lading"].values[0].value == "GPXGG10013119"


def test_dossier_8_distinct_components_are_not_a_conflict():
    result = _shipment(_document("a", {"species": ("radiata", "Component A")}), _document("b", {"species": ("taeda", "Component B")}))
    assert result.canonical_fields["species"].state is ReconciliationState.SUPPORTED_MULTIPLE


def test_dossier_9_unassociated_different_plant_values_need_review():
    result = _shipment(_document("a", {"species": "radiata"}), _document("b", {"species": "taeda"}))
    assert result.canonical_fields["species"].state is ReconciliationState.REVIEW_REQUIRED
    assert any(issue.issue_type == "AMBIGUOUS_ASSOCIATION" for issue in result.issues)


def test_dossier_10_party_near_match_preserves_identity():
    result = _shipment(_document("a", {"consignee_name": "WOOD BROKERAGE INTERNATIONAL"}), _document("b", {"consignee_name": "WOOD BROKERAGE INTERNATIONAL LLC"}))
    assert result.canonical_fields["consignee_name"].state is ReconciliationState.NEAR_MATCH


def test_source_authority_changes_discrepancy_outcome():
    result = _shipment(_document("bl", {"bill_of_lading": "MAEU111"}), _document("invoice", {"bill_of_lading": "MAEU222"}, DocumentType.COMMERCIAL_INVOICE))
    assert result.canonical_fields["bill_of_lading"].state is ReconciliationState.REVIEW_REQUIRED
    assert any(issue.issue_type == "INCONSISTENT_SET" for issue in result.issues)


def test_equal_authority_bill_of_lading_disagreement_conflicts():
    result = _shipment(_document("a", {"bill_of_lading": "MAEU111"}), _document("b", {"bill_of_lading": "MAEU222"}))
    assert result.canonical_fields["bill_of_lading"].state is ReconciliationState.CONFLICT


def test_hts_and_money_normalization_are_deterministic():
    result = _shipment(_document("a", {"hts_code": ("4407.11", "Line 1"), "entered_value": ("USD 45000", "Line 1")}), _document("b", {"hts_code": ("440711", "Line 1"), "entered_value": ("USD 45,000.00", "Line 1")}))
    assert result.canonical_fields["hts_code"].state is ReconciliationState.SUPPORTED_MULTIPLE
    assert normalize_money("USD 45000") == normalize_money("USD 45,000.00")
    assert normalize_money("EUR 45000")[0] != normalize_money("USD 45000")[0]


def test_low_authority_only_does_not_auto_populate_important_value():
    result = _shipment(_document("invoice", {"country_of_harvest": "CHILE"}, DocumentType.COMMERCIAL_INVOICE))
    assert result.canonical_fields["country_of_harvest"].state is ReconciliationState.REVIEW_REQUIRED
    assert any(issue.issue_type == "LOW_AUTHORITY_ONLY" for issue in result.issues)


def test_readiness_states_and_truthful_metrics():
    ready = _shipment(_document("a", {"bill_of_lading": "MAEU1"}))
    review = _shipment(_document("a", {"bill_of_lading": "MAEU1"}), _document("b", {"bill_of_lading": "MAEU2"}, DocumentType.COMMERCIAL_INVOICE))
    blocked = _shipment(_document("a", {"container_number": "MSKU"}))
    assert ready.readiness is ShipmentReadiness.READY
    assert review.readiness is ShipmentReadiness.REVIEW_REQUIRED
    assert blocked.readiness is ShipmentReadiness.BLOCKED
    assert any(issue.issue_type == "MISSING_REQUIRED" for issue in blocked.issues)
    assert "rejected_candidates" not in ready.metrics and ready.metrics["issues_total"] == len(ready.issues)


def test_unresolved_party_role_is_an_explicit_review_issue():
    result = _shipment(_document("a", {"party_name": ("UNKNOWN PARTY", "Party")}))
    assert any(issue.issue_type == "UNRESOLVED_ROLE" for issue in result.issues)


def test_validation_and_evidence_ids_are_deterministic():
    doc = _document("a", {"bill_of_lading": "MAEU1"})
    try:
        process_shipment(documents=[ShipmentDocumentInput("same", "a", resolution=doc), ShipmentDocumentInput("same", "b", resolution=doc)])
    except ValueError as error:
        assert str(error) == "Shipment document_id values must be unique."
    else:
        raise AssertionError("duplicate document IDs must be rejected")
    try:
        process_shipment(documents=[ShipmentDocumentInput("empty", "empty.pdf")])
    except ValueError as error:
        assert str(error) == "ShipmentDocumentInput requires resolution or non-empty content."
    else:
        raise AssertionError("empty document input must be rejected")


def test_line_scoped_hts_reconciliation_is_independent_and_conservative():
    different_lines = _shipment(_document("a", {"hts_code": ("440711", "Line 1")}), _document("b", {"hts_code": ("440799", "Line 2")}))
    same_line = _shipment(_document("a", {"hts_code": ("440711", "Line 1")}), _document("b", {"hts_code": ("440799", "Line 1")}))
    unknown_lines = _shipment(_document("a", {"hts_code": "440711"}), _document("b", {"hts_code": "440799"}))
    assert different_lines.canonical_fields["hts_code"].state is ReconciliationState.SUPPORTED_MULTIPLE
    assert same_line.canonical_fields["hts_code"].state is ReconciliationState.CONFLICT
    assert unknown_lines.canonical_fields["hts_code"].state is ReconciliationState.REVIEW_REQUIRED


def test_component_partitions_allow_corroboration_and_isolate_values():
    result = _shipment(
        _document("a", {"species": ("radiata", "Component A"), "country_of_harvest": ("CHILE", "Component A")}, DocumentType.SUPPLIER_DECLARATION),
        _document("b", {"species": ("radiata", "Component A"), "country_of_harvest": ("CHILE", "Component A")}, DocumentType.SUPPLIER_DECLARATION),
        _document("c", {"species": ("taeda", "Component B"), "country_of_harvest": ("ARGENTINA", "Component B")}, DocumentType.SUPPLIER_DECLARATION),
    )
    assert result.canonical_fields["species"].state is ReconciliationState.SUPPORTED_MULTIPLE
    assert result.canonical_fields["country_of_harvest"].state is ReconciliationState.SUPPORTED_MULTIPLE


def test_shipment_quantity_semantics_exclude_gross_weight_from_plant_quantity():
    compatible = _shipment(_document("a", {"plant_quantity": ("20350 KG", "Plant Material Quantity Component A")}), _document("b", {"plant_quantity": ("20.35 metric tons", "Plant Material Quantity Component A")}))
    mixed = _shipment(_document("a", {"plant_quantity": ("20350 KG", "Gross Weight Component A")}), _document("b", {"plant_quantity": ("20350 KG", "Plant Material Quantity Component A")}))
    assert compatible.canonical_fields["plant_quantity"].state is ReconciliationState.SUPPORTED_MULTIPLE
    assert len(mixed.canonical_fields["plant_quantity"].supporting_evidence) == 1


def test_money_reconciliation_respects_currency_and_line_scope():
    equivalent = _shipment(_document("a", {"entered_value": ("USD 45000", "Line 1")}), _document("b", {"entered_value": ("USD 45,000.00", "Line 1")}))
    mismatch = _shipment(_document("a", {"entered_value": ("USD 45000", "Line 1")}), _document("b", {"entered_value": ("EUR 45000", "Line 1")}))
    distinct_lines = _shipment(_document("a", {"entered_value": ("USD 45000", "Line 1")}), _document("b", {"entered_value": ("EUR 30000", "Line 2")}))
    assert equivalent.canonical_fields["entered_value"].state is ReconciliationState.SUPPORTED_MULTIPLE
    assert mismatch.canonical_fields["entered_value"].state is ReconciliationState.REVIEW_REQUIRED
    assert distinct_lines.canonical_fields["entered_value"].state is ReconciliationState.SUPPORTED_MULTIPLE


def test_master_house_labels_survive_gate1_and_route_end_to_end():
    document = process_document(filename="bol.pdf", content=_text_pdf("OCEAN BILL OF LADING\nMaster BOL: MAEU274342495\nHouse BOL: GPXGG10013119"))
    candidates = document.fields["bill_of_lading"].candidates
    assert {candidate.raw.label for candidate in candidates} >= {"Master BOL", "House BOL"}
    result = process_shipment(documents=[ShipmentDocumentInput("bol", "bol.pdf", resolution=document)])
    assert result.canonical_fields["master_bill_of_lading"].values[0].value == "MAEU274342495"
    assert result.canonical_fields["house_bill_of_lading"].values[0].value == "GPXGG10013119"


def test_typed_bol_preparation_requirement_accepts_master_generic_and_house():
    master = _shipment(_document("master", {"bill_of_lading": ("MAEU274342495", "Master B/L")}))
    generic = _shipment(_document("generic", {"bill_of_lading": "MAEU274342495"}))
    both = _shipment(_document("both", {"bill_of_lading": ("MAEU274342495", "Master BOL")}), _document("house", {"bill_of_lading": ("GPXGG10013119", "House BOL")}))
    missing = _shipment(_document("none", {"container_number": "MSKU9228574"}))
    assert master.readiness is not ShipmentReadiness.BLOCKED
    assert generic.readiness is not ShipmentReadiness.BLOCKED
    assert both.readiness is not ShipmentReadiness.BLOCKED
    assert missing.readiness is ShipmentReadiness.BLOCKED
    assert not any(issue.issue_type == "MISSING_REQUIRED" for issue in master.issues)


def test_all_master_house_label_variants_route_end_to_end():
    variants = (("Master BOL", "master_bill_of_lading"), ("Master B/L", "master_bill_of_lading"), ("Master Bill of Lading", "master_bill_of_lading"), ("House BOL", "house_bill_of_lading"), ("House B/L", "house_bill_of_lading"), ("House Bill of Lading", "house_bill_of_lading"))
    for label, field_key in variants:
        document = process_document(filename="variant.pdf", content=_text_pdf(f"OCEAN BILL OF LADING\n{label}: MAEU274342495"))
        assert any(label.casefold() == (candidate.raw.label or "").casefold() for candidate in document.fields["bill_of_lading"].candidates)
        result = process_shipment(documents=[ShipmentDocumentInput("variant", "variant.pdf", resolution=document)])
        assert result.canonical_fields[field_key].values[0].value == "MAEU274342495"


def test_dossier_1_real_multi_document_preparation_readiness():
    result = _shipment(
        _document("invoice", {"country_of_origin": "NEW ZEALAND", "container_number": "MSKU9228574"}, DocumentType.COMMERCIAL_INVOICE),
        _document("packing", {"container_number": "MSKU9228574"}, DocumentType.PACKING_LIST),
        _document("bol", {"bill_of_lading": "MAEU274342495", "container_number": "MSKU9228574"}, DocumentType.BILL_OF_LADING),
        _document("supplier", {"species": ("radiata", "Component A"), "genus": ("Pinus", "Component A")}, DocumentType.SUPPLIER_DECLARATION),
        _document("harvest", {"country_of_harvest": ("CHILE", "Component A")}, DocumentType.HARVEST_DECLARATION),
    )
    assert result.readiness is ShipmentReadiness.READY
    assert result.canonical_fields["bill_of_lading"].state is ReconciliationState.SUPPORTED
    assert result.canonical_fields["container_number"].state is ReconciliationState.SUPPORTED_MULTIPLE
    assert result.canonical_fields["species"].state is ReconciliationState.SUPPORTED
    assert result.canonical_fields["country_of_harvest"].values[0].value == "CHILE"
    assert not result.issues


def test_document_level_conflict_propagates_all_candidates():
    document = _document("a", {"bill_of_lading": "MAEU111"})
    first = document.fields["bill_of_lading"].winning_candidate
    assert first is not None
    second_raw = RawCandidate("bill_of_lading", "MAEU222", "MAEU222", first.raw.source_block, EvidenceClass.EXPLICIT, "test", "1")
    second = AdmittedCandidate(second_raw, first.provenance, 90, DocumentType.BILL_OF_LADING)
    document.fields["bill_of_lading"] = ResolvedField("bill_of_lading", FieldStatus.CONFLICT, None, None, (first, second))
    result = process_shipment(documents=[ShipmentDocumentInput("a", "a.pdf", resolution=document)])
    assert {item.normalized_value for item in result.canonical_fields["bill_of_lading"].supporting_evidence} == {"MAEU111", "MAEU222"}


def test_resolution_serialization_round_trips_without_infrastructure():
    document = _document("a", {"bill_of_lading": "MAEU274342495", "container_number": "MSKU9228574"})
    restored_document = deserialize_document_resolution(serialize_document_resolution(document))
    restored_shipment = deserialize_shipment_resolution(serialize_shipment_resolution(_shipment(document)))
    assert restored_document.fields["bill_of_lading"].winning_candidate.raw.normalized_value == "MAEU274342495"
    assert restored_shipment.canonical_fields["container_number"].supporting_evidence[0].candidate_id
