from litoral_trace.lacey_engine.domain import AdmittedCandidate, DocumentResolution, DocumentType, EvidenceClass, FieldStatus, LayoutBlock, ParsedLayout, Provenance, RawCandidate, ResolvedField
from litoral_trace.lacey_engine.shipment import LaceyRuleset, ReconciliationState, ShipmentDocumentInput, ShipmentPreparationContext, ShipmentReadiness, normalize_mass, normalize_money, normalize_quantity, process_shipment


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
