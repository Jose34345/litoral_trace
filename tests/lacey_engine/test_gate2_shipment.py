from litoral_trace.lacey_engine.domain import AdmittedCandidate, DocumentResolution, DocumentType, EvidenceClass, FieldStatus, LayoutBlock, ParsedLayout, Provenance, RawCandidate, ResolvedField
from litoral_trace.lacey_engine.shipment import ReconciliationState, ShipmentDocumentInput, process_shipment


def _document(identifier, values, document_type=DocumentType.BILL_OF_LADING):
    fields = {}
    for index, (key, value) in enumerate(values.items()):
        block = LayoutBlock(f"{identifier}-{index}", 1, None, value, "TEXT_LINE")
        raw = RawCandidate(key, value, value, block, EvidenceClass.EXPLICIT, "test", "1")
        candidate = AdmittedCandidate(raw, Provenance(f"{identifier}.pdf", 1, None, block.block_id, value, "test", "1", EvidenceClass.EXPLICIT), 90, document_type)
        fields[key] = ResolvedField(key, FieldStatus.MATCHED, value, candidate, (candidate,))
    return DocumentResolution(f"{identifier}.pdf", "test", document_type, 1, ParsedLayout((), 1), (), fields)


def _shipment(*docs):
    return process_shipment(documents=[ShipmentDocumentInput(str(i), f"{i}.pdf", resolution=doc) for i, doc in enumerate(docs)])


def test_dossier_supported_evidence_and_multiple_containers():
    result = _shipment(_document("bl", {"bill_of_lading": "MAEU274342495", "container_number": "MSKU9228574", "species": "radiata", "country_of_harvest": "NEW ZEALAND"}), _document("packing", {"container_number": "MSKU9228574"}), _document("more", {"container_number": "MSCU1234567"}))
    assert result.canonical_fields["bill_of_lading"].state is ReconciliationState.SUPPORTED
    assert result.canonical_fields["container_number"].state is ReconciliationState.SUPPORTED_MULTIPLE
    assert {value.value for value in result.canonical_fields["container_number"].values} == {"MSKU9228574", "MSCU1234567"}


def test_real_species_disagreement_is_conflict():
    result = _shipment(_document("supplier-a", {"species": "radiata"}), _document("supplier-b", {"species": "taeda"}))
    assert result.canonical_fields["species"].state is ReconciliationState.CONFLICT


def test_origin_never_populates_harvest_country():
    result = _shipment(_document("origin", {"country_of_origin": "NEW ZEALAND"}), _document("harvest", {"country_of_harvest": "CHILE"}, DocumentType.HARVEST_DECLARATION))
    assert result.canonical_fields["country_of_harvest"].values[0].value == "CHILE"
    assert "country_of_origin" not in result.canonical_fields["country_of_harvest"].field_key
