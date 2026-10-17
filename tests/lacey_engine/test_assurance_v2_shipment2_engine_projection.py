"""Second shipment: Engine2 candidates must reach canonical merchandise identity.

This is parsed-document-text acceptance. Raw PDF byte integration is a separate
release gate and must not be represented by this fixture-based test.
"""
from litoral_trace.lacey_engine.serialization import serialize_shipment_resolution
from litoral_trace.lacey_engine.shipment import ShipmentDocumentInput, process_shipment
from litoral_trace.us_lacey.canonical_shipment_truth import build_canonical_shipment_truth

from test_shipment1_whitespace_labeled_pdf import _document
from test_us_lacey_pdf_documentary_reuse_postgres import _shipment_two_documents


def test_six_document_shipment2_keeps_commercial_line_without_botanicals():
    sources = _shipment_two_documents()
    inputs = [
        ShipmentDocumentInput(
            document_id=f"pdf-{index}",
            filename=source["filename"],
            resolution=_document(source["filename"], *source["lines"]),
        )
        for index, source in enumerate(sources, 1)
    ]
    resolution = process_shipment(documents=inputs)
    payload = serialize_shipment_resolution(resolution)
    truth = build_canonical_shipment_truth(payload)
    line_keys = tuple(line.entity_key for line in truth.plant_lines)
    assert line_keys == ("sku:bam-coast-04",), {
        "line_keys": line_keys,
        "states": {key: value.get("state") for key, value in payload["canonical_fields"].items()},
        "unresolved": truth.unresolved_component_keys,
    }
    line = truth.plant_lines[0]
    assert "hts_code" in line.fields
    assert line.fields["hts_code"].evidence
    for field_name in ("genus", "species", "country_of_harvest"):
        assert field_name not in line.fields or not line.fields[field_name].values
    # A previously reviewed species is absent by design; never hallucinate.
    assert not any(
        "phyllostachys" in str(x.values).casefold() or "edulis" in str(x.values).casefold()
        for x in line.fields.values()
    )
