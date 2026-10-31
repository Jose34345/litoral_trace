"""P0 regression: Engine 2 evidence must reach customer review for one provable SKU."""
import json
from copy import deepcopy
from pathlib import Path

from litoral_trace.lacey_engine.serialization import serialize_shipment_resolution
from litoral_trace.lacey_engine.shipment import ShipmentDocumentInput, process_shipment
from litoral_trace.us_lacey.canonical_shipment_truth import build_canonical_shipment_truth
from litoral_trace.us_lacey.single_sku_recovery import bind_single_sku_evidence


def _fixture_payload(source_docs=None):
    # Reuse the exact 214 source text lines from the real synthetic production run.
    from test_shipment1_whitespace_labeled_pdf import _document
    docs = source_docs or json.loads(
        (Path(__file__).resolve().parents[1] / "fixtures" / "us_lacey_shipment1_inline_20261008.json").read_text(encoding="utf-8")
    )
    inputs = [
        ShipmentDocumentInput(f"doc-{index}", source["filename"],
                              resolution=_document(source["filename"], *source["lines"]))
        for index, source in enumerate(docs, 1)
    ]
    return serialize_shipment_resolution(process_shipment(documents=inputs))


def test_shipment_one_full_pdf_extraction_is_actually_published_into_one_line():
    payload = _fixture_payload()
    truth = build_canonical_shipment_truth(payload)
    assert len(truth.plant_lines) == 1, [
        (name, item.get("state")) for name, item in payload["canonical_fields"].items()
    ]
    assert truth.unresolved_component_keys == ()
    line = truth.plant_lines[0]
    assert line.entity_key == "sku:bam-coast-04"
    for key in ("hts_code", "genus", "species", "country_of_harvest",
                "plant_quantity", "metric_unit", "article_component"):
        assert key in line.fields, (key, tuple(line.fields))
        assert len(line.fields[key].values) == 1, key
        assert line.fields[key].evidence, key
    assert "importer_name" in truth.shipment_fields
    assert "consignee_name" in truth.shipment_fields
    assert "manufacturer_id" in truth.shipment_fields
    assert not line.fields.get("entered_value") or line.fields["entered_value"].evidence


def test_unbound_fields_from_multi_invoice_rows_are_never_associated():
    payload = _fixture_payload()
    fake = deepcopy(payload)
    invoice_blocks = fake["documents"][0]["resolution"]["layout"]["blocks"]
    invoice_blocks.append({
        "block_type":"TEXT_LINE","text":"2 / BAM-COAST-05 Other bamboo product 100 SET USD 100.00",
    })
    assert not bind_single_sku_evidence(fake["canonical_fields"], fake["documents"])
    assert all(
        not row.get("line_key")
        for field in fake["canonical_fields"].values()
        for row in field.get("supporting_evidence", ())
        if row.get("scope") in ("PLANT_COMPONENT","MERCHANDISE_LINE")
    )


def test_conflicting_supplier_sku_is_not_joined_even_with_single_invoice_line():
    payload = _fixture_payload()
    fake = deepcopy(payload)
    blocks = fake["documents"][-1]["resolution"]["layout"]["blocks"]
    for block in blocks:
        if block.get("text") == "Manufacturer SKU BAM-COAST-04":
            block["text"] = "Manufacturer SKU OTHER-TRAY-99"
    assert not bind_single_sku_evidence(fake["canonical_fields"], fake["documents"])


def test_missing_invoice_identity_and_conflicting_taxon_fail_closed():
    payload = _fixture_payload()
    no_invoice = deepcopy(payload)
    no_invoice["documents"][0]["resolution"]["layout"]["blocks"] = [
        block for block in no_invoice["documents"][0]["resolution"]["layout"]["blocks"]
        if not block.get("text", "").startswith("1 / BAM-COAST-04")
    ]
    assert not bind_single_sku_evidence(no_invoice["canonical_fields"], no_invoice["documents"])
    other = deepcopy(payload)
    other["canonical_fields"]["species"]["state"] = "CONFLICT"
    assert not bind_single_sku_evidence(other["canonical_fields"], other["documents"])


def test_reconciliation_does_not_modify_input_document_evidence():
    payload = _fixture_payload()
    original = deepcopy(payload)
    _ = build_canonical_shipment_truth(payload)
    assert payload == original
