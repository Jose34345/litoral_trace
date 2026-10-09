"""Golden single-SKU PDF corpus and ambiguity rejection; never synthesize botanicals."""
import json
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

from litoral_trace.us_lacey.pdf_documentary_identity import discover_single_product_pdf_identity
from litoral_trace.us_lacey.identity_memory import _supplier_candidates, _line_sku_observations


def _docs():
    return json.loads((Path(__file__).parent / "fixtures" / "us_lacey_shipment1_inline_20261008.json").read_text(encoding="utf-8"))


def _fields(docs):
    return tuple(SimpleNamespace(
        assurance_document_id=index,
        field_name="raw.document_text",
        original_value="\n".join(doc["lines"]),
    ) for index, doc in enumerate(docs, 1))


def test_golden_pdf_proves_exact_supplier_sku_and_line_without_bom():
    proof = discover_single_product_pdf_identity(_fields(_docs()), line_references=("1",))
    assert proof is not None
    assert proof.sku == "BAM-COAST-04"
    assert proof.line_reference == "1"
    assert proof.supplier_document_count >= 2
    assert proof.corroborating_sku_document_count >= 2
    assert _line_sku_observations(proof.virtual_fields) == {"1": {"BAM-COAST-04"}}
    supplier_candidates = _supplier_candidates(proof.virtual_fields)
    assert len(supplier_candidates) >= 2
    assert len({c.normalized_name for c in supplier_candidates}) == 1
    assert all(c.identifiers for c in supplier_candidates)


def test_pdf_identity_denies_missing_sku_cross_document_proof():
    docs = _docs()
    for doc in docs:
        doc["lines"] = [line for line in doc["lines"] if not line.startswith("Manufacturer SKU")]
    assert discover_single_product_pdf_identity(_fields(docs), line_references=("1",)) is None


def test_pdf_identity_denies_second_invoice_line_or_second_plant_line():
    docs = _docs()
    invoice = docs[0]["lines"]
    invoice.append("2 / OTHER-SKU-01 other product")
    assert discover_single_product_pdf_identity(_fields(docs), line_references=("1",)) is None
    assert discover_single_product_pdf_identity(_fields(_docs()), line_references=("1","2")) is None


def test_pdf_identity_denies_supplier_disagreement():
    docs = _docs()
    for i,line in enumerate(docs[1]["lines"]):
        if line.startswith("Supplier / manufacturer "):
            docs[1]["lines"][i] = "Supplier / manufacturer OTHER VENDOR LTD"
            break
    assert discover_single_product_pdf_identity(_fields(docs), line_references=("1",)) is None


def test_pdf_identity_denies_manufacturer_sku_conflict():
    docs = _docs()
    for i,line in enumerate(docs[-1]["lines"]):
        if line.startswith("Manufacturer SKU "):
            docs[-1]["lines"][i] = "Manufacturer SKU DIFFERENT-SKU-999"
            break
    assert discover_single_product_pdf_identity(_fields(docs), line_references=("1",)) is None
