"""Identity → Memory prerequisite: supplier+SKU without botanicals.

Representative exact PDF text layout fragments from the six-page source pack
(2026-10-09), not a substitute for byte-for-byte PDF E2E acceptance.
"""
from types import SimpleNamespace

from litoral_trace.us_lacey.pdf_documentary_identity import (
    discover_single_product_pdf_identity,
)
from litoral_trace.us_lacey.identity_memory import (
    _line_sku_observations, _supplier_candidates,
)


def _fields():
    docs = [
        [
            "COMMERCIAL INVOICE",
            "Supplier / manufacturer",
            "LONGSHENG BAMBOO HOUSEWARES CO., LTD.",
            "Supplier address",
            "88 Jinghua Industrial Road, Cangshan District, Fuzhou, Fujian 350000, China (fictional",
            "address)",
            "Product / SKU",
            "Bamboo coaster set (4 pieces) / BAM-COAST-04",
            "LINE / SKU",
            "1 / BAM-COAST-04",
        ],
        [
            "PACKING LIST",
            "Supplier / manufacturer",
            "LONGSHENG BAMBOO HOUSEWARES CO., LTD.",
            "Supplier address",
            "88 Jinghua Industrial Road, Cangshan District, Fuzhou, Fujian 350000, China (fictional",
            "address)",
            "SKU", "CARTONS", "QUANTITY", "NET", "DESCRIPTION", "BAM-COAST-04",
        ],
        ["HOUSE BILL OF LADING", "LSBH-B31405 / SKU BAM-COAST-04"],
        [
            "SUPPLIER ORDER CONFIRMATION",
            "Manufacturer",
            "LONGSHENG BAMBOO HOUSEWARES CO., LTD.",
            "Manufacturer address",
            "88 Jinghua Industrial Road, Cangshan District, Fuzhou, Fujian 350000, China (fictional",
            "address)",
            "MANUFACTURER", "SKU", "PRODUCT", "ORDER QTY", "BAM-COAST-04",
        ],
        [
            "ENTRY PREPARATION WORKSHEET", "Manufacturer SKU", "BAM-COAST-04",
            "Scientific genus", "TO BE FILLED FROM REVIEWED SUPPLIER MEMORY",
            "Scientific species", "TO BE FILLED FROM REVIEWED SUPPLIER MEMORY",
            "Country of harvest", "TO BE CONFIRMED FROM VALID SUPPLIER EVIDENCE",
        ],
        ["ARRIVAL NOTICE", "Merchandise / SKU", "Bamboo coaster sets / BAM-COAST-04"],
    ]
    return tuple(
        SimpleNamespace(assurance_document_id=i, field_name="raw.document_text",
                        original_value="\n".join(lines))
        for i, lines in enumerate(docs, 1)
    )


def test_shipment2_supplier_sku_binding_does_not_need_botanicals():
    result = discover_single_product_pdf_identity(_fields(), line_references=("1",))
    assert result is not None
    assert result.sku == "BAM-COAST-04"
    assert result.line_reference == "1"
    assert result.supplier_document_count >= 2
    assert result.corroborating_sku_document_count >= 2
    assert result.product_name == "Bamboo coaster set (4 pieces)"
    assert _line_sku_observations(result.virtual_fields) == {"1": {"BAM-COAST-04"}}
    suppliers = _supplier_candidates(result.virtual_fields)
    assert suppliers
    assert len({supplier.normalized_name for supplier in suppliers}) == 1
    assert all(supplier.identifiers for supplier in suppliers)


def test_split_source_evidence_with_conflicting_supplier_fails_closed():
    fields = list(_fields())
    data = fields[1].original_value.replace(
        "LONGSHENG BAMBOO HOUSEWARES CO., LTD.", "OTHER SUPPLIER INC.",
    )
    fields[1] = SimpleNamespace(
        assurance_document_id=fields[1].assurance_document_id,
        field_name="raw.document_text", original_value=data,
    )
    assert discover_single_product_pdf_identity(fields, line_references=("1",)) is None
