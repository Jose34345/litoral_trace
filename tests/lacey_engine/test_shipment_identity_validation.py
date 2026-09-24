from __future__ import annotations

from litoral_trace.lacey_engine.ai_shadow import extraction_result_from_payload
from litoral_trace.lacey_engine.trade_validation import (
    is_valid_entity_name,
    normalize_invoice_total,
    normalize_iso6346_container,
    normalize_mid,
    normalize_seal_number,
)


def _ai_candidate(field_key: str, value: str, source_text: str) -> dict[str, object]:
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


def test_iso6346_container_requires_valid_check_digit():
    assert normalize_iso6346_container("MSKU0857658") == "MSKU0857658"
    assert normalize_iso6346_container("MSKU 085765 8") == "MSKU0857658"
    assert normalize_iso6346_container("MSKU0857657") is None
    assert normalize_iso6346_container("SEAL778899") is None


def test_seal_is_distinct_from_container_identity():
    assert normalize_seal_number("6548158") == "6548158"
    assert normalize_seal_number("SEAL-778899") == "SEAL-778899"
    assert normalize_seal_number("MSKU0857658") is None


def test_invoice_total_requires_financial_numeric_value():
    assert normalize_invoice_total("USD 31,110.21") == "31110.21"
    assert normalize_invoice_total("$31,110.2100") == "31110.21"
    assert normalize_invoice_total("vtcNT...KG") is None
    assert normalize_invoice_total("USD 1O,000.00") is None


def test_mid_rejects_scac_context_and_accepts_cbp_structural_candidate():
    assert normalize_mid("WESTCFSSCAC") is None
    assert (
        normalize_mid(
            "PEAMAPLA1145LIM",
            source_text="Manufacturer ID: PEAMAPLA1145LIM",
            label="Manufacturer ID",
        )
        == "PEAMAPLA1145LIM"
    )
    assert (
        normalize_mid(
            "PEAMAPLA1145LIM",
            source_text="Carrier SCAC: CTII",
            label="",
        )
        is None
    )


def test_entity_name_filter_rejects_ocr_fragments_and_phone_numbers():
    assert not is_valid_entity_name("A")
    assert not is_valid_entity_name("AB")
    assert not is_valid_entity_name("+1 (305) 555-1212")
    assert is_valid_entity_name("3M")
    assert is_valid_entity_name("3A-CRYOGENIC FZE")


def test_ai_postprocessing_rejects_scac_as_mid_and_accepts_anchored_identity_fields():
    payload = {
        "candidates": [
            _ai_candidate(
                "manufacturer_id",
                "WESTCFSSCAC",
                "ROSE CONTAINERLINE C/O AZ MIDWEST CFS SCAC: CTII",
            ),
            _ai_candidate(
                "container_number",
                "MSKU0857658",
                "Container Number: MSKU0857658",
            ),
            _ai_candidate(
                "seal_number",
                "6548158",
                "Seal: 6548158",
            ),
            _ai_candidate(
                "invoice_total",
                "USD 31,110.21",
                "Invoice Total: USD 31,110.21",
            ),
            _ai_candidate(
                "supplier_name",
                "+1 (305) 555-1212",
                "Supplier: +1 (305) 555-1212",
            ),
        ]
    }

    result = extraction_result_from_payload(
        payload=payload,
        provider="fixture",
        model="fixture",
    )

    assert [(item.field_key, item.normalized_value) for item in result.candidates] == [
        ("container_number", "MSKU0857658"),
        ("seal_number", "6548158"),
        ("invoice_total", "31110.21"),
    ]
