from __future__ import annotations

import json

from litoral_trace.lacey_engine.ai_providers import AIProviderConfig
from litoral_trace.lacey_engine.ai_shadow import extraction_result_from_payload
from litoral_trace.lacey_engine.multi_agent.gemini_specialist_adapter import GeminiSpecialistProvider


def _candidate(
    value: str,
    source_text: str,
    *,
    source_line_key: str | None = None,
    source_table_id: str | None = None,
    source_row_index: int | None = None,
) -> dict[str, object]:
    return {
        "field_key": "bill_of_lading",
        "value": value,
        "evidence_class": "EXPLICIT",
        "page": 1,
        "source_text": source_text,
        "confidence": 0.99,
        "bbox": None,
        "reason": None,
        "source_line_key": source_line_key,
        "source_table_id": source_table_id,
        "source_row_index": source_row_index,
    }


def test_bill_of_lading_semantic_filter_rejects_context_fields_and_party_names():
    payload = {
        "candidates": [
            _candidate("MSC AMBAR", "Vessel: MSC AMBAR"),
            _candidate("SANTOS", "POD: SANTOS"),
            _candidate("SHANGHAI", "POL: SHANGHAI"),
            _candidate("2026-10-12", "ETA: 2026-10-12"),
            _candidate("24,500 KG", "Gross Weight: 24,500 KG"),
            _candidate("FOREST EXPORT S.A.", "Shipper: FOREST EXPORT S.A."),
            _candidate("WOOD IMPORT LLC", "Consignee: WOOD IMPORT LLC"),
            _candidate("MAEU123456789", "Bill of Lading No: MAEU123456789"),
        ]
    }

    result = extraction_result_from_payload(
        payload=payload,
        provider="gemini",
        model="test-model",
    )

    assert [(candidate.field_key, candidate.value) for candidate in result.candidates] == [
        ("bill_of_lading", "MAEU123456789")
    ]


def test_bill_of_lading_requires_value_to_be_bound_to_the_explicit_bol_label():
    payload = {
        "candidates": [
            _candidate(
                "MSC AMBAR",
                "BILL OF LADING\nVessel: MSC AMBAR\nPOL: SHANGHAI\nPOD: SANTOS",
            ),
            _candidate(
                "HLCUSS5123456789",
                "B/L No. HLCUSS5123456789 Vessel: MSC AMBAR",
            ),
            _candidate(
                "MEDU123456789",
                "Conocimiento de Embarque N° MEDU123456789",
            ),
            _candidate(
                "MSCU123456789",
                "Conhecimento de Embarque Nº MSCU123456789",
            ),
        ]
    }

    result = extraction_result_from_payload(
        payload=payload,
        provider="gemini",
        model="test-model",
    )

    assert [candidate.value for candidate in result.candidates] == [
        "HLCUSS5123456789",
        "MEDU123456789",
        "MSCU123456789",
    ]


def test_specialist_semantic_filter_keeps_row_identity_sidecars_aligned(monkeypatch):
    import litoral_trace.lacey_engine.multi_agent.gemini_specialist_adapter as module

    config = AIProviderConfig(
        mode="SHADOW",
        provider="gemini",
        model="gemini-test",
        base_url="https://example.invalid/v1beta/interactions",
        api_key="test-key",
        timeout_seconds=30.0,
        max_pages=8,
        allow_external=True,
    )

    monkeypatch.setattr(
        module,
        "_document_images",
        lambda filename, content, max_pages: [b"page-1"],
    )
    monkeypatch.setattr(
        module,
        "_post_json",
        lambda **kwargs: {
            "output_text": json.dumps(
                {
                    "candidates": [
                        _candidate(
                            "MSC AMBAR",
                            "Vessel: MSC AMBAR",
                            source_table_id="shipment-header",
                            source_row_index=0,
                        ),
                        _candidate(
                            "MAEU123456789",
                            "Bill of Lading No: MAEU123456789",
                            source_table_id="shipment-header",
                            source_row_index=1,
                        ),
                    ]
                }
            )
        },
    )

    result = GeminiSpecialistProvider(config).extract_scoped(
        filename="fixture.pdf",
        content=b"%PDF-fixture",
        pages=(1,),
        allowed_fields=frozenset({"bill_of_lading"}),
        prompt="Extract the explicit bill of lading number only.",
    )

    assert [candidate.value for candidate in result.candidates] == ["MAEU123456789"]
    assert len(result.row_identities) == 1
    identity = result.row_identities[0]
    assert identity is not None
    assert identity.table_id == "shipment-header"
    assert identity.row_index == 1
