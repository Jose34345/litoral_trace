from __future__ import annotations

import json

from litoral_trace.lacey_engine.ai_providers import AIProviderConfig
from litoral_trace.lacey_engine.ai_shadow import extraction_result_from_payload
from litoral_trace.lacey_engine.multi_agent.gemini_specialist_adapter import GeminiSpecialistProvider


def _candidate(
    field_key: str,
    value: str,
    source_text: str,
    *,
    source_table_id: str | None = None,
    source_row_index: int | None = None,
) -> dict[str, object]:
    return {
        "field_key": field_key,
        "value": value,
        "evidence_class": "EXPLICIT",
        "page": 1,
        "source_text": source_text,
        "confidence": 0.99,
        "bbox": None,
        "reason": None,
        "source_line_key": None,
        "source_table_id": source_table_id,
        "source_row_index": source_row_index,
    }


def test_pack3_logistics_triple_survives_only_with_explicit_field_anchors():
    payload = {
        "candidates": [
            _candidate(
                "bill_of_lading",
                "RPT-HOU-260913-42",
                "Bill of Lading No: RPT-HOU-260913-42",
            ),
            _candidate(
                "container_number",
                "CMAU8842118",
                "Container No: CMAU8842118",
            ),
            _candidate(
                "estimated_arrival_date",
                "2026-10-10",
                "ETA: 2026-10-10",
            ),
        ]
    }

    result = extraction_result_from_payload(
        payload=payload,
        provider="gemini",
        model="test-model",
    )

    assert [(item.field_key, item.value) for item in result.candidates] == [
        ("bill_of_lading", "RPT-HOU-260913-42"),
        ("container_number", "CMAU8842118"),
        ("estimated_arrival_date", "2026-10-10"),
    ]


def test_container_and_eta_semantic_traps_are_rejected_before_persistence():
    payload = {
        "candidates": [
            _candidate("container_number", "MSC AMBAR", "Vessel: MSC AMBAR"),
            _candidate("container_number", "SEAL778899", "Seal No: SEAL778899"),
            _candidate("container_number", "40HC", "Equipment Type: 40HC"),
            _candidate("estimated_arrival_date", "2026-09-30", "ETD: 2026-09-30"),
            _candidate("estimated_arrival_date", "2026-09-13", "Issue Date: 2026-09-13"),
            _candidate("estimated_arrival_date", "2026-09-20", "Departure Date: 2026-09-20"),
        ]
    }

    result = extraction_result_from_payload(
        payload=payload,
        provider="gemini",
        model="test-model",
    )

    assert result.candidates == ()


def test_specialist_filter_keeps_logistics_sidecars_aligned_after_trap_rejection(monkeypatch):
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
                            "container_number",
                            "SEAL778899",
                            "Seal No: SEAL778899",
                            source_table_id="shipment-header",
                            source_row_index=0,
                        ),
                        _candidate(
                            "bill_of_lading",
                            "RPT-HOU-260913-42",
                            "Bill of Lading No: RPT-HOU-260913-42",
                            source_table_id="shipment-header",
                            source_row_index=1,
                        ),
                        _candidate(
                            "container_number",
                            "CMAU8842118",
                            "Container No: CMAU8842118",
                            source_table_id="shipment-header",
                            source_row_index=2,
                        ),
                        _candidate(
                            "estimated_arrival_date",
                            "2026-09-30",
                            "ETD: 2026-09-30",
                            source_table_id="shipment-header",
                            source_row_index=3,
                        ),
                        _candidate(
                            "estimated_arrival_date",
                            "2026-10-10",
                            "ETA: 2026-10-10",
                            source_table_id="shipment-header",
                            source_row_index=4,
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
        allowed_fields=frozenset(
            {"bill_of_lading", "container_number", "estimated_arrival_date"}
        ),
        prompt="Extract only B/L, container number, and ETA.",
    )

    assert [(item.field_key, item.value) for item in result.candidates] == [
        ("bill_of_lading", "RPT-HOU-260913-42"),
        ("container_number", "CMAU8842118"),
        ("estimated_arrival_date", "2026-10-10"),
    ]
    assert [identity.row_index if identity else None for identity in result.row_identities] == [
        1,
        2,
        4,
    ]
