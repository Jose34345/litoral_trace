from __future__ import annotations

from litoral_trace.lacey_engine.ai_shadow import extraction_result_from_payload


def _candidate(field_key: str, value: str) -> dict[str, object]:
    return {
        "field_key": field_key,
        "value": value,
        "evidence_class": "EXPLICIT",
        "page": 1,
        "source_text": value,
        "confidence": 0.99,
        "bbox": None,
        "reason": None,
    }


def test_deterministic_filter_drops_numeric_and_blacklisted_merchandise_rows():
    payload = {
        "candidates": [
            _candidate("article_component", "1"),
            _candidate("description", " PAL "),
            _candidate("description", "Bandeja de madera"),
        ]
    }

    result = extraction_result_from_payload(
        payload=payload,
        provider="gemini",
        model="test-model",
    )

    assert [(candidate.field_key, candidate.value) for candidate in result.candidates] == [
        ("description", "Bandeja de madera")
    ]


def test_deterministic_filter_is_scoped_to_article_component_and_description():
    payload = {
        "candidates": [
            _candidate("plant_quantity", "1"),
            _candidate("description", "Bandeja de madera"),
        ]
    }

    result = extraction_result_from_payload(
        payload=payload,
        provider="gemini",
        model="test-model",
    )

    assert [(candidate.field_key, candidate.value) for candidate in result.candidates] == [
        ("plant_quantity", "1"),
        ("description", "Bandeja de madera"),
    ]
