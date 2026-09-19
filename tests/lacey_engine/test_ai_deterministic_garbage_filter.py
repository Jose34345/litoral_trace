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


def test_deterministic_filter_drops_dirty_numeric_and_blacklisted_merchandise_rows():
    payload = {
        "candidates": [
            _candidate("article_component", " 1. \n"),
            _candidate("description", " PAL. \n"),
            _candidate("description", "AUX-02"),
            _candidate("description", "carton packaging"),
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


def test_deterministic_filter_uses_punctuation_free_numeric_normalization():
    payload = {
        "candidates": [
            _candidate("description", "\t2,\n"),
            _candidate("article_component", " 003. "),
            _candidate("description", "Bandeja de madera"),
        ]
    }

    result = extraction_result_from_payload(
        payload=payload,
        provider="gemini",
        model="test-model",
    )

    assert [candidate.value for candidate in result.candidates] == ["Bandeja de madera"]


def test_deterministic_filter_is_scoped_to_article_component_and_description():
    payload = {
        "candidates": [
            _candidate("plant_quantity", " 1. \n"),
            _candidate("description", "Bandeja de madera"),
        ]
    }

    result = extraction_result_from_payload(
        payload=payload,
        provider="gemini",
        model="test-model",
    )

    assert [(candidate.field_key, candidate.value) for candidate in result.candidates] == [
        ("plant_quantity", "1."),
        ("description", "Bandeja de madera"),
    ]


def test_deterministic_filter_keeps_na_and_none_blocked_after_cleanup():
    payload = {
        "candidates": [
            _candidate("description", " N/A. "),
            _candidate("article_component", " NONE! "),
            _candidate("description", "Bandeja de madera"),
        ]
    }

    result = extraction_result_from_payload(
        payload=payload,
        provider="gemini",
        model="test-model",
    )

    assert [candidate.value for candidate in result.candidates] == ["Bandeja de madera"]


def test_deterministic_filter_does_not_destroy_legitimate_words_containing_markers():
    payload = {
        "candidates": [
            _candidate("description", "Palo Santo wood boards"),
            _candidate("description", "Palm wood tray"),
            _candidate("description", "Wooden toolbox handle"),
            _candidate("description", "Auxiliary wooden frame"),
        ]
    }

    result = extraction_result_from_payload(
        payload=payload,
        provider="gemini",
        model="test-model",
    )

    assert [candidate.value for candidate in result.candidates] == [
        "Palo Santo wood boards",
        "Palm wood tray",
        "Wooden toolbox handle",
        "Auxiliary wooden frame",
    ]


def test_deterministic_filter_rejects_punctuation_obfuscated_exact_marker():
    payload = {
        "candidates": [
            _candidate("description", " P.A.L. "),
            _candidate("description", "Bandeja de madera"),
        ]
    }

    result = extraction_result_from_payload(
        payload=payload,
        provider="gemini",
        model="test-model",
    )

    assert [candidate.value for candidate in result.candidates] == ["Bandeja de madera"]
