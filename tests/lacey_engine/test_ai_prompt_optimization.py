from __future__ import annotations

import json

from litoral_trace.lacey_engine.ai_providers import AIProviderConfig
from litoral_trace.lacey_engine.ai_shadow import AI_FIELDS
from litoral_trace.lacey_engine import gemini_provider
from litoral_trace.lacey_engine.gemini_provider import GeminiInteractionsProvider, _GEMINI_CANDIDATE_SCHEMA, _GEMINI_EXTRACTION_PROMPT


def _config() -> AIProviderConfig:
    return AIProviderConfig("SHADOW", "gemini", "gemini-3.5-flash-lite", "https://generativelanguage.googleapis.com/v1beta/interactions", "test-key", 30, 8, True)


def _candidate(field_key: str, value: str, source_text: str) -> dict[str, object]:
    return {"field_key": field_key, "value": value, "evidence_class": "EXPLICIT", "page": 77, "source_text": source_text, "confidence": 0.97, "bbox": None, "reason": None}


def test_gemini_prompt_is_customs_table_and_line_item_aware():
    assert "You are an expert U.S. Customs and Lacey Act auditor." in _GEMINI_EXTRACTION_PROMPT
    assert "Extract EVERY line item" not in _GEMINI_EXTRACTION_PROMPT
    assert "You MUST ONLY extract line items that represent actual botanical merchandise/products" in _GEMINI_EXTRACTION_PROMPT
    assert "You MUST ACTIVELY IGNORE packaging materials" in _GEMINI_EXTRACTION_PROMPT
    assert "pallets, PAL, cartons, boxes" in _GEMINI_EXTRACTION_PROMPT
    assert "auxiliary lines (e.g., AUX)" in _GEMINI_EXTRACTION_PROMPT
    assert "numerical line headers standing alone" in _GEMINI_EXTRACTION_PROMPT
    assert "If a row does not contain a tradeable plant product, skip it entirely." in _GEMINI_EXTRACTION_PROMPT
    assert "Do not merge different HTS codes or species into a single string" in _GEMINI_EXTRACTION_PROMPT
    assert "Look for Importer and Consignee specifically in Entry Worksheets or Bills of Lading" in _GEMINI_EXTRACTION_PROMPT
    assert "multi-line or visually aligned tables" in _GEMINI_EXTRACTION_PROMPT


def test_candidate_schema_explicitly_models_repeatable_line_item_occurrences():
    candidates_schema = _GEMINI_CANDIDATE_SCHEMA["properties"]["candidates"]
    assert candidates_schema["type"] == "array"
    assert "one object per field occurrence" in candidates_schema["description"]
    assert "Never concatenate multiple HTS codes" in candidates_schema["items"]["properties"]["value"]["description"]
    assert {"importer_name", "entered_value", "article_component"}.issubset(set(AI_FIELDS))


def test_gemini_preserves_multiple_hts_species_values_and_quantities(monkeypatch):
    captured: dict[str, object] = {}
    monkeypatch.setattr(gemini_provider, "_document_images", lambda *_args, **_kwargs: [b"image"])
    candidates = [
        _candidate("importer_name", "ACME IMPORTS LLC", "Importer: ACME IMPORTS LLC"),
        _candidate("consignee_name", "ACME WAREHOUSE", "Consignee: ACME WAREHOUSE"),
        _candidate("hts_code", "4418.99.90", "4418.99.90 | 12500 | Pinus | radiata | 20 M3"),
        _candidate("entered_value", "12500", "4418.99.90 | 12500 | Pinus | radiata | 20 M3"),
        _candidate("genus", "Pinus", "4418.99.90 | 12500 | Pinus | radiata | 20 M3"),
        _candidate("species", "radiata", "4418.99.90 | 12500 | Pinus | radiata | 20 M3"),
        _candidate("plant_quantity", "20", "4418.99.90 | 12500 | Pinus | radiata | 20 M3"),
        _candidate("hts_code", "4421.99.98", "4421.99.98 | 8300 | Eucalyptus | grandis | 12 M3"),
        _candidate("entered_value", "8300", "4421.99.98 | 8300 | Eucalyptus | grandis | 12 M3"),
        _candidate("genus", "Eucalyptus", "4421.99.98 | 8300 | Eucalyptus | grandis | 12 M3"),
        _candidate("species", "grandis", "4421.99.98 | 8300 | Eucalyptus | grandis | 12 M3"),
        _candidate("plant_quantity", "12", "4421.99.98 | 8300 | Eucalyptus | grandis | 12 M3"),
    ]
    def fake_post_json(**kwargs):
        captured.update(kwargs)
        return {"output_text": json.dumps({"candidates": candidates})}
    monkeypatch.setattr(gemini_provider, "_post_json", fake_post_json)
    result = GeminiInteractionsProvider(_config()).extract(filename="invoice.pdf", content=b"pdf")
    assert [c.value for c in result.candidates if c.field_key == "hts_code"] == ["4418.99.90", "4421.99.98"]
    assert [c.value for c in result.candidates if c.field_key == "species"] == ["radiata", "grandis"]
    assert [c.value for c in result.candidates if c.field_key == "plant_quantity"] == ["20", "12"]
    payload = captured["payload"]
    prompt = payload["input"][0]["text"]
    assert "Extract EVERY line item" not in prompt
    assert "You MUST ONLY extract line items that represent actual botanical merchandise/products" in prompt
    assert "You MUST ACTIVELY IGNORE packaging materials" in prompt
    assert payload["response_format"]["schema"]["properties"]["candidates"]["type"] == "array"
