from __future__ import annotations

from litoral_trace.us_lacey.engine2_suggestions import supported_engine2_suggestions


def _evidence(*, value: str, document_id: int, association: str, text: str, authority: float = 20.0):
    return {
        "document_id": str(document_id),
        "normalized_value": value,
        "source_authority": authority,
        "candidate_score": 90.0,
        "line_key": association if ":row:" in association else None,
        "component_key": association if not ":row:" in association else None,
        "candidate": {
            "score": 90.0,
            "raw": {"normalized_value": value, "evidence_class": "EXPLICIT"},
            "provenance": {
                "page": 1,
                "source_text": text,
                "evidence_class": "EXPLICIT",
            },
        },
    }


def test_supported_multiple_emits_one_suggestion_per_semantic_association():
    payload = {
        "engine_version": "lacey-engine-test",
        "canonical_fields": {
            "plant_quantity": {
                "state": "SUPPORTED_MULTIPLE",
                "values": [
                    {"value": "30.000", "evidence_ids": ["a"]},
                    {"value": "16.000", "evidence_ids": ["b"]},
                ],
                "supporting_evidence": [
                    _evidence(
                        value="30.000",
                        document_id=11,
                        association="taxon:pinus:taeda",
                        text="Pinus taeda quantity 30.000 m3",
                    ),
                    _evidence(
                        value="16.000",
                        document_id=11,
                        association="taxon:eucalyptus:grandis",
                        text="Eucalyptus grandis quantity 16.000 m3",
                    ),
                ],
            }
        },
    }

    suggestions = supported_engine2_suggestions(payload)

    assert [(item.value, item.association_key) for item in suggestions] == [
        ("30.000", "taxon:pinus:taeda"),
        ("16.000", "taxon:eucalyptus:grandis"),
    ]
    assert all(item.requires_review is False for item in suggestions)


def test_low_authority_harvest_country_is_review_only_per_taxon():
    payload = {
        "engine_version": "lacey-engine-test",
        "canonical_fields": {
            "country_of_harvest": {
                "state": "REVIEW_REQUIRED",
                "values": [],
                "supporting_evidence": [
                    _evidence(
                        value="Brasil",
                        document_id=12,
                        association="taxon:pinus:taeda",
                        text="Pinus taeda - pais de colheita Brasil",
                        authority=5.0,
                    ),
                    _evidence(
                        value="Brasil",
                        document_id=12,
                        association="taxon:eucalyptus:grandis",
                        text="Eucalyptus grandis - pais de colheita Brasil",
                        authority=5.0,
                    ),
                ],
            }
        },
    }

    suggestions = supported_engine2_suggestions(payload)

    assert [item.association_key for item in suggestions] == [
        "taxon:pinus:taeda",
        "taxon:eucalyptus:grandis",
    ]
    assert all(item.value == "Brasil" for item in suggestions)
    assert all(item.requires_review is True for item in suggestions)
