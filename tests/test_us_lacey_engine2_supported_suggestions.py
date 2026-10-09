from __future__ import annotations

from litoral_trace.us_lacey.engine2_suggestions import supported_engine2_suggestions


def _evidence(value: str, *, document_id: str = "17", evidence_class: str = "EXPLICIT", authority: float = 25.0):
    return {
        "document_id": document_id,
        "normalized_value": value,
        "candidate_score": 91.0,
        "source_authority": authority,
        "candidate": {
            "score": 91.0,
            "raw": {
                "normalized_value": value,
                "evidence_class": evidence_class,
            },
            "provenance": {
                "page": 2,
                "source_text": f"Container Number: {value}",
                "evidence_class": evidence_class,
            },
        },
    }


def test_supported_single_value_becomes_non_authoritative_suggestion():
    payload = {
        "engine_version": "lacey-engine-2.0.0",
        "canonical_fields": {
            "container_number": {
                "state": "SUPPORTED",
                "values": [{"value": "MSKU9228574", "evidence_ids": ["e1"]}],
                "supporting_evidence": [_evidence("MSKU9228574")],
            },
            "description": {
                "state": "SUPPORTED_MULTIPLE",
                "values": [{"value": "PINE PARTS", "evidence_ids": ["e2", "e3"]}],
                "supporting_evidence": [
                    _evidence("PINE PARTS", document_id="18", authority=15.0),
                    _evidence("PINE PARTS", document_id="19", authority=20.0),
                ],
            },
        },
    }
    suggestions = {item.field_name: item for item in supported_engine2_suggestions(payload)}
    assert suggestions["container_number"].value == "MSKU9228574"
    assert suggestions["container_number"].operation_document_id == 17
    assert suggestions["merchandise_description"].value == "PINE PARTS"
    assert suggestions["merchandise_description"].operation_document_id == 19
    assert suggestions["merchandise_description"].confidence > suggestions["container_number"].confidence


def test_conflicts_multiple_values_and_inferred_evidence_are_not_promoted():
    payload = {
        "engine_version": "lacey-engine-2.0.0",
        "canonical_fields": {
            "country_of_harvest": {
                "state": "CONFLICT",
                "values": [
                    {"value": "Chile", "evidence_ids": ["a"]},
                    {"value": "Peru", "evidence_ids": ["b"]},
                ],
                "supporting_evidence": [_evidence("Chile"), _evidence("Peru", document_id="18")],
            },
            "container_number": {
                "state": "SUPPORTED_MULTIPLE",
                "values": [
                    {"value": "MSKU9228574", "evidence_ids": ["a"]},
                    {"value": "MSKU1111111", "evidence_ids": ["b"]},
                ],
                "supporting_evidence": [_evidence("MSKU9228574"), _evidence("MSKU1111111", document_id="18")],
            },
            "species": {
                "state": "SUPPORTED",
                "values": [{"value": "radiata", "evidence_ids": ["c"]}],
                "supporting_evidence": [_evidence("radiata", evidence_class="INFERRED")],
            },
        },
    }
    assert supported_engine2_suggestions(payload) == ()



def test_canonical_stale_line_review_condition_does_not_fail_worker(monkeypatch) -> None:
    from types import SimpleNamespace

    from litoral_trace.us_lacey import engine2_suggestions as module

    class _Session:
        def __init__(self):
            self.rollbacks = 0
            self.commits = 0
            self.closed = 0

        def rollback(self):
            self.rollbacks += 1

        def commit(self):
            self.commits += 1

        def close(self):
            self.closed += 1

    session = _Session()

    monkeypatch.setattr(module, "engine2_mode", lambda: module.ENGINE2_SHADOW)
    monkeypatch.setattr(module, "get_us_lacey_db_session", lambda: session)
    monkeypatch.setattr(module, "set_tenant_db_context", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        module,
        "prepare_canonical_publication",
        lambda *_args, **_kwargs: SimpleNamespace(),
    )

    def _raise_review(*_args, **_kwargs):
        raise RuntimeError("CANONICAL_STALE_LINE_REQUIRES_REVIEW")

    monkeypatch.setattr(module, "publish_canonical_shipment_truth", _raise_review)

    result = module.project_engine2_supported_suggestions(
        organization_id=7,
        operation_id=11,
    )

    assert result == 0
    assert session.rollbacks == 1
    assert session.commits == 0
    assert session.closed == 1


def test_unexpected_canonical_runtime_error_still_fails_closed(monkeypatch) -> None:
    import pytest

    from litoral_trace.us_lacey import engine2_suggestions as module

    class _Session:
        def rollback(self):
            pass

        def close(self):
            pass

    monkeypatch.setattr(module, "engine2_mode", lambda: module.ENGINE2_SHADOW)
    monkeypatch.setattr(module, "get_us_lacey_db_session", lambda: _Session())
    monkeypatch.setattr(module, "set_tenant_db_context", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        module,
        "prepare_canonical_publication",
        lambda *_args, **_kwargs: object(),
    )

    def _raise_unexpected(*_args, **_kwargs):
        raise RuntimeError("CANONICAL_PPQ_FIELD_SLOT_MISSING")

    monkeypatch.setattr(module, "publish_canonical_shipment_truth", _raise_unexpected)

    with pytest.raises(RuntimeError, match="CANONICAL_PPQ_FIELD_SLOT_MISSING"):
        module.project_engine2_supported_suggestions(
            organization_id=7,
            operation_id=11,
        )
