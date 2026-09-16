from __future__ import annotations

from litoral_trace.lacey_engine.ai_providers import AIProviderConfig
from litoral_trace.us_lacey import specialized_shadow
from litoral_trace.us_lacey.specialized_inference_cache import specialized_computation_fingerprint


def _document(*, sha256: str, role_hint: str, filename: str, operation_document_id: int, assurance_document_id: int):
    return {
        "sha256": sha256,
        "role_hint": role_hint,
        "filename": filename,
        "operation_document_id": operation_document_id,
        "assurance_document_id": assurance_document_id,
    }


def _fingerprint(documents, *, model: str = "gemini-3.5-flash-lite"):
    return specialized_computation_fingerprint(
        organization_id=7,
        documents=documents,
        engine_version="lacey-engine-2.3.0",
        provider="gemini",
        model=model,
        max_pages=8,
        judge_mode="enforce",
        projection_mode="enforce",
        specialized_schema_version="lacey_multi_agent_shadow_v2",
        field_judge_version="lacey_field_judge_v1",
        projection_version="lacey_specialized_projection_v1",
    )


def test_cache_identity_ignores_operation_specific_document_ids():
    first = [
        _document(sha256="a" * 64, role_hint="ENTRY_WORKSHEET", filename="entry.pdf", operation_document_id=11, assurance_document_id=101),
        _document(sha256="b" * 64, role_hint="SUPPLIER_ORIGIN", filename="origin.pdf", operation_document_id=12, assurance_document_id=102),
    ]
    second = [
        _document(sha256="a" * 64, role_hint="ENTRY_WORKSHEET", filename="entry.pdf", operation_document_id=211, assurance_document_id=301),
        _document(sha256="b" * 64, role_hint="SUPPLIER_ORIGIN", filename="origin.pdf", operation_document_id=212, assurance_document_id=302),
    ]

    assert _fingerprint(first) == _fingerprint(second)


def test_cache_identity_invalidates_when_model_or_content_changes():
    documents = [
        _document(sha256="a" * 64, role_hint="ENTRY_WORKSHEET", filename="entry.pdf", operation_document_id=11, assurance_document_id=101)
    ]
    changed_content = [dict(documents[0], sha256="c" * 64)]

    baseline = _fingerprint(documents)
    assert baseline != _fingerprint(changed_content)
    assert baseline != _fingerprint(documents, model="gemini-next")


def test_cache_hit_returns_before_constructing_or_calling_provider(monkeypatch):
    sentinel = object()
    monkeypatch.setattr(
        specialized_shadow,
        "_try_cache",
        lambda **_kwargs: ("cache-fingerprint", sentinel),
    )

    def provider_must_not_run(_config):
        raise AssertionError("Gemini provider must not be constructed on a cache hit")

    monkeypatch.setattr(specialized_shadow, "GeminiSpecialistProvider", provider_must_not_run)
    config = AIProviderConfig(
        mode="SHADOW",
        provider="gemini",
        model="gemini-3.5-flash-lite",
        base_url="https://example.invalid",
        api_key="test-key",
        timeout_seconds=30.0,
        max_pages=8,
        allow_external=True,
    )

    result = specialized_shadow.run_specialized_shadow_operation(
        documents=(object(),),
        config=config,
    )

    assert result is sentinel
