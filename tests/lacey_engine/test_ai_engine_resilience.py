from __future__ import annotations

import json
from types import SimpleNamespace
from urllib.error import HTTPError, URLError

import pytest

from litoral_trace.lacey_engine import ai_providers
from litoral_trace.lacey_engine.ai_providers import AIProviderConfig
from litoral_trace.lacey_engine.ai_routing import AITask
from litoral_trace.lacey_engine.ai_shadow import AIShadowError, candidate_from_payload
from litoral_trace.us_lacey import ai_review
from litoral_trace.us_lacey.ai_review import ReviewCandidate


def _candidate_payload(bbox):
    return {
        "field_key": "description",
        "value": "Wooden cutting boards",
        "evidence_class": "EXPLICIT",
        "page": 1,
        "source_text": "Wooden cutting boards",
        "confidence": 0.9,
        "bbox": bbox,
        "reason": None,
    }


def test_inverted_bbox_is_normalized_instead_of_rejecting_candidate():
    candidate = candidate_from_payload(
        payload=_candidate_payload([90, 80, 10, 20]),
        provider="gemini",
        model="gemini-test",
    )
    assert candidate.bbox is not None
    assert (candidate.bbox.x0, candidate.bbox.top, candidate.bbox.x1, candidate.bbox.bottom) == (10.0, 20.0, 90.0, 80.0)


@pytest.mark.parametrize("bbox", [[1, 2, 3], [1, None, 3, 4], [1, 2, float("nan"), 4], "not-a-box"])
def test_invalid_bbox_is_dropped_but_text_candidate_survives(bbox):
    candidate = candidate_from_payload(
        payload=_candidate_payload(bbox),
        provider="gemini",
        model="gemini-test",
    )
    assert candidate.value == "Wooden cutting boards"
    assert candidate.bbox is None


class _Response:
    def __init__(self, payload: dict[str, object]):
        self._raw = json.dumps(payload).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def read(self):
        return self._raw


def test_post_json_retries_http_500_and_succeeds_on_third_attempt(monkeypatch):
    calls = []

    def fake_urlopen(request, timeout):
        calls.append((request, timeout))
        if len(calls) < 3:
            raise HTTPError(request.full_url, 500, "Internal Server Error", None, None)
        return _Response({"ok": True})

    sleeps = []
    monkeypatch.setattr(ai_providers, "urlopen", fake_urlopen)
    monkeypatch.setattr(ai_providers.time, "sleep", lambda seconds: sleeps.append(seconds))

    result = ai_providers._post_json(url="https://example.test/v1", payload={"x": 1}, timeout=3)

    assert result == {"ok": True}
    assert len(calls) == 3
    assert sleeps == [2.0, 2.0]


def test_post_json_retries_url_error_then_recovers(monkeypatch):
    attempts = 0

    def fake_urlopen(_request, timeout):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise URLError("temporary timeout")
        return _Response({"ok": True})

    monkeypatch.setattr(ai_providers, "urlopen", fake_urlopen)
    monkeypatch.setattr(ai_providers.time, "sleep", lambda _seconds: None)

    assert ai_providers._post_json(url="https://example.test/v1", payload={}, timeout=3) == {"ok": True}
    assert attempts == 2


def test_post_json_raises_only_after_three_transient_failures(monkeypatch):
    attempts = 0

    def fake_urlopen(request, timeout):
        nonlocal attempts
        attempts += 1
        raise HTTPError(request.full_url, 503, "Unavailable", None, None)

    monkeypatch.setattr(ai_providers, "urlopen", fake_urlopen)
    monkeypatch.setattr(ai_providers.time, "sleep", lambda _seconds: None)

    with pytest.raises(AIShadowError, match="after 3 attempts"):
        ai_providers._post_json(url="https://example.test/v1", payload={}, timeout=3)
    assert attempts == 3


def test_ai_review_provider_failure_degrades_to_no_recommendation(monkeypatch):
    config = AIProviderConfig(
        "SHADOW", "gemini", "gemini-test", "https://example.test/interactions", "test-key", 3, 8, True
    )
    issue = SimpleNamespace(
        id=77,
        rule_code="US_LACEY_FIELD_CONFLICT",
        severity="BLOCKING",
        left_value="Chile",
        right_value="Peru",
    )
    candidates = (
        ReviewCandidate(1, "Chile", 1, "page:1", 0.9),
        ReviewCandidate(2, "Peru", 1, "page:1", 0.9),
    )
    monkeypatch.setattr(
        ai_review,
        "_call_gemini_decision",
        lambda **_kwargs: (_ for _ in ()).throw(AIShadowError("upstream unavailable")),
    )

    result = ai_review._safe_call_review_decision(
        provider_config=config,
        model="gemini-test",
        field_name="country_of_harvest",
        issue=issue,
        candidates=candidates,
        task=AITask.ADJUDICATE,
    )

    assert result is None
