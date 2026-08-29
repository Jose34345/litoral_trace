from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
ASSET = ROOT / "src/litoral_trace/static/src/js/assurance-progress-retry.js"
BASE_TEMPLATE = ROOT / "src/litoral_trace/templates/base.html"


def test_assurance_progress_retry_is_loaded_from_runtime_source_path():
    base = BASE_TEMPLATE.read_text(encoding="utf-8")
    assert "/src/js/assurance-progress-retry.js" in base


def test_assurance_progress_retry_never_exhausts_transient_errors():
    source = ASSET.read_text(encoding="utf-8")

    assert "MAX_ATTEMPTS" not in source
    assert "while (true)" in source
    assert "TRANSIENT_STATUS" in source
    for status in (408, 425, 429, 500, 502, 503, 504):
        assert str(status) in source


def test_assurance_progress_retry_preserves_abort_and_caps_delay():
    source = ASSET.read_text(encoding="utf-8")

    assert "signal?.aborted" in source
    assert '"AbortError"' in source
    assert "return MAX_DELAY_MS" in source
    assert "Math.pow" not in source
    assert "**" not in source


def test_retry_wrapper_is_scoped_to_assurance_progress_gets():
    source = ASSET.read_text(encoding="utf-8")

    assert 'requestMethod(input, init) !== "GET"' in source
    assert "ASSURANCE_PROGRESS_PATH.test(url.pathname)" in source
    assert "url.origin === window.location.origin" in source
