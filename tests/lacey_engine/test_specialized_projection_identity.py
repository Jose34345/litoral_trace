from __future__ import annotations

from litoral_trace.lacey_engine.multi_agent.field_judge import FieldJudgeMode
from litoral_trace.us_lacey.specialized_shadow import specialized_engine_version


def test_specialized_engine_identity_changes_with_projection_mode() -> None:
    common = {
        "provider": "gemini",
        "model": "fixture-specialist-model",
        "source_set_fingerprint": "source-set-fixture",
        "judge_mode": FieldJudgeMode.OFF,
    }

    off = specialized_engine_version(**common, projection_mode="off")
    shadow = specialized_engine_version(**common, projection_mode="shadow")
    enforce = specialized_engine_version(**common, projection_mode="enforce")

    assert len({off, shadow, enforce}) == 3
