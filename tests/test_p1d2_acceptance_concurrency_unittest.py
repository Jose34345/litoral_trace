from __future__ import annotations

from types import SimpleNamespace
from uuid import uuid4

from litoral_trace.config.eudr_acceptance import EudrAcceptanceSettings
from litoral_trace.services.eudr_acceptance_submission import (
    EudrAcceptanceSubmissionService,
)


class _RecordingSession:
    def __init__(self) -> None:
        self.statements = []
        self._rows = [
            SimpleNamespace(candidate_id=91),
            SimpleNamespace(id=91),
        ]

    def scalar(self, statement):
        self.statements.append(statement)
        return self._rows.pop(0)


def test_attempt_claim_uses_for_update_before_submit_state_transition() -> None:
    session = _RecordingSession()
    service = EudrAcceptanceSubmissionService(
        session=session,  # type: ignore[arg-type]
        organization_id=7,
        settings=EudrAcceptanceSettings(),
    )

    service._attempt_with_candidate(uuid4(), for_update=True)

    assert len(session.statements) == 2
    assert session.statements[0]._for_update_arg is not None
    assert session.statements[1]._for_update_arg is None


def test_submit_path_claims_attempt_with_for_update() -> None:
    # This regression couples the behavioral boundary to the public submit path:
    # a future refactor must not silently fall back to an unlocked PREPARED read.
    import inspect

    source = inspect.getsource(EudrAcceptanceSubmissionService.submit)
    assert "for_update=True" in source
    assert source.index("for_update=True") < source.index('row.state = "SENT"')
