"""A botanical group must be one explicit source-inspected human authorization."""
from uuid import uuid4

import pytest

from litoral_trace.us_lacey.assurance_v2_botanical_batch import (
    approve_botanical_evidence_group,
)

ONE = str(uuid4())
TWO = str(uuid4())


def _claim(memory_id, field_name, line="1"):
    return {"memory_id": memory_id, "field_name": field_name, "line_reference": line}


@pytest.mark.parametrize("selections", [
    (),
    (_claim(ONE, "genus"), _claim(ONE, "genus")),
    (_claim(ONE, "genus"), _claim(TWO, "genus")),
    (_claim(ONE, "genus"), _claim(TWO, "country_of_harvest", "2")),
    (_claim(ONE, "entered_value"),),
    ({"memory_id": ONE, "field_name": "species",
      "line_reference": "1", "selected_value": "guessed"},),
    (_claim("invalid-uuid", "genus"),),
])
def test_invalid_groups_fail_before_opening_database(monkeypatch, selections):
    from litoral_trace.us_lacey import assurance_v2_botanical_batch as module
    monkeypatch.setattr(module, "get_us_lacey_db_session", lambda: pytest.fail("DB opened"))
    with pytest.raises(ValueError):
        approve_botanical_evidence_group(
            organization_id=1,
            operation_public_id="9cb165be-6933-4a5d-86b1-f0a4209240a2",
            authenticated_user_id=2, selected=selections,
            reason="Checked original botanical evidence and country of harvest",
            explicit_confirmation=True,
            expected_source_set_fingerprint="a" * 64,
            expected_case_fingerprint="b" * 64,
        )


def test_no_group_authority_without_explicit_consent(monkeypatch):
    from litoral_trace.us_lacey import assurance_v2_botanical_batch as module
    monkeypatch.setattr(module, "get_us_lacey_db_session", lambda: pytest.fail("DB opened"))
    with pytest.raises(ValueError, match="Explicit"):
        approve_botanical_evidence_group(
            organization_id=1,
            operation_public_id="9cb165be-6933-4a5d-86b1-f0a4209240a2",
            authenticated_user_id=2,
            selected=(_claim(ONE, "genus"),),
            reason="Checked original botanical evidence",
            explicit_confirmation=False,
            expected_source_set_fingerprint="a" * 64,
            expected_case_fingerprint="b" * 64,
        )
