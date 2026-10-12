"""P1 regression: unchanged review hints are NEVER imported as authority."""
from types import SimpleNamespace
import pytest

from litoral_trace.us_lacey.assurance_v2_review_delta import (
    aggregate_human_effort, classify_prior_review, _safe_int,
)


def doc(doc_id="23", *, operation_doc=11, version=1, sha="a" * 64):
    return {
        "id": doc_id, "operation_document_id": operation_doc,
        "version": str(version), "sha256": sha, "raw_available": True,
    }


def ref(doc_id="23", *, operation_doc=11, version=1, sha="a" * 64):
    return {
        "document_id": doc_id, "operation_document_id": operation_doc,
        "version": version, "sha256": sha,
    }


def compare(**override):
    state = {
        "prior_value": "288.0",
        "prior_source_fingerprint": "f" * 64,
        "current_source_fingerprint": "f" * 64,
        "prior_sources": (ref(),),
        "current_documents": (doc(),),
        "current_choice": {"document_id": 23, "value": "288.0"},
        "current_authority": "REVIEW_SOURCE",
    }
    state.update(override)
    return classify_prior_review(**state)


def test_exact_same_bytes_and_field_needs_explicit_reconfirmation():
    assert compare() == "UNCHANGED_RECONFIRM_REQUIRED"


@pytest.mark.parametrize("override,expected", [
    ({"prior_sources": ()}, "SOURCE_UNVERIFIED"),
    ({"current_documents": (doc(version=2),)}, "SOURCE_CHANGED"),
    ({"current_documents": (doc(sha="b"*64),)}, "SOURCE_CHANGED"),
    ({"current_documents": ({**doc(), "raw_available": False},)}, "SOURCE_CHANGED"),
    ({"prior_source_fingerprint": "x"*64}, "SOURCE_SET_CHANGED"),
    ({"current_choice": None}, "REVIEW_NEEDED"),
    ({"current_choice": {"document_id": 23, "value": "999"}}, "REVIEW_NEEDED"),
    ({"current_choice": {"document_id": 22, "value": "288.0"}}, "SOURCE_CHANGED"),
])
def test_delta_never_reuses_disputed_changed_or_expired_evidence(override, expected):
    assert compare(**override) == expected


def test_current_verified_authority_only_when_all_sources_still_current():
    assert compare(current_authority="HUMAN_CONFIRMED", current_value="288.0") == "CURRENTLY_AUTHORIZED"
    assert compare(current_authority="HUMAN_CONFIRMED", current_value="999") == "VALUE_REVIEWED_DIFFERENTLY"
    assert compare(
        current_authority="HUMAN_CONFIRMED", current_value="288.0",
        current_documents=(doc(version=2),)
    ) == "SOURCE_CHANGED"


def test_effort_uses_only_completed_user_group_audit_events():
    def event(action, count, seconds=None, actor="USER"):
        return SimpleNamespace(
            actor_type=actor, event_type="HUMAN_REVIEW",
            details={
                "action": action,
                "field_count" if action == "BOTANICAL_GROUP_REVIEW" else "reviewed_count": count,
                **({"client_review_seconds": seconds} if seconds is not None else {}),
            },
        )
    metrics = aggregate_human_effort([
        event("BATCH_REVIEW_ACCEPT", 8, seconds=70),
        event("BOTANICAL_GROUP_REVIEW", 3, seconds=35),
        event("BATCH_REVIEW_ACCEPT", 12, seconds=180, actor="SYSTEM"),
        event("IDENTITY_CONFIRM", 1),
    ])
    assert metrics["grouped_review_actions"] == 2
    assert metrics["fields_authorized_by_group"] == 11
    assert metrics["estimated_clicks_avoided"] == 9
    assert metrics["observed_review_seconds"] == 105
    assert metrics["time_basis"] == "CLIENT_REPORTED_ACTIVE_INTERVAL"


def test_no_review_time_is_never_falsely_reported_as_zero_savings():
    metrics = aggregate_human_effort([])
    assert metrics["time_basis"] == "NOT_MEASURED"
    assert metrics["estimated_clicks_avoided"] == 0


@pytest.mark.parametrize("value", [True, False, None, -1, "abc", 10000000000])
def test_untrusted_counts_are_bounded(value):
    assert _safe_int(value) == 0
