"""P0 contract: documentary completion is not filing-readiness."""
from types import SimpleNamespace
from pathlib import Path

from litoral_trace.web.us_lacey_operational_views import _readiness_summary


ROOT=Path(__file__).resolve().parents[1]
TEMPLATES=ROOT / "src" / "litoral_trace" / "templates" / "us_lacey"
WEB=(ROOT / "src" / "litoral_trace" / "web" / "us_lacey_pilot_app.py").read_text(encoding="utf-8")
REVIEW=(ROOT / "src" / "litoral_trace" / "us_lacey" / "review.py").read_text(encoding="utf-8")


def _detail(*, status="REVIEW_REQUIRED", review_result=None):
    return SimpleNamespace(status=status,review_result=review_result,fields=())


def _pending(name="filing_entry_reference"):
    return SimpleNamespace(field_name=name,status="REVIEW",effective_value=None)


def _processing():
    return SimpleNamespace(terminal=True,failed=False)


def test_sole_unissued_cbp_entry_allows_document_review_milestone_not_lawgs_xml():
    readiness=_readiness_summary(
        _detail(),attention_fields=(_pending(),),processing=_processing(),regulatory_action_items=(),
    )
    assert readiness["pre_entry_eligible"] is True
    assert readiness["awaiting_entry_review"] is False
    assert readiness["package_ready"] is False
    assert readiness["exception_count"] == 1
    assert readiness["overall"] == "NOT READY"


def test_audited_pre_entry_milestone_still_blocks_filing_and_does_not_forge_entry():
    readiness=_readiness_summary(
        _detail(review_result="DOCUMENT_REVIEW_COMPLETE_AWAITING_ENTRY"),
        attention_fields=(_pending(),),processing=_processing(),regulatory_action_items=(),
    )
    assert readiness["pre_entry_eligible"] is True
    assert readiness["awaiting_entry_review"] is True
    assert not readiness["package_ready"]
    assert "ENTRY PENDING" in readiness["overall"]
    export=(TEMPLATES / "fragments" / "export_declaration_package.html").read_text(encoding="utf-8")
    assert "data-pre-entry-review-form" in export
    assert "Continue to next shipment" in export
    assert "/review/pre-entry" in export
    assert "No placeholder entry number or LAWGS XML was created" in export
    workspace=(TEMPLATES / "fragments" / "operation_workspace.html").read_text(encoding="utf-8")
    assert '[data-pre-entry-review-confirm]' in workspace
    assert 'activateTab(root, "resolved")' in workspace
    assert "if export_ready" in export


def test_other_missing_or_regulatory_blockers_never_offer_document_completion():
    for fields,checks in [
        ((_pending("species"),),()),
        ((_pending(),_pending("species")),()),
        ((_pending(),),({"rule_id":"HTS_APPLICABILITY"},)),
    ]:
        readiness=_readiness_summary(
            _detail(),attention_fields=fields,processing=_processing(),regulatory_action_items=checks,
        )
        assert not readiness["pre_entry_eligible"]
        assert not readiness["package_ready"]


def test_pre_entry_endpoint_is_authenticated_csrf_checked_and_audited():
    assert '@app.post("/operations/{operation_public_id}/review/pre-entry"' in WEB
    assert "verify_us_lacey_csrf(" in WEB
    assert "finish_document_review_awaiting_entry(" in WEB
    assert 'purpose=f"complete:{operation_public_id}"' in WEB
    assert "PRE_ENTRY_REVIEW_RESULT" in REVIEW
    assert 'event_key="review:awaiting-entry"' in REVIEW
    assert 'operation.status = "REVIEW_REQUIRED"' in REVIEW
    assert "regulatory_blockers" in REVIEW
