from pathlib import Path
from types import SimpleNamespace

from litoral_trace.us_lacey.review import _AUTO_CONFIRMABLE_FIELD_STATUSES
from litoral_trace.web.us_lacey_operational_views import _review_field_groups


ROOT = Path(__file__).resolve().parents[1]
DETAIL_TEMPLATE = ROOT / "src/litoral_trace/templates/us_lacey/operation_detail.html"
WORKSPACE_TEMPLATE = ROOT / "src/litoral_trace/templates/us_lacey/fragments/operation_workspace.html"


def _field(*, status: str, effective_value=None, extractor=None):
    return SimpleNamespace(
        status=status,
        effective_value=effective_value,
        extractor=extractor,
    )


def test_empty_optional_placeholder_is_not_counted_as_confirmed():
    detail = SimpleNamespace(
        fields=(
            _field(status="MATCHED", effective_value=None),
            _field(status="MATCHED", effective_value="MSKU9228574"),
            _field(status="MISSING", effective_value=None),
        )
    )

    attention, supported, settled = _review_field_groups(detail)

    assert len(attention) == 1
    assert supported == ()
    assert len(settled) == 1
    assert settled[0].effective_value == "MSKU9228574"



def test_review_required_status_is_customer_action_required():
    detail = SimpleNamespace(
        fields=(
            _field(status="REVIEW", effective_value="Brasil"),
            _field(status="REVIEW_REQUIRED", effective_value=None),
            _field(status="MATCHED", effective_value="Gulf Wood Supply Inc."),
        )
    )

    attention, supported, settled = _review_field_groups(detail)

    assert [field.status for field in attention] == ["REVIEW", "REVIEW_REQUIRED"]
    assert supported == ()
    assert len(settled) == 1

def test_document_dossier_does_not_claim_final_preparation_readiness():
    source = WORKSPACE_TEMPLATE.read_text(encoding="utf-8")
    assert "Advanced evidence details" in source
    assert "Technical evidence remains available for audit and troubleshooting" in source
    assert "Preparation readiness:" not in source



def test_review_workspace_is_exception_first():
    source = WORKSPACE_TEMPLATE.read_text(encoding="utf-8")

    assert "You only need to handle missing facts, review-required evidence, or genuine conflicts." in source
    assert "Only missing information, review-required evidence, or genuinely conflicting evidence appears here." in source
    assert "Auto-Resolved Data" in source


def test_auto_resolved_contract_includes_canonical_supported_statuses():
    detail = SimpleNamespace(
        fields=(
            _field(status="SUPPORTED", effective_value="USD"),
            _field(status="FOUND", effective_value="VNMKHOM123HCM"),
            _field(status="SUPPORTED MULTIPLE", effective_value="MAEU2609240001"),
            _field(status="SUPPORTED_MULTIPLE", effective_value="MSCU1234566"),
        )
    )

    attention, supported, settled = _review_field_groups(detail)

    assert attention == ()
    assert settled == ()
    assert len(supported) == 4


def test_bulk_confirmation_accepts_all_auto_resolved_status_variants():
    assert set(_AUTO_CONFIRMABLE_FIELD_STATUSES) == {
        "SUPPORTED",
        "FOUND",
        "SUPPORTED MULTIPLE",
        "SUPPORTED_MULTIPLE",
    }
