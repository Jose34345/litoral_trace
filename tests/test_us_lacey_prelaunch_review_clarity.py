from pathlib import Path
from types import SimpleNamespace

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


def test_document_dossier_does_not_claim_final_preparation_readiness():
    source = WORKSPACE_TEMPLATE.read_text(encoding="utf-8")
    assert "Advanced evidence details" in source
    assert "Technical evidence remains available for audit and troubleshooting" in source
    assert "Preparation readiness:" not in source



def test_review_workspace_is_exception_first():
    source = WORKSPACE_TEMPLATE.read_text(encoding="utf-8")

    assert "You only need to handle missing facts or genuine conflicts." in source
    assert "Only missing information or genuinely conflicting evidence appears here." in source
    assert "Auto-Resolved Data" in source
