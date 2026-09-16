from pathlib import Path
from types import SimpleNamespace

from litoral_trace.web.us_lacey_operational_views import _review_field_sets


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

    exceptions, settled = _review_field_sets(detail)

    assert len(exceptions) == 1
    assert len(settled) == 1
    assert settled[0].effective_value == "MSKU9228574"


def test_document_dossier_does_not_claim_final_preparation_readiness():
    for path in (DETAIL_TEMPLATE, WORKSPACE_TEMPLATE):
        source = path.read_text(encoding="utf-8")
        assert "Document evidence status:" in source
        assert "Final preparation readiness is determined by the human review below." in source
        assert "Preparation readiness:" not in source


def test_review_workspace_distinguishes_operator_metadata_from_document_evidence():
    source = WORKSPACE_TEMPLATE.read_text(encoding="utf-8")

    assert 'field.extractor == "operator-entered-metadata"' in source
    assert "From operation details:" in source
    assert "Operation details" in source
    assert "Entered by user" in source
