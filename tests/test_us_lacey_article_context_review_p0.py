"""Pack 2: original description may be reviewed, never auto-authorized."""
from pathlib import Path
from types import SimpleNamespace

import pytest
from jinja2 import Environment

from litoral_trace.us_lacey.article_context_review import (
    contexts_for_detail, validate_article_original_context,
)


def _field(id, name, value, document_id, filename, scope, line, status="REVIEW"):
    return SimpleNamespace(
        id=id, field_name=name, effective_value=value, proposed_value=value,
        source_assurance_document_id=document_id, source_filename=filename,
        source_page=1, source_locator=f"canonical:{id}:source",
        scope=scope, line_reference=line, status=status,
    )


def _detail():
    return SimpleNamespace(fields=[
        _field(1,"article_component","solid bamboo coasters",23,
               "05_Entry_Preparation_Worksheet_Shipment2.pdf","PLANT_LINE","1"),
        _field(2,"merchandise_description",
               "retail boxed 4 piece solid bamboo coaster sets",20,
               "02_Packing_List_Shipment2.pdf","SHIPMENT","__shipment__"),
        _field(3,"plant_quantity","288",20,
               "02_Packing_List_Shipment2.pdf","PLANT_LINE","1"),
        _field(4,"metric_unit","kg",20,
               "02_Packing_List_Shipment2.pdf","PLANT_LINE","1"),
    ])


def test_pack2_proposes_original_context_without_approval():
    detail = _detail()
    context = contexts_for_detail(detail)[1]
    assert context.description_field_id == 2
    assert context.document_id == 20 and context.page == 1
    assert "retail boxed 4 piece" in context.description
    assert detail.fields[0].status == "REVIEW"


@pytest.mark.parametrize("mutation", [
    "different_mass_source", "different_unit_source", "second_plant_line",
    "worksheet_description", "no_locator", "no_description", "different_line",
])
def test_pack2_refuses_ambiguous_or_derived_context(mutation):
    detail = _detail()
    rows = detail.fields
    if mutation == "different_mass_source":
        rows[2].source_assurance_document_id = 31
    elif mutation == "different_unit_source":
        rows[3].source_assurance_document_id = 31
    elif mutation == "second_plant_line":
        rows.append(_field(5,"article_component","table",23,"05_Entry_Preparation_Worksheet_Shipment2.pdf","PLANT_LINE","2"))
    elif mutation == "worksheet_description":
        rows[1].source_filename = "05_Entry_Preparation_Worksheet_Shipment2.pdf"
    elif mutation == "no_locator":
        rows[1].source_locator = None
    elif mutation == "no_description":
        rows[1].effective_value = None
    elif mutation == "different_line":
        rows[2].line_reference = "2"
    assert not contexts_for_detail(detail)


def test_template_forces_human_reason_and_retains_request_evidence_fallback():
    root = Path(__file__).resolve().parents[1]
    html = (root / "src/litoral_trace/templates/us_lacey/fragments/operation_workspace.html").read_text(encoding="utf8")
    view = (root / "src/litoral_trace/web/us_lacey_operational_views.py").read_text(encoding="utf8")
    web = (root / "src/litoral_trace/web/us_lacey_pilot_app.py").read_text(encoding="utf8")
    review = (root / "src/litoral_trace/us_lacey/review.py").read_text(encoding="utf8")
    Environment().parse(html)
    assert "Inspect packing list" in html
    assert "Review with packing list" in html
    assert "Request evidence" in html
    assert "context_description_field_id" in html and 'required minlength="15"' in html
    assert view.count("article_original_contexts=contexts_for_detail(detail)") == 2
    assert web.count("context_description_field_id=context_description_field_id") == 2
    assert 'field.extractor = "human-reviewed-original-context"' in review
    assert "contextual_original_document_id" in review
    assert "contextual_review_reason" in review
