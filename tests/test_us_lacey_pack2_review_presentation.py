"""Pack 2 human-review presentation: one primary action, truthful evidence phase."""
from pathlib import Path
from types import SimpleNamespace

from jinja2 import Environment

from litoral_trace.us_lacey.document_evidence_class import classify_document
from litoral_trace.us_lacey.quick_review_eligibility import quick_accept_eligible

WORKSPACE = Path(__file__).resolve().parents[1] / "src/litoral_trace/templates/us_lacey/fragments/operation_workspace.html"


def _field(*, name="plant_quantity", filename="02_Packing_List_Shipment2.pdf",
           source_id=20, value="288"):
    return SimpleNamespace(
        id=1189, field_name=name, label="Quantity of Plant Material",
        scope="LINE", line_reference="1", status="REVIEW",
        proposed_value=value, effective_value=value,
        source_filename=filename, source_page=1, source_locator="canonical:1:test",
        source_assurance_document_id=source_id, provenance="current_shipment",
        candidates=(),
    )


def _field_html(field, paired_review_ids=()):
    template = WORKSPACE.read_text(encoding="utf8")
    start = template.index("{% macro action_field(")
    end = template.index("{% endmacro %}", start) + len("{% endmacro %}")
    render = template[start:end] + "{{ action_field(field, 1, paired_ids) }}"
    return Environment(autoescape=True).from_string(render).render(
        field=field,
        paired_ids=paired_review_ids,
        detail=SimpleNamespace(public_id="610e78df-3aa3-46a0-9975-a9ebdb2cb17a"),
        review_csrf={field.id: "test-csrf"},
        field_input_values={}, field_errors={},
        missing_guidance=lambda _: "Source required",
        action_status=lambda _: "Needs confirmation",
        quick_accept_eligible=quick_accept_eligible,
        source_class_for_filename=classify_document,
        button=lambda text, **_: f"<button>{text}</button>",
    )


def test_pack2_paired_fields_have_only_one_shared_original_inspector_and_review():
    quantity = _field()
    html = _field_html(quantity, paired_review_ids=(1189, 1190))
    assert "Confirm together above" in html
    assert "Correct this value" in html
    assert "Enter verified value" not in html
    assert "Request evidence" not in html
    assert "Confirm value" not in html
    assert 'data-paired-source' in html
    assert "data-source-inspect" not in html
    assert "data-override-open" in html  # Human correction remains available.


def test_pack2_derived_worksheet_is_not_a_confirmation_action():
    worksheet = _field(
        name="entered_value", filename="05_Entry_Preparation_Worksheet_Shipment2.pdf",
        source_id=23, value="11250",
    )
    html = _field_html(worksheet)
    assert "Original required" in html
    assert "Request original evidence" in html
    assert "Confirm value" not in html
    assert "Enter verified value" not in html


def test_entry_number_is_visibly_pending_without_waiving_filing_requirement():
    field = _field(
        name="filing_entry_reference", filename="",
        source_id=None, value=None,
    )
    field.candidates = ()
    field.source_page = None
    field.source_locator = None
    html = _field_html(field)
    assert "Pending for filing" in html
    assert "Keep pending" in html
    assert "Enter verified number" in html
    assert "Request evidence" not in html
    assert "It blocks filing-ready output" in html


def test_work_queue_counts_are_explicitly_fields_not_automatic_approvals():
    source = WORKSPACE.read_text(encoding="utf8")
    assert "data-review-phase-counts" in source
    assert "non_entry_attention_count" in source
    assert "data-review-group=\"plant-mass\"" in source
    assert "Counts represent individual fields, not reviewer clicks." in source
    assert 'data-action-tab-count data-action-required-count>{{ combined_action_count }}' in source
    Environment().parse(source)

def test_pending_entry_has_one_control_for_its_dialog():
    field = _field(name="filing_entry_reference", filename="", source_id=None, value=None)
    html = _field_html(field)
    assert html.count('aria-controls="review-override-1189"') == 1
    assert html.count("Enter verified number") == 1


def test_candidate_link_does_not_show_filename_from_another_document():
    field = _field(name="bill_of_lading", filename="02_Botanical_Supplier_Declaration.pdf")
    field.candidates = (SimpleNamespace(
        id=23, source_document_id=31, source_page=2, source_locator="p2:bol",
        normalized_value="TEST-BOL", original_value="TEST-BOL",
    ),)
    html = _field_html(field)
    assert "/source/31" in html
    assert 'data-source-page="2"' in html
    assert "02_Botanical_Supplier_Declaration.pdf" not in html
    assert html.count("data-source-inspect") == 1
    assert "Confirm value" not in html
