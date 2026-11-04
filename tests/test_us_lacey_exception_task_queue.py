"""Customer task queue contracts using the production render and review route."""
from dataclasses import replace
from types import SimpleNamespace

from litoral_trace.us_lacey._operations_core import OperationFieldView, FieldCandidateView
from litoral_trace.web import us_lacey_operational_views as views
from tests.test_us_lacey_regulatory_assessment_ui import _detail, _request, _engine2


def field(id, name, value=None, *, source=None, filename=None, status="REVIEW", candidates=(), line="__shipment__"):
    return OperationFieldView(
        id=id, line_reference=line, field_name=name, label=name.replace("_", " ").title(),
        ppq_number=2, scope="SHIPMENT" if line == "__shipment__" else "PLANT_LINE",
        proposed_value=value, effective_value=value, status=status, confidence=0.9,
        source_assurance_document_id=source, source_page=1 if source else None,
        source_locator="row:1" if source else None, extractor=None, extractor_version=None,
        reviewed_by_user_id=None, reviewed_at=None, validation_status="REVIEW_REQUIRED",
        validation_error=None, not_required_reason_code=None, candidates=candidates,
        source_filename=filename,
    )


def candidate(id=11, value="11250", source=20):
    return FieldCandidateView(id=id, original_value=value, normalized_value=value,
        source_document_id=source, source_page=1, source_locator="row:1",
        confidence=0.9, validation_status="VALID", validation_error=None, decision="PENDING")


def render(monkeypatch, fields, *, post=False):
    monkeypatch.setattr(views, "_semantic_evidence_for_detail", lambda *_: {})
    monkeypatch.setattr(views, "_product_intelligence_for_detail", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(views, "_regulatory_assessment_for_detail", lambda *_args, **_kwargs: None)
    detail = _detail()
    detail.fields = tuple(fields)
    request = _request()
    if post:
        request.scope["method"] = "POST"
    return views.render_operation_workspace(request=request,
        identity=SimpleNamespace(organization_id=7), detail=detail, engine2_dossier=_engine2(),
        complete_csrf="complete", review_csrf={f.id: f"csrf-{f.id}" for f in fields})


def test_complete_evidence_task_retains_exact_field_candidate_and_csrf(monkeypatch):
    html = render(monkeypatch, [field(101, "entered_value", "11250", source=20,
        filename="Commercial_Invoice.pdf", candidates=(candidate(),))])
    assert 'data-task-category="verify"' in html
    assert "Verify existing evidence" in html
    assert 'data-quick-accept' in html and "Confirm value" in html
    assert 'name="candidate_id" value="11"' in html
    assert 'name="csrf_token" value="csrf-101"' in html
    assert 'data-source-page="1"' in html and '/source/20' in html
    assert 'min-w-[70rem]' not in html


def test_missing_importer_details_are_one_task_without_merging_decisions(monkeypatch):
    html = render(monkeypatch, [field(101, "importer_name"), field(102, "importer_address")])
    assert html.count('data-exception-task=') == 1
    assert "Importer details" in html and "2 fields" in html
    assert 'data-task-category="complete"' in html
    for id in (101, 102):
        assert f'id="review-field-{id}"' in html
        assert f'/review/actions/fields/{id}' in html
        assert f'name="csrf_token" value="csrf-{id}"' in html
    assert 'data-review-action-kind="accept" data-quick-accept>' not in html


def test_worksheet_task_prepares_request_without_claiming_delivery(monkeypatch):
    html = render(monkeypatch, [field(101, "entered_value", "11250", source=20,
        filename="Entry_Preparation_Worksheet.pdf", candidates=(candidate(),))])
    assert 'data-task-category="documents"' in html
    assert "Obtain missing or original documents" in html
    assert "Prepare evidence request" in html
    assert "Original document needed" in html
    assert "does not send supplier email" in html
    assert 'data-review-action-kind="accept" data-quick-accept>' not in html


def test_entry_number_is_future_task_while_package_remains_blocked(monkeypatch):
    html = render(monkeypatch, [field(101, "filing_entry_reference")])
    assert 'data-task-category="filing"' in html
    assert "Required before filing" in html
    assert "Not yet issued by CBP?" in html
    assert 'data-review-keep-pending' in html
    assert 'data-review-action-kind="accept" data-quick-accept>' not in html
    assert 'data-export-generate' not in html


def test_real_conflict_keeps_both_sources_and_never_exposes_candidate_accept(monkeypatch):
    html = render(monkeypatch, [field(101, "entered_value", "11250", source=20,
        filename="Commercial_Invoice.pdf", status="CONFLICT", candidates=(
            candidate(), candidate(22, "11900", 21)))])
    assert "Conflicting information" in html
    assert 'data-source-url=' in html and '/source/20' in html and '/source/21' in html
    assert 'data-review-action-kind="accept"' not in html
    assert 'data-review-action-kind="accept" data-quick-accept>' not in html


def test_htmx_authoritative_rerender_replaces_counts_and_task_identity(monkeypatch):
    before = render(monkeypatch, [field(101, "importer_name"), field(102, "filing_entry_reference")])
    after = render(monkeypatch, [field(101, "importer_name", "Acme", status="MATCHED"),
        field(102, "filing_entry_reference")], post=True)
    assert 'data-action-required-count>2<' in before
    assert 'data-action-required-count>1<' in after
    queue = after.split('id="action-required-fields"', 1)[1].split('id="request-evidence-dialog"', 1)[0]
    assert 'data-review-field-id="101"' not in queue
    assert queue.count('data-review-field-id="102"') == 1
    assert 'hx-swap-oob=' in after



def _case_fields(case):
    """Public synthetic cases for offline browser QA; no customer files or bypasses."""
    complete = [field(101, "entered_value", "11250", source=20,
        filename="Commercial_Invoice.pdf", candidates=(candidate(),))]
    if case in ("A", "F"):
        return complete
    if case == "B":
        return [field(101, "entered_value", "11250", source=20,
            filename="Entry_Preparation_Worksheet.pdf", candidates=(candidate(),))]
    if case == "C":
        return [field(101, "filing_entry_reference")]
    if case == "D":
        return [field(101, "entered_value", "11250", source=20,
            filename="Commercial_Invoice.pdf", status="CONFLICT", candidates=(
                candidate(), candidate(22, "11900", 21)))]
    if case == "E":
        return [replace(complete[0], status="MATCHED"), field(102, "filing_entry_reference")]
    if case == "GROUP":
        return [field(101, "importer_name"), field(102, "importer_address")]
    raise ValueError(case)


def render_case(case, *, post=False):
    """Render the production fragment with only document/DB reads stubbed offline."""
    from pytest import MonkeyPatch
    with MonkeyPatch.context() as patch:
        return render(patch, _case_fields(case), post=post)


def test_groups_never_cross_existing_line_or_document_identity(monkeypatch):
    html = render(monkeypatch, [
        field(101, "genus", line="SKU-A"), field(102, "species", line="SKU-B"),
        field(103, "importer_name", source=20), field(104, "importer_address", source=21),
    ])
    assert html.count('data-exception-task=') == 4
    assert 'Plant line SKU-A' in html and 'Plant line SKU-B' in html


def test_task_queue_uses_real_http_review_handler_and_fresh_server_fragment(monkeypatch):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from starlette.staticfiles import StaticFiles
    from litoral_trace.web import us_lacey_pilot_app as pilot
    from litoral_trace.us_lacey.csrf import UsLaceyCsrfError

    identity = SimpleNamespace(organization_id=7, user_id=9, email="reviewer@example.test")
    detail = _detail()
    detail.fields = tuple(_case_fields("A") + [field(102, "filing_entry_reference")])
    operation = str(detail.public_id)
    calls = []
    csrf_calls = []
    monkeypatch.setattr(pilot, "_operational_mutation_context", lambda *_: (identity, SimpleNamespace()))
    monkeypatch.setattr(pilot, "UsLaceyOperationService", lambda: SimpleNamespace(get_detail=lambda **_: detail))
    monkeypatch.setattr(pilot, "UsLaceyEngineDossierService", lambda: SimpleNamespace(get_dossier=lambda **_: _engine2()))
    monkeypatch.setattr(pilot, "safe_record_outreach_event", lambda **_: None)
    monkeypatch.setattr(pilot, "us_lacey_csrf_token", lambda **kw: "fresh-" + kw["purpose"])
    monkeypatch.setattr(views, "_semantic_evidence_for_detail", lambda *_: {})
    monkeypatch.setattr(views, "_product_intelligence_for_detail", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(views, "_regulatory_assessment_for_detail", lambda *_args, **_kwargs: None)

    def verify(**kw):
        csrf_calls.append(kw)
        if kw["submitted_token"] != "valid":
            raise UsLaceyCsrfError("Invalid CSRF token.")

    def review(**kw):
        calls.append(kw)
        detail.fields = (replace(detail.fields[0], status="MATCHED"), detail.fields[1])

    monkeypatch.setattr(pilot, "verify_us_lacey_csrf", verify)
    monkeypatch.setattr(pilot, "review_us_lacey_field", review)
    app = FastAPI()
    app.mount("/static", StaticFiles(directory="src/litoral_trace/static"), name="static")
    app.add_api_route("/operations/{operation_public_id}/review/actions/fields/{field_id:int}",
        pilot.operation_review_action_fragment, methods=["POST"])
    client = TestClient(app)
    response = client.post(f"/operations/{operation}/review/actions/fields/101",
        data={"csrf_token": "invalid", "action": "accept", "candidate_id": 11}, headers={"HX-Request": "true"})
    assert calls == []
    assert 'data-review-field-id="101"' in response.text
    assert "Invalid CSRF token." in response.text
    response = client.post(f"/operations/{operation}/review/actions/fields/101",
        data={"csrf_token": "valid", "action": "accept", "candidate_id": 11}, headers={"HX-Request": "true"})
    assert response.status_code == 200
    assert calls[0]["organization_id"] == 7 and calls[0]["user_id"] == 9
    assert calls[0]["field_id"] == 101 and calls[0]["candidate_id"] == 11
    assert csrf_calls[-1]["purpose"] == f"review:{operation}:101"
    queue = response.text.split('id="action-required-fields"', 1)[1].split('id="request-evidence-dialog"', 1)[0]
    assert 'data-review-field-id="101"' not in queue
    assert queue.count('data-review-field-id="102"') == 1
    assert 'data-action-required-count>1<' in response.text
    assert response.text.count('id="operation-workspace"') == 1


def test_each_task_has_at_most_one_initial_primary_action():
    from html.parser import HTMLParser

    class Controls(HTMLParser):
        def __init__(self):
            super().__init__()
            self.stack = []
            self.primary = {}

        def handle_starttag(self, tag, attrs):
            attrs = dict(attrs)
            parent = self.stack[-1] if self.stack else None
            task = next((a[1]["data-exception-task"] for a in reversed(self.stack)
                if "data-exception-task" in a[1]), None)
            classes = attrs.get("class", "").split()
            if task and ("lt-btn--primary" in classes or "lt-task-expand--primary" in classes):
                visible = True
                for ancestor in self.stack:
                    if ancestor[0] == "dialog" and "open" not in ancestor[1]:
                        visible = False
                    if ancestor[0] == "details" and "open" not in ancestor[1]:
                        if not (tag == "summary" and ancestor is parent):
                            visible = False
                if visible:
                    self.primary[task] = self.primary.get(task, 0) + 1
            if tag not in {"area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta", "param", "source", "track", "wbr"}:
                self.stack.append((tag, attrs))

        def handle_endtag(self, tag):
            for index in range(len(self.stack) - 1, -1, -1):
                if self.stack[index][0] == tag:
                    del self.stack[index:]
                    return

    for case in ("A", "B", "C", "D", "GROUP"):
        controls = Controls()
        controls.feed(render_case(case))
        assert controls.primary and max(controls.primary.values()) == 1, (case, controls.primary)


def test_embedded_queue_does_not_duplicate_status_cards_but_partial_get_updates_them(monkeypatch):
    html=render(monkeypatch,[field(101,"filing_entry_reference")])
    for id in ("workflow-stepper","shipment-readiness-card","species-status","country-status"):
        assert html.count(f'id="{id}"')==1
    assert html.count('hx-swap-oob="true"') == 1  # workflow only; cards belong to the summary
    assert html.count('hx-swap-oob="outerHTML"') == 1
    assert html.count('id="shipment-summary"') == 1
    source=__import__("pathlib").Path(views.__file__).parents[1]/"templates/us_lacey/fragments/operation_workspace.html"
    footer=source.read_text(encoding="utf-8").split('{% set shell_oob = true %}',1)[1]
    assert footer.rstrip().endswith('{% endif %}')
