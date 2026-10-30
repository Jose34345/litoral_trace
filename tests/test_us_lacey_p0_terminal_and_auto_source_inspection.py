"""P0 postprocessing readiness coherence and original source inspection contracts."""
from pathlib import Path
import re

from jinja2 import Environment

ROOT = Path(__file__).resolve().parents[1]
WORKSPACE = (ROOT / "src/litoral_trace/templates/us_lacey/fragments/operation_workspace.html").read_text(encoding="utf-8")
POLL = (ROOT / "src/litoral_trace/templates/us_lacey/fragments/processing_fragment.html").read_text(encoding="utf-8")
DETAIL = (ROOT / "src/litoral_trace/templates/us_lacey/operation_detail.html").read_text(encoding="utf-8")
SUMMARY = (ROOT / "src/litoral_trace/templates/us_lacey/fragments/shipment_workspace_summary.html").read_text(encoding="utf-8")
JS = (ROOT / "src/litoral_trace/static/src/js/us-lacey-workspace.js").read_text(encoding="utf-8")
BASE = (ROOT / "src/litoral_trace/templates/base.html").read_text(encoding="utf-8")


def test_terminal_htmx_workspace_refreshes_server_readiness_instead_of_stale_processing():
    assert 'hx-trigger="every 2s"' in POLL
    assert 'data-processing-terminal="{{ \'true\' if processing.terminal else \'false\' }}"' in POLL
    assert 'shipment_workspace_summary.html' in DETAIL
    assert 'data-readiness-summary' in SUMMARY
    assert 'data-first-value-processing' in SUMMARY
    assert '#operation-workspace[data-review-tabs]' in JS
    assert '#processing-panel[data-processing-terminal="true"]' in JS
    assert 'document.addEventListener("htmx:afterSettle"' in JS
    assert 'synchronizeTerminalOverview' in JS
    assert 'fetch(window.location.href' in JS
    assert 'cache: "no-store"' in JS
    assert 'new DOMParser().parseFromString' in JS
    assert 'current.replaceWith(document.importNode(next, true))' in JS
    assert 'next.querySelector("[data-first-value-processing]")' in JS
    assert 'Refresh results' in JS
    assert '?v=20261018-review-state-p1' in BASE


def test_auto_resolved_uses_exact_current_document_id_and_same_inspector_as_pending():
    assert 'data-auto-resolved-field="{{ field.id }}"' in WORKSPACE
    assert 'field.source_assurance_document_id and field.source_page' in WORKSPACE
    assert 'data-source-url="/operations/{{ detail.public_id }}/source/{{ field.source_assurance_document_id }}"' in WORKSPACE
    assert 'data-source-label="{{ field.source_filename or \'Shipment source PDF\' }}"' in WORKSPACE
    assert 'data-auto-source-unavailable' in WORKSPACE
    assert 'data-reused-evidence-inspect' in WORKSPACE
    assert 'href="/evidence"' in WORKSPACE
    assert 'data-source-inspect' in WORKSPACE
    # Reused claims with no current document must never inherit a shipment PDF URL.
    reused_section = WORKSPACE.split('<a class="inline-flex text-xs font-bold text-emerald-800 underline" href="/evidence" data-reused-evidence-inspect>', 1)
    assert len(reused_section) == 2
    assert '{% elif field.source_assurance_document_id and field.source_page %}' in reused_section[1]


def test_confidence_is_not_presented_as_verified_or_regulatory_filing_approval(monkeypatch):
    from tests.test_us_lacey_exception_task_queue import candidate, field, render
    html = render(monkeypatch, [field(101, "entered_value", "11250", source=20,
        filename="Commercial_Invoice.pdf", status="SUPPORTED", candidates=(candidate(),))])
    card = re.search(r'<article data-auto-resolved-field="101".*?</article>', html, re.S).group()
    technical = re.search(r"<details>.*?</details>", card, re.S).group()
    assert "Extraction score:" in technical
    assert "Extraction confidence is not evidence verification or filing approval" in technical
    assert "Extraction score:" not in card.replace(technical, "")
    assert 'Confirm Supported Fields' in WORKSPACE
    assert 'Confirm All Auto-Resolved Data' not in WORKSPACE
    assert 'hx-post="/operations/{{ detail.public_id }}/review/actions/accept-supported"' in WORKSPACE


def test_templates_compile():
    for text in (WORKSPACE, POLL, DETAIL, SUMMARY):
        Environment().parse(text)
