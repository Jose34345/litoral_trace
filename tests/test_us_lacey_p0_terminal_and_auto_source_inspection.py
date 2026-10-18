"""P0 postprocessing readiness coherence and original source inspection contracts."""
from pathlib import Path

from jinja2 import Environment

ROOT = Path(__file__).resolve().parents[1]
WORKSPACE = (ROOT / "src/litoral_trace/templates/us_lacey/fragments/operation_workspace.html").read_text(encoding="utf-8")
POLL = (ROOT / "src/litoral_trace/templates/us_lacey/fragments/processing_fragment.html").read_text(encoding="utf-8")
DETAIL = (ROOT / "src/litoral_trace/templates/us_lacey/operation_detail.html").read_text(encoding="utf-8")
JS = (ROOT / "src/litoral_trace/static/src/js/us-lacey-workspace.js").read_text(encoding="utf-8")
BASE = (ROOT / "src/litoral_trace/templates/base.html").read_text(encoding="utf-8")


def test_terminal_htmx_workspace_refreshes_server_readiness_instead_of_stale_processing():
    assert 'hx-trigger="every 2s"' in POLL
    assert 'data-processing-terminal="{{ \'true\' if processing.terminal else \'false\' }}"' in POLL
    assert 'data-readiness-summary' in DETAIL
    assert 'data-first-value-processing' in DETAIL
    assert 'target.hasAttribute("data-review-tabs")' in JS
    assert 'target.dataset.processingTerminal === "true"' in JS
    assert 'synchronizeTerminalOverview' in JS
    assert 'fetch(window.location.href' in JS
    assert 'cache: "no-store"' in JS
    assert 'new DOMParser().parseFromString' in JS
    assert 'current.replaceWith(document.importNode(next, true))' in JS
    assert 'next.querySelector("[data-first-value-processing]")' in JS
    assert 'Refresh results' in JS
    assert '?v=20261017-postprocessing-p0' in BASE


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


def test_confidence_is_not_presented_as_verified_or_regulatory_filing_approval():
    assert '>Extraction score</th>' in WORKSPACE
    assert 'Extraction confidence, not regulatory approval' in WORKSPACE
    assert 'Confirm Supported Fields' in WORKSPACE
    assert 'Confirm All Auto-Resolved Data' not in WORKSPACE
    assert 'hx-post="/operations/{{ detail.public_id }}/review/actions/accept-supported"' in WORKSPACE


def test_templates_compile():
    for text in (WORKSPACE, POLL, DETAIL):
        Environment().parse(text)
