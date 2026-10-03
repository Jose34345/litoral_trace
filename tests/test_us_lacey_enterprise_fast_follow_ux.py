from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TEMPLATE_ROOT = ROOT / "src" / "litoral_trace" / "templates" / "us_lacey"
STATIC_ROOT = ROOT / "src" / "litoral_trace" / "static" / "src"
WORKSPACE = TEMPLATE_ROOT / "fragments" / "operation_workspace.html"
DETAIL = TEMPLATE_ROOT / "operation_detail.html"
ALIAS = TEMPLATE_ROOT / "fragments" / "operation_alias.html"
EXPORT = TEMPLATE_ROOT / "fragments" / "export_declaration_package.html"
BASE = TEMPLATE_ROOT / "base.html"
JS = STATIC_ROOT / "js" / "us-lacey-workspace.js"
DESIGN = STATIC_ROOT / "design-system.css"
PROGRESS = STATIC_ROOT / "progress.css"
EXPORT_CSS = STATIC_ROOT / "us-lacey-export.css"
PILOT_APP = ROOT / "src" / "litoral_trace" / "web" / "us_lacey_pilot_app.py"
INTELLIGENT = ROOT / "src" / "litoral_trace" / "web" / "us_lacey_intelligent_workflow.py"
OPS_CORE = ROOT / "src" / "litoral_trace" / "us_lacey" / "_operations_core.py"
EXPORT_ROUTER = ROOT / "src" / "litoral_trace" / "routers" / "operations.py"


def test_review_validation_errors_are_contextual_and_accessible():
    workspace = WORKSPACE.read_text(encoding="utf-8")
    pilot = PILOT_APP.read_text(encoding="utf-8")
    intelligent = INTELLIGENT.read_text(encoding="utf-8")

    assert "field_errors.get(field.id)" in workspace
    assert 'data-inline-field-error' in workspace
    assert 'aria-invalid="true"' in workspace
    assert 'aria-describedby="field-error-{{ field.id }}"' in workspace
    assert "lt-inline-error" in workspace
    assert "field_errors={int(field_id): str(exc)}" in pilot
    assert "field_errors={int(field_id): str(exc)}" in intelligent


def test_operation_alias_is_editable_in_place_without_changing_identity():
    detail = DETAIL.read_text(encoding="utf-8")
    alias = ALIAS.read_text(encoding="utf-8")
    pilot = PILOT_APP.read_text(encoding="utf-8")
    core = OPS_CORE.read_text(encoding="utf-8")

    assert 'include "us_lacey/fragments/operation_alias.html"' in detail
    assert "Operation ID · {{ detail.public_id }}" in detail
    assert "data-operation-alias-edit" in alias
    assert "data-operation-alias-form" in alias
    assert 'hx-post="/operations/{{ detail.public_id }}/alias"' in alias
    assert '@app.post("/operations/{operation_public_id}/alias"' in pilot
    assert "def update_client_reference(" in core
    assert "operation.client_reference = reference" in core


def test_lawgs_xml_has_authenticated_quick_preview_modal():
    export = EXPORT.read_text(encoding="utf-8")
    router = EXPORT_ROUTER.read_text(encoding="utf-8")
    styles = EXPORT_CSS.read_text(encoding="utf-8")

    assert "data-xml-preview-trigger" in export
    assert 'aria-haspopup="dialog"' in export
    assert 'aria-modal="true"' in export
    assert "data-xml-preview-code" in export
    assert "data-xml-copy" in export
    assert "/preview/lawgs-xml" in export
    assert '@router.get("/operations/{operation_id}/preview/lawgs-xml")' in router
    assert "build_lawgs_xml(snapshot)" in router
    assert ".lt-xml-modal__backdrop" in styles
    assert "backdrop-filter: blur(8px)" in styles


def test_review_save_advances_focus_with_smooth_scroll():
    script = JS.read_text(encoding="utf-8")

    assert "const focusReviewCard" in script
    assert 'card.scrollIntoView({ behavior: "smooth", block: "center" });' in script
    assert "input.focus({ preventScroll: true })" in script
    assert "previousIndex" in script
    assert "cards[index]" in script


def test_b2b_keyboard_entry_supports_enter_and_logical_tab_navigation():
    script = JS.read_text(encoding="utf-8")

    assert 'event.key === "Enter"' in script
    assert "form?.requestSubmit?.();" in script
    assert 'event.key !== "Tab"' in script
    assert '#action-required-fields [data-review-entry-input]' in script
    assert "nextIndex" in script


def test_zero_action_required_transitions_to_declaration_package():
    script = JS.read_text(encoding="utf-8")
    workspace = WORKSPACE.read_text(encoding="utf-8")

    assert "transition.previousActionCount > 0 && cards.length === 0" in script
    assert 'document.getElementById("declaration-package")' in script
    assert 'behavior: "smooth"' in script
    assert "lt-empty-state-transition" in workspace


def test_toasts_replace_success_banners_and_auto_expire():
    detail = DETAIL.read_text(encoding="utf-8")
    base = BASE.read_text(encoding="utf-8")
    script = JS.read_text(encoding="utf-8")
    styles = DESIGN.read_text(encoding="utf-8")

    assert "data-toast-bootstrap" in detail
    assert 'id="lt-toast-region"' in base
    assert "const showToast" in script
    assert "window.setTimeout(remove, duration)" in script
    assert "lt-toast__progress" in script
    assert "@keyframes lt-toast-progress" in styles
    assert "--lt-toast-duration" in styles


def test_workflow_stepper_is_navigable():
    detail = DETAIL.read_text(encoding="utf-8")
    styles = PROGRESS.read_text(encoding="utf-8")

    for target in ("#documents", "#processing-status", "#review-summary", "#declaration-package"):
        assert target in detail
    assert "data-workflow-step-link" in detail
    assert 'aria-current="step"' in detail
    assert ".lt-progress-step__link" in styles


def test_sticky_master_action_bar_keeps_primary_workflow_actions_visible():
    workspace = WORKSPACE.read_text(encoding="utf-8")
    styles = DESIGN.read_text(encoding="utf-8")

    assert "lt-sticky-action-bar" in workspace
    assert "Confirm All" in workspace
    assert "Proceed to Final Confirmation" in workspace
    assert "Review next item" in workspace
    assert "position: sticky" in styles
    assert "backdrop-filter: blur(14px)" in styles


def test_exception_cards_have_clear_blocking_and_confirmation_severity():
    workspace = WORKSPACE.read_text(encoding="utf-8")
    styles = DESIGN.read_text(encoding="utf-8")

    assert "lt-review-card--blocking" in workspace
    assert "lt-review-card--confirmation" in workspace
    assert "lt-review-status--blocking" in workspace
    assert "lt-review-status--confirmation" in workspace
    assert "border-left-color: #dc2626" in styles
    assert "border-left-color: #f59e0b" in styles


def test_motion_respects_reduced_motion_and_htmx_swap_states():
    styles = DESIGN.read_text(encoding="utf-8")
    workspace = WORKSPACE.read_text(encoding="utf-8")

    assert "#operation-workspace.htmx-swapping" in styles
    assert "#operation-workspace.htmx-added" in styles
    assert "prefers-reduced-motion: reduce" in styles
    assert 'hx-swap="outerHTML transition:true"' in workspace
