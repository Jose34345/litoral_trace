"""UX12 responsive operations and batch-picker regression contract."""
from pathlib import Path


def test_operations_template_avoids_intrinsic_width_overflow_and_native_batch_popup() -> None:
    html = Path("src/litoral_trace/templates/traceability_operations.html").read_text(encoding="utf-8")
    assert "data-dynamic-batch-rows=\"shipment\"" in html
    assert "data-add-batch-row=\"shipment\"" in html
    assert html.count("data-batch-picker") >= 2
    assert "sm:grid-cols-[minmax(0,1fr)_9rem]" in html
    assert "2xl:grid-cols-3" in html
    assert "md:grid-cols-2 xl:grid-cols-3" not in html
    assert "grid-cols-[1fr_9rem]" not in html
    assert "min-w-0" in html


def test_app_js_enhances_real_selects_and_keeps_progressive_form_contract() -> None:
    js = Path("src/litoral_trace/static/src/js/app.js").read_text(encoding="utf-8")
    for marker in (
        "function enhanceBatchPicker(select)",
        "function installBatchPickers()",
        "function installDynamicBatchRows()",
        "select.dispatchEvent(new Event(\"change\", { bubbles: true }))",
        "Buscar lote o producto…",
        "Limpiar selección",
        'select.value = "";',
    ):
        assert marker in js
    assert "select.classList.add(\"sr-only\")" in js
    assert "select[name='shipment_batch']" in js
