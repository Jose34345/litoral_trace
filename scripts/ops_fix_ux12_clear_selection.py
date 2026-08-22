from __future__ import annotations

import sys
from pathlib import Path


def replace_once(text: str, old: str, new: str, *, label: str) -> str:
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{label}: expected exactly one match, found {count}")
    return text.replace(old, new)


def patch(root: Path) -> None:
    app = root / "src/litoral_trace/static/src/js/app.js"
    js = app.read_text(encoding="utf-8")

    old = '''  const optionButtons = [];
  Array.from(select.options).forEach((option) => {
'''
    new = '''  const clearSelection = document.createElement("button");
  clearSelection.type = "button";
  clearSelection.className = "mb-1 flex w-full items-center gap-2 rounded-lg px-3 py-2 text-left text-xs font-semibold text-slate-500 transition hover:bg-slate-50 hover:text-slate-800 disabled:cursor-not-allowed disabled:opacity-40";
  clearSelection.innerHTML = '<i class="fa-solid fa-rotate-left" aria-hidden="true"></i>Limpiar selección';
  clearSelection.addEventListener("click", () => {
    select.value = "";
    select.dispatchEvent(new Event("change", { bubbles: true }));
    menu.hidden = true;
    trigger.setAttribute("aria-expanded", "false");
    shell.dataset.batchPickerMenuOpen = "false";
    trigger.focus({ preventScroll: true });
  });
  list.appendChild(clearSelection);

  const optionButtons = [];
  Array.from(select.options).forEach((option) => {
'''
    js = replace_once(js, old, new, label="clear action insertion")

    old_sync = '''  const syncSelection = () => {
    const selected = select.options[select.selectedIndex];
    if (selected && selected.value) {
'''
    new_sync = '''  const syncSelection = () => {
    const selected = select.options[select.selectedIndex];
    clearSelection.disabled = !(selected && selected.value);
    if (selected && selected.value) {
'''
    js = replace_once(js, old_sync, new_sync, label="clear state sync")
    app.write_text(js, encoding="utf-8")

    test = root / "tests/test_operations_ux12_unittest.py"
    content = test.read_text(encoding="utf-8")
    anchor = '''        "Buscar lote o producto…",
    ):
'''
    replacement = '''        "Buscar lote o producto…",
        "Limpiar selección",
        'select.value = "";',
    ):
'''
    content = replace_once(content, anchor, replacement, label="regression markers")
    test.write_text(content, encoding="utf-8")


if __name__ == "__main__":
    root = Path(sys.argv[1] if len(sys.argv) > 1 else ".").resolve()
    patch(root)
