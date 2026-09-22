from __future__ import annotations

import json
import re
import time
from pathlib import Path

from playwright.sync_api import sync_playwright

BASE_URL = "https://lacey.litoraltrace.com"
ROOT = Path(__file__).resolve().parents[1]
FIXTURES = [
    ROOT / "tests/fixtures/us_lacey_quality_output/01_Commercial_Invoice_Entry_Worksheet.pdf",
    ROOT / "tests/fixtures/us_lacey_quality_output/02_Botanical_Supplier_Declaration.pdf",
    ROOT / "tests/fixtures/us_lacey_quality_output/03_BOM_Packing_List.xlsx",
]
ARTIFACT_DIR = ROOT / "artifacts/prod-sanity-pr298"
ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)

summary: dict[str, object] = {
    "base_url": BASE_URL,
    "fixture_files": [p.name for p in FIXTURES],
}

for path in FIXTURES:
    if not path.is_file():
        raise AssertionError(f"Missing Golden Fixture file: {path}")

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    context = browser.new_context(viewport={"width": 1440, "height": 1100})
    page = context.new_page()

    page.goto(f"{BASE_URL}/sandbox/start", wait_until="networkidle", timeout=60000)
    assert page.get_by_role("heading", name="U.S. Lacey Act Sandbox").is_visible()
    page.screenshot(path=str(ARTIFACT_DIR / "01-sandbox-start.png"), full_page=True)

    page.get_by_role("button", name="Start Document Analysis").click()
    page.wait_for_url(re.compile(r".*/operations/new$"), timeout=60000)

    client_reference = f"PR298-PROD-SANITY-{int(time.time())}"
    summary["client_reference"] = client_reference
    page.locator("#client_reference").fill(client_reference)
    page.get_by_role("button", name="Create workspace & upload documents").click()
    page.wait_for_url(re.compile(r".*/operations/[0-9a-fA-F-]{36}$"), timeout=60000)

    operation_url = page.url
    operation_id = operation_url.rstrip("/").split("/")[-1]
    summary["operation_id"] = operation_id

    page.locator("#operation-documents").set_input_files([str(p) for p in FIXTURES])
    page.get_by_role("button", name="Upload and analyze").click()
    page.wait_for_load_state("domcontentloaded", timeout=60000)

    loader = page.get_by_role("heading", name="Extracting and cross-checking evidence...")
    loader.wait_for(state="visible", timeout=30000)
    summary["loader_visible"] = True
    page.screenshot(path=str(ARTIFACT_DIR / "02-processing-loader.png"), full_page=True)

    action_tab = page.locator('[data-review-tab-button="action"]')
    action_tab.wait_for(state="visible", timeout=240000)
    page.wait_for_timeout(1500)
    page.screenshot(path=str(ARTIFACT_DIR / "03-action-required.png"), full_page=True)

    tab_names = [
        " ".join(page.locator(f'[data-review-tab-button="{name}"]').inner_text().split())
        for name in ("action", "resolved", "regulatory")
    ]
    summary["tabs"] = tab_names
    assert tab_names[0].startswith("Action Required")
    assert tab_names[1].startswith("Auto-Resolved")
    assert tab_names[2] == "Regulatory Analysis"

    action_statuses = page.locator(
        '[data-action-required-list] [data-review-field]'
    ).evaluate_all(
        "(els) => els.map((e) => e.getAttribute('data-review-status'))"
    )
    summary["action_required_statuses"] = action_statuses
    summary["action_required_count"] = len(action_statuses)
    assert all(status in {"MISSING", "CONFLICT"} for status in action_statuses), action_statuses

    body_text = page.locator("body").inner_text()
    ghost_lines = [n for n in range(4, 12) if f"Plant line {n}" in body_text]
    summary["ghost_plant_lines"] = ghost_lines
    assert ghost_lines == [], ghost_lines

    page.locator('[data-review-tab-button="regulatory"]').click()
    page.locator('[data-review-tab-panel="regulatory"]:not(.hidden)').wait_for(
        state="visible", timeout=10000
    )
    not_applicable = page.locator("details[data-regulatory-not-applicable]")
    assert not_applicable.count() == 1
    not_applicable.locator("summary").click()

    special_rule_statuses = {}
    for rule_id in ("SPECIAL_COMPOSITE", "SPECIAL_RECYCLED"):
        rule = page.locator(f'[data-regulatory-rule="{rule_id}"]')
        assert rule.count() == 1, f"{rule_id} missing"
        status = rule.get_attribute("data-regulatory-rule-status")
        special_rule_statuses[rule_id] = status
        assert status == "NOT_APPLICABLE", (rule_id, status)
    summary["special_rule_statuses"] = special_rule_statuses
    page.screenshot(path=str(ARTIFACT_DIR / "04-regulatory-analysis.png"), full_page=True)

    page.locator('[data-review-tab-button="resolved"]').click()
    page.locator('[data-review-tab-panel="resolved"]:not(.hidden)').wait_for(
        state="visible", timeout=10000
    )
    auto_fields = page.locator("[data-auto-resolved-field]")
    before_count = auto_fields.count()
    summary["auto_resolved_before_confirm"] = before_count
    assert before_count > 0, "Golden Fixture produced no auto-resolved fields"
    page.screenshot(path=str(ARTIFACT_DIR / "05-auto-resolved-before.png"), full_page=True)

    bulk_button = page.get_by_role("button", name="Confirm All Auto-Resolved Data")
    assert bulk_button.is_visible()
    with page.expect_response(
        lambda response: (
            "/review/actions/accept-supported" in response.url
            and response.request.method == "POST"
        ),
        timeout=60000,
    ) as response_info:
        bulk_button.click()
    response = response_info.value
    summary["bulk_confirm_http_status"] = response.status
    assert response.status == 200, response.status

    page.wait_for_function(
        "() => document.querySelectorAll('[data-auto-resolved-field]').length === 0",
        timeout=30000,
    )
    page.wait_for_timeout(1000)
    after_count = page.locator("[data-auto-resolved-field]").count()
    summary["auto_resolved_after_confirm"] = after_count
    assert after_count == 0

    # HTMX swaps the entire workspace and initializes the Action Required tab again.
    action_tab = page.locator('[data-review-tab-button="action"]')
    action_tab.wait_for(state="visible", timeout=10000)
    page.screenshot(path=str(ARTIFACT_DIR / "06-after-bulk-confirm.png"), full_page=True)

    summary["final_url"] = page.url
    browser.close()

(ARTIFACT_DIR / "summary.json").write_text(
    json.dumps(summary, indent=2, sort_keys=True),
    encoding="utf-8",
)
print("SANITY_SUMMARY=" + json.dumps(summary, sort_keys=True))
