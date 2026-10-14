# U.S. Lacey P0 — Smart Entry Guidance and Instant Evidence Preview

**Status:** Implementation-ready plan, subject to reconciliation with the unpublished local design specification.
**Written against:** `feature/us-lacey-pilot-platform` at `4d229b306d65f46a949cc32e48bc571858c81123`.
**Scope:** Customer shipment review only; no regulatory-domain, filing, database, RLS or export-authority change.
**Related delivery:** PR #443 (applicability, authenticated source PDF) and PR #445 (evidence-bound applicability attestations).

> Provenance caveat: The local design specification reportedly committed as `bbb9fa95` at
> `work/litoral-trace-ux-ui-p0` is **not published to GitHub**, and the remote desktop is offline.
> This is a new, independently grounded implementation plan based on the present source tree
> and the user-approved functional brief, **not** a claim to have read or approved the absent local file.
> Before coding, reconcile any differences with that specification. Never overwrite the local document.

## Goals and user journey

A customs broker/importer should be able to:
1. Understand CBP entry procedures through clear business language without losing the exact official code.
2. See distinct **customs entry evidence** and **supplier-origin evidence** instead of a generic form.
3. Hover or focus a source and see an **actual authorized PDF page**, without a click, at legible size.
4. Click to pin a 40/60 evidence-review workspace; navigate pages and change zoom.
5. Explicitly confirm or leave pending the source-backed applicability review. Opening a source cannot approve it.

Preferred state machine:

| State | Entry event | Rendering | Exit / transition |
| --- | --- | --- | --- |
| CLOSED | Initial, Escape, dismissal | Compact selection and evidence cards; no source fetched | Pointer/focus intent |
| PREVIEW | Mouse hover after ~180 ms or keyboard focus | Anchored nonmodal preview using authorized real PDF | Exit both trigger and panel after ~250 ms, Escape; click to PINNED |
| PINNED | Click/Enter on source or preview | Inline two-column workspace (~40% decision / ~60% PDF), true zoom/page controls | Close returns to prior focus and releases source memory |
| MOBILE_PINNED | Tap | Full-screen modal/dialog | Close/Escape/back action |

These are states of **presentation**, not regulatory statuses.

## Verified existing code and constraints

- Existing secure endpoint: `GET /operations/{operation_public_id}/source/{assurance_document_id}` in `src/litoral_trace/web/us_lacey_pilot_app.py`, delegating to `src/litoral_trace/web/us_lacey_source_inspector.py`.
- Server performs organization scoping, current finalized source membership, Vault SHA verification, retained-original availability check, no-store inline PDF streaming, and records authorized access. Keep this security authority and audit behavior.
- Existing frontend: `src/litoral_trace/templates/us_lacey/fragments/source_inspector.html`, `src/litoral_trace/static/js/us_lacey_source_inspector.js`, `src/litoral_trace/static/css/us_lacey_source_inspector.css`; currently uses a native PDF iframe in a modal dialog with a click trigger.
- Current applicable UI: `src/litoral_trace/templates/us_lacey/fragments/applicability_triage.html` and `src/litoral_trace/templates/us_lacey/fragments/operation_workspace.html`, injected by `src/litoral_trace/templates/us_lacey/operation_detail.html`.
- Current attestation service: `src/litoral_trace/us_lacey/regulatory/applicability/attestation.py`. It explicitly requires confirmed HTS, selected entry type, original entry PDF and, for cultivated/wild bamboo, a separate supplier-origin PDF. It is advisory and **does not unlock LAWGS**.
- `package.json` has Tailwind CLI, HTMX and vendor script, **not PDF.js**. Never assume programmable zoom/page controls exist in the native iframe.
- `.github/workflows/lacey-visual-qa.yml` currently targets an older experimental landing/demo app; it is **not** end-to-end QA for the authenticated shipment inspector. Add a suitable scoped browser gate rather than claiming coverage from this old workflow.

## Tasks and commit order

### Task 0 — Checkout, baseline and local-spec reconciliation

1. Run `git status --short`, `git rev-parse HEAD`, `git branch --show-current`, and `git log -5 --oneline` in the isolated worktree. Check it is based on the production branch, not `main`; preserve any uncommitted local work.
2. Read `AGENTS.md`, `src/litoral_trace/us_lacey/AGENTS.md`, and current `docs/us-lacey/{README,ARCHITECTURE,CAPABILITIES,INVARIANTS,TEST_MATRIX}.md` (CAPABILITIES is TOML).
3. Read local `docs/superpowers/specs/2026-10-13-us-lacey-smart-entry-evidence-preview-design.md` when that file is accessible. Reconcile material disagreements in a separate review note before proceeding.
4. Rerun `pytest -q tests/test_us_lacey_p0_applicability_source_inspector.py tests/test_us_lacey_p0_applicability_attestation.py`. The user's report of **20 passed** is not a remotely reproduced result.
5. Start an implementation branch from the currently verified production base. Keep separate commits for entry selector, source descriptors, hover viewer, pinned viewer, and acceptance/QA.

**Exit:** working tree known, baseline green, no local spec data overwritten.

### Task 1 — Business-friendly entry selection (no rule changes)

- Introduce a presentation-only entry-type mapping/view model, e.g. `src/litoral_trace/web/us_lacey_entry_type_options.py`. Key by **exact** CBP code already supported by `regulatory/applicability/triage.py`. The mapping contains label, short description, group, and whether the code requires specialist confirmation.
- Replace the raw long select in `fragments/applicability_triage.html` with an enhanced, searchable combobox while retaining a semantic native-select progressive fallback. Suggested group labels: Standard commercial import, Informal entry, Bonded warehouse, Withdrawal, Temporary, FTZ, Special procedure, I'm not sure.
- Grouping is only discovery. Preserve 01/02/03/06/07/11/12/13/21/22/23/31/32/34/38/52/86 as **individual** selectable codes with truthful details; do not equate a group with a single code. Code 06 remains FTZ-special; code 86 must not be described as automatically eligible solely by shipment value.
- Unknown `""` must stay unknown (no default to 01). Search supports code and plain-English name; Enter/Arrow/Escape/focus interactions and labels must be keyboard/screen-reader friendly.
- Preserve the GET `entry_type` query contract and hidden POST `entry_type` value used by the attestation; never invent another source of filing authority.
- Microcopy: "How is this shipment entering the U.S.?", "Your customs broker can confirm the entry type", "I'm not sure", "Check applicability".

**Tests:** a contract test of every supported code and its form value, unknown-fails-closed, 06 special, 86 caution, HTML escaping and keyboard navigation.

### Task 2 — Read-only source descriptors / evidence cards

- Build a single read-only evidence-card projection from the **existing** `applicability_sources` and source-linked candidate data. Required fields: assurance document ID, current operation public ID, current source revision identifier or fingerprint where available, filename, source class, referenced page, availability, and claimed fact/scope.
- Present **two separate cards** when appropriate: "Customs entry evidence" and "Supplier cultivation evidence". Do not suggest an arrival notice is supplier cultivation evidence. Derived worksheets remain visibly non-primary and disabled for independent attestation.
- Keep real `source_document_id`, `source_page`, `botanical_document_id`, `botanical_page` form fields and CSRF. Presentation should not replace server validation or the explicit checked-review box.
- No auto-selected source if evidence is unavailable or unverified; avoid "Evidence available" badges that imply verified authority.
- Avoid duplicating the same source metadata across overview, preview and pinned panel; use one descriptor/viewmodel for all three.

**Tests:** one source vs two distinct sources, expired/deleted, worksheet, unknown role, missing HTS, previously recorded current/stale decision, and mobile card order.

### Task 3 — Secure ephemeral source loading

Reuse the existing GET endpoint; **do not** add a second download route for hover as the default.

- Central client `EvidenceSession` abstraction owns: key `(operationId, sourceId, page, sourceRevision)`, state, one in-flight `AbortController`, a single transient PDF buffer/object URL or renderer object, current consumer count, and deterministic cleanup.
- `pointerenter` after intent delay (~180 ms) and `focusin` initiate fetching. Do not eagerly load source bytes at first page render or simply when document cards scroll into view.
- Before display call the authenticated same-origin route (credentials same-origin, `cache: "no-store"`); inspect HTTP status and content-type **before** creating an object URL / renderer. Redirect/login HTML, missing PDF or altered hash must never appear inside a PDF frame.
- Handle 401 (sign in), 403 (no access), 404 (original unavailable or expired), 409 (source changed / integrity), 503 (temporarily unavailable), browser abort and network error with distinct short UI states; do not log PDF payloads.
- Reuse downloaded bytes from PREVIEW into PINNED without a second GET while the same revision and session remain authorized. Zoom and page changes must use that in-memory document rather than re-requesting the original. The initial verified GET remains auditable; do not suppress the existing server audit or add one event per zoom.
- Dismissal: cancel an unfinished fetch; close renderer/worker and revoke object URL after the last consumer releases it. On signout, source-set revision change, HTMX replacement, navigation or visibility lifecycle invalidate and discard session state. Never use localStorage, IndexedDB, service-worker cache or persistent blob cache.
- Do not claim that an already-downloaded client-side PDF is re-authorized on every page turn: server authorization occurs on GET. Define an explicit short-lived session lifecycle and always refetch/revalidate when reopening after closure or revision invalidation.
- Keep `Cache-Control: private, no-store`; review CSP for locally generated `blob:` URL viewer and same-origin iframe restrictions. No storage URLs exposed to the browser.

**Tests:** 1 GET per preview→pinned session; zero GET until intent; rapid hover cancels stale result; distinct errors; tenant denial; revision mutation; SHA mismatch; no second network GET on zoom; memory cleanup on close; no POST/attestation created by viewing.

### Task 4 — Actual PDF renderer decision gate

Make this a **measured spike**, not an assumption.

1. Run a real authenticated original PDF through the existing native iframe, test page selection and zoom in supported Chromium/Edge, Firefox and mobile browser.
2. Native iframe is acceptable for full-source fallback, but is **not** sufficient for custom guaranteed previous/next, current-page indication and controlled zoom unless browser evidence proves that these controls are controllable across supported browsers.
3. If native cannot satisfy the agreed controls, add a locally bundled and version-pinned PDF.js renderer. Update lockfile and vendor/build packaging (no CDN, no remote worker, no inline remote scripts); render to canvas at device-pixel ratio with bounded zoom and a single active page for the hover preview. Review CSP/worker-src and avoid exposing storage tokens.
4. Use an in-memory PDF byte array from Task 3; ensure renderer disposes canvases, page objects, workers and buffer references correctly when the session ends.
5. Browser-level test with at least one real authorized multi-page fixture, including missing/expired original. Fixture must be synthetic or explicitly authorized and never embedded as confidential production data.

**Decision record:** document native-vs-local-renderer evidence (functions, bytes, render latency, browser support, security constraints). Do not label a decorative screenshot as successful real-PDF rendering.

### Task 5 — UI state machine and responsive composition

- Add isolated components/styles: `evidence_hover_preview` (nonmodal anchored popover) and `evidence_pinned_inspector`. Integrate with the existing source inspector module rather than installing a competing globally mounted modal.
- CLOSED: compact evidence cards; no empty 70%-width visual block.
- PREVIEW: opens after 180 ms pointer intent or keyboard focus. Fade/slide ~140–180 ms / 6–10 px; window positioning checks viewport edges, available height and sticky navigation; panel large enough to **read** a page, not just a thumbnail. Keep open while pointer moves between trigger and popover; ~250 ms exit grace; Esc dismisses. For keyboard users, provide a reachable "Open full inspector" control; Escape returns focus appropriately. Reduced motion removes movement.
- PINNED desktop: split panel (~40% review / ~60% original) with minimum useful widths, independent PDF scroll, zoom and actual page navigation. The review column should remain visible and usable: a *nonmodal* pinned region is preferable to a full modal with inert review controls. Restore scroll/focus on close.
- MOBILE_PINNED: a genuine accessible full-screen dialog (no hover), with close, page controls and back-to-review behavior.
- Touch/fine-pointer detection via media query, not user-agent string. Do not accidentally activate preview on touch, scrolling or unintentional pointer movement.
- Ensure HTMX swaps detach old listeners correctly, preserve review focus/scroll position, and never leave a stale overlay over a new operation or source revision.

**Tests:** Playwright keyboard/focus, pointer bridge flicker, Escape, mobile tapping, sticky-bar overlap, 390/768/1280/1440 widths, no horizontal scroll and no unintended clicks on `Record review`.

### Task 6 — Explicit review, language and policy

- Keep the existing POST `/operations/{operation_public_id}/applicability/attest` and corresponding server authority. Do not relax `applicability_hts_confirmed`, original PDF, botanical evidence, explicit user confirmation, current source-set checks, tenant controls or CSRF.
- Opening/rendering/scanning/zooming a PDF can only create **access events** as required by the existing audit policy, never `HUMAN_REVIEW` or field acceptance.
- Always distinguish: source available vs classified vs verified; previewed vs attested; scenario preview vs legal applicability verdict; recorded review vs filing authorization.
- Short customer-facing English: "Check original evidence", "Open full inspector", "Page 1 of N", "Original no longer available", "This source is a derived worksheet", "Record review".
- Accessibility: WCAG hover/focus content must be dismissible, hoverable and persistent; no aggressive auto-close while actively reading; background interaction remains coherent for nonmodal desktop state.

**Tests:** `Record review` remains disabled on HTS not confirmed, missing original, missing supplier evidence, expired source, stale review, insufficient auth. Successful human POST alone records attestation; underlying operation readiness and LAWGS remain unchanged.

### Task 7 — Test matrix, release and non-regression

Focused tests (augment, do not delete):
- `tests/test_us_lacey_p0_applicability_source_inspector.py`
- `tests/test_us_lacey_p0_applicability_attestation.py`
- `tests/test_us_lacey_fast_review_p14_p15.py`
- `tests/test_us_lacey_review_ux_normalization.py`
- `tests/test_us_lacey_p2_exception_first.py`
- `tests/test_us_lacey_portal_http_postgres_e2e.py` when changed auth/storage behavior requires live DB acceptance
- Add `tests/test_us_lacey_entry_type_selector.py`, `tests/test_us_lacey_evidence_preview_contract.py` and real browser acceptance (dedicated Playwright suite, locally and CI).

Run from the isolated worktree:
1. Narrow unit/SSR/HTMX contract tests.
2. Authenticated real-PDF Playwright test including page and zoom; negative unauthorized, stale, retention states. Capture desktop and mobile screenshots **from the actual shipment review**, not a mockup.
3. `npm ci && npm run build` (if vendor changed), or `npm ci && npm run build:css` for CSS-only changes. Commit the tracked generated CSS if required.
4. `python -m compileall -q src/litoral_trace`; whole repo pytest/CI.
5. Frontend CSS Drift Gate, General CI, and U.S. Lacey PostgreSQL Gate. For authenticated preview use negative RLS/cross-tenant verification; a purely DOM unit test is not sufficient.
6. Review current security headers/CSP/iframe permissions and cache/resource lifecycle in a production-like environment.
7. Demonstrate Pack 1 and Pack 2 with **fresh synthetic/authorized originals** and source revisions; never mutate existing production customer decisions just to test UI.

**Target measurements**, not claimed results: visual popover visible <250 ms after the hover wait, P95 first-page render <1.2 s for a declared fixture/network/browser profile, zero duplicate GETs on preview→pin, zero artificial HUMAN_REVIEW events, zero unbounded PDF memory growth. Publish measured samples and test conditions.

## Rollout

- PR off a dedicated branch targeting `feature/us-lacey-pilot-platform`. Attach before/after screenshots, actual PDF pages, performance data and a native-vs-PDF.js decision note.
- Merge only after required green checks; deployment is managed via current Northflank web and worker pipeline. Confirm exact merged commit on both services and health/ready 200. If UI changes do not require a worker image update, still verify deployed expected versions.
- Rollback: revert presentation JS/templates/CSS while preserving the existing authenticated endpoint and all attestation data. Keep a server-rendered accessible fallback if JS fails.
- **No deployment to live** based on a localhost mockup or the reported 20 baseline tests alone.

## Definition of Done

- A nonexpert can find the correct CBP code with plain-English assistance, without auto-inferred regulatory certainty.
- Hover/focus yields real, page-correct authorized PDF contents; touch/click opens a usable inspector.
- Pin preserves decision and source side by side; zoom and page controls perform actual render changes.
- Derived, unavailable and stale sources are not labeled sufficient or accepted as original evidence.
- Access and approval are separate and audited appropriately.
- No cross-tenant leakage, no stale source revival, no cache persistence, no auth HTML in PDF frame.
- CSS/CI/PostgreSQL gates and **real browser/PDF acceptance** pass, and the exact Northflank release is verified.
