# Smart Entry Guidance and Instant Evidence Preview Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver a source-led, accessible shipment review that previews an authorized real PDF on hover/focus and pins a controllable document inspector without changing regulatory authority.

**Architecture:** Extend the existing P0 applicability/source-inspector capability. Preserve its authenticated same-origin PDF endpoint and attestation writer; add presentation-only entry options, evidence descriptors and one ephemeral client evidence session. Desktop pinning is nonmodal to keep review visible; mobile pinning is a full-screen dialog.

**Tech Stack:** FastAPI, Jinja2, HTMX, vanilla JavaScript, CSS, pytest, Playwright, npm vendor build; local version-pinned PDF.js only if the native iframe spike cannot meet functional controls.

**Spec:** `docs/superpowers/specs/2026-10-13-us-lacey-smart-entry-evidence-preview-design.md`

## Global Constraints

- Preserve `/operations/{operation_public_id}/source/{assurance_document_id}` as the sole original-byte route; do not expose storage URLs or add a hover download route.
- Preserve tenant/RLS, source-set currentness, retention, SHA-256, access auditing, CSRF and immutable human attestation.
- Viewing, hover, focus, zoom and page navigation never create `HUMAN_REVIEW`, change regulatory state or unlock filing.
- Exact CBP codes remain the only scenario value; `I'm not sure` remains unknown; entry type `06` and `86` retain their special copy/semantics.
- Derived worksheets never become independent source authority; cultivated/wild bamboo still needs a distinct current supplier-origin PDF.
- No React, CDN, persistent browser storage, external PDF service, migration, RLS change or filing-rule change.
- Validate a real authorized Pack 2 PDF in the real shipment UI before PR readiness; do not claim performance goals without measurements.

## Review Focus

- A rapid pointer move from one card to another must not leave the first document or stale error visible; Task 3 owns cancellation-token tests.
- A document rendered before source revision invalidation must not be reusable afterward; Task 3 owns lifecycle invalidation tests.
- A keyboard user must reach and dismiss preview/pinned states without a focus trap in desktop review; Task 5 owns focus tests.
- A server HTML/login response must never enter the PDF renderer; Task 3 owns content-type/error tests.
- A visually available source must not imply authority for bamboo or filing; Task 2 and Task 6 own source-class/attestation tests.

---

### Task 1: Exact-code entry selector

**Files:**
- Create: `src/litoral_trace/web/us_lacey_entry_type_options.py`
- Modify: `src/litoral_trace/templates/us_lacey/fragments/applicability_triage.html`
- Modify: `src/litoral_trace/static/js/us_lacey_source_inspector.js`
- Test: `tests/test_us_lacey_entry_type_selector.py`

**Interfaces:**
- Produces `entry_type_options() -> tuple[EntryTypeOption, ...]`; each option has `code`, `group`, `label`, `official_title`, `description`, and `caution`.
- Consumes the supported code constants from `regulatory.applicability.triage`; the GET/POST field remains `entry_type`.

- [ ] **Step 1: Write failing selector tests** for every current supported code, unknown empty value, code `06`, code `86`, HTML-safe labels and keyboard contract markers.
- [ ] **Step 2: Run `python -m pytest -q tests/test_us_lacey_entry_type_selector.py`** and verify the new tests fail.
- [ ] **Step 3: Implement `EntryTypeOption` and `entry_type_options()`** without adding regulatory logic; group codes only for discovery.
- [ ] **Step 4: Replace the visible raw select with progressive enhanced combobox/listbox markup** while retaining a semantic native-select fallback and exact selected form value.
- [ ] **Step 5: Implement search, Arrow, Enter and Escape behavior** in the existing JS module; `I'm not sure` sets `entry_type` to empty.
- [ ] **Step 6: Run selector and existing P0 tests**; commit `feat: add smart entry type selector`.

### Task 2: Evidence-card projection and compact closed state

**Files:**
- Create: `src/litoral_trace/web/us_lacey_evidence_cards.py`
- Modify: `src/litoral_trace/us_lacey/regulatory/applicability/attestation.py`
- Modify: `src/litoral_trace/templates/us_lacey/fragments/applicability_triage.html`
- Modify: `src/litoral_trace/templates/us_lacey/fragments/operation_workspace.html`
- Test: `tests/test_us_lacey_evidence_preview_contract.py`
- Test: `tests/test_us_lacey_p0_applicability_attestation.py`

**Interfaces:**
- Produces `EvidenceCardDescriptor` with source ID, filename, classification, page, availability, review scope and source-set fingerprint.
- Consumes existing tenant-scoped `ApplicabilitySource` rows and current attestation state; does not write anything.

- [ ] **Step 1: Write failing descriptor/template tests** for primary customs evidence, distinct supplier cultivation evidence, derived worksheet, unavailable original, current/stale attestation and missing HTS.
- [ ] **Step 2: Run focused tests** and verify failure.
- [ ] **Step 3: Implement the read-only descriptor builder** using the existing source reader/classifier and conservative availability labels.
- [ ] **Step 4: Render compact focusable evidence cards** with one source of metadata, clear customs/supplier scopes and exact existing hidden form fields/CSRF.
- [ ] **Step 5: Keep `Record review` disabled with the most specific unmet existing requirement**; server validation remains authoritative.
- [ ] **Step 6: Run focused P0/attestation contracts**; commit `feat: add compact evidence cards`.

### Task 3: Ephemeral secure evidence session

**Files:**
- Modify: `src/litoral_trace/static/js/us_lacey_source_inspector.js`
- Modify: `src/litoral_trace/templates/us_lacey/fragments/source_inspector.html`
- Modify: `src/litoral_trace/static/css/us_lacey_source_inspector.css`
- Test: `tests/test_us_lacey_evidence_preview_contract.py`
- Test: `tests/test_us_lacey_p0_applicability_source_inspector.py`

**Interfaces:**
- Produces `EvidenceSession` keyed by operation ID, source ID, page and source-set fingerprint with `acquire()`, `release()`, `invalidate()` and status callbacks.
- Consumes the existing same-origin PDF route and returns one in-memory response/renderer resource per live key.

- [ ] **Step 1: Add failing contract tests** for no eager request, one request across preview-to-pin, abort/discard on rapid change, no persistent storage, error mapping and no review POST.
- [ ] **Step 2: Run focused contracts** and verify failure.
- [ ] **Step 3: Implement `EvidenceSession`** with an `AbortController`, monotonic token, no-store fetch, content-type validation and deterministic resource release.
- [ ] **Step 4: Render distinct 401/403/404/409/503/network states** without placing HTML in a PDF frame and without suppressing the existing authorized-access audit.
- [ ] **Step 5: Invalidate on close, HTMX replacement, navigation, visibility lifecycle and source revision key change**; revoke object URLs/destroy renderer references only after final consumer release.
- [ ] **Step 6: Run contracts and P0 inspector tests**; commit `feat: add ephemeral evidence session`.

### Task 4: Real-PDF renderer decision and local packaging

**Files:**
- Modify when required: `package.json`, `package-lock.json`, `scripts/copy_frontend_vendor.mjs`
- Create when required: `docs/us-lacey/SMART_ENTRY_EVIDENCE_VIEWER_DECISION.md`
- Modify: `src/litoral_trace/static/js/us_lacey_source_inspector.js`
- Test: dedicated Playwright real-PDF acceptance suite

**Interfaces:**
- Consumes `EvidenceSession` bytes; produces `render(page, zoom)`, `nextPage()`, `previousPage()`, `setZoom()` and `destroy()`.

- [ ] **Step 1: Exercise the native iframe with an authenticated authorized multi-page fixture** in supported desktop/mobile browsers and record whether page/zoom can be controlled reliably.
- [ ] **Step 2: Document the decision, browser/version conditions, fixture, byte count and observed first-render timing.**
- [ ] **Step 3: If native control fails, write failing renderer acceptance tests and install a version-pinned local PDF.js package** through the existing vendor build only; no CDN/remote worker.
- [ ] **Step 4: Implement actual canvas page rendering, bounded zoom, current-page and previous/next controls** using one in-memory PDF document, with PDF.js worker/page/task destruction on release.
- [ ] **Step 5: Build assets and run the real-PDF acceptance test**; commit either `docs: record native PDF viewer decision` or `feat: add local PDF viewer`.

### Task 5: Hover preview, desktop pinning and mobile dialog

**Files:**
- Modify: `src/litoral_trace/templates/us_lacey/fragments/source_inspector.html`
- Modify: `src/litoral_trace/static/js/us_lacey_source_inspector.js`
- Modify: `src/litoral_trace/static/css/us_lacey_source_inspector.css`
- Test: Playwright suite for desktop, keyboard and mobile states

**Interfaces:**
- Consumes `EvidenceCardDescriptor` and `EvidenceSession`.
- Produces presentation states `CLOSED`, `PREVIEW`, `PINNED_DESKTOP`, `PINNED_MOBILE` only.

- [ ] **Step 1: Write browser tests** for 180 ms hover/focus opening, 250 ms bridge persistence, Escape, click/Enter pin, touch direct-pin, reduced motion and 390/768/1280/1440 layouts.
- [ ] **Step 2: Run the browser suite** and verify the new state tests fail.
- [ ] **Step 3: Implement viewport-clamped nonmodal preview** with 140–180 ms fade/6–10 px motion, pointer bridge and reachable `Open full inspector` action.
- [ ] **Step 4: Implement desktop nonmodal right-side panel** with 40/60 useful minimum widths, independent document scroll and review retained; implement mobile full-screen dialog with focus return.
- [ ] **Step 5: Wire functional close/open-new-tab/page/zoom controls** and coordinate teardown through `EvidenceSession`.
- [ ] **Step 6: Run browser and existing UX tests**; commit `feat: add hover preview and pinned evidence inspector`.

### Task 6: Attestation safety, Pack acceptance and release checks

**Files:**
- Modify: `tests/test_us_lacey_p0_applicability_source_inspector.py`
- Modify: `tests/test_us_lacey_p0_applicability_attestation.py`
- Create: browser fixture/tests under the repository’s existing frontend test convention
- Modify: docs/runbook only if CI invocation needs a documented command

**Interfaces:**
- Consumes complete UI/reader behavior and existing `record_applicability_attestation()` authority unchanged.

- [ ] **Step 1: Add regression tests** proving interaction never creates attestation, stale/deleted/cross-tenant/SHA-mismatched sources fail closed, derived worksheets cannot satisfy bamboo evidence and filing stays blocked.
- [ ] **Step 2: Add authenticated Pack 1/Pack 2 real-PDF tests** for page correctness, zoom/page navigation and current/stale/expired states using synthetic or authorized originals.
- [ ] **Step 3: Run focused pytest, browser tests and CSS build**; fix regressions without changing regulatory semantics.
- [ ] **Step 4: Run `python -m compileall -q src/litoral_trace`, full pytest, CSS Drift/visual gate and U.S. Lacey PostgreSQL gate.** Record exact commands and outcomes.
- [ ] **Step 5: Capture real shipment-page screenshots and measured render timings**; commit `test: cover smart evidence review acceptance`.

### Task 7: PR, gate and deployment handoff

**Files:**
- Create: PR description/release evidence from existing repository conventions

- [ ] **Step 1: Review the complete branch diff against `feature/us-lacey-pilot-platform`** for unauthorized authority/storage/RLS changes.
- [ ] **Step 2: Open a PR targeting `feature/us-lacey-pilot-platform`** with real-PDF screenshots, viewer decision, Pack results and gate outputs.
- [ ] **Step 3: Run/observe required CI and PostgreSQL gates.** Do not merge/deploy while any are pending or failed.
- [ ] **Step 4: If all gates and user authorization permit deployment, verify exact web/worker commit plus `/health` and `/ready`; otherwise report the exact external blocker and leave production unchanged.**

## Reconciliation Record

- The local specification’s former desktop-modal wording is superseded by the approved requirement to keep the review visible: desktop pinning is nonmodal; mobile is modal.
- PR #447’s plan is retained as source research. This local plan adds the required plan header, test-first task structure and the reconciled desktop/mobile distinction.
- The base remains `4d229b306d65f46a949cc32e48bc571858c81123`; the local specification commit `bbb9fa95` is preserved and not overwritten.
