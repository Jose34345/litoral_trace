# U.S. Lacey Smart Entry Guidance and Instant Evidence Preview

## Purpose and scope

Turn the existing P0 applicability and source inspector surfaces into a compact, source-led review experience for a customs broker. The user must understand the entry procedure, inspect the exact authorized original PDF with zero-click hover/focus intent, pin a full inspector on demand, and record an explicit, traceable review without changing any regulatory authority.

This is a presentation and interaction extension of the existing `us_lacey_applicability_source_inspector_p0` capability. Its authority remains `advisory_read_only_no_filing_waiver`; it does not persist entry type or bamboo facts as verified regulatory evidence, alter PPQ/LAWGS filing gates, or change canonical shipment truth.

## Existing contracts to preserve

- `GET /operations/{operation_public_id}/source/{assurance_document_id}` stays the only document-byte route. It performs tenant/current-source membership, retention, storage-integrity and SHA-256 checks, records legitimate document access, has `private, no-store`, and does not expose an object-store address.
- `record_applicability_attestation()` remains the only writer. It requires a finalized current source set, valid current PDF sources, a confirmed HTS, explicit confirmation and separate supplier-origin PDF evidence for cultivated/wild bamboo.
- The immutable human-review event remains advisory and has `filing_authorization: false`.
- Read/open/hover/zoom/page navigation are not human decisions and must never call a writer or mutate a regulatory state.
- Derived preparation worksheets remain informational and cannot become independent botanical or cultivation proof.

## Interaction model

### Smart entry type selector

Replace the visible raw-code dropdown with an accessible combobox/listbox that groups the current allowed CBP codes beneath plain-English navigation categories. Each result shows commercial label, code and one concise explanation. Searching matches both code and official title. `I'm not sure` clears the scenario entry type; it does not select a regulatory code.

The value submitted to the existing GET scenario is the exact existing code. No value is inferred from shipment value. Code `06` preserves the distinct FTZ/PGA-timing message; `86` preserves its official title and a current-eligibility warning; `02`, `03`, `07` and all presently supported codes retain their current handling.

### Evidence cards and closed state

Replace the long source controls with compact cards in the applicability area and workspace. Each has title, filename, page, conservative document class and availability state. A card exposes Preview and Open original in the same keyboard order, but the entire card can receive focus.

The page begins with no document request. The review control is visibly disabled with the most specific unmet requirement: current source unavailable, no confirmed HTS, unconfirmed entry type, missing supplier evidence for the selected bamboo claim, missing explicit checkbox, or existing server-side authorization failure.

### Hover/focus preview

On a fine pointer, hover intent of 180 ms begins a source request. Keyboard focus uses the same preview without moving focus into it. The ephemeral popover includes filename, page, source class, availability and the original PDF page as its primary surface. It is positioned after measuring the trigger and viewport, preferring the side with the most usable room and clamping within viewport margins.

The popover enters with a 160 ms opacity/6–8px transform transition. A 250 ms departure grace period prevents flicker while the pointer moves from card to popover. Leaving both closes it, clears the viewer resource and restores no focus. Escape closes it. `prefers-reduced-motion` eliminates transitions.

On coarse pointers there is no simulated hover: a first touch/click pins the inspector. Enter/Space from a focused card also pins it. The preview never makes the rest of the page inert or traps focus.

### Pinned inspector

Clicking a card, preview or explicit action opens a nonmodal right-side inspector at desktop widths so the review remains interactive and visible beside the document. It takes 55–65% of the viewport, leaving an approximately 40/60 review-to-document composition visible; below the responsive threshold it becomes a full-width modal dialog with focus handling and Escape-to-close.

Its compact header shows the document filename, page, classification and availability. PDF content dominates. Close, open in new tab, page previous/next and zoom controls must operate against the actual viewer rather than merely change labels.

## Viewer decision

The current browser-native iframe is retained as the secure byte transport fallback but cannot guarantee controllable page/zoom behavior across browsers. The implementation will first verify whether the browser controls satisfy the functional acceptance tests. If they do not, it will add a pinned, versioned local PDF.js distribution through the existing `package.json` vendor-copy build step; no CDN, remote script, document proxy or additional download endpoint is permitted.

PDF.js, if selected, fetches only the same-origin existing route after hover/focus/click. The inspection controller owns one ephemeral viewer session, uses an `AbortController`/monotonic request token to discard stale loads, reuses the live document while the same source remains open, and destroys the PDF.js document/page/render task and object references on close. No PDF bytes, data URLs or page images are stored in browser persistence. The initial page is the evidence-referenced page; navigation changes pages only inside the viewer and has no domain effect.

The server contract is not loosened. Each new network fetch continues to perform the existing source-set, RLS, retention, SHA-256 and access-audit checks. `401`, `403`, `404`, `409` and `503` render a concise inspector state and never render an authentication page as a PDF.

## Data and rendering boundaries

`triage.py` remains an advisory scenario evaluator. `attestation.py` remains the source-set-fenced authority boundary. `us_lacey_source_inspector.py` remains the sole source-byte controller; source-card metadata comes from the existing tenant-scoped source reader/classification path. Templates only render those supplied facts and presentation-derived states; they do not decide document authority, currentness or review readiness.

No migration, RLS change, source-set lifecycle change, regulatory rule modification or new canonical status is in scope. Presentation states (`Not reviewed`, `Evidence available`, `Awaiting supplier evidence`, `Review recorded`, `Evidence changed — review again`, `Original unavailable`) are derived from existing source, attestation and availability facts.

## Error handling and accessibility

- Current/authorized but loading: an immediate non-blocking loading state.
- 401/403: access is unavailable; do not expose source details.
- 404: original unavailable under current revision/retention; request new original evidence.
- 409: evidence changed; review current version before recording.
- 503: source temporarily unavailable; retry later.
- Keyboard: combobox arrows/typeahead/Enter/Escape, focus-preview, Enter/Space pinning, inspector focus containment and return to trigger on close.
- Touch: direct pinned open with touch targets; no hover-only essential control.
- Reduced motion: no preview or dialog transition.

## Verification plan

Add focused unit, integration and browser coverage for:

1. Exact entry-code preservation, unknown choice, code 06 FTZ copy, code 86 warning, special-code treatment and keyboard selector behavior.
2. No document request until intent; hover opens without click; preview bridge does not flicker; Escape, touch and reduced-motion behavior; obsolete preview work is cancelled/discarded.
3. Correct original/document page, unavailable/expired/stale/error states, cross-tenant denial, SHA mismatch, no storage URL exposure and no persistent client cache.
4. Real PDF rendering with Pack 2 source bytes: page selection, page navigation and zoom behavior in the pinned inspector.
5. Entry type/bamboo/supplier evidence consistency in all three visible states. A derived worksheet cannot satisfy a cultivation claim.
6. Opening and viewing source produces no review event or regulatory change; record review still requires current authorized evidence and explicit confirmation.
7. Existing P0 source-inspector/attestation, Pack 1/2, Assurance V2, CSS build/visual and PostgreSQL/RLS gates.

The feature is not ready to merge or deploy solely from unit or visual tests. It requires a manually observed authenticated Pack 2 original-PDF flow in a browser, followed by required CI and PostgreSQL gates.

## Non-goals

- No automatic approval, filing authorization, entry-code inference or new regulatory equivalence.
- No source-byte analytics, mouse-movement audit events or persisted client copy of originals.
- No React rewrite, external PDF service, CDN viewer or broad workspace redesign.
