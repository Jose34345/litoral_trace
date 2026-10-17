# US Lacey Commercial Reliability V2

**Date:** 2026-09-13
**Base:** `46188306f313299ae25e48b458b66488bccd5f62`
**Branch:** `feature/us-lacey-commercial-reliability-v2`

## Objective

Make U.S. Lacey document preparation resilient enough for assisted commercial pilots without allowing one unreadable source or one legitimate second product line to poison the whole dossier.

Primary safety invariant: **false-safe = 0**.

## Confirmed Pack 3 failures

1. Seven uploaded/current documents now persist correctly.
2. Image-only scans require OCR, but the current Render native build does not provide the Tesseract executable.
3. A document-level Engine 2 exception currently persists a FAILED run and returns immediately, preventing later documents, shipment aggregation and specialized/Field Judge shadow execution.
4. Repeated processing of the same failed document can collide with the immutable document-run uniqueness constraint.
5. Multilingual document classification is weak for BOL, packing list, entry worksheet and arrival notice.
6. Two legitimate product rows exist but cross-contaminate HTS, genus/species and descriptions during aggregation/binding.
7. Legacy review candidates still contain semantic pollutants such as `Vessel`, `POD`, `ETA` and `Gross Weight` for bill of lading.

## Design principles

- Keep exact evidence/provenance and fail closed.
- A failed document means `unknown because source unreadable`, not authoritative absence.
- Continue processing independent documents after one document failure.
- Never promote evidence from an unreadable or unverified source.
- Preserve row/table identity through line binding before fusion.
- Keep specialized/Field Judge changes non-authoritative until golden-pack metrics pass.
- Do not silently convert AI-supported values into human/legal confirmation.

## Execution plan

### Phase 1 — document-processing resilience

1. Add RED tests for a per-document Engine 2 failure followed by a successful document.
2. Add RED test for repeated failed processing being idempotent.
3. Change `resolve_operation_with_engine2` so failed documents are persisted/reused without aborting subsequent documents.
4. Aggregate only successfully resolved documents.
5. Represent incomplete source sets explicitly as partial/blocked telemetry; do not claim full success.
6. Verify worker completion semantics remain safe.

### Phase 2 — OCR runtime reliability

1. Add a deterministic OCR dependency/runtime smoke check.
2. Harden PDF layout inspection so metadata/image-layout library errors cannot crash the whole page when OCR fallback can proceed.
3. Verify repository Docker runtime contains `tesseract-ocr` and add/adjust CI coverage for `tesseract --version` plus an image-only PDF smoke test.
4. Do not switch the live Render service to Docker in this PR.

### Phase 3 — multilingual classification

Add golden classifier fixtures for Commercial Invoice, Bill of Lading, Packing List, Botanical Declaration, Supplier Origin Declaration, Entry Worksheet and Arrival Notice in EN/ES/PT. Filename is supporting evidence, never sole regulatory evidence.

### Phase 4 — row/line identity

1. Preserve table id + row index/SKU identity on extracted candidates.
2. Bind HTS, genus, species, harvest country, description, quantity, unit and entered value to the same line before shipment fusion.
3. Pack 3 gates: `PT-38` and `EG-22` remain separate and do not conflict merely because values differ across lines.

### Phase 5 — semantic pollution controls

Add deterministic negative gates so labels/values such as `Vessel`, `POD`, `POL`, `ETA`, `Gross Weight` and party labels cannot become a BOL candidate. Prefer missing/review over a wrong auto-supported value.

### Phase 6 — Field Judge + projection shadow

Run the existing/specialized judge only after exact evidence verification, persist its decisions separately, and compare legacy vs specialized projection without changing current authority unless golden evaluations prove safe.

### Phase 7 — golden regression

- Pack 1: clean shipment, no fabricated conflicts.
- Pack 2: preserve true value, ETA and harvest-country conflicts.
- Pack 3: seven documents accounted for, two correct plant lines, correct BOL/container/ETA, zero semantic BOL pollutants, zero false line conflicts.

## Verification gates

1. Targeted unit/integration tests RED before each behavior change and GREEN after implementation.
2. PostgreSQL integration gate passes on exact head SHA.
3. Full GitHub Actions CI passes on the same exact head SHA.
4. Compare branch against `feature/us-lacey-pilot-platform` and inspect the full PR diff.
5. Open PR only. No merge or production deployment without explicit authorization.
