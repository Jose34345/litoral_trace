# Issue 473 documentary quality implementation plan

> For agentic workers: execute inline, without further delegation. Use TDD and systematic debugging.

**Goal:** Recover explicitly documented merchandise descriptions and avoid a false consignee-address conflict caused by two physical renderings of one PDF cell.

**Architecture:** `lacey_engine` owns literal extraction. Keep existing shipment binding, canonical publication and review authority unchanged. A visual line may use the complete structured party cell only when exact text, label, page and physical bounding box establish the same source.

**Tech Stack:** Python, pdfplumber, pytest; existing XLSX parser.

**Spec:** User request, ENTREGA 2 issue #473; tracked synthetic quality-output fixtures.

## Constraints

No AI calls, customer documents, production DB, schema changes, authority expansion or automatic human confirmation. No change to DI-01/DI-02, canonical publication, OCR, source-set generation or review rules. A normal Engine 2 version bump is authorized to invalidate prior extraction identity. No commits/push until coordinator review.

## Review focus

- Partial address outside the matching physical cell remains a conflicting observation.
- Conflicting structured/visual party values are never reconciled by prefix alone.
- BOM composition is never synthesized into a commercial description.
- Entry worksheet evidence remains derived and cannot independently authorize review.
- Importer, MID, B/L, HTS, species, harvest country and entry reference remain non-regression controls.

### Task 1: Pin source evidence and reproduce

- [x] Run baseline existing DI-03, Pack 1/2 and 288 kg tests.
- [x] Read literal PDF blocks and XLSX cells from the three tracked fixtures.
- [x] Add `tests/lacey_engine/test_issue473_document_quality.py`: real PDF consignee name/address, explicit label and shipment descriptions, same-cell versus separate-cell conflicting addresses, and worksheet authority.
- [x] Verify RED before implementation; errors in setup do not count as RED.

### Task 2: Minimal upstream corrections

- [x] Add explicit `Description of Merchandise` label in `pipeline.py`, preserving existing table scopes and source text.
- [x] Preserve PDF key/value row geometry in `layout_parser.py`.
- [x] Add focused same-source party helper in `party_source.py`; integrate through `_append_party` without dropping independent observations.
- [x] Verify GREEN and source anchors through serialized shipment and canonical read model.

### Task 3: Regression evidence and report

- [x] Run relevant engine, projection, review-authority, canonical and adversarial tests offline, plus Pack 1/2 and 288 kg controls.
- [x] Add Spanish report with per-field before/after, literal page/cell evidence, normalization, association, authority and remaining blockers.
- [x] Return changed paths, actual command/results and GO/NO-GO to coordinator. PostgreSQL/CI/commit/PR handled by coordinator.

## Investigation refinement

The actual three-file packet keeps the existing Engine 2 XLSX PARTIAL boundary. A focused immutable literal-row normalizer was therefore also integrated into the legacy projector (two-line seam), with its own fixture RED/GREEN cycle. See `docs/us-lacey/P0_QUALITY473.md`. PostgreSQL/CI/commit/PR and independent review remain coordinator gates, not completion claims made here.
