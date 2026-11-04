# DI-03 Complete Party Labels Implementation Plan

> For agentic workers: execute inline with superpowers:executing-plans and test-driven-development.

**Goal:** Prevent a generic Consignee/Shipper prefix from becoming part of a complete Name label value.
**Architecture:** Longest complete labels and explicit separators; structured label/value data is not reinterpreted by the inline fallback. Preserve regex disagreements and existing candidate de-duplication by value/block.
**Tech stack:** Existing Python 3.11 / pytest; no dependencies.
**Spec:** Current DI-03 user request and DI01_ACCEPTANCE_CLOSURE.md evidence.

## Constraints and review focus
- Base 1a245702; independent branch fix/di03-consignee-label-parser.
- Do not touch DI-01 worktree/test, DI-02, caches, migrations or production configuration.
- Read the private PDF in place; never copy or commit it.
- Complete labels, punctuation, empty headings, legitimate names beginning Name, real conflicts and logical source provenance must remain distinguishable.
- Do not revise expected values to conceal another PDF defect; hold NO-GO if encountered.

## Task 1 - parser and contracts
- [x] Add failing synthetic tests and reproduce private PDF using process_bundle before production edits.
- [x] Apply minimal complete-label/fallback correction in pipeline.py.
- [x] Verify duplicate parsing routes, conflicts, source anchors and logical identities.
- [x] Compare private PDF base/corrected outputs; preserve all unaffected resolutions.
- [x] Run focused, golden/adversarial and applicable CI/PostgreSQL gates.
- [x] Review diff and preservation hashes; commit/push/PR only when acceptance conditions pass.


## Verification ledger (2026-10-29)

- Baseline before edits: 20 failing / 11 passing synthetic cases; private PDF
  process_bundle acceptance fails on consignee_name CONFLICT.
- Final same 40 contracts against base: 24 failing / 16 passing. Corrected: 40/40.
- Review identified two Important cases (alternate valid label disagreement and
  bilingual Shipper alias). Both reproduced red (5 failures / 2 controls) and
  corrected; seven added cases now pass. No extra correction outside party parsing.
- Final engine/golden/adversarial suite: 461 passed, four conditional skips.
  Three PostgreSQL golden cases ran separately; private PDF was read in place.
- Final general CI: 2739 passed, 366 existing conditional skips; no new skip added.
- PostgreSQL application contracts: 63 passed; E2E/golden: 10 passed. Zero skipped.
- Syntax and Alembic head checked; migrated local test cluster is at 079.
- Private PDF: same 2737 blocks, 159 table rows, logical ids/pages/types unchanged.
  All fields except consignee_name/shipper_name identical to baseline snapshots.
  Four correct consignee anchors retained exactly, including p5-l14; two spurious
  Name-prefixed interpretations removed. Target name is MATCHED; shipment
  conflicts go from 1 to 0. Shipper Name has the same prefix defect corrected.
- Complete inherited PDF acceptance STILL FAILS on the previously documented
  address representation suffix `; US`. No address expectation or normalization
  was changed. This is not a new regression introduced by DI-03.

## Acceptance closure (2026-10-29)

The original NO-GO above records the inherited address reference failure, not a
new address regression. Source inspection confirms literal Country Code US at
p5-l19 and p5-l27 within the same consignee group. Both base and DI-03 already
assemble the same DERIVED address including this country. The old expected value
omitted it and is corrected only in the test, preserving its exact prior body.

Name and address now have independently runnable contracts; the complete golden
regression remains. Tests use process_bundle and can read the private PDF in place
through a test-only environment path. See docs/us-lacey/DI03_PARSER_ACCEPTANCE.md.
No address parser, canonical, cache, schema or production configuration changes.

User authorization for this closure permits commit/push/PR with independent,
pre-existing failures documented. Publication requires the current focused/CI/
PostgreSQL checks and a clean reviewed allowlist; no merge or deployment.

## Final closure verification ledger

- Same 40 parser contracts: base 24 failed / 16 passed; DI-03 40 passed.
- Three independent/integral real-PDF contracts: base address passes, name and
  integral fail; DI-03 all three pass. Combined: base 26 failed / 17 passed;
  DI-03 43 passed. Old incomplete address reference remains red on both SHAs
  in the retained external audit, independently of the name correction.
- Focused engine/golden/adversarial/benchmark/lifecycle: 464 passed / 3 skipped.
  The three conditional PostgreSQL golden tests ran in the PostgreSQL group.
- Full local CI equivalent: 2742 passed / 365 conditional skips / 5 warnings.
  No new skip condition; two new independently runnable PDF tests. All three
  real-PDF contracts ran locally using the original external fixture path.
- PostgreSQL canonical/source-set/CAS/RLS/dossier: 63 passed / 0 skipped.
  PostgreSQL worker/E2E/Pack1/Pack2/plywood: 10 passed / 0 skipped.
- Syntax (main.py/src/tests), one Alembic head 079 and diff --check passed.
- A first PostgreSQL closure attempt omitted required test runtime variables;
  its failures/logs are retained outside Git. Repeating with explicit local test
  configuration passes. No production configuration or parser change was needed.
- GO for the exclusive DI-03 PR after allowlist/integrity verification. Merge
  remains subject to remote CI and review. No merge, deployment or V2 activation.

Private PDFs, rendered excerpts, local probe scripts, raw evidence snapshots,
JUnit/logs and databases remain outside Git. Full command arrays and results are
retained in the local DI-03 closure audit directory.
