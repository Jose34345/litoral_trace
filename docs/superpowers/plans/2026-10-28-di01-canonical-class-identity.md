# DI-01 canonical class identity implementation plan

> For agentic workers: execute inline with superpowers:executing-plans and test-driven-development.

**Goal:** Reject structural equivalence classes that would collapse distinct rows from the same document, without changing extraction, source authority, DI-02 or caches.

**Architecture:** Keep current relationship discovery. Validate the complete connected class before exposing any union. An incompatible class abstains as a whole, preserving each source-local identity and its evidence instead of selecting a greedy partial match.

**Tech stack:** Existing Python 3.11, pytest, PostgreSQL 17 gates; no new dependencies.

**Spec:** DI-01 audit of 1a245702 and its recovered transitive same-document regression; current user implementation scope.

## Global constraints
- Dedicated branch/worktree from feature/us-lacey-pilot-platform.
- No changes to DI-02, caches, migrations, source-set lifecycle, RLS or regulatory authority.
- No merge or deployment; publish PR only if required local checks pass.

## Review focus
- Ambiguous connected classes must not retain an arbitrary partial union.
- Evidence order must not select a different matching.
- Repeated HTS/values must not collapse distinct same-document rows.
- Valid cross-document rows and their conflicts remain intact.
- Pack1/Pack2 and stale-generation guards remain intact.

## Task 1: Complete-class structural compatibility
- [x] Recover the audit fixture and execute the original DI-01 test before production edits (RED).
- [x] Add public-builder regression/positive tests for transitivity, order, repeated values, legitimate same-HTS rows and evidence preservation.
- [x] Apply the smallest complete-class compatibility guard in canonical_shipment_truth.py (GREEN).
- [x] Run focused identity, canonical, Pack1/Pack2 and source-set tests.
- [x] Update the owning capability and test matrix with this contract.
- [x] Run syntax, Alembic head, full CI pytest and applicable PostgreSQL gates; record blockers.
- [x] Review diff and publish only if safety/local gates pass.


## Verification ledger and review disposition

Base: `1a245702dc3ec1c750c0274b7ab4aaf004b92928` on
`feature/us-lacey-pilot-platform`; isolated branch `fix/di01-canonical-class-identity`.
No extraction, DI-02, cache, migration, RLS or source-set lifecycle changes.

- Recovered audit regression: RED (1 failure), three original rows collapsed into one.
- Initial public-builder contracts: RED (8 failures, 4 passes).
- Review found an Important gap: merely splitting rows could still publish FOUND
  and close a conflict. Added RED pure/PostgreSQL tests (2 failures) plus RED
  shipment-description composition test (1 failure), then retained an explicit
  identity-review flag through field state, shipment composition and conflict
  resolution. Existing CONFLICT/MISSING states and provenance are preserved.
- Review also found a standalone test import issue; fixed with a qualified helper
  import and verified the PostgreSQL file without custom test-path entries.
- Final focused suite: 98 passed, zero skipped, including 18 new pure DI-01 cases,
  recovered audit case, canonical/identity tests, Pack1/Pack2, benchmarks and lifecycle.
- General CI-style suite: 2715 passed, 367 skipped. PostgreSQL disabled for that
  invocation; integration gates were run separately. The private real-PDF test
  was absent and skipped in this run; its later result is recorded below.
- Applicable PostgreSQL gates: 64 passed, zero skipped (canonical publication,
  source sets, stale generations/CAS, Engine 2 persistence/dossier and tenant RLS).
- Additional PostgreSQL E2E/golden gates: 10 passed, zero skipped (upload/queue/
  worker/CSV/XLSX, Pack1 applicability, Pack2 and plywood golden cases).
- Python 3.11 syntax compilation and Alembic single head `079_us_lacey_assurance_v2_claim_snapshots`: passed.
- PostgreSQL 17.11/PostGIS 3.6 ran only in an isolated temporary local cluster;
  CI declares PostgreSQL 17/PostGIS 3.5. No production database was accessed.

### Blocker: recovered real-PDF regression (outside DI-01)

The locally available private fixture `import_info_wood_brokerage_real.pdf` was
copied from the other worktree without modifying its original. SHA-256:
`48bc6c4e6dbe7369d44c7b90b287b305216bf1b1e5ded91bcae722a41634e7c6`.
The copy remains **untracked and excluded from the commit**.

`test_real_import_info_gate1_regression_fixture` fails both on the base SHA and on
the final implementation: `process_document()` raises `LaceyEngineError` because
the physical PDF contains multiple logical documents and requires `process_bundle()`.
The failure occurs in unchanged `lacey_engine/pipeline.py:739`, before canonical
construction. No unrelated compatibility/test update was integrated.

### Environment diagnostics

Repeating all PostgreSQL fixtures on the original populated test database caused
three failures from global queue/run counts. A second migration attempt in another
empty database exposed an existing role-alteration privilege limitation when roles
already exist. Neither was bypassed in application code. Final gates used clean
schema clones inside the verified task-owned cluster, retaining the original test
database. The first E2E invocation lacked the shared fixture plugin because
`--noconftest` was used; registering the existing plugin explicitly produced 10/10.
All initial failing logs are retained alongside final results.

### Decision and risks

**NO-GO for push/PR under the requested all-gates condition.** DI-01 is locally
implemented and verified; the real-PDF gate needs a separately scoped decision/fix
and rerun before publication. No PR, merge or deployment was performed.

Rejecting an entire incompatible class intentionally retains three/four source-local
identities for review rather than guessing a partial match. Existing legitimate
cross-document joins and value conflicts remain unchanged. Previously persisted
outputs are not automatically reprocessed; any historical repair needs a separate,
reviewed replay decision. No claim of general identity correctness beyond DI-01.

Local evidence bundle: `%TEMP%/litoral_di01_20261028_223155/` (JUnit, exact commands,
red/green logs and before/after JSON). The bundle and private source PDF are not
included in Git. Revert the isolated implementation commit to roll back the fix;
no schema rollback is needed.
