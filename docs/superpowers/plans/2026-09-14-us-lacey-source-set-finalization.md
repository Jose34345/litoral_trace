# U.S. Lacey Source-Set Finalization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make an operation-level finalization occur exactly once for each explicitly sealed, current U.S. Lacey document source set.

**Architecture:** Persist an immutable source-set revision owned by an operation. Uploads build the revision before jobs become eligible, seal it with the canonical `source_set_fingerprint`, and bind each job to that revision. Under the existing operation advisory lock, a worker atomically claims finalization only when every revision member has completed; publication rechecks that the claimed revision is still current.

**Tech Stack:** Python, SQLAlchemy, PostgreSQL RLS, Alembic, pytest.

**Spec:** User-approved source-set lifecycle design in this Codex task, 2026-09-14.

## Global Constraints

- Reuse `lacey_engine_service.source_set_fingerprint`; do not define a parallel identity.
- Preserve Engine2, Judge, projection and draft PR #227 authority boundaries.
- No timed debounce, merge, deployment, or source-content logging.
- Use tenant/RLS context for every source-set query and mutation.

---

### Task 1: Central label defence for PDF B/L candidates

**Files:**
- Modify: `src/litoral_trace/us_lacey/ppq505.py`, `src/litoral_trace/us_lacey/projection.py`
- Test: `tests/test_us_lacey_structured_bol_regression.py`

- [ ] Write a failing test proving schema labels are invalid as B/L values while an adjacent PDF KV value remains valid.
- [ ] Add one shared schema-label normalizer used by validation and candidate admission; retain identifier validation and do not encode fixture values.
- [ ] Run the regression test and the specialized projection tests.

### Task 2: Persist source-set revisions and bind jobs

**Files:**
- Modify: `src/litoral_trace/db/models/us_lacey.py`, `src/litoral_trace/db/models/us_lacey_commercial.py`, `src/litoral_trace/db/models/__init__.py`
- Create: `alembic/versions/<revision>_us_lacey_source_set_revisions.py`
- Test: `tests/test_us_lacey_source_set_lifecycle.py`, PostgreSQL migration gate tests

- [ ] Write RED tests for tenant-isolated OPEN/SEALED revisions, current generation replacement, and one job identity per revision member.
- [ ] Add `UsLaceySourceSetRevision` and member identity with generation, canonical fingerprint, lifecycle status, claimed/finalized metadata and uniqueness per operation/generation.
- [ ] Add `source_set_revision_id` to processing jobs and migrate with restrictive tenant foreign keys and indexes.
- [ ] Run model/migration tests against PostgreSQL.

### Task 3: Seal batches before queue eligibility

**Files:**
- Modify: `src/litoral_trace/us_lacey/workflow.py`, `src/litoral_trace/us_lacey/operations.py`, `src/litoral_trace/us_lacey/jobs.py`
- Test: `tests/test_us_lacey_source_set_lifecycle.py`

- [ ] Write the 7-document RED test: AI calls stay zero for members 1–6 and are one only after all seven are attached, sealed and completed.
- [ ] Add a batch upload path that attaches all evidence, constructs one revision with the canonical fingerprint, seals it, then enqueues bound jobs.
- [ ] Make a later independent attachment create and seal generation N+1; retain N historically.
- [ ] Verify retry enqueue preserves the existing revision-bound job.

### Task 4: Atomic finalization claim and stale-publication guard

**Files:**
- Modify: `src/litoral_trace/us_lacey/worker.py`, `src/litoral_trace/us_lacey/lacey_engine_service.py`, `src/litoral_trace/us_lacey/shadow_evidence_snapshot.py`
- Test: `tests/test_us_lacey_source_set_lifecycle.py`, `tests/test_us_lacey_worker_operation_lock.py`

- [ ] Write RED tests for two finalizers, retry after finalized, mutation during readiness, and N completing after N+1.
- [ ] Under the operation lock, claim SEALED → FINALIZING with a conditional update; only the winner calls operation-level Engine2/AI/reconciliation/shadow work.
- [ ] Before active publication and completion, compare current revision id/fingerprint with the claim; mark a changed claim superseded without changing current operation state.
- [ ] Run worker unit tests and PostgreSQL RLS isolation test.

### Task 5: Verification and evidence

**Files:**
- Modify: `docs/us-lacey-production-stabilization.md`
- Test: focused source-set/BOL/cache/shadow tests and repository gates

- [ ] Add structured stage logging with revision, fingerprint, job, attempt and elapsed milliseconds; do not include document contents.
- [ ] Run targeted tests, full pytest, migration gate, US Lacey PostgreSQL gate and CI on one SHA.
- [ ] Record exact green evidence and remaining live-environment limitations without changing PR state.
