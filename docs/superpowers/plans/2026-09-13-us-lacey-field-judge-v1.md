# U.S. Lacey Field Judge V1 — Implementation Plan

**Base:** `feature/us-lacey-pilot-platform@944f48c9dbe11b8f622a5826e54571cc55a80669`

**Sprint branch:** `feature/us-lacey-field-judge-v1`

**Design:** `docs/superpowers/specs/2026-09-13-us-lacey-field-judge-v1-design.md`

## Goal

Deliver a fail-safe semantic Judge for the specialized U.S. Lacey extractor, while first repairing the two structural conditions that invalidate the current Pack 3 benchmark: independent UNKNOWN uploads being superseded and multi-line evidence lacking an explicit deterministic projection path.

## Delivery rules

- Test-first for every behavior change.
- GitHub Actions is the authoritative runner.
- Judge defaults `off`.
- Specialized projection defaults `off`.
- No model-generated business value is permitted.
- No AI suggestion is represented as human confirmation.
- No benchmark claim is made without a reproducible corpus and measured output.

## Task 1 — P0 multidocument current-link semantics

**Files**
- Modify: `src/litoral_trace/us_lacey/operations.py`
- Test: reuse the nearest existing U.S. Lacey operation-service test module; add a focused module if no suitable test exists.

**RED tests**
1. Attaching two distinct `UNKNOWN` assurance documents leaves both links `is_current=True`.
2. Reattaching the exact same `UNKNOWN` assurance document returns the same current link and does not duplicate it.
3. Attaching two distinct `BILL_OF_LADING` documents keeps explicit semantic-role versioning: first historical, second current.
4. Distinct `OTHER` documents are preserved independently for the same safety reason as UNKNOWN.

**GREEN implementation**
- First perform exact-current-assurance idempotence lookup.
- Only apply role replacement/versioning to explicit semantic roles.
- Preserve unrelated current UNKNOWN/OTHER links.
- Recompute operation document count from current links.

## Task 2 — Field Judge closed domain contract and mode parser

**Files**
- Create: `src/litoral_trace/lacey_engine/multi_agent/field_judge.py`
- Create: `tests/lacey_engine/test_field_judge.py`
- Modify: `src/litoral_trace/lacey_engine/multi_agent/__init__.py`

**RED tests**
1. `LT_AI_FIELD_JUDGE_MODE` accepts `off`, `shadow`, `enforce` case-insensitively.
2. Missing/invalid values resolve to `off`; invalid values log a warning.
3. Decision contract accepts only closed decisions/reasons.
4. Candidate request identity is stable and contains no writable business-value field.
5. Provider output with an unknown candidate ID degrades that expected candidate to `NEEDS_REVIEW`.
6. Field-key mismatch, line-key mismatch, duplicate decision, malformed output, or provider exception degrade safely to `NEEDS_REVIEW`.
7. An unverified candidate can never become `ACCEPT`.

**GREEN implementation**
- Add `FieldJudgeMode`, `FieldJudgeDecisionState`, `FieldJudgeReasonCode`.
- Add immutable request/decision/run dataclasses or Pydantic closed response model as appropriate.
- Use `candidate_identity()` as the only candidate reference.
- Add a Python validation barrier that reconciles provider output to the exact expected candidate set.

## Task 3 — Gemini Field Judge adapter

**Files**
- Modify/create inside `src/litoral_trace/lacey_engine/multi_agent/field_judge.py` or a narrow adapter module if separation is clearer.
- Test: `tests/lacey_engine/test_field_judge.py`

**RED tests**
1. Request schema forbids generated business values.
2. Adapter uses only existing candidate metadata/evidence.
3. Gemini usage metadata is propagated to Judge telemetry.
4. Non-JSON/invalid schema is contained by the barrier.

**GREEN implementation**
- Reuse existing Gemini transport helpers and `AIProviderConfig`.
- Require structured JSON response.
- Set a low/zero creative budget where supported by the current transport contract.
- Record latency and token usage.

## Task 4 — Insert Judge seam after binding and before fusion

**Files**
- Modify: `src/litoral_trace/lacey_engine/multi_agent/orchestrator.py`
- Modify: `tests/lacey_engine/test_multi_agent_orchestrator.py`

**RED tests**
1. Judge/gate receives candidates whose `line_item_key` is already bound.
2. Fusion receives the gate output, not the pre-gate candidates.
3. No gate preserves current behavior exactly.

**GREEN implementation**
- Add one optional candidate-gate callback after `bind_line_items()` and before `fuse_candidates()`.
- Keep deterministic role ordering and partial-failure semantics unchanged.

## Task 5 — Specialized shadow integration and telemetry

**Files**
- Modify: `src/litoral_trace/us_lacey/specialized_shadow.py`
- Modify: `src/litoral_trace/us_lacey/lacey_engine_service.py` only if orchestration/persistence plumbing is required.
- Modify: `tests/lacey_engine/test_specialized_shadow.py`

**RED tests**
1. `off`: Judge is never called and specialized fused candidates are unchanged.
2. `shadow`: Judge runs and telemetry/decisions are captured, but fusion uses the original verified+bound candidate set.
3. `enforce`: only `ACCEPT` candidates are eligible for fusion.
4. Judge provider failure in `shadow` does not fail the specialized operation.
5. Serialized specialized payload contains Judge version/mode/status/counts/latency/tokens and bounded decisions.
6. Specialized engine identity changes when Judge effective mode/version changes.

**GREEN implementation**
- Build Judge once per operation after specialist extraction.
- In shadow, evaluate with a non-mutating gate.
- In enforce, gate to ACCEPT only.
- Persist Judge telemetry in the existing specialized JSON payload.
- Do not change legacy projection authority.

## Task 6 — Deterministic multi-line materialization planner

**Files**
- Create: `src/litoral_trace/us_lacey/specialized_projection.py`
- Create: `tests/us_lacey/test_specialized_projection.py` or nearest existing U.S. Lacey test location.

**RED tests**
1. Distinct non-empty `line_item_key` values produce distinct deterministic target-line references.
2. Re-running the planner is idempotent.
3. Existing human-created line references are retained and never reordered/deleted.
4. Non-line candidates cannot create a plant line.
5. Fingerprint-only keys remain reviewable but are not auto-materialized unless the mapping is unambiguous under the defined policy.

**GREEN implementation**
- Add a pure planning layer first.
- Materialize only stable line identities (`SKU`, `LINE`, and unambiguous `ROW` under deterministic source identity).
- Keep fallback fingerprint lines out of automatic materialization in V1 unless explicitly proven safe.

## Task 7 — Safe specialized projection gate

**Files**
- Modify: `src/litoral_trace/us_lacey/specialized_projection.py`
- Integrate from worker/service only after tests establish authority boundary.
- Tests in the same module.

**Configuration**
`LT_AI_SPECIALIZED_PROJECTION_MODE=off|shadow|enforce`, default `off`.

**RED tests**
1. `off` performs no projection.
2. `shadow` computes decisions/counters without mutating `UsLaceyOperationField`.
3. `enforce` can create/update only a `FOUND` suggestion when candidate is evidence-verified, non-inferred, PPQ-valid, unambiguous, target unreviewed, and non-conflicting.
4. Existing human-reviewed value is immutable.
5. Competing accepted values for one target remain `REVIEW`; no silent winner.
6. Shipment fields and plant-line fields cannot cross scopes.
7. Generated suggestions remain unconfirmed until existing human review action accepts them.

**GREEN implementation**
- Reuse `validate_ppq_value()`.
- Reuse current field-candidate provenance/fingerprints.
- Never set `reviewed_at`, `reviewed_by_user_id`, or `human_value`.

## Task 8 — Regression benchmark harness

**Files**
- Add deterministic fixtures under the existing test fixture convention.
- Add focused benchmark assertions rather than brittle whole-report snapshots.

**Gates**
1. Seven uploaded source documents remain seven current documents.
2. Known BOL trap values (`Vessel`, `POD`, `ETA`, `Gross Weight`) cannot be safe BOL suggestions.
3. Intentionally contradictory authoritative evidence remains a conflict/review condition.
4. Pinus and Eucalyptus line evidence remains separated when a stable line identity exists.
5. False-safe count is zero on the committed regression corpus.

## Task 9 — Full verification and PR

1. Run all PR GitHub Actions workflows on the RED test commit and capture the expected failing evidence.
2. Implement minimal GREEN behavior task by task.
3. Re-run targeted and full CI through the PR.
4. Inspect failed job logs rather than guessing when a check is red.
5. Review changed-file diff for authority-boundary regressions and accidental scope creep.
6. Final state must retain:
   - Judge default `off`;
   - specialized projection default `off`;
   - legacy UI behavior intact unless the explicit projection gate is enabled.
7. Report exact branch, commits, PR number, checks, and any deliberately deferred cutover work.
