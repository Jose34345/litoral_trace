# U.S. Lacey Hito 8 Regulatory Rules Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build deterministic, versioned, non-canonical `DE_MINIMIS` and `SPECIAL_COMPOSITE` assessments with source-set-scoped persistence, worker/UI integration and PostgreSQL tenant isolation.

**Architecture:** Pure rule contracts/evaluators live under `src/litoral_trace/us_lacey/regulatory/rules/`. A separate `regulatory_assessment_snapshot.py` adapter translates only supported Product Intelligence/operation facts into explicit rule inputs and persists immutable source-set-versioned assessment snapshots. Rule outputs remain advisory/non-canonical and never write PPQ505, LAWGS or canonical shipment truth.

**Tech Stack:** Python 3.11, dataclasses/Enum/Decimal/hashlib/json, SQLAlchemy 2.x, PostgreSQL 17 + RLS, Alembic, pytest, existing U.S. Lacey worker/HTMX UI.

**Spec:** `docs/superpowers/specs/2026-09-17-us-lacey-regulatory-rules-h8-design.md`

## Global Constraints
- False-safe count must remain 0.
- `PASS` / `FAIL` / `INDETERMINATE` are rule-scoped states, not shipment compliance states.
- Missing/ambiguous/unsupported facts yield `INDETERMINATE`.
- Use `Decimal` for regulatory arithmetic; 5.00% and 2.900 kg are inclusive thresholds.
- Protected-plant status must be explicit; `UNKNOWN` yields `INDETERMINATE`.
- SPECIAL/COMPOSITE requires explicit due-care species-determinability input; material name alone cannot establish due care.
- No fuzzy material or taxonomy guessing.
- No writes to canonical shipment truth, PPQ505, LAWGS/ACE or billing/auth paths.
- Migration 049 is immutable; schema changes use revision 050.
- Any new tenant-owned persistence requires RLS + FORCE RLS and negative cross-tenant tests.

---

### Task 1: Pure rule contracts and de minimis evaluator

**Files:**
- Create: `src/litoral_trace/us_lacey/regulatory/rules/domain.py`
- Create: `src/litoral_trace/us_lacey/regulatory/rules/de_minimis.py`
- Create: `src/litoral_trace/us_lacey/regulatory/rules/__init__.py`
- Test: `tests/test_us_lacey_regulatory_rules.py`

**Interfaces:**
- Produces `RuleStatus`, `EvidenceRef`, `ProtectedPlantStatus`, `RuleAssessment`, `DeMinimisInput`, `evaluate_de_minimis()`.

- [ ] **Step 1: Write failing rule tests**

```python
def test_de_minimis_exact_boundaries_pass():
    result = evaluate_de_minimis(DeMinimisInput(
        subject_ref="SKU-1",
        hts10="9401692010",
        plant_mass_per_unit_kg=Decimal("0.500"),
        total_unit_mass_kg=Decimal("10.000"),
        entry_same_hts_plant_mass_kg=Decimal("2.900"),
        protected_status=ProtectedPlantStatus.CLEAR,
    ))
    assert result.status is RuleStatus.PASS
    assert result.rule_id == "DE_MINIMIS"


def test_de_minimis_missing_or_unknown_fails_closed():
    result = evaluate_de_minimis(DeMinimisInput(
        subject_ref="SKU-1", hts10=None,
        plant_mass_per_unit_kg=None, total_unit_mass_kg=None,
        entry_same_hts_plant_mass_kg=None,
        protected_status=ProtectedPlantStatus.UNKNOWN,
    ))
    assert result.status is RuleStatus.INDETERMINATE
    assert result.review_required is True
```

Also cover 5.01%, 2.901 kg, protected PRESENT, invalid/zero total mass, and malformed HTS10.

- [ ] **Step 2: Verify RED**
Run focused pytest; expected import/module failures before implementation.

- [ ] **Step 3: Implement immutable contracts and evaluator**
Use `Decimal`, stable reason codes and explicit calculation trace. `PASS` only if all required facts are present and both numeric thresholds plus protected status satisfy the APHIS rule.

- [ ] **Step 4: Verify GREEN**
Run focused rule tests.

- [ ] **Step 5: Commit**
`feat(lacey): add deterministic de minimis rule`

### Task 2: SPECIAL / COMPOSITE evaluator and deterministic exact material facts

**Files:**
- Create: `src/litoral_trace/us_lacey/regulatory/rules/special_composite.py`
- Modify: `src/litoral_trace/us_lacey/regulatory/rules/domain.py`
- Modify: `src/litoral_trace/us_lacey/regulatory/rules/__init__.py`
- Test: `tests/test_us_lacey_regulatory_rules.py`

**Interfaces:**
- Produces `TriState`, `SpecialCompositeInput`, `classify_composite_material_name()`, `evaluate_special_composite()`.

- [ ] **Step 1: Add failing tests**

```python
def test_special_composite_pass_requires_due_care_fact():
    facts = classify_composite_material_name("MDF")
    result = evaluate_special_composite(SpecialCompositeInput(
        subject_ref="SKU-1:panel",
        small_fibers_more_than_one_plant_kind=facts.small_fibers_more_than_one_plant_kind,
        mechanically_processed_mixed_chemically_bonded=facts.mechanically_processed_mixed_chemically_bonded,
        thin_solid_plies_or_layers=facts.thin_solid_plies_or_layers,
        species_determinable_after_due_care=TriState.NO,
    ))
    assert result.status is RuleStatus.PASS


def test_plywood_is_not_special_composite():
    facts = classify_composite_material_name("plywood")
    assert facts.thin_solid_plies_or_layers is TriState.YES
    assert evaluate_special_composite(...).status is RuleStatus.FAIL
```

Cover HDF, OSB, particle board, paper/paperboard/cardboard, unknown material => UNKNOWN, due care UNKNOWN => INDETERMINATE, species determinable YES => FAIL.

- [ ] **Step 2: Verify RED**
- [ ] **Step 3: Implement exact curated aliases only; no fuzzy matching**
- [ ] **Step 4: Verify GREEN**
- [ ] **Step 5: Commit**
`feat(lacey): add special composite rule`

### Task 3: Regulatory snapshot model and migration 050

**Files:**
- Create: `src/litoral_trace/db/models/us_lacey_regulatory_assessment.py`
- Modify: `src/litoral_trace/db/models/__init__.py`
- Create: `alembic/versions/050_add_lacey_regulatory_assessment_snapshots.py`
- Test: `tests/test_us_lacey_regulatory_assessment_snapshot.py`
- Test: `tests/test_us_lacey_regulatory_assessment_snapshot_postgres.py`

**Interfaces:**
- Produces SQLAlchemy model `UsLaceyRegulatoryAssessmentSnapshot`.

- [ ] **Step 1: Add failing model/metadata tests**
Assert table name, unique source-set+ruleset key, statuses `CURRENT|STALE`, ruleset/input fingerprints and model export.

- [ ] **Step 2: Verify RED**
- [ ] **Step 3: Implement model and revision 050**
Migration mirrors 049 tenant hardening: tenant-aware FKs, RLS/FORCE RLS, four policies, revoke PUBLIC, runtime CRUD, no direct worker-executor table/sequence privilege.

- [ ] **Step 4: Add PostgreSQL negative tenant tests**
Tenant A insert/read is invisible/forbidden to tenant B; verify RLS and privileges.

- [ ] **Step 5: Commit**
`feat(lacey): persist regulatory assessment snapshots`

### Task 4: Source-set-scoped assessment adapter

**Files:**
- Create: `src/litoral_trace/us_lacey/regulatory_assessment_snapshot.py`
- Test: `tests/test_us_lacey_regulatory_assessment_snapshot.py`
- Test: `tests/test_us_lacey_regulatory_assessment_snapshot_postgres.py`

**Interfaces:**
- Produces `RULESET_VERSION`, `RegulatoryAssessmentView`, `build_regulatory_assessment_snapshot()`, `get_current_regulatory_assessment_view()`, `mark_regulatory_assessment_snapshots_stale()`, `fingerprint_rule_inputs()`.

- [ ] **Step 1: Add failing integration tests**
Require deterministic fingerprints, idempotency per source-set/ruleset, claim fencing, current-only reads, and stale supersession.

- [ ] **Step 2: Verify RED**
- [ ] **Step 3: Implement adapter**
Read current Product Intelligence snapshot and exact operation context. Build assessments only from supported inputs. If total unit mass, exact HTS10, protected status or due-care fact is absent, persist `INDETERMINATE` rather than deriving an unsafe value.

- [ ] **Step 4: Verify GREEN**
- [ ] **Step 5: Commit**
`feat(lacey): build source scoped regulatory assessments`

### Task 5: Worker integration and supersession lifecycle

**Files:**
- Modify: `src/litoral_trace/us_lacey/worker.py`
- Modify: `src/litoral_trace/us_lacey/source_sets.py` if stale lifecycle requires the existing source-set seam
- Test: `tests/test_us_lacey_regulatory_assessment_worker.py`

**Interfaces:**
- Worker ordering: canonical publication -> Product Intelligence -> regulatory assessment -> multilingual shadow -> source-set finalize -> COMPLETED.

- [ ] **Step 1: Add failing worker-order tests**
Verify stage ordering, exact claim handoff, stale generation cannot publish, and regulatory exception does not create a safe/current assessment.

- [ ] **Step 2: Verify RED**
- [ ] **Step 3: Integrate adapter at the existing Product Intelligence boundary**
Do not move canonical publication or source-set finalization.

- [ ] **Step 4: Verify GREEN**
- [ ] **Step 5: Commit**
`feat(lacey): run regulatory rules in worker`

### Task 6: Non-canonical operation UI

**Files:**
- Modify: `src/litoral_trace/web/us_lacey_operational_views.py`
- Test: `tests/test_us_lacey_regulatory_assessment_ui.py`

**Interfaces:**
- Current operation workspace renders a `Regulatory assessment` panel using current-only view.

- [ ] **Step 1: Add failing UI tests**
Assert PASS/FAIL/INDETERMINATE are labeled by rule and copy explicitly says the panel is not a final filing/compliance determination. Assert no global `shipment PASS` copy.

- [ ] **Step 2: Verify RED**
- [ ] **Step 3: Render compact panel with reasons/calculation trace**
- [ ] **Step 4: Verify GREEN**
- [ ] **Step 5: Commit**
`feat(lacey): show regulatory assessment panel`

### Task 7: Control-plane docs and CI/PostgreSQL gates

**Files:**
- Modify: `docs/us-lacey/CAPABILITIES.toml`
- Modify: `docs/us-lacey/ROADMAP.md`
- Modify: `docs/us-lacey/PIPELINE.md`
- Modify: `docs/us-lacey/TEST_MATRIX.md`
- Modify: `src/litoral_trace/us_lacey/AGENTS.md`
- Modify: `.github/workflows/ci.yml` only if architecture-contract head assertions require it
- Modify: `.github/workflows/us-lacey-postgres-gate.yml`
- Test: `tests/test_us_lacey_architecture_docs.py`

**Interfaces:**
- Canonical docs mark regulatory rules ACTIVE/NON_CANONICAL and make exception-first review the next milestone.

- [ ] **Step 1: Add/adjust architecture-contract tests**
- [ ] **Step 2: Update canonical docs**
- [ ] **Step 3: Extend PostgreSQL Gate**
Migrate 049 -> 050, expect Alembic head 050, include new table policy/privilege checks and run focused regulatory snapshot tests with skips forbidden.

- [ ] **Step 4: Run/observe full CI + PostgreSQL gate on exact PR head**
Expected: 0 failures; any red is debugged before merge.

- [ ] **Step 5: Commit**
`docs(lacey): activate deterministic regulatory rules`

### Task 8: PR review, verification and merge

**Files:** no production change unless review finds a defect.

- [ ] **Step 1: Open draft PR from `feat/us-lacey-regulatory-rules-h8` to `feature/us-lacey-pilot-platform`**
- [ ] **Step 2: Confirm RED-first evidence existed for new contracts**
- [ ] **Step 3: Make PR ready after focused tests are GREEN**
- [ ] **Step 4: Review all inline threads; fix valid findings with regression tests**
- [ ] **Step 5: Re-run/observe fresh CI and U.S. Lacey PostgreSQL Gate on final exact SHA**
- [ ] **Step 6: Merge only when PR is mergeable, review blockers resolved, and both gates are GREEN**

