# U.S. Lacey Automatic BOM Flow Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build and persist a source-set-versioned Product Intelligence/BOM snapshot automatically during U.S. Lacey operation processing and expose it in the operation UI without changing canonical regulatory truth.

**Architecture:** Reuse the existing `product_intelligence` parser/ingestion contracts. Add one tenant-scoped immutable snapshot model and one focused U.S. Lacey orchestration/read module. Invoke it only at the existing source-set finalization barrier. Product Intelligence is additive and fail-safe: its errors become snapshot state and never publish canonical Lacey facts.

**Tech Stack:** Python 3.11, FastAPI/Jinja, SQLAlchemy 2, PostgreSQL 17, Alembic, pytest, existing Vault/Assurance parsers.

**Spec:** `docs/superpowers/specs/2026-09-17-us-lacey-automatic-bom-flow.md`

## Global Constraints

- Product Intelligence remains non-canonical.
- No PPQ505/LAWGS/canonical-shipment-truth/regulatory decision mutation.
- Reuse Assurance parsers; no second CSV/XLS/XLSX parser.
- Preserve source-set fencing, tenant scope, RLS/FORCE RLS and immutable migration history.
- Snapshot failure must not fail the established U.S. Lacey processing job.
- No new external dependency.

---

### Task 1: RED — persistence and source-set contract tests

**Files:**
- Create: `tests/test_us_lacey_product_intelligence_snapshot.py`
- Create: `tests/test_us_lacey_product_intelligence_snapshot_postgres.py`

**Interfaces:**
- Produces expected ORM class `UsLaceyProductIntelligenceSnapshot`.
- Produces expected service calls `build_product_intelligence_snapshot(...)`, `get_current_product_intelligence_view(...)`, `mark_product_intelligence_snapshots_stale(...)`.

- [ ] Write unit metadata/status/serialization tests first.
- [ ] Write PostgreSQL tenant-FK/RLS/idempotency/stale-generation tests first using existing U.S. Lacey test graph helpers.
- [ ] Commit tests only.
- [ ] Run CI and confirm RED because the model/service/migration do not yet exist.

### Task 2: GREEN — migration 049 and ORM model

**Files:**
- Create: `alembic/versions/049_add_lacey_product_intelligence_snapshots.py`
- Create: `src/litoral_trace/db/models/us_lacey_product_intelligence.py`
- Modify: `src/litoral_trace/db/models/__init__.py`

**Interfaces:**
- `UsLaceyProductIntelligenceSnapshot` fields exactly follow the approved spec.
- Composite tenant FKs reference operation and source-set revision.

- [ ] Implement migration 049 with one table, constraints, indexes, RLS/FORCE RLS and privilege hardening.
- [ ] Implement ORM model matching migration.
- [ ] Export model from `db.models`.
- [ ] Run focused metadata/schema tests and make them GREEN.

### Task 3: GREEN — snapshot builder and read model

**Files:**
- Create: `src/litoral_trace/us_lacey/product_intelligence_snapshot.py`
- Extend: `tests/test_us_lacey_product_intelligence_snapshot.py`

**Interfaces:**
- `build_product_intelligence_snapshot(*, organization_id: int, operation_id: int, claim: SourceSetClaim) -> UsLaceyProductIntelligenceSnapshot | None`
- `mark_product_intelligence_snapshots_stale(session, *, organization_id: int, operation_id: int) -> int`
- `get_current_product_intelligence_view(*, organization_id: int, operation_id: int) -> ProductIntelligenceView | None`

- [ ] Add failing tests for CSV READY, XLSX READY, NOT_APPLICABLE, PARTIAL, FAILED, idempotency and stale claim rejection.
- [ ] Implement exact source-set member loading.
- [ ] Materialize bounded original tabular files through existing Vault service and call existing Assurance parser + `ingest_bom_table`.
- [ ] Serialize `Decimal`, source anchors, compositions and issues deterministically into payload v1.
- [ ] Persist one snapshot per exact source-set revision.
- [ ] Implement current-only tenant-scoped read view.
- [ ] Run focused tests GREEN.

### Task 4: GREEN — supersession and worker integration

**Files:**
- Modify: `src/litoral_trace/us_lacey/source_sets.py`
- Modify: `src/litoral_trace/us_lacey/worker.py`
- Extend: `tests/test_us_lacey_product_intelligence_snapshot.py`
- Extend: `tests/test_us_lacey_source_set_lifecycle.py` or add focused worker contract tests.

**Interfaces:**
- Source-set seal marks earlier Product Intelligence snapshot metadata STALE.
- Worker invokes Product Intelligence only for `source_set_claim.claimed` generations.

- [ ] Add failing source-set supersession test.
- [ ] Add failing worker test proving one invocation on final source-set claim.
- [ ] Add failing worker test proving Product Intelligence exception is swallowed/logged and canonical queue completion remains unchanged.
- [ ] Implement minimal integration via focused wrapper `_build_product_intelligence_snapshot(...)` in worker.
- [ ] Execute snapshot build inside the existing operation projection lock after claim and before source-set finalization; do not feed its result into canonical publication.
- [ ] Run source-set/worker regressions GREEN.

### Task 5: GREEN — customer operation UI

**Files:**
- Modify: `src/litoral_trace/web/us_lacey_pilot_app.py`
- Modify: `src/litoral_trace/web/us_lacey_operational_views.py`
- Modify: `src/litoral_trace/templates/us_lacey/operation_detail.html`
- Create or extend: `tests/test_us_lacey_product_intelligence_ui.py`

**Interfaces:**
- `_detail_page` loads current Product Intelligence view independently of Engine 2 dossier.
- `render_operation_detail(..., product_intelligence=...)` passes safe view to template.

- [ ] Write failing render/HTTP contract tests first.
- [ ] Load current snapshot by tenant + operation id; read failure must not hide canonical review UI.
- [ ] Render Product Intelligence card with status/counts/SKU/components/material/source location.
- [ ] Add explicit copy: product-composition evidence, not final declaration data.
- [ ] Run UI tests GREEN.

### Task 6: GREEN — CI/PostgreSQL gate integration

**Files:**
- Modify: `.github/workflows/us-lacey-postgres-gate.yml`

**Interfaces:**
- Gate triggers on `src/litoral_trace/product_intelligence/**`, new model/migration/tests.
- Canonical Alembic head becomes `049_lacey_product_intelligence_snapshots`.

- [ ] Add path filters.
- [ ] Update canonical-head assertions from 048 to 049.
- [ ] Add new snapshot table to RLS policy count and direct privilege checks.
- [ ] Run non-skippable Product Intelligence PostgreSQL acceptance test in gate.

### Task 7: Documentation/control-plane update

**Files:**
- Modify: `src/litoral_trace/us_lacey/AGENTS.md`
- Modify: `docs/us-lacey/ARCHITECTURE.md`
- Modify: `docs/us-lacey/CAPABILITIES.toml`
- Modify: `docs/us-lacey/PIPELINE.md`
- Modify: `docs/us-lacey/TEST_MATRIX.md`
- Modify: `docs/us-lacey/ROADMAP.md`

- [ ] Mark automatic BOM integration as ACTIVE/non-canonical only after runtime tests exist.
- [ ] Document new persistence/read path, source-set fencing and required tests.
- [ ] Keep Taxonomy/De Minimis/Composite as future work.

### Task 8: Verification, review, PR and merge gate

- [ ] Run focused Product Intelligence tests.
- [ ] Run source-set/worker/UI regressions.
- [ ] Run full `python -m pytest -q -rs` in CI.
- [ ] Run U.S. Lacey PostgreSQL Gate and require no Product Intelligence acceptance skips.
- [ ] Review diff for canonical truth/export/auth/billing changes; expected none.
- [ ] Open PR to `feature/us-lacey-pilot-platform`.
- [ ] Resolve legitimate review findings with TDD.
- [ ] Merge only after all required gates are GREEN and no unresolved blocking review finding remains.
