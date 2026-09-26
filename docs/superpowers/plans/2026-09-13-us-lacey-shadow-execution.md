# U.S. Lacey Shadow Execution Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Integrate the specialized multi-agent extractor behind `LT_AI_ARCHITECTURE` with safe shadow execution, dual persistence, and comparative telemetry while preserving legacy authority.

**Architecture:** Keep the existing legacy AI shadow path unchanged as the authoritative result. Add a dispatcher in the U.S. Lacey Engine 2 service that can run legacy, specialized, or both; specialized runs use the Phase 6 orchestrator and persist under a distinct schema version so existing UI projection filters continue to ignore them.

**Tech Stack:** Python 3.11, asyncio, SQLAlchemy, pytest, existing Gemini Interactions transport, GitHub Actions CI.

**Spec:** `docs/superpowers/specs/2026-09-13-us-lacey-shadow-execution-design.md`

## Global Constraints

- `LT_AI_ARCHITECTURE` accepts `legacy`, `specialized`, `shadow`; unset/invalid resolves to `legacy`.
- In `shadow`, legacy remains authoritative and specialized failures never fail a successful legacy worker job.
- Specialized persistence must use a schema/version distinct from `AI_SHADOW_SCHEMA_VERSION`.
- Existing `project_verified_ai_suggestions()` must continue to consume only legacy schema runs.
- Token counters are recorded only when reported by the provider; never estimate token usage.
- Specialized candidates must be exact-evidence verified against Engine 2 before fusion/persistence.

---

### Task 1: Runtime architecture contract

**Files:**
- Create: `src/litoral_trace/lacey_engine/architecture.py`
- Modify: `.env.example`
- Test: `tests/lacey_engine/test_ai_architecture.py`

**Interfaces:**
- Produces: `AIArchitecture` enum and `ai_architecture(environ: Mapping[str, str] | None = None) -> AIArchitecture`.

- [ ] Write failing tests proving default, invalid, `legacy`, `specialized`, and `shadow` behavior.
- [ ] Run focused pytest and confirm RED because the module does not exist.
- [ ] Implement the minimal enum/parser with warning on invalid values.
- [ ] Add `LT_AI_ARCHITECTURE=legacy` documentation to `.env.example`.
- [ ] Run focused pytest and confirm GREEN.

### Task 2: Provider usage telemetry

**Files:**
- Modify: `src/litoral_trace/lacey_engine/ai_shadow.py`
- Modify: `src/litoral_trace/lacey_engine/gemini_provider.py`
- Modify: `src/litoral_trace/lacey_engine/multi_agent/gemini_specialist_adapter.py`
- Modify: `src/litoral_trace/lacey_engine/multi_agent/contracts.py`
- Modify: `src/litoral_trace/lacey_engine/multi_agent/specialist_runtime.py`
- Test: `tests/lacey_engine/test_gemini_provider.py`
- Test: `tests/lacey_engine/test_multi_agent_specialists.py`

**Interfaces:**
- Extend `AIExtractionResult` with optional `input_tokens`, `output_tokens`, `total_tokens` fields defaulting to `None`.
- Extend `SpecialistResult` with optional aggregate token fields defaulting to `None`.

- [ ] Write failing tests with Gemini fixture responses containing usage metadata and assert counters are parsed/aggregated.
- [ ] Run focused tests and confirm RED.
- [ ] Add a shared deterministic helper that reads supported Gemini usage keys without estimating missing values.
- [ ] Propagate usage into `AIExtractionResult`, then aggregate across scoped specialist calls.
- [ ] Keep existing constructors source-compatible with defaults.
- [ ] Run focused tests and confirm GREEN.

### Task 3: Specialized shadow runner with evidence verification

**Files:**
- Create: `src/litoral_trace/us_lacey/specialized_shadow.py`
- Modify: `src/litoral_trace/lacey_engine/multi_agent/orchestrator.py`
- Test: `tests/us_lacey/test_specialized_shadow.py`

**Interfaces:**
- Produces: `SPECIALIZED_SHADOW_SCHEMA_VERSION = "lacey_multi_agent_shadow_v1"`.
- Produces: `run_specialized_shadow_operation(...)` returning a serializable result with architecture, operation status, candidates, failures, latency, and tokens.
- Orchestrator accepts an optional deterministic candidate verification callback applied before line binding/fusion.

- [ ] Write failing tests using fake documents/providers proving routing of multiple documents, verification-before-fusion, partial specialist failure, and telemetry aggregation.
- [ ] Run focused tests and confirm RED.
- [ ] Implement page-text derivation from Engine 2 layout, routing plan construction, specialist construction, bounded orchestration, exact evidence verification mapping by document id, and serialization.
- [ ] Ensure no specialized result mutates UI-facing operation fields.
- [ ] Run focused tests and confirm GREEN.

### Task 4: Dual persistence and dispatcher

**Files:**
- Modify: `src/litoral_trace/us_lacey/lacey_engine_service.py`
- Test: `tests/us_lacey/test_lacey_engine_service.py`
- Test: `tests/us_lacey/test_ai_suggestions.py`

**Interfaces:**
- Legacy persisted payload includes `architecture="legacy"` plus latency/token metadata.
- Specialized persisted runs use `SPECIALIZED_SHADOW_SCHEMA_VERSION` and `architecture="specialized"`.
- New internal dispatcher selects execution from `ai_architecture()`.

- [ ] Write failing tests for `legacy`, `specialized`, and `shadow` dispatch.
- [ ] Write failing test: legacy succeeds + specialized raises => service/worker legacy result remains successful.
- [ ] Write failing persistence test showing legacy and specialized runs coexist for the same document/source SHA.
- [ ] Write/retain test proving `project_verified_ai_suggestions()` filters only `AI_SHADOW_SCHEMA_VERSION`.
- [ ] Run focused tests and confirm RED.
- [ ] Implement dispatcher and persistence with isolated specialized exception boundary.
- [ ] Add structured logs with architecture, latency, token counts, candidate count, organization, operation, and document identifiers.
- [ ] Run focused tests and confirm GREEN.

### Task 5: Worker integration and regression boundary

**Files:**
- Modify: `src/litoral_trace/us_lacey/worker.py` only if needed to invoke the dispatcher at the existing Engine 2 shadow seam.
- Test: `tests/us_lacey/test_worker.py`

**Interfaces:**
- Existing queue completion, projection, reconciliation, and UI behavior remain legacy-authoritative in `shadow` mode.

- [ ] Write failing regression test around `process_one_us_lacey_job`: specialized shadow failure cannot change `COMPLETED` legacy job semantics.
- [ ] Run focused test and confirm RED if worker wiring is required.
- [ ] Make the smallest wiring change necessary; do not add fire-and-forget threads.
- [ ] Run focused test and confirm GREEN.

### Task 6: Full verification and PR

**Files:**
- Review all changed files.

- [ ] Run Python syntax gate.
- [ ] Run Alembic canonical head gate.
- [ ] Run full pytest suite.
- [ ] Review diff for accidental authority changes, new free-text AI authority, or specialized schema leakage into UI queries.
- [ ] Open PR #222 against `feature/us-lacey-pilot-platform` with RED→GREEN evidence and leave it unmerged.
