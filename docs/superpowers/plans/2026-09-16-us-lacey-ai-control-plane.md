# U.S. Lacey AI Development Control Plane Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a safe, canonical navigation/control layer for AI agents working on U.S. Lacey without changing production behavior.

**Architecture:** Keep the existing modular monolith intact. Add root/package agent guidance, canonical architecture documentation, a dependency-free TOML capability map, and a tiny validation test. Record cleanup candidates instead of deleting or moving runtime assets in this phase.

**Tech Stack:** Markdown, TOML (`tomllib` on Python 3.11), Python/pytest, existing GitHub Actions CI.

**Spec:** `docs/superpowers/specs/2026-09-16-us-lacey-ai-control-plane-design.md`

## Global Constraints
- No runtime behavior changes.
- No schema or migration changes.
- No RLS/auth/billing/export/canonical-truth/source-set behavior changes.
- No runtime file moves/deletions.
- No new dependency solely for docs validation.
- Existing U.S. Lacey branch remains the merge target; work occurs on a dedicated branch.

---

### Task 1: Canonical agent guidance

**Files:**
- Create: `AGENTS.md`
- Create: `src/litoral_trace/us_lacey/AGENTS.md`
- Create: `src/litoral_trace/lacey_engine/AGENTS.md`

**Produces:** Explicit ownership and guardrails for future agents.

- [ ] Add root repository authority/read-order/invariants guidance.
- [ ] Add U.S. Lacey application ownership map and hotspot warnings.
- [ ] Add document-engine ownership/authority guidance.
- [ ] Verify every referenced path exists.

### Task 2: Canonical current architecture docs

**Files:**
- Create: `docs/us-lacey/README.md`
- Create: `docs/us-lacey/ARCHITECTURE.md`
- Create: `docs/us-lacey/DATA_MODEL.md`
- Create: `docs/us-lacey/PIPELINE.md`
- Create: `docs/us-lacey/INVARIANTS.md`
- Create: `docs/us-lacey/TEST_MATRIX.md`
- Create: `docs/us-lacey/ROADMAP.md`
- Create: `docs/us-lacey/CLEANUP_CANDIDATES.md`

**Produces:** One canonical current engineering directory for U.S. Lacey.

- [ ] Document current bounded contexts and authority boundaries.
- [ ] Document persistence/evidence/source-set model map.
- [ ] Document deterministic/AI/shadow/human/canonical pipeline paths.
- [ ] Document safety invariants.
- [ ] Route capabilities to focused/broad tests and CI gates.
- [ ] Keep the roadmap limited to NOW/NEXT/LATER commercial priorities.
- [ ] Record cleanup candidates without deleting anything.

### Task 3: Machine-readable capability map — RED

**Files:**
- Create: `docs/us-lacey/CAPABILITIES.toml`
- Create: `tests/test_us_lacey_architecture_docs.py`

**Produces:** A test contract requiring the capability map and all referenced paths to be valid.

- [ ] Write a pytest that loads `CAPABILITIES.toml` with stdlib `tomllib` and invokes a validator module.
- [ ] Confirm the test fails because the validator module does not yet exist.

### Task 4: Machine-readable capability map validator — GREEN

**Files:**
- Create: `scripts/validate_us_lacey_architecture_docs.py`

**Interfaces:**
- Produces: `validate_capabilities(repo_root: Path, capabilities_path: Path) -> list[str]`

- [ ] Implement dependency-free TOML loading/path validation.
- [ ] Validate list-valued `implementation`, `models`, `migrations`, `tests`, and `workflows` entries.
- [ ] Return human-readable errors rather than mutating repository state.
- [ ] Run `python scripts/validate_us_lacey_architecture_docs.py`.
- [ ] Run `python -m pytest -q tests/test_us_lacey_architecture_docs.py`.
- [ ] Run general CI pytest / relevant checks through the pull request.

### Task 5: Review/merge readiness

- [ ] Compare branch against `feature/us-lacey-pilot-platform`.
- [ ] Confirm no existing runtime file was modified/deleted.
- [ ] Confirm no migrations/dependencies were added.
- [ ] Confirm CI reports no regressions.
- [ ] Keep PR reviewable/squashable.
