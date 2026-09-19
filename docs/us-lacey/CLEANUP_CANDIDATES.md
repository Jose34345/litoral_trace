# U.S. Lacey Cleanup Candidates

This file records cleanup/debt candidates. **It is not authorization to delete or move them.** Every destructive action requires fresh reference/import/runtime/test evidence and a separate change.

See `CLEANUP_AUDIT.md` for Phase 2 evidence-backed decisions.

## COMPLETED IN PHASE 2 — archived with compatibility stubs
The following historical root documents were audited and archived without breaking their old paths:
- `US_LACEY_COMPLETION_CHECKLIST.md` -> `docs/archive/us-lacey/US_LACEY_COMPLETION_CHECKLIST.md`
- `US_LACEY_UI_PARITY_AUDIT.md` -> `docs/archive/us-lacey/US_LACEY_UI_PARITY_AUDIT.md`

Their root files are now compatibility stubs that direct agents to `docs/us-lacey/README.md`. They were not hard-deleted because repository code search cannot prove the absence of external/inbound links.

## SAFE_TO_ARCHIVE — documentation only, after link audit
- older U.S. Lacey plans/specs in `docs/` whose implementation has already landed and whose current guidance conflicts with `docs/us-lacey/`;
- historical status documents that embed obsolete test counts, migration heads or rollout state.

Before archiving: search repository references, PR/runbook links and onboarding references. Preserve Git history; prefer compatibility stubs when an old stable path may be referenced externally.

## PROBABLY_OBSOLETE — needs evidence before action
- superseded historical implementation plans whose status is no longer current;
- scripts that are not referenced by CI, deployment, docs, runtime entrypoints or operator runbooks;
- redundant local deployment helpers if Render/CI no longer uses them.

No concrete runtime file in this section is approved for deletion yet; classify only after import/reference/runtime analysis.

## INVESTIGATED — KEEP

### Multiple web entrypoints
Phase 2 found that several U.S. Lacey web modules are layered composition rather than simple duplicates:
- `src/litoral_trace/web/us_lacey_pilot_app.py` — authenticated portal base: **KEEP**;
- `src/litoral_trace/web/us_lacey_free_app.py` — free-tier portal + inline worker: **KEEP**;
- `src/litoral_trace/web/us_lacey_unified_app.py` — customer-facing composition root: **KEEP**;
- `src/litoral_trace/web/us_lacey_worker_app.py` — dedicated worker topology: **KEEP**;
- `src/litoral_trace/web/us_lacey_platform_admin.py` — admin router included by unified app: **KEEP**;
- `src/litoral_trace/web/lacey_gtm.py` — GTM router used by unified and experiment apps: **KEEP**;
- `src/litoral_trace/web/lacey_experiment_app.py` — independent GTM/private-beta surface: **KEEP / REVIEW LATER** until deployment evidence proves it unused.

Do not consolidate these only because multiple ASGI entrypoints exist. See `CLEANUP_AUDIT.md`.

## NEEDS_INVESTIGATION

### Large application hotspots
- `src/litoral_trace/us_lacey/_operations_core.py`
- `src/litoral_trace/us_lacey/worker.py`
- `src/litoral_trace/us_lacey/canonical_shipment_truth.py`
- `src/litoral_trace/us_lacey/projection.py`
- `src/litoral_trace/us_lacey/review.py`
- `src/litoral_trace/us_lacey/lacey_engine_service.py`
- `src/litoral_trace/us_lacey/shadow_evidence_snapshot.py`

These are candidates for gradual extraction only when feature work naturally touches a responsibility. Avoid a mass refactor solely for aesthetics.

### Documentation sprawl
Root runbooks and `docs/` contain valuable operational/history material. Continue moving historical-only material into `docs/archive/` only after validating links. The canonical current U.S. Lacey navigation layer is `docs/us-lacey/`.

### Workflow count
`.github/workflows/` contains many historical milestone/gate workflows. Some may still be deliberate release gates. Before consolidating, inspect branch protection/rulesets, workflow dispatch use, documentation and recent runs. Never delete a gate based only on filename age.

### Script inventory
Audit old scripts against:
- active GitHub Actions workflows;
- Docker/Render startup commands;
- operator runbooks;
- direct imports/callers;
- manual recovery procedures.

A script with no import is not automatically dead if it is an operator entrypoint.

## DO_NOT_TOUCH in cleanup-only changes
- deployed Alembic migrations;
- PostgreSQL RLS/security functions/policies;
- organization/auth/audit models;
- U.S. Lacey source-set revision/finalization logic;
- canonical shipment truth/publication guards;
- evidence snapshots/text spans/semantic evidence graph;
- human review/audit behavior;
- PPQ505/export contracts;
- active CI/PostgreSQL/live release gates;
- tests protecting any of the above.

## Cleanup decision checklist
Before deleting/moving a candidate, capture:
1. `git grep`/code-search references;
2. imports/callers;
3. tests referencing it;
4. workflows/deploy/runbooks referencing it;
5. runtime entrypoint/config references;
6. replacement path if superseded;
7. focused tests before/after;
8. full relevant gate result.

If any runtime ownership remains uncertain, classify as `NEEDS_INVESTIGATION` and leave it in place.
