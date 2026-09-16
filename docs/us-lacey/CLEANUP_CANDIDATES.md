# U.S. Lacey Cleanup Candidates

This file records cleanup/debt candidates. **It is not authorization to delete or move them.** Every destructive action requires fresh reference/import/runtime/test evidence and a separate change.

## SAFE_TO_ARCHIVE — documentation only, after link audit
These files appear primarily historical/status-oriented and are candidates to move under a documentation archive once inbound references are checked:
- `US_LACEY_COMPLETION_CHECKLIST.md`
- `US_LACEY_UI_PARITY_AUDIT.md`
- older U.S. Lacey plans/specs in `docs/` whose implementation has already landed and whose current guidance conflicts with `docs/us-lacey/`

Before archiving: search repository references, PR/runbook links and onboarding references. Preserve Git history; do not delete useful rationale.

## PROBABLY_OBSOLETE — needs evidence before action
- superseded historical implementation plans whose status is no longer current;
- scripts that are not referenced by CI, deployment, docs, runtime entrypoints or operator runbooks;
- old experimental entrypoints that have been replaced by the unified U.S. Lacey application but may still be used by previews/tests;
- redundant local deployment helpers if Render/CI no longer uses them.

No concrete file in this section is approved for deletion yet; classify only after import/reference/runtime analysis.

## NEEDS_INVESTIGATION

### Multiple web entrypoints
`src/litoral_trace/web/` contains several U.S. Lacey apps/views, including experiment/free/pilot/unified/worker/admin paths. Determine which are:
- production entrypoints;
- preview/demo entrypoints;
- test-only;
- historical.

Do not consolidate them until `render.yaml`, entrypoints, tests and workflows are traced.

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
Root runbooks and `docs/` contain valuable operational/history material. Establish a future `docs/archive/` or topic-based index only after validating links. The canonical current U.S. Lacey navigation layer is now `docs/us-lacey/`.

### Workflow count
`.github/workflows/` contains many historical milestone/gate workflows. Some may still be deliberate release gates. Before consolidating, inspect branch protection/rulesets, workflow dispatch use, documentation and recent runs. Never delete a gate based only on filename age.

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