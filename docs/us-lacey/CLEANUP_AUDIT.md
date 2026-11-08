# U.S. Lacey cleanup audit — Phase 2

**Date:** 2026-09-16

## Scope
This audit reduces documentation ambiguity and classifies U.S. Lacey web entrypoints before any destructive runtime cleanup. It does **not** authorize deleting runtime modules, migrations, workflows, RLS/security logic, source-set logic, canonical truth, evidence, review or exports.

## Documentation decisions

### `US_LACEY_COMPLETION_CHECKLIST.md` — ARCHIVE_WITH_COMPAT_STUB
The file is a point-in-time beta checklist. It records historical test counts and an Alembic head (`038_us_lacey_pilot_activation`) that are no longer current. Its rationale remains useful, but its status assertions are unsafe as current guidance.

Action:
- historical content moved to `docs/archive/us-lacey/US_LACEY_COMPLETION_CHECKLIST.md`;
- root path retained as a small compatibility stub;
- current guidance points to `docs/us-lacey/README.md`.

### `US_LACEY_UI_PARITY_AUDIT.md` — ARCHIVE_WITH_COMPAT_STUB
The file is a historical migration audit. It records point-in-time test counts and Alembic head `039_us_lacey_pilot_fix`, while the current codebase has advanced beyond that state.

Action:
- historical content moved to `docs/archive/us-lacey/US_LACEY_UI_PARITY_AUDIT.md`;
- root path retained as a compatibility stub;
- current guidance points to `docs/us-lacey/README.md`.

Repository code search found no default-branch references to either exact root filename during this audit. Because GitHub code search is default-branch scoped and external links cannot be exhaustively proven absent, compatibility stubs are intentionally retained instead of deleting the paths.

## Web entrypoint audit

| Module | Classification | Decision | Evidence / rationale |
|---|---|---|---|
| `src/litoral_trace/web/us_lacey_unified_app.py` | customer-facing composition root | **KEEP** | Composes the portal/inline-worker app with GTM, hosted billing, intelligent workflow, admin and export routers; explicitly designed for the full customer hostname flow. |
| `src/litoral_trace/web/us_lacey_free_app.py` | active free-tier runtime substrate | **KEEP** | Imports the pilot portal app and wraps it with inline-worker lifecycle/readiness behavior. It is directly imported by `us_lacey_unified_app.py`. |
| `src/litoral_trace/web/us_lacey_pilot_app.py` | authenticated portal base | **KEEP** | Owns signup/login/billing/operations/review/export portal routes and is directly imported by `us_lacey_free_app.py`. |
| `src/litoral_trace/web/us_lacey_worker_app.py` | dedicated worker service entrypoint | **KEEP** | Provides the separate least-privilege worker topology needed when the inline free-tier worker is replaced/scaled. Removing it would erase a valid production topology. |
| `src/litoral_trace/web/lacey_experiment_app.py` | standalone GTM/private-beta microsite | **KEEP / REVIEW LATER** | Deliberately avoids the authenticated app and exposes a standalone `/lacey` microsite. It is not part of the unified-app import chain inspected here, but may remain useful for independent preview/market-validation deployments. Delete only after deployment/runbook/history evidence proves it unused. |
| `src/litoral_trace/web/us_lacey_platform_admin.py` | superadmin router | **KEEP** | Included directly by `us_lacey_unified_app.py`; backed by reviewed platform-admin database capabilities and persistent U.S. sessions. |
| `src/litoral_trace/web/lacey_gtm.py` | public GTM router | **KEEP** | Used by both unified customer composition and the standalone experiment app. |

## Runtime conclusion
There is **no safe runtime deletion in Phase 2** based on the evidence inspected. The apparent multiplicity of apps is largely layered composition rather than pure duplication:

`pilot_app -> free_app -> unified_app`

with `worker_app` as the dedicated-worker topology and `lacey_experiment_app` as an independent GTM surface.

Deleting any of those merely because several ASGI entrypoints exist would be unsafe.

## Hotspot decision
The large application modules listed in `CLEANUP_CANDIDATES.md` remain **gradual-extraction only**. No mass refactor is justified in a cleanup-only PR. New Product Intelligence/BOM/Taxonomy work should use focused modules rather than increasing those hotspots.

## Workflow decision
No GitHub Actions workflow is deleted in Phase 2. Workflow cleanup still requires branch-protection/ruleset, recent-run, runbook and release-gate evidence. Filename age is not sufficient.

## Next cleanup candidates
1. Audit old scripts against workflows, Docker/Render startup commands and operator runbooks.
2. Audit older U.S. Lacey plans/specs for archival, using compatibility links where inbound references may exist.
3. Revisit `lacey_experiment_app.py` only after deployment evidence establishes whether an independent GTM service still exists.
4. Perform hotspot extraction only when feature work naturally touches a bounded responsibility.
