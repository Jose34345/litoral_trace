# P1.7 UI/UX 2.0 — Acceptance and non-production validation

## Scope

This document closes the stacked P1.7 UI/UX 2.0 cycle after A through I.
The acceptance branch is `p1.7-j-acceptance`, stacked on `p1.7-i-geospatial`.
Do not merge or deploy this branch to `p2-enterprise-production` while performing the checks below.

## Safety boundary

Use only non-production credentials and data.

- `ENVIRONMENT=development` (or `test` for pytest).
- Use a dedicated local/test PostgreSQL/PostGIS database; never point `DATABASE_URL`, `WORKER_DATABASE_URL` or `MIGRATION_DATABASE_URL` at production.
- Keep `EUDR_ACCEPTANCE_ENABLED=0` for UI validation unless an explicit ACCEPTANCE connectivity test is being performed with authorized non-LIVE credentials.
- Do not run `deploy_production.sh`.
- Do not start `docker-compose.prod.yml` with production secrets.
- The frontend build is build-only: Jinja2 + HTMX + Tailwind + vendor assets.

## Automated acceptance

From the repository root:

```bash
npm ci
npm run build
npm audit
python -m pip install -r requirements.txt pytest alembic
python -m compileall main.py src tests
alembic heads
python -m pytest -q tests/test_ui_design_system_p17_unittest.py \
  tests/test_ui_app_shell_p17_unittest.py \
  tests/test_ui_data_table_p17_unittest.py \
  tests/test_ui_form_fields_p17_unittest.py \
  tests/test_ui_dialog_p17_unittest.py \
  tests/test_ui_dropdown_p17_unittest.py \
  tests/test_ui_skeleton_p17_unittest.py \
  tests/test_ui_progress_p17_unittest.py \
  tests/test_ui_geospatial_p17_unittest.py \
  tests/test_ui_p17_acceptance_unittest.py
python -m pytest -q -rs
```

Expected invariant: a single Alembic head, currently `026_add_eudr_acceptance_attempts (head)` on this stack.

## Local application smoke test

Create an untracked `.env` from `.env.example` and replace all placeholder values with isolated development/test values. At minimum, provide a development PostgreSQL runtime URL and a 32+ character JWT secret. Keep EUDR ACCEPTANCE disabled for this UI pass.

Build the frontend assets before starting FastAPI:

```bash
npm ci
npm run build
```

Then start the application locally:

```bash
uvicorn main:app --host 127.0.0.1 --port 8000 --reload
```

Use only `http://127.0.0.1:8000` during this validation.

## Manual UI checklist

1. Open `/health` and `/ready`; confirm the local instance is the one responding.
2. Log in with a development/test account.
3. Verify the App Shell: sidebar, active navigation, mobile drawer, skip-link, account popover and CSRF-protected logout.
4. Open the UI Catalog route used by the development environment and inspect buttons, badges, alerts, data table, fields and native dialog.
5. Open Settings and submit the demo-user form only against the isolated test database; confirm the existing HTMX endpoint and feedback target still work.
6. Verify the account dropdown opens/closes through the native Popover API and that the existing sidebar logout still works.
7. Verify the native dialog opens, closes with Escape/backdrop/close controls as designed, and restores focus to its trigger.
8. Check keyboard navigation and visible focus across sidebar, topbar, forms, dialog and dropdown.
9. Check responsive layouts at approximately 375 px, 768 px, 1024 px and desktop width.
10. Open Dashboard and verify the real `#map` Leaflet viewport renders, zoom controls work, markers/popups work with test data, and `#map-scope` updates without changing the existing runtime contract.
11. Confirm the satellite panel is not interpreted as an EUDR certificate and that no UI claims official approval/compliance.
12. Confirm loading/progress primitives are only used where an actual pending/measurable state exists; no fabricated percentages or risk states.
13. In browser DevTools, confirm no unexpected JavaScript exceptions and no missing CSS/JS/vendor assets.
14. Perform a hard reload and repeat the primary paths to catch asset-cache issues.

## Final no-production verification

Before ending the validation session:

```bash
git branch --show-current
git status
git diff --name-only p2-enterprise-production...HEAD
```

The current branch must be `p1.7-j-acceptance`. The diff should remain limited to UI/static/templates/tests/documentation for the P1.7 stack. Do not merge, push a production deployment, or reuse production database/storage/EUDR credentials as part of this acceptance pass.
