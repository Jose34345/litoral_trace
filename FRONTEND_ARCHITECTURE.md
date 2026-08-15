# Litoral Trace — Enterprise Frontend Architecture

## Status

P2.FE establishes the server-rendered enterprise frontend foundation used before
P2.4G (Enterprise Batch Import UI).

The frontend is **hypermedia-first**:

- FastAPI owns authentication, authorization, tenant context and workflow state.
- Jinja2 renders pages and fragments.
- HTMX follows server-provided transitions.
- Tailwind CSS is compiled at build time; no Tailwind Play/CDN in production.
- Vanilla JavaScript is restricted to browser-only behavior that hypermedia
  cannot express cleanly (drawer accessibility, map widgets, advanced charts).
- `/api/v1/*` remains the JSON integration contract.
- UI endpoints call services directly rather than making HTTP calls back into
  the same process.

## Security invariants

1. Tenant identity never comes from an editable browser field.
2. RBAC is enforced by FastAPI/services. Navigation only mirrors capabilities;
   hiding a control is never the authorization boundary.
3. Every state-changing HTML/HTMX transition is CSRF-protected.
4. CSRF tokens are signed, short-lived and bound to username +
   `organization_id` + `session_id` for authenticated users.
5. No database credentials, storage object keys, JWTs, lease tokens or other
   secrets are rendered into HTML or JavaScript.
6. Browser-facing error messages are bounded and sanitized.
7. Existing PostgreSQL RLS remains authoritative for tenant isolation.

## Delivery plan

### P2.FE-A — Frontend Foundation

- Pin frontend build dependencies.
- Introduce Tailwind v4 CSS-first source.
- Introduce local HTMX vendoring build step.
- Add modular base JavaScript.
- Add stateless CSRF primitives.
- Add RBAC-derived navigation model.
- No production HTML route behavior changes yet.

### P2.FE-B — Web Runtime + App Shell

- Mount `/static`.
- Introduce reusable Jinja environment/context.
- Wire CSRF into rendered pages and mutations.
- Replace CDN-based `base.html`.
- Add responsive sidebar/topbar/user menu.
- Move HTML routing responsibilities out of `main.py` into `litoral_trace.web`.
- Preserve current public HTML URLs during migration.

### P2.FE-C — Design System + Dashboard

- Jinja macros/components for buttons, inputs, badges, alerts, modals, tables,
  pagination, empty/loading/error states and steppers.
- Dashboard uses tenant-scoped read models, never demo numbers.
- Activity feed derives from authorized audit data.
- Regulatory copy only claims capabilities the backend actually supports.

### P2.4G — Enterprise Batch Import UI

- XLSX upload.
- Server-side structural + semantic validation.
- Row preview/error viewer.
- Idempotent import.
- Import result and evidence linkage.
- Lote/import tables reuse the enterprise component library.

## Asset build

Install pinned packages and create the lockfile:

```powershell
npm install
```

Build production assets:

```powershell
npm run build
```

Generated runtime assets:

- `src/litoral_trace/static/dist/app.css`
- `src/litoral_trace/static/vendor/htmx/htmx.min.js`

The generated assets are wired into the HTML shell in P2.FE-B.
