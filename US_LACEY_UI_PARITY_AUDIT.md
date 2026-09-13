# U.S. Lacey UI parity audit

## BEFORE

The U.S. Lacey customer portal had an independent document shell assembled in
Python (`shell()`), including a large inline `<style>` block, hard-coded
palette, bespoke cards, inputs, buttons, badges, responsive breakpoints and
navigation. Its operational pages constructed large HTML strings in Python.
The legal endpoints repeated the same pattern. The public GTM routes also load
`static/css/lacey_beta.css`, a separate Lacey visual system.

| Route/screen | State | Canonical archetype | Litoral Trace reference | Divergence | Severity | Solution | State |
|---|---|---|---|---|---|---|---|
| `/signup` | default/error | AUTH SHELL | `public/base_public.html`, `components/ui.html` | Independent inline shell/forms | P0 | Jinja + canonical shell/primitives | migrated |
| `/check-email` | success | AUTH SHELL | `public/base_public.html` | Independent inline shell | P1 | Jinja + canonical alert/card | migrated |
| `/verify-email` | invalid/expired | AUTH SHELL | `public/base_public.html` | Independent inline shell | P1 | Jinja + canonical alert | migrated |
| `/login` | default/error/verified | AUTH SHELL | `public/base_public.html`, form controls | Independent inline shell/forms | P0 | Jinja + canonical shell/primitives | migrated |
| `/billing` | PAYMENT_PENDING/PILOT/ACTIVE/error | APP SHELL | public shell + shared UI primitives | Bespoke cards/statuses | P1 | Jinja + canonical alert/badge/card | migrated |
| `/operations` | list/empty/limit | APP SHELL | shared UI primitives | Bespoke list/table/statuses | P1 | Jinja + canonical components | migrated |
| `/operations/new` | default/error | APP SHELL | shared UI primitives/form controls | Bespoke form | P1 | Jinja + canonical form primitives | migrated |
| `/operations/{id}` | upload/review/complete/export/error | APP SHELL | shared UI primitives/form controls | Large Python HTML renderer | P0 | Jinja + canonical components | migrated |
| `/legal/terms` | default | LEGAL SHELL | `public/base_public.html` | Inline legal CSS | P0 | Jinja + canonical shell/alert/card | migrated |
| `/legal/privacy` | default | LEGAL SHELL | `public/base_public.html` | Inline legal CSS | P1 | Jinja + canonical shell/alert/card | migrated |
| `/legal/private-beta` | default | LEGAL SHELL | `public/base_public.html` | Inline legal CSS | P1 | Jinja + canonical shell/alert/card | migrated |
| `/lacey` | public landing | PUBLIC SHELL | `public/base_public.html` | `lacey_beta.css` custom shell/palette | P0 | replace with canonical public template | pending |
| `/lacey/demo` | synthetic demo | PUBLIC SHELL | `public/base_public.html` | `lacey_beta.css` custom shell/palette | P0 | replace with canonical public template | pending |

Initial findings: **P0 5, P1 7, P2 0, P3 0**.

## INTERMEDIATE

- Portal, operations and legal rendering were migrated from Python HTML/CSS
  strings to request-aware Jinja templates.
- Two divergences remained: `/lacey` and `/lacey/demo` still consumed the
  standalone `lacey_beta.css` visual system.
- The local `.venv` referenced a Python executable that could not run in the
  original sandbox context. It was safely recreated with the installed Python
  3.11.9 and dependencies were restored without changing project manifests.

## CHANGES

- Replaced the U.S. Lacey portal and operational Python HTML/CSS renderers
  with Jinja templates in `templates/us_lacey/`.
- Added one U.S. Lacey base template that extends the live canonical public
  shell and imports the shared `components/ui.html` primitives.
- Reused canonical button, badge, status, alert, page-header, empty-state and
  form-control primitives for auth, billing, operations, review and legal UI.
- Preserved POST actions, field names, CSRF fields, upload form encoding,
  export URLs, redirects, and application response handling.
- Added an architectural UI contract test to prevent new inline portal CSS,
  custom Lacey tokens, a parallel stylesheet, or loss of required actions and
  statuses.
- Transplanted `/lacey` and `/lacey/demo` content, CTA forms, synthetic demo
  and aggregate tracking behavior into `public/base_public.html` using shared
  components and canonical utilities.
- Deleted `static/css/lacey_beta.css` and mounted the canonical static asset
  directory in the isolated U.S. Lacey FastAPI app.
- Added synthetic HTTP contracts for auth GET/POST forms, verification errors,
  logout cookie handling, PAYMENT_PENDING, PILOT, operations, CSRF fields and
  upload/complete actions.

## FINAL

| Route | Shell | Components | Tokens | Responsive | A11y | Visual QA | Result |
|---|---|---|---|---|---|---|---|
| Portal auth and verification routes | Canonical public | shared UI/form primitives | canonical | canonical | labels, landmarks, visible focus | pending manual review | PASS (architectural) |
| Billing and operations routes | Canonical public with workspace subnavigation | shared UI/form primitives | canonical | canonical | semantic forms/tables/status alerts | pending manual review | PASS (architectural) |
| Legal routes | Canonical public | shared UI primitives | canonical | canonical | semantic sections/status alert | pending manual review | PASS (architectural) |
| `/lacey` | Canonical public | shared UI/form primitives | canonical | canonical | labels, landmarks, live status | pending manual review | PASS |
| `/lacey/demo` | Canonical public | shared UI/table/status primitives | canonical | canonical | landmarks, live progress/status | pending manual review | PASS |

`/lacey`: PASS

`/lacey/demo`: PASS

P0 remaining: 0
P1 remaining: 0
P2 remaining: 0
P3 remaining: 0

The previously reported final custom token represented the standalone
`lacey_beta.css` root token set (`--ink`, `--muted`, `--line`, `--soft`,
`--green`, `--green2`, `--navy`, `--navy2`, `--amber`, `--red`, `--white`).
It was used by `public/lacey.html` and `public/lacey_demo.html`; canonical
equivalents are the shared `--lt-*` tokens, `components/ui.html` primitives and
the utilities compiled from `static/src/app.css`. The stylesheet and both
references were removed.

CUSTOM LACEY DESIGN SYSTEM: REMOVED

CUSTOM LACEY DESIGN TOKENS REMAINING: 0

UNJUSTIFIED UI DIVERGENCES REMAINING: 0

## Verification notes

Python 3.11.9 core imports and `compileall` pass. Alembic reports exactly
`039_us_lacey_pilot_fix (head)`. The relevant UI/Lacey selection passes with
264 passed, 0 failed and 0 skipped; the full suite passes with 1,437 passed,
0 failed and 285 explicitly skipped tests. `npm ci` and `npm run build` pass
without changing either package manifest. No production database, AWS service,
Brevo, Render or customer document was contacted. Visual QA remains pending
manual review for authenticated billing/operations states; browser QA passed
for `/lacey` and `/lacey/demo` in desktop and 390 px mobile viewports,
including the synthetic-demo interaction, responsive page width and canonical
status tones. Architectural parity is verified by shared inheritance, source
scans and tests.
