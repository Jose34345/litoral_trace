# P1.7 — Litoral Trace UI/UX 2.0

## Design-system baseline

This document is the product UI contract for P1.7. It intentionally changes presentation architecture without changing domain behavior, authorization, tenancy, compliance semantics, persistence, integrations, or API contracts.

### Baseline architecture

- Server-rendered Jinja2 templates.
- HTMX for progressive interactions.
- Tailwind CSS v4 as utility/build layer.
- Native ES modules for client behavior.
- Leaflet for current geospatial rendering.
- Font Awesome for the current icon set.
- FastAPI/Pydantic remains the domain and validation source of truth.

P1.7 does **not** introduce React, Next.js, a second component runtime, or client-side domain state.

## Product design principles

1. **Action before decoration.** Every operational screen must make the next useful action obvious.
2. **Semantic status language.** A business status must look the same everywhere, independent of the page that renders it.
3. **Auditability is visible.** Provenance, evidence, risk and integration boundaries should be legible without opening implementation details.
4. **Progressive disclosure.** Dense compliance detail is available, but the first viewport prioritizes decision-grade information.
5. **Server truth, client assistance.** HTMX/JavaScript improve interaction; they do not invent domain state.
6. **Keyboard and responsive behavior are first-class.** Mobile/tablet states are product states, not later patches.
7. **One visual grammar.** Pages consume shared primitives instead of rebuilding buttons, badges, alerts and empty states ad hoc.

## Semantic token model

### Surfaces

- `canvas`: application background.
- `surface`: primary content surface.
- `surface-muted`: grouped or secondary content.
- `surface-subtle`: low-emphasis controls and rows.
- `border`: default separator.
- `border-strong`: emphasized separator.

### Text

- `text-primary`: headings and primary data.
- `text-secondary`: body copy.
- `text-muted`: metadata and supporting labels.
- `text-inverse`: content on dark surfaces.

### Action

- `accent`: primary action / product identity.
- `accent-hover`: emphasized primary action.
- `focus`: keyboard focus indicator.

### Operational status

The visual mapping is semantic and must not depend on literal backend wording.

- `ready` / `verified` / `active` / `succeeded` → positive.
- `pending` / `review` / `staged` / `partial` → warning.
- `blocked` / `conflict` / `failed` / `rejected` → danger.
- `inactive` / `unknown` / `unassessed` → neutral.

### Risk

- `low` → positive.
- `medium` → warning.
- `high` → high-attention.
- `critical` → danger.

## Density and geometry

- Default application radius: 12 px.
- Compact control radius: 10 px.
- Primary page gap: 24 px.
- Card padding: 20–24 px depending on density.
- Minimum interactive target: 40 px when practical.
- Page maximum width remains 1536 px unless a workspace explicitly needs a wider canvas.

## Core primitives — phase A

The initial reusable Jinja primitives are:

- `button`
- `badge`
- `page_header`
- `alert`
- `empty_state`

Subsequent P1.7 phases add table, field, dialog, dropdown, skeleton, progress and geospatial primitives after their interaction contracts are validated.

## Accessibility contract

- Focus must remain visible with `:focus-visible`.
- Color never carries status meaning alone when the status is operationally important.
- Icon-only controls require an accessible label.
- Alerts use appropriate `role=status` or `role=alert` semantics.
- Reduced-motion preferences remain honored.
- Drawer focus trapping and skip navigation behavior are preserved.

## Parallel-development boundary

P1.7 is isolated on `p1.7-ui-ux-2.0` and should avoid backend-owned files while parallel P1 work is active.

Owned by P1.7 during this phase:

- `src/litoral_trace/static/src/design-system.css`
- `src/litoral_trace/templates/components/*`
- narrowly scoped shared-template changes required to load the design system
- UI-only tests/docs introduced by P1.7

Not owned by P1.7 during this phase:

- API and web route modules
- DB models and Alembic revisions
- RLS / tenancy code
- EUDR transport/submission services
- integration domain behavior
- CI gates owned by parallel backend work

## Exit criteria for UX2.0-A

- Semantic tokens are loaded globally without changing domain behavior.
- Shared Jinja primitives exist and are safe to consume from server-rendered pages.
- Existing navigation, CSRF and HTMX behavior remains intact.
- At least one canary surface adopts primitives without changing its form actions or backend contract.
- The generated Tailwind asset remains reproducible; P1.7 does not bypass or mutate it manually.
