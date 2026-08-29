# U.S. Lacey Private Beta — Landing UI specification

## Goal

One-page validation landing for a narrow B2B audience. It must look credible enough for a customs broker or import-compliance professional, but it must not imply a finished U.S. product or a regulatory certification service.

## Visual direction

- Reuse Litoral Trace brand language: dark slate, emerald accents, white cards, compact evidence/status UI.
- English-only shell for this page; do not expose the Spanish public navigation as the primary navigation.
- Enterprise, operational and evidence-first — not “startup neon”, not consumer SaaS.
- One primary action repeated throughout: `Test one completed shipment`.
- Avoid stock photography. The central visual should be a product-style workflow mockup built from HTML cards using capabilities LT actually has.

## Desktop hierarchy

### 1. Minimal header
- Litoral Trace logo/name.
- Small descriptor: `Document intelligence for import operations`.
- Right side: anchor `How it works`, `Private beta` CTA.

### 2. Hero — two columns
Left:
- Eyebrow: `Private Beta · Lacey Act Phase VII`.
- H1: `Stop chasing Lacey data across supplier documents.`
- Supporting paragraph from copy spec.
- Primary CTA.
- Trust line: no ERP, no production integration, redaction allowed.

Right:
- Product mockup titled `Shipment evidence review`.
- Documents: Commercial Invoice, Supplier Sheet, Packing List.
- Extracted fields/status examples:
  - Shipment reference — MATCHED
  - Quantity — MATCHED
  - Species evidence — REVIEW
  - Harvest country — MISSING
- Bottom status: `REVIEW · 2 items need attention`.
- Small caption: illustrative private-beta workflow; not a filing result.

### 3. Three outcome cards
`Extract` / `Reconcile` / `Evidence`.
Each card gets one sentence and one icon.

### 4. Workflow section
Four numbered steps with a single horizontal/stacked flow.
Documents → extraction → exception review → filing preparation.

### 5. Pain / “where manual work hides”
Dark section with 4 concise pain statements. Purpose: recognition, not feature dumping.

### 6. Credibility / scope block
Two columns:
- `What the beta already uses`: PDF/Excel/CSV, OCR, structured extraction, confidence/provenance, Vault, reconciliation.
- `What it does not claim`: no filing, no legal determination, no guaranteed acceptance, no live ACE/LAWGS integration.

### 7. Conversion section
Large card with a maximum-five-field form.
CTA wording: `Request a 15-minute walkthrough`.
Secondary privacy line: historical shipment; redaction permitted.

### 8. FAQ
Four questions from LANDING_COPY_V1.md.

### 9. Footer
Litoral Trace + commercial email + precise regulatory disclaimer.

## Mobile hierarchy

- Header stays one line; no full navigation drawer required.
- Hero copy first, CTA second, product mockup third.
- Benefits stack vertically.
- Workflow stacks as numbered cards.
- Form inputs full width with minimum 44px touch targets.
- Sticky CTA is intentionally avoided for v1 to reduce intrusive mobile behavior.

## Accessibility

- Semantic `main`, `section`, headings in order.
- Form labels always visible; placeholders never replace labels.
- Status is communicated with text, not color alone.
- Focus rings visible.
- Buttons/links have descriptive accessible names.
- No auto-playing animation/video.
- Contrast should remain WCAG-AA compatible using current slate/emerald palette.

## Conversion instrumentation

Only non-PII event names are measured:
- `lacey_visit`
- `lacey_cta_click`
- `lacey_form_start`
- `lacey_form_submit`

Do not put emails, form answers, document names or customer data into application logs or metrics labels.

## Lead delivery v1

To avoid a database migration or external SaaS setup during the weekend experiment, the page builds a pre-filled email to `comercial@litoraltrace.com` after local validation. The application only records aggregate/non-PII conversion events. If the experiment reaches GREEN/YELLOW and recurring lead capture is required, replace this with a persistent CRM/form integration in a separate change.
