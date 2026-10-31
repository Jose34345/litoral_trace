# Market-driven release decision log — 2026-10-15

## Prioritization (working hypotheses, ordinal scores 1–5, not statistical certainty)

Weights: pain .25, manual effort .25, compliance/trust .20, activation .15, feasibility .15.

| Candidate | Pain | Effort | Trust | Activation | Feasible | Weighted /5 | Decision |
|---|---:|---:|---:|---:|---:|---:|---|
| First visible actionable review / source-linked KPI | 4 | 3 | 4 | 5 | 5 | 4.05 | P0 small UX |
| Clear sample → owned-shipment journey + privacy | 4 | 3 | 5 | 5 | 4 | 4.00 | P0 audit/gate |
| Verified supplier/SKU memory controls | 4 | 5 | 5 | 4 | 2 | 4.20 | KEEP existing / golden tests; no rewrite |
| Additional AI extraction fields | 3 | 2 | 2 | 2 | 1 | 2.15 | DEFER |
| Rebuild PDF inspector (already improved) | 2 | 1 | 3 | 3 | 1 | 1.95 | DISCARD; preserve stable |
| Full ABI integration/filing | 3 | 2 | 1 | 2 | 1 | 1.95 | DISCARD pending counsel |

Scores represent evidence-weighted *engineering judgment*, not user-study results or predicted ROI.

## Changes approved within initial release candidate

- **P0-L1:** summary header puts current pending exceptions and next safe action first; compact hierarchy preserves source-backed status and full filing caveat. Measurement: scroll distance/first visible action in 900px desktop screenshot and DOM contract, initial baseline screenshot review only (no real-user timing yet).
- **P0-L2:** make read-only synthetic samples and own-shipment pathway clear, without extra account gating or AI-driven approval; only change copy where contract incorrect.
- **P0-L3:** final GO/NO-GO checklist with REAL test statuses, hard-stop if docs/RLS/exceptions/Pack2 fail.
- **P1:** measure first-use time, PDF latency, click/key interactions and verify whether broker workflows value companion product.
- **Discard:** no lab activation, no new data model, no autonomous filing, no fabricated ROI, no broad UI rebuild.

## Guardrails

No backend regulatory logic changed by first screen UX; no evidence becomes verified merely because source snippet is displayed. No source-wiping changes. Preserve source-set id, audit, retention, authorization and export blockers. No automatic outreach.
