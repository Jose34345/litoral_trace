# Engine2 line-aware projection implementation plan

## Goal
Project Engine2 shipment evidence into the existing PPQ human-review fields without collapsing valid multi-line facts or weakening fail-closed behavior.

## Regression first
1. Add a unit regression proving a two-line `SUPPORTED_MULTIPLE` field produces one suggestion per semantic association instead of being discarded.
2. Add a regression proving low-authority explicit `country_of_harvest` evidence in `REVIEW_REQUIRED` is surfaced as review-only suggestions, never silently accepted.
3. Preserve fail-closed behavior for ambiguous/unassociated evidence.

## Implementation
1. Extend `Engine2Suggestion` with semantic association and review-only metadata.
2. Replace single-value suggestion construction with evidence-grouped construction keyed by `line_key` / `component_key`.
3. Resolve explicit `:row:N` associations against plant-line ordinal/reference and `taxon:genus:species` associations against the operation's existing genus/species fields.
4. Require exactly one target field for every proposed value; otherwise skip it.
5. Materialize supported values as `FOUND` and low-authority harvest-country values as `REVIEW`, retaining source provenance and candidate audit rows.

## Verification
Run the focused Engine2 suggestion regressions, existing line-semantics smoke regressions, and the full repository CI before merge.
