# U.S. Lacey Engineering Invariants

These are change constraints, not aspirational copy. If a change needs to violate one, stop and redesign or document an explicit architecture decision before implementation.

1. **False-safe = 0.** Missing, ambiguous, conflicting or unsupported information must never be silently promoted to a safe/ready result.
2. **No evidence -> no supported claim.** Derived fields may exist as suggestions/candidates, but support status requires provenance.
3. **Ambiguity remains ambiguity.** Common names, product descriptions, countries, quantities and cross-document identities may legitimately resolve to REVIEW_REQUIRED/INDETERMINATE.
4. **AI proposes; authority decides.** AI/specialist output cannot silently create canonical regulatory truth.
5. **Canonical publication is a boundary.** Customer/export truth must use the canonical publication path rather than ad-hoc writes from extraction or UI code.
6. **Source-set identity is preserved.** Every processing generation belongs to a defined source set/revision.
7. **Stale generations cannot win.** Older/abandoned generations must not overwrite current canonical results.
8. **Human review is auditable.** Accept/reject/correct/select-alternative actions must retain actor/time/reason/evidence as supported by the current review model.
9. **Tenant isolation is non-negotiable.** New tenant-owned persistence must preserve organization scoping and PostgreSQL RLS semantics.
10. **Historical migrations are immutable.** Create a new migration for schema changes; never rewrite deployed migration history.
11. **Sensitive source contents do not belong in logs.** Log IDs, stage/status/duration/error codes rather than full customer documents or extracted confidential payloads.
12. **Original values are preserved.** Normalization/conversion must not destroy the raw source value/unit/text needed for audit.
13. **Deterministic rules are versionable.** Regulatory calculations should identify the ruleset/version/effective context used to produce a decision.
14. **Missing inputs yield indeterminate, not guessed values.** This is especially important for future taxonomy, de minimis and material-composition decisions.
15. **Projection is presentation, not authority creation.** New domain logic should produce explicit decisions upstream and allow projection to render them.
16. **One provenance system.** Future Product Intelligence/Taxonomy/Regulatory decisions must reference the existing evidence/document chain rather than inventing a separate unverifiable evidence store.
17. **No live-integration claims without live integration.** Export preparation/PPQ505/LAWGS builders must not be described as successful federal submission unless a verified live submission path exists.
18. **Tests are part of the contract.** Changes to source sets, RLS, workers, canonical truth, review or export require the relevant focused tests and broader gates before merge.
