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

## Taxonomic comparison identity

- Taxonomic comparison identities are not evidence and are never published as raw, normalized source, PPQ505, LAWGS or human-confirmed values.
- An epithet may inherit genus context only inside the same proven line/component and only from one verified high-confidence genus (>=0.90) or a human-confirmed genus.
- Missing, low-confidence, competing or cross-line genus context must remain ambiguous/review-required.
- Comparison-equivalence may suppress a false conflict, but it must preserve every original candidate value and provenance row for audit.

## Quantitative line binding

- Quantitative plant facts may bind across documents only through explicit SKU, source-local unambiguous line identity, or an exact unique HTS plus taxon signature.
- HTS alone, row/ordinal proximity, numeric similarity and document position are never sufficient identity evidence.
- Ambiguous quantities/units remain unbound; quantitative rows do not create plant-component identity by themselves.
- Strong source-line signatures are frozen before canonical derived-key rewrites so binding is deterministic and independent of field iteration order.
- A shipment total entered value may seed one line only when there is exactly one resolved declarable line and exactly one supported total; the resulting line value is always REVIEW_REQUIRED.
- Derived binding keys may exist in the canonical working copy, but raw candidate values, source spans and provenance must remain unchanged.

## Complete source SKU identity (DI-02)

- A SKU sidecar is identity evidence only when the complete token occurs in the verified anchor/row; prefix, suffix and substring matches cannot authorize association. Contradictory explicit SKU labels retain source-local identity rather than choosing one by order.
- Only ASCII case and surrounding whitespace normalize for SKU comparison. Significant characters, including trailing separators, remain intact. Unsupported token spellings abstain; no punctuation removal creates an alias. Source delimiters are whitespace, colon, comma, semicolon and brackets.
- A supported source value remains a candidate and may remain unbound to an entity; admission and exact association do not constitute human review or canonical regulatory approval.

- SKU identity checks use the original Engine 2 anchor, not Unicode-casefolded text or an AI quote cropped inside another identifier. Label modifiers No/No./Number/ID never become product identities. Literal quotes and source contents remain unchanged.
- An explicit unsupported SKU label vetoes packet-global SKU association, including literal Legacy aliases; retain source-local ROW/FP identity and unchanged candidate evidence instead.
- Source-local FP fallback for an unsupported explicit SKU preserves distinguishing punctuation and Unicode; abstention must not merge distinct source products. A verified explicit sidecar does not require a punctuation separator to retain an admitted alphanumeric SKU.
- Explicit unsupported-SKU abstention survives subsequent semantic reconciliation; equal descriptions cannot undo it. Unicode whitespace may delimit an ASCII SKU in comparison text but never expands its alphabet or joins internal SKU parts, and original evidence remains literal.
- Verified-anchor SKU abstention travels as explicit negative metadata even when a literal quote omits the SKU. It survives serialization/rehydration and cannot grant global identity; pre-abstention artifacts are fenced by the line-binding version.
