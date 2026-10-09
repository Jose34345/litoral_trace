# U.S. Lacey canonical shipment truth — design

## Problem

The extraction engine already models evidence cardinality and semantic identity (`SHIPMENT`, `MERCHANDISE_LINE`, `PLANT_COMPONENT`, `line_key`, `component_key`). Downstream code currently loses that invariant because several projection paths independently decide which `UsLaceyOperationField` owns a candidate. A value can therefore be correct in `ShipmentResolution` and still appear as missing, conflicting, or attached to the wrong plant line in customer review.

The production seven-document regression exposed this class of failure: two valid HTS/description/value lines were interpreted as conflicts, plant-species evidence leaked between lines, and low-authority harvest-country evidence existed in the shipment resolution while the review field remained `MISSING`.

## Decision

Introduce one final publication model, `CanonicalShipmentTruth`, between evidence reconciliation and human review:

`documents -> extracted evidence -> ShipmentResolution -> CanonicalShipmentTruth -> review fields -> human decisions -> outputs`

`ShipmentResolution` remains the evidence/reconciliation engine. `CanonicalShipmentTruth` is the deterministic adapter that converts its scoped evidence into the exact shipment/plant-line structure consumed by review and exports.

No downstream projector may independently reinterpret line cardinality after canonical publication.

## Canonical line identity

1. Merchandise-line evidence is grouped by explicit `line_key`.
2. Plant-component evidence is grouped by explicit `component_key`.
3. A plant component joins a merchandise line only when deterministic identity evidence exists: the component taxon (`taxon:<genus>:<species>`) is explicitly present in that merchandise line's description evidence, or there is exactly one line and one component.
4. Ambiguous joins fail closed. They are not assigned to line 1 and are not turned into invented conflicts.
5. Line ordering uses explicit row ordinals where available and remains stable otherwise.

This is an identity/reconciliation rule, not an AI inference.

## Canonical field state

Every canonical field preserves its target PPQ field, semantic state (`SUPPORTED`, `SUPPORTED_MULTIPLE`, `NEAR_MATCH`, `REVIEW_REQUIRED`, `CONFLICT`, `MISSING`), distinct normalized values, entity-local evidence, strongest provenance and source authority.

Low-authority harvest-country evidence remains a visible candidate with `REVIEW_REQUIRED`; it must never become `MISSING` merely because automatic acceptance is unsafe.

## Publication semantics

Canonical publication runs after operation-level Engine 2 reconciliation on a sealed source set. For each unreviewed line field, human-reviewed values are immutable; earlier machine values are superseded; older machine candidates are marked `REJECTED`; canonical candidates are published as `PENDING` and line-isolated; stale blocking conflicts are resolved when values are proven parallel; true same-entity conflicts stay blocking; and `MISSING` means there is genuinely no canonical evidence for that entity.

Customer operation detail must hide `REJECTED` candidates. Audit/history remains persisted.

## Progressive retirement

During migration, per-document projection may still populate provisional values while documents arrive. It is not authoritative after source-set finalization.

- `engine2_suggestions` becomes a compatibility entry point delegating final publication to canonical truth.
- verified AI may enrich evidence but must not publish line-scoped review fields independently.
- specialized AI remains an evidence producer/judge; canonical publication is the final line publisher.
- existing review/finalization/export code continues consuming `UsLaceyOperationField`, synchronized from one canonical truth.

This avoids a flag-day database migration while removing parallel final authority.

## Fail-closed invariants

Canonical publication never overwrites `human_value` or a reviewed field, never assigns ambiguous component evidence by convenience, never turns low-authority evidence into automatic acceptance, never suppresses a true same-entity conflict, and never invents a missing regulatory fact.

## Golden acceptance contract

For the seven-document regression set:

- exactly two canonical plant lines;
- line 1: `4407110190`, `18300`, `Pinus`, `Pinus taeda`, `30`, `m3`;
- line 2: `4407990190`, `12640`, `Eucalyptus`, `Eucalyptus grandis`, `16`, `m3`;
- each line contains only its own botanical/merchandise evidence;
- HTS and merchandise descriptions from parallel lines are not conflicts;
- low-authority harvest country is visible as `REVIEW_REQUIRED`, not `MISSING`;
- truly absent data remains `MISSING`;
- human-reviewed values survive canonical republishing unchanged.

The branch is not mergeable until these invariants and the existing full/PostgreSQL suites are green.