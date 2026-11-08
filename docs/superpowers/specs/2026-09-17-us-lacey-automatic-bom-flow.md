# U.S. Lacey Automatic BOM Flow — Design Spec

## Goal

Integrate the existing deterministic Product Intelligence/BOM foundation into the automatic U.S. Lacey operation workflow so a customer can upload normal shipment files and, when an explicit compatible BOM exists in CSV/XLS/XLSX, Litoral Trace builds a source-backed product-composition snapshot automatically inside the operation.

## Authority boundary

Product Intelligence remains **non-canonical**. Its output is structured evidence for later taxonomy/regulatory work and for customer review. It MUST NOT write to `canonical_shipment_truth`, PPQ 505 fields, LAWGS exports, declaration readiness, billing, auth, or legal/compliance decisions.

## Runtime placement

The integration runs only after the exact sealed source set has been claimed for finalization. It consumes the immutable `UsLaceySourceSetRevision` membership and produces one snapshot tied to that exact revision/fingerprint. The worker invokes the Product Intelligence snapshot builder as operation-level work while holding the existing operation projection lock, before source-set finalization.

The snapshot builder may inspect only source-set member originals. Explicit tabular BOM recognition is fail-closed: CSV/XLS/XLSX tables are considered BOMs only when `bind_bom_headers()` can deterministically bind required SKU/component/material columns. Unsupported files and non-BOM tables are skipped, not guessed.

## Persistence

Add `us_lacey_product_intelligence_snapshots` in Alembic revision `049_lacey_product_intelligence_snapshots` with:

- tenant scope: `organization_id`;
- operation and exact `source_set_revision_id`;
- `generation` and `source_set_fingerprint`;
- status: `READY`, `PARTIAL`, `FAILED`, `NOT_APPLICABLE`, `STALE`;
- summary counts: source documents, eligible tabular documents, recognized BOM tables, unique SKUs, components, materials, issues;
- immutable `payload_json` containing source-backed composition results and issues;
- optional safe error code/message;
- creation/finalization timestamps.

There is at most one snapshot per tenant/source-set revision. Payload contents are immutable after creation. When a new source-set generation is sealed, prior non-stale Product Intelligence snapshots for that operation are marked `STALE`; customer reads only the snapshot associated with the current source set.

## Snapshot payload v1

The persisted JSON is a stable, JSON-safe representation. It contains:

- `schema_version`;
- operation/source-set identity;
- summary counts;
- `sources[]`, each with assurance/operation-document identity, original filename, source hash, and recognized BOM tables;
- each recognized table contains its source locator, compositions and issues;
- each component/material preserves its existing `SourceAnchor` and normalized mass (`Decimal` serialized as strings).

No raw whole-document text is copied into logs or snapshot metadata beyond the BOM cell values already represented by the Product Intelligence domain objects.

## Status semantics

- `NOT_APPLICABLE`: no source-set member contains a deterministically recognized explicit BOM table and no eligible tabular parser failed.
- `READY`: at least one composition exists and there are no Product Intelligence ERROR issues or parser failures.
- `PARTIAL`: at least one composition exists, but one or more recognized rows/tables/documents produced errors.
- `FAILED`: eligible tabular processing produced errors and no usable composition could be built.
- `STALE`: snapshot belongs to a superseded source-set generation.

A non-applicable or failed Product Intelligence snapshot must **not fail the U.S. Lacey processing job**. BOM is additive/non-authoritative in this milestone; its failure is surfaced as Product Intelligence state, while the established Lacey processing path remains usable.

## Read/UI contract

Add a tenant-scoped read service returning only the Product Intelligence snapshot for the current source-set revision. The operation page receives a safe view model with status, counts, compositions and issues. UI copy must state that this is product-composition evidence, not final Lacey declaration data.

The operation detail page shows a Product Intelligence card only when a current snapshot exists. `NOT_APPLICABLE` may be shown compactly; `READY/PARTIAL/FAILED` expose counts, SKU/component/material structure and source filename/table/row/locator where available.

## Source-set fencing and idempotency

- Snapshot creation requires an exact claimed `SourceSetClaim` and revalidates tenant, operation, revision id, fingerprint, current flag, `FINALIZING` state, and claim token.
- One revision produces one snapshot; retries return the existing snapshot rather than duplicate it.
- A later source-set seal marks prior snapshots stale.
- If the claim is stale or invalid, no snapshot is published.

## Security

- RLS + FORCE RLS using `app.current_organization_id`.
- Runtime application role receives normal table CRUD needed by the service.
- Dedicated worker executor role must not receive direct table privileges beyond its existing role architecture; the current worker uses the tenant-aware application session seam for operation-level persistence.
- Cross-tenant composite foreign keys prevent linking a snapshot to another tenant's operation/source-set revision.
- Public role receives no table/sequence privileges.

## Tests / acceptance

Acceptance requires:

1. ORM/migration metadata contract and single Alembic head 049.
2. PostgreSQL RLS/tenant FK/idempotency acceptance.
3. Unit test: explicit CSV BOM -> `READY` with source rows preserved.
4. Unit test: explicit XLSX BOM -> `READY`.
5. Unit test: no compatible BOM -> `NOT_APPLICABLE`.
6. Unit test: mixed good + bad BOM rows/document failures -> `PARTIAL`; no usable composition -> `FAILED`.
7. Source-set supersession marks prior snapshot `STALE`.
8. Invalid/stale claim cannot publish.
9. Worker integration calls Product Intelligence once for a claimed final source set and does not let Product Intelligence failure fail the canonical Lacey job.
10. Customer read is tenant-scoped/current-only and UI renders source-backed product composition without changing canonical review/export semantics.
11. `src/litoral_trace/product_intelligence/**`, the new model/migration and Product Intelligence tests trigger the U.S. Lacey PostgreSQL gate.
12. Full CI and U.S. Lacey PostgreSQL gate pass before merge.
