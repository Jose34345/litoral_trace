# U.S. Lacey Data Model Map

This document is a navigation map, not a replacement for SQLAlchemy models or Alembic migrations.

## Main persistence families

### Organization / tenant boundary
Relevant code includes `src/litoral_trace/db/models/organization.py` plus shared authentication/audit infrastructure. Tenant-owned U.S. Lacey state must remain organization-scoped and consistent with existing PostgreSQL RLS policies.

### U.S. Lacey operational state
- `src/litoral_trace/db/models/us_lacey.py`
- `src/litoral_trace/db/models/us_lacey_commercial.py`
- `src/litoral_trace/db/models/us_lacey_payment_event.py`

These model families back operation/commercial/self-service/billing behavior. Consult migrations 034–044 before modifying persistent contracts.

### Source documents and extracted text
- `src/litoral_trace/db/models/assurance_document.py`
- `src/litoral_trace/db/models/document_text.py`

Source-document identity must remain distinct from interpreted/normalized assertions. Source-linked review should navigate back through this layer.

### Evidence snapshots / semantic evidence
- `src/litoral_trace/db/models/us_lacey_evidence_snapshot.py`
- `src/litoral_trace/db/models/semantic_evidence.py`

Migrations 045–047 introduced evidence snapshots, multilingual text spans and the semantic evidence graph. These structures remain the preferred provenance foundation for downstream Product Intelligence/taxonomy/regulatory decisions.

### Product Intelligence snapshots
- `src/litoral_trace/db/models/us_lacey_product_intelligence.py`
- `src/litoral_trace/us_lacey/product_intelligence_snapshot.py`
- migration `049_add_lacey_product_intelligence_snapshots.py`

Each Product Intelligence snapshot is tenant-owned and bound to the U.S. Lacey operation plus the source-set revision/fingerprint from which it was computed. The lifecycle is `READY / PARTIAL / FAILED / NOT_APPLICABLE / STALE`.

Important contracts:
- tenant ownership is enforced with PostgreSQL RLS/FORCE RLS and tenant-aware foreign keys;
- generation/source-set fencing prevents stale work from replacing the current result;
- superseded snapshots become `STALE` rather than being silently treated as current;
- the read path exposes only the current non-stale snapshot for the operation;
- payload data preserves file/table/sheet/row provenance from the explicit BOM parser;
- this table stores non-canonical product-composition evidence and must not be treated as canonical shipment truth or direct PPQ505/LAWGS input.

### Audit trail
- `src/litoral_trace/db/models/audit_log.py`

Human and operational actions with compliance significance should remain auditable using existing patterns rather than private ad-hoc logs.

## Source-set revisions
Migration `048_add_lacey_source_set_revisions.py` and `src/litoral_trace/us_lacey/source_sets.py` define the generation/source-set boundary used by current processing. Derived results such as Product Intelligence snapshots must be attributable to the exact source-set revision from which they were computed.

## Evidence-oriented conceptual model

```text
Organization
  -> Operation
      -> SourceSetRevision
          -> SourceDocument / DocumentVersion
              -> DocumentText / TextSpan
                  -> Evidence / SemanticNode
                      -> Candidate / Assertion
      -> ProductIntelligenceSnapshot
          -> payload: SKU / Component / Material
          -> source anchors: file / sheet / row
      -> Canonical field or ReviewAction
```

The Product Intelligence snapshot is parallel non-canonical derived evidence scoped to a source-set revision. It does not sit above or overwrite canonical shipment truth.

## Current Product Intelligence persistence rules
When changing Product Intelligence persistence:
- preserve `organization_id` tenant ownership and RLS/FORCE RLS;
- preserve source-set revision/fingerprint identity;
- preserve idempotency for repeated computation of the same source set;
- preserve `STALE` supersession semantics;
- preserve raw/source provenance in the payload;
- keep status/read behavior fail-safe when no current snapshot exists;
- do not introduce a direct write into canonical shipment truth, PPQ505 or LAWGS.

## Future extension constraints
When Taxonomy and Regulatory Decision entities are added:
- every tenant-owned row must carry/derive tenant ownership compatible with RLS;
- every derived decision should identify the operation/source-set revision and, where relevant, the Product Intelligence snapshot/component/material it evaluated;
- source-backed values should reference existing evidence/span/document identity or Product Intelligence source anchors;
- normalized values should preserve raw source representation;
- regulatory decisions should store ruleset/version and a calculation/explanation trace;
- human overrides should be separate audited actions, not destructive overwrites of source observations.
