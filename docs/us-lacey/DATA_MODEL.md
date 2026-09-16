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

Source-document identity must remain distinct from interpreted/normalized assertions. Future source-linked review should navigate back through this layer.

### Evidence snapshots / semantic evidence
- `src/litoral_trace/db/models/us_lacey_evidence_snapshot.py`
- `src/litoral_trace/db/models/semantic_evidence.py`

Migrations 045–047 introduced evidence snapshots, multilingual text spans and the semantic evidence graph. These structures are the preferred provenance foundation for future Product Intelligence and regulatory decisions.

### Audit trail
- `src/litoral_trace/db/models/audit_log.py`

Human and operational actions with compliance significance should remain auditable using existing patterns rather than private ad-hoc logs.

## Source-set revisions
Migration `048_add_lacey_source_set_revisions.py` and `src/litoral_trace/us_lacey/source_sets.py` define the generation/source-set boundary used by current processing. Any future derived result should be attributable to the source-set revision from which it was computed.

## Evidence-oriented conceptual model

```text
Organization
  -> Operation
      -> SourceSetRevision
          -> SourceDocument / DocumentVersion
              -> DocumentText / TextSpan
                  -> Evidence / SemanticNode
                      -> Candidate / Assertion
                          -> Canonical field or ReviewAction
```

The exact deployed schema remains authoritative; this diagram states the intended dependency direction.

## Future extension constraints
When Product Composition/BOM, Taxonomy and Regulatory Decision entities are added:
- every tenant-owned row must carry/derive tenant ownership compatible with RLS;
- every derived decision should identify the operation/source-set revision;
- source-backed values should reference existing evidence/span/document identity;
- normalized values should preserve raw source representation;
- regulatory decisions should store ruleset/version and a calculation/explanation trace;
- human overrides should be separate audited actions, not destructive overwrites of source observations.

Do not add those future tables as part of documentation/control-plane work.