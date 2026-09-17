# U.S. Lacey Architecture

## Architectural style
U.S. Lacey is a modular monolith inside Litoral Trace. The product uses FastAPI/application code, PostgreSQL with tenant isolation/RLS, workers/jobs, a document-understanding engine, evidence/provenance structures, human review and export builders. Keep this shape while validating product demand; do not split into microservices without a demonstrated operational need.

## Bounded contexts

### 1. Document Understanding — `src/litoral_trace/lacey_engine/`
Purpose: read documents and produce evidence-backed observations/candidates.

Owns:
- document admission/classification;
- layout/segmentation;
- source authority/ranking;
- AI provider routing and shadow behavior;
- specialist orchestration;
- SKU/line binding;
- candidate fusion/resolution;
- semantic evidence representation.

Does not own final customer-facing regulatory truth.

### 2. Product Intelligence — `src/litoral_trace/product_intelligence/`
Purpose: deterministically transform explicit parsed BOM tables into reusable product composition observations.

Owns:
- immutable SKU/component/material contracts;
- explicit BOM header binding;
- Decimal quantity/mass normalization;
- source anchors to document/table/sheet/row;
- row-level issues;
- SKU isolation.

Consumes the existing Assurance CSV/XLSX parser authority. It has no persistence, taxonomy inference, regulatory authority or canonical-publication authority.

### 3. U.S. Lacey Application — `src/litoral_trace/us_lacey/`
Purpose: execute the Lacey product workflow around document-engine results.

Owns:
- operation lifecycle;
- upload/storage integration;
- job/worker lifecycle;
- source-set generation/finalization;
- reconciliation and publication support;
- canonical shipment truth;
- customer projection;
- human review;
- PPQ505/export preparation;
- portal/access/self-service/billing integration.

### 4. Persistence — `src/litoral_trace/db/models/` + Alembic
Purpose: persist tenant-owned operational, evidence, audit, commercial and review state.

Relevant model families include U.S. Lacey core/commercial/payment state, evidence snapshots, document text/assurance documents, semantic evidence, organizations and audit logs. Historical migrations are immutable.

U.S. Lacey schema evolution is visible from migrations 034–048, including core pilot, self service, portal auth, PPQ505 contract, billing, Engine 2 shadow, evidence snapshots, multilingual text spans, semantic evidence graph and source-set revisions.

### 5. Presentation — `src/litoral_trace/web/`
Purpose: customer/admin views and controller-style application entrypoints. Business/regulatory decisions should not be invented in templates/views.

### 6. Verification/Delivery — `tests/` and `.github/workflows/`
Purpose: executable contracts and release gates. U.S. Lacey has dedicated PostgreSQL/Neon/Render gates in addition to general CI.

## Core runtime direction

```text
source documents
    -> ingestion/storage
    -> operation + source-set generation
    -> job/worker
    -> lacey_engine document understanding
    -> specialist/AI candidates + evidence
    -> application reconciliation
    -> evidence/canonical publication support
    -> canonical shipment truth
    -> projection + human review
    -> export/review package
```

For explicit BOM inputs, a non-canonical side path is now available:

```text
CSV/XLSX
    -> existing Assurance parser
    -> Product Intelligence header binding
    -> SKU/Component/Material composition + source anchors/issues
    -> future taxonomy/regulatory layers
```

AI/shadow outputs are advisory until application-level authority/reconciliation permits publication. Human review remains an explicit authority step for ambiguity and exceptions.

## Critical architectural boundaries

### Source-set boundary
A set/generation of source documents must retain identity. Results from stale generations must not overwrite current published truth.

### Evidence boundary
A supported field/decision must remain traceable to evidence. Product Intelligence and future regulatory decisions must reuse the existing evidence chain instead of creating a parallel provenance subsystem.

### Canonical-truth boundary
Customer-facing/exportable truth must pass through canonical publication rules. Product Intelligence output, extraction and specialist output are not equivalent to canonical truth.

### Human-review boundary
Review actions must remain auditable and must not be bypassed when the system lacks sufficient evidence or has unresolved conflicts.

### Tenant boundary
Organization-owned data must remain isolated, including any future product/BOM/taxonomy/review persistence.

## Hotspots — preserve, do not grow casually
The following files are central and comparatively large. They are not targets for broad refactoring in feature work:
- `src/litoral_trace/us_lacey/_operations_core.py`
- `src/litoral_trace/us_lacey/worker.py`
- `src/litoral_trace/us_lacey/canonical_shipment_truth.py`
- `src/litoral_trace/us_lacey/projection.py`
- `src/litoral_trace/us_lacey/review.py`
- `src/litoral_trace/us_lacey/lacey_engine_service.py`
- `src/litoral_trace/us_lacey/shadow_evidence_snapshot.py`

Rule: if a new capability has its own domain language or lifecycle, prefer a focused module/package and integrate through an explicit interface rather than adding more unrelated branches to a hotspot.

## Target extension seams

### Product Intelligence (active foundation)
Explicit CSV/XLSX BOM composition now lives in `src/litoral_trace/product_intelligence/`. Extend this domain rather than hiding product structure inside prompts. Preserve source references and non-canonical authority.

### U.S. Lacey Regulatory Intelligence (planned)
Taxonomy resolution, versioned rule sets, de minimis, composite/special-use classification and readiness decisions should be explicit and auditable. They should not be hidden solely in prompts or UI code.

### Exception-first review (planned)
Extend the existing review model so specialists primarily see unresolved/blocked/review-required items with direct source links rather than re-reading every document.

## Design rule for agents
Before implementing a new feature, answer:
1. Which bounded context owns it?
2. What is its authority level: observation, inferred, deterministic, review-required, human-confirmed or canonical?
3. Which existing evidence/source-set/canonical interfaces must it use?
4. Which tests/gates protect those interfaces?

If those answers are unclear, update the architecture/control-plane docs before adding runtime code.
