# U.S. Lacey Architecture

## Architectural style
U.S. Lacey is a modular monolith inside Litoral Trace. The product uses FastAPI/application code, PostgreSQL with tenant isolation/RLS, workers/jobs, a document-understanding engine, Product Intelligence, deterministic regulatory rules, evidence/provenance structures, human review and export builders. Keep this shape while validating product demand; do not split into microservices without a demonstrated operational need.

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

### 2. Product Intelligence — `src/litoral_trace/product_intelligence/` + U.S. Lacey snapshot integration
Purpose: deterministically transform explicit parsed BOM tables into reusable product-composition observations and persist the current source-set result without making it canonical.

Owns in `src/litoral_trace/product_intelligence/`:
- immutable SKU/component/material contracts;
- explicit BOM header binding;
- Decimal quantity/mass normalization;
- source anchors to document/table/sheet/row;
- row-level issues;
- SKU isolation.

U.S. Lacey integration owns:
- source-set-scoped Product Intelligence snapshots in `src/litoral_trace/us_lacey/product_intelligence_snapshot.py`;
- tenant persistence via `src/litoral_trace/db/models/us_lacey_product_intelligence.py` and migration 049;
- READY/PARTIAL/FAILED/NOT_APPLICABLE/STALE lifecycle;
- generation/fingerprint fencing and current-only reads;
- worker invocation after canonical publication and before regulatory/multilingual/source-set finalization;
- customer presentation as non-canonical product-composition evidence.

It consumes the existing Assurance CSV/XLS/XLSX parser authority. Product Intelligence has no canonical PPQ505/LAWGS publication authority and no permission to overwrite canonical shipment truth. Taxonomy and regulatory semantics are owned by the U.S.-specific regulatory package.

### 3. U.S. Lacey Regulatory Intelligence — `src/litoral_trace/us_lacey/regulatory/`
Purpose: provide explicit, versioned, auditable U.S.-specific taxonomy and deterministic rule evaluations without silently converting them into canonical filing truth.

Taxonomy owns:
- the small versioned exact-match taxonomy catalog/resolver;
- explicit resolved/review-required/ambiguous/no-match outcomes;
- candidate reason/confidence/authority provenance;
- no fuzzy species promotion.

Regulatory Rules own:
- immutable rule inputs/results/evidence references;
- versioned deterministic ruleset identity;
- de minimis calculation/evaluation;
- SPECIAL/COMPOSITE construction evaluation;
- explicit `PASS / FAIL / INDETERMINATE` per-rule outputs;
- calculation traces and reason codes.

Regulatory Assessment integration owns:
- source-set/ruleset-versioned persistence in `regulatory_assessment_snapshot.py`;
- tenant persistence via `us_lacey_regulatory_assessment.py` and migration 050;
- stable rule-input fingerprinting;
- CURRENT/STALE lifecycle and current-only reads;
- worker invocation after Product Intelligence and before multilingual/source-set finalization;
- non-canonical customer presentation.

It does not own overall shipment compliance status, canonical shipment truth, PPQ505/LAWGS/ACE publication or human-confirmed authority. Missing facts must stay indeterminate/review-required.

### 4. U.S. Lacey Application — `src/litoral_trace/us_lacey/`
Purpose: execute the Lacey product workflow around document-engine, Product Intelligence and Regulatory Intelligence results.

Owns:
- operation lifecycle;
- upload/storage integration;
- job/worker lifecycle;
- source-set generation/finalization;
- Product Intelligence and Regulatory Assessment snapshot lifecycle integration;
- reconciliation and publication support;
- canonical shipment truth;
- customer projection;
- human review;
- PPQ505/export preparation;
- portal/access/self-service/billing integration.

### 5. Persistence — `src/litoral_trace/db/models/` + Alembic
Purpose: persist tenant-owned operational, evidence, Product Intelligence, Regulatory Assessment, audit, commercial and review state.

Relevant model families include U.S. Lacey core/commercial/payment state, evidence snapshots, Product Intelligence snapshots, Regulatory Assessment snapshots, document text/assurance documents, semantic evidence, organizations and audit logs. Historical migrations are immutable.

U.S. Lacey schema evolution is visible from migrations 034–050, including core pilot, self service, portal auth, PPQ505 contract, billing, Engine 2 shadow, evidence snapshots, multilingual text spans, semantic evidence graph, source-set revisions, Product Intelligence snapshots and Regulatory Assessment snapshots.

### 6. Presentation — `src/litoral_trace/web/` + `src/litoral_trace/templates/us_lacey/`
Purpose: customer/admin views and controller-style application entrypoints. Business/regulatory decisions should not be invented in templates/views. Product Intelligence is displayed as source-backed, non-final composition evidence; Regulatory Assessment displays already-computed rule-scoped non-final work product.

### 7. Verification/Delivery — `tests/` and `.github/workflows/`
Purpose: executable contracts and release gates. U.S. Lacey has dedicated PostgreSQL/Neon/Render gates in addition to general CI. Product Intelligence and Regulatory Assessment persistence/RLS are explicitly exercised by the U.S. Lacey PostgreSQL Gate.

## Core runtime direction

```text
source documents
    -> ingestion/storage
    -> operation + source-set generation
    -> job/worker
    -> lacey_engine document understanding
    -> specialist/AI candidates + evidence
    -> application reconciliation
    -> canonical publication support
    -> canonical shipment truth
    -> Product Intelligence snapshot (non-canonical)
    -> Regulatory Assessment snapshot (non-canonical, rule-scoped)
    -> multilingual shadow snapshot
    -> source-set finalize
    -> job COMPLETED / operation refresh
    -> projection + human review
    -> export/review package
```

For explicit BOM inputs and downstream deterministic rules:

```text
CSV/XLS/XLSX
    -> existing Assurance parser
    -> Product Intelligence header binding
    -> SKU/Component/Material composition + source anchors/issues
    -> tenant/source-set Product Intelligence snapshot
       READY / PARTIAL / FAILED / NOT_APPLICABLE / STALE
    -> taxonomy enrichment where available
    -> regulatory/rules exact deterministic evaluation
    -> tenant/source-set/ruleset Regulatory Assessment snapshot
       CURRENT / STALE
    -> current-only customer reads
```

The Product Intelligence and Regulatory Assessment stages are deliberately downstream of canonical publication for the source set, but their own outputs remain non-canonical. They cannot mutate `canonical_shipment_truth`, PPQ505, LAWGS or ACE filing data.

AI/shadow outputs are advisory until application-level authority/reconciliation permits publication. Human review remains an explicit authority step for ambiguity and exceptions.

## Critical architectural boundaries

### Source-set boundary
A set/generation of source documents must retain identity. Results from stale generations must not overwrite current published truth. Product Intelligence and Regulatory Assessment snapshots are keyed/fenced by source-set identity; superseded snapshots become STALE rather than being silently reused.

### Ruleset boundary
A regulatory result must identify the ruleset version and exact input fingerprint that produced it. A new ruleset is a new deterministic interpretation, not an in-place rewrite of historical assessment payloads.

### Evidence boundary
A supported field/decision must remain traceable to evidence. Product Intelligence preserves file/sheet/row anchors and Regulatory Assessment carries evidence references forward rather than creating a parallel provenance subsystem.

### Canonical-truth boundary
Customer-facing/exportable declaration truth must pass through canonical publication rules. Product Intelligence, Regulatory Assessment, extraction and specialist output are not equivalent to canonical truth. A rule-level PASS is never an overall shipment compliance verdict.

### Human-review boundary
Review actions must remain auditable and must not be bypassed when the system lacks sufficient evidence or has unresolved conflicts. `INDETERMINATE` stays unresolved until supported facts/review change the inputs.

### Tenant boundary
Organization-owned data must remain isolated. Product Intelligence and Regulatory Assessment snapshots use tenant ownership plus RLS/FORCE RLS and remain covered by negative cross-tenant tests.

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

### Product Intelligence (active)
Explicit CSV/XLS/XLSX BOM composition lives in `src/litoral_trace/product_intelligence/` and is persisted through the U.S. Lacey Product Intelligence snapshot integration. Extend this domain rather than hiding product structure inside prompts. Preserve source references, generation fencing, RLS and non-canonical authority.

### U.S. Lacey Regulatory Intelligence (active)
Taxonomy resolution and versioned regulatory rules live under `src/litoral_trace/us_lacey/regulatory/`. Extend explicit inputs/results/ruleset versions and evidence references rather than hiding compliance conclusions solely in prompts or UI code. Keep every result rule-scoped and non-canonical until a separately designed authority/review boundary says otherwise.

### Exception-first review (planned)
Extend the existing review model so specialists primarily see unresolved/blocked/review-required items with direct source links rather than re-reading every document.

## Design rule for agents
Before implementing a new feature, answer:
1. Which bounded context owns it?
2. What is its authority level: observation, inferred, deterministic, review-required, human-confirmed or canonical?
3. Which existing evidence/source-set/canonical interfaces must it use?
4. Which tests/gates protect those interfaces?

If those answers are unclear, update the architecture/control-plane docs before adding runtime code.
