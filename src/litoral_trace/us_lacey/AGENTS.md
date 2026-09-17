# U.S. Lacey Application Agent Guide

This package owns the U.S. Lacey application/workflow layer. It consumes document-engine outputs; it should not become a second document-extraction engine.

## Where to work
- Upload/storage: `ingestion.py`, `storage.py`, `batch_hardening.py`
- Operation lifecycle: `_operations_core.py`, `operations.py`, `workflow.py`
- Jobs/workers/locking: `jobs.py`, `worker.py`, `worker_db.py`, `worker_runner.py`, `operation_lock.py`
- Source-set identity/finalization: `source_sets.py`
- Engine integration: `lacey_engine_service.py`, `lacey_engine_dossier.py`, `engine2_suggestions.py`
- Candidate normalization/reconciliation: `candidate_normalization.py`, `candidate_reconciliation.py`, `reconciliation_invariants.py`, `semantic_reconciliation.py`, `cross_document_line_identity.py`
- Evidence/canonical publication: `semantic_evidence_read.py`, `shadow_evidence_snapshot.py`, `canonical_publication_support.py`, `canonical_shipment_truth.py`
- Specialized shadow/projection: `specialized_shadow.py`, `specialized_projection.py`, `specialized_projection_runtime.py`, `specialized_inference_cache.py`
- Human review: `review.py`, `bulk_review.py`, `ai_review.py`, `ai_suggestions.py`
- Customer-facing projection: `projection.py`
- Product Intelligence snapshot/read path: `product_intelligence_snapshot.py`
- U.S.-specific taxonomy support: `regulatory/taxonomy/`
- Regulatory/export contract: `ppq505.py`, `exporters/`
- Access/auth/security: `access.py`, `csrf.py`, `portal_auth.py`, `portal_config.py`, `config.py`, `db.py`
- Commercial/self-service: `commercial.py`, `self_service.py`, `live_readiness.py`, `lemon_billing.py`, `lemon_squeezy.py`, `email_delivery.py`

## Do not implement here
Raw document classification, segmentation, specialist extraction, source-authority ranking, line binding or multi-agent fusion belong in `src/litoral_trace/lacey_engine/` unless the existing architecture proves otherwise.

Reusable BOM composition remains in `src/litoral_trace/product_intelligence/`; U.S.-specific taxonomy and future regulatory rules belong under `src/litoral_trace/us_lacey/regulatory/`.

## Protected boundaries
- Do not bypass `source_sets.py` when publishing results from a document generation.
- Do not write directly to customer-facing truth from an AI suggestion path.
- Do not downgrade REVIEW_REQUIRED/ambiguous states merely to increase automation.
- Do not bypass organization scoping/RLS for convenience.
- Do not add new domain rules directly to `projection.py`; projection should render/shape already-decided state.
- Treat `_operations_core.py`, `worker.py`, `review.py`, `canonical_shipment_truth.py`, and `projection.py` as hotspots. New capabilities should normally live in focused modules/packages and integrate through explicit interfaces.
- Taxonomy is advisory/non-canonical evidence. `regulatory/taxonomy/` must never write directly to canonical shipment truth, PPQ505 or LAWGS.
- Taxonomy v1 is exact-match only. Do not add fuzzy matching or promote genus/common/commercial aliases to species facts without a separately reviewed design and tests.
- Product Intelligence readiness must not become safer merely because taxonomy returned a candidate; unresolved taxonomy remains an exception for later review/regulatory logic.

## Current domain placement
For the current commercial roadmap:
- reusable product/BOM composition lives outside this package in `src/litoral_trace/product_intelligence/`;
- active U.S.-specific taxonomy support lives in `src/litoral_trace/us_lacey/regulatory/taxonomy/`;
- future deterministic regulatory rules should live under `src/litoral_trace/us_lacey/regulatory/rules/`;
- exception-first review should extend the existing review workflow rather than create a parallel review system;
- all new decisions should reuse the existing evidence/provenance chain.

## Taxonomy change checklist
When changing `regulatory/taxonomy/` or taxonomy serialization:
1. run `tests/test_us_lacey_taxonomy_resolver.py`;
2. run `tests/test_us_lacey_product_intelligence_taxonomy.py` plus Product Intelligence snapshot/worker tests;
3. preserve catalog version/reason/authority metadata;
4. prove ambiguous/no-match/genus-only inputs fail closed;
5. run general CI and U.S. Lacey PostgreSQL Gate before merge.

## Required reading
Read root `AGENTS.md`, then `docs/us-lacey/ARCHITECTURE.md`, `CAPABILITIES.toml`, `INVARIANTS.md`, and `TEST_MATRIX.md` before changing this package.
