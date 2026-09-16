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
- Regulatory/export contract: `ppq505.py`, `exporters/`
- Access/auth/security: `access.py`, `csrf.py`, `portal_auth.py`, `portal_config.py`, `config.py`, `db.py`
- Commercial/self-service: `commercial.py`, `self_service.py`, `live_readiness.py`, `lemon_billing.py`, `lemon_squeezy.py`, `email_delivery.py`

## Do not implement here
Raw document classification, segmentation, specialist extraction, source-authority ranking, line binding or multi-agent fusion belong in `src/litoral_trace/lacey_engine/` unless the existing architecture proves otherwise.

## Protected boundaries
- Do not bypass `source_sets.py` when publishing results from a document generation.
- Do not write directly to customer-facing truth from an AI suggestion path.
- Do not downgrade REVIEW_REQUIRED/ambiguous states merely to increase automation.
- Do not bypass organization scoping/RLS for convenience.
- Do not add new domain rules directly to `projection.py`; projection should render/shape already-decided state.
- Treat `_operations_core.py`, `worker.py`, `review.py`, `canonical_shipment_truth.py`, and `projection.py` as hotspots. New capabilities should normally live in focused modules/packages and integrate through explicit interfaces.

## Planned domain placement
For the current commercial roadmap:
- reusable product/BOM composition should live outside this package in a focused `product_intelligence` domain;
- U.S.-specific taxonomy/regulatory rules should live under a focused `us_lacey/regulatory/` package when implemented;
- exception-first review should extend the existing review workflow rather than create a parallel review system;
- all new decisions should reuse the existing evidence/provenance chain.

## Required reading
Read root `AGENTS.md`, then `docs/us-lacey/ARCHITECTURE.md`, `CAPABILITIES.toml`, `INVARIANTS.md`, and `TEST_MATRIX.md` before changing this package.