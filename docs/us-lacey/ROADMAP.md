# U.S. Lacey Engineering Roadmap

This is the short current roadmap for agent navigation. It intentionally excludes historical milestone detail.

## NOW — protect and consolidate the existing product
1. Keep the current source-set/canonical finalization pipeline stable.
2. Use this AI development control plane (`AGENTS.md` + `docs/us-lacey/`) as the canonical navigation layer.
3. Keep Product Intelligence / explicit BOM and Taxonomy Resolver regression-protected and non-canonical.
4. Do not perform broad package moves or destructive cleanup while the commercial product is being validated.
5. Keep evidence, tenant isolation, human review, canonical publication and exports regression-protected.

## DELIVERED — Product Intelligence / explicit BOM foundation
The reusable `src/litoral_trace/product_intelligence/` domain now provides:
- explicit CSV/XLSX BOM ingestion through the existing Assurance parser authority;
- immutable `SKU -> Component -> Material` composition contracts;
- raw + normalized mass/unit values using `Decimal`;
- source anchors to table/sheet/row/document identity;
- explicit issue records for incomplete/invalid rows;
- deterministic SKU isolation;
- no persistence, taxonomy inference or canonical/regulatory authority inside the reusable BOM domain.

The U.S. Lacey Product Intelligence integration persists the BOM as a tenant-scoped, source-set-versioned, non-canonical snapshot and exposes only the current non-stale generation.

## DELIVERED — Taxonomy Resolver
The focused `src/litoral_trace/us_lacey/regulatory/taxonomy/` package now provides a deliberately small, versioned, deterministic v1 resolver for BOM material names.

Current scope:
- exact accepted scientific-name resolution;
- exact curated synonym/common/commercial/genus aliases;
- deterministic confidence/reason/provenance metadata;
- explicit `RESOLVED`, `REVIEW_REQUIRED`, `AMBIGUOUS` and `NO_MATCH` states;
- no fuzzy matching;
- genus-only/commercial names are never promoted automatically to a species fact;
- taxonomy is embedded as non-canonical evidence in Product Intelligence material payloads while preserving the original source anchor.

Current safety boundary:
- the catalog is intentionally small and does not claim broad botanical coverage;
- taxonomy output does not write to canonical shipment truth, PPQ505 or LAWGS;
- `REVIEW_REQUIRED`, `AMBIGUOUS` and `NO_MATCH` remain unresolved for later human/regulatory handling;
- no database migration was introduced for this milestone because enrichment lives inside the existing immutable Product Intelligence snapshot JSON.

## NEXT — build the work-reduction product

### 1. Deterministic Regulatory Rules
Goal: explicit, versioned, reproducible decisions rather than prompt-only compliance conclusions.

Initial scope:
- versioned ruleset model/contract;
- unit normalization and calculation trace;
- de minimis assessment;
- composite/special-use classification support;
- PASS / FAIL / INDETERMINATE outputs;
- evidence/ruleset/version attached to every decision.

Recommended ownership: `src/litoral_trace/us_lacey/regulatory/rules/`.

### 2. Exception-first Human Review
Goal: reviewers inspect only unresolved or risky items instead of re-reading full shipments.

Initial scope:
- missing evidence;
- conflicting documents;
- taxonomy ambiguity;
- indeterminate regulatory calculations;
- direct source navigation;
- accept/reject/correct/request-evidence actions using existing auditable review patterns.

### 3. Source-linked Review Package
Goal: one customer-facing output showing product composition, species/taxonomy, rule results, conflicts, missing evidence and reviewer status, with direct provenance.

Initial formats:
- UI;
- JSON/CSV structured export;
- human-readable report only if it helps pilots.

## MARKET GATE
After the above is good enough on a small golden dataset, stop expanding features and test with real companies using redacted documents where necessary.

Primary validation metrics:
- human review minutes per SKU;
- percent of supported decisions with source navigation;
- percent auto-resolved vs review-required;
- reviewer override rate;
- false-safe count (target 0);
- missing/conflict detection usefulness.

## LATER — only after demand evidence
- broader curated taxonomy coverage driven by real shipment evidence;
- deeper ACE/LAWGS/broker/ERP integration;
- supplier orchestration;
- enterprise SSO/security certifications as demanded by customers;
- higher-scale infrastructure only when measured load requires it;
- broader autonomous-agent behavior only where ROI and authority boundaries are clear.

## Explicitly not a current priority
Do not spend the next cycle on generic dashboards, more document types without a customer case, microservices, Kafka, vector databases, mobile apps, or broad UI polish disconnected from the review workflow.
