# U.S. Lacey Engineering Roadmap

This is the short current roadmap for agent navigation. It intentionally excludes historical milestone detail.

## NOW — protect and consolidate the existing product
1. Keep the current source-set/canonical finalization pipeline stable.
2. Use this AI development control plane (`AGENTS.md` + `docs/us-lacey/`) as the canonical navigation layer.
3. Keep Product Intelligence / explicit BOM, Taxonomy Resolver and deterministic Regulatory Rules regression-protected and non-canonical.
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
The focused `src/litoral_trace/us_lacey/regulatory/taxonomy/` package provides a deliberately small, versioned, deterministic v1 resolver for BOM material names.

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

## DELIVERED — Deterministic Regulatory Rules
The focused `src/litoral_trace/us_lacey/regulatory/rules/` package and source-set-scoped regulatory assessment snapshot now provide explicit, versioned, reproducible rule evaluations.

Current scope:
- versioned ruleset identity and deterministic result contracts;
- `Decimal` calculations with explicit calculation traces;
- de minimis evaluation with exact-input requirements and fail-closed `INDETERMINATE` output when required facts are absent;
- composite/special-use evaluation with explicit material/construction facts and due-care determinability kept separate;
- rule-scoped `PASS / FAIL / INDETERMINATE` results only — never an overall shipment compliance verdict;
- evidence references, reason codes and review-required state attached to rule outputs;
- tenant-scoped persistence in migration 050, fenced to source-set revision/fingerprint and ruleset version;
- prior regulatory snapshots become `STALE` when a newer source-set generation wins;
- customer UI presents the assessments as non-canonical work product, not a final filing or compliance determination.

Current safety boundary:
- Product Intelligence/taxonomy observations do not become canonical declaration values through this engine;
- missing HTS/weight/protected-status/due-care facts stay indeterminate rather than being inferred for a safer-looking outcome;
- regulatory assessment snapshots do not write to canonical shipment truth, PPQ505, LAWGS or ACE;
- human review remains required where the rule result says so.

## NEXT — build the work-reduction product

### 1. Exception-first Human Review
Goal: reviewers inspect only unresolved or risky items instead of re-reading full shipments.

Initial scope:
- missing evidence;
- conflicting documents;
- taxonomy ambiguity;
- indeterminate regulatory calculations;
- direct source navigation;
- accept/reject/correct/request-evidence actions using existing auditable review patterns.

### 2. Source-linked Review Package
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
