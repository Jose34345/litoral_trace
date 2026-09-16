# U.S. Lacey Engineering Roadmap

This is the short current roadmap for agent navigation. It intentionally excludes historical milestone detail.

## NOW — protect and consolidate the existing product
1. Keep the current source-set/canonical finalization pipeline stable.
2. Use this AI development control plane (`AGENTS.md` + `docs/us-lacey/`) as the canonical navigation layer.
3. Do not perform broad package moves or destructive cleanup while the commercial product is being validated.
4. Keep evidence, tenant isolation, human review, canonical publication and exports regression-protected.

## NEXT — build the work-reduction product

### 1. Product Intelligence / BOM
Goal: structured `Product -> SKU -> Component -> Material` composition backed by source evidence.

Initial scope:
- explicit XLSX/CSV BOM first;
- PDF tables only where extraction is reliable;
- preserve raw + normalized values/units;
- prevent cross-SKU/component leakage;
- attach every supported component/material/weight to existing provenance.

Recommended ownership: reusable `src/litoral_trace/product_intelligence/` domain, integrated with U.S. Lacey rather than hidden inside `lacey_engine` prompts.

### 2. Taxonomy Resolver
Goal: turn commercial/common/scientific plant names into versioned taxonomic candidates without fabricating certainty.

Initial scope:
- accepted scientific names;
- synonyms;
- curated commercial/common aliases;
- APHIS-relevant species grouping context where applicable;
- confidence/reason/provenance;
- explicit AMBIGUOUS / NO_MATCH / REVIEW_REQUIRED states.

Recommended ownership: focused `src/litoral_trace/us_lacey/regulatory/taxonomy/` package.

### 3. Deterministic Regulatory Rules
Goal: explicit, versioned, reproducible decisions rather than prompt-only compliance conclusions.

Initial scope:
- versioned ruleset model/contract;
- unit normalization and calculation trace;
- de minimis assessment;
- composite/special-use classification support;
- PASS / FAIL / INDETERMINATE outputs;
- evidence/ruleset/version attached to every decision.

Recommended ownership: `src/litoral_trace/us_lacey/regulatory/rules/`.

### 4. Exception-first Human Review
Goal: reviewers inspect only unresolved or risky items instead of re-reading full shipments.

Initial scope:
- missing evidence;
- conflicting documents;
- taxonomy ambiguity;
- indeterminate regulatory calculations;
- direct source navigation;
- accept/reject/correct/request-evidence actions using existing auditable review patterns.

### 5. Source-linked Review Package
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
- deeper ACE/LAWGS/broker/ERP integration;
- supplier orchestration;
- enterprise SSO/security certifications as demanded by customers;
- higher-scale infrastructure only when measured load requires it;
- broader autonomous-agent behavior only where ROI and authority boundaries are clear.

## Explicitly not a current priority
Do not spend the next cycle on generic dashboards, more document types without a customer case, microservices, Kafka, vector databases, mobile apps, or broad UI polish disconnected from the review workflow.