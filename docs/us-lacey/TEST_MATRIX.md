# U.S. Lacey Test Matrix

Use this as a routing guide. Exact test names evolve; search existing tests before adding duplicates.

| Capability changed | Minimum focused validation | Broader validation |
|---|---|---|
| `lacey_engine` classification/parsing | relevant `tests/lacey_engine/` tests | general CI pytest |
| AI routing/provider/shadow | AI architecture/routing/shadow/resilience tests under `tests/lacey_engine/` | general CI pytest |
| specialist routing/fusion | specialist + line-binding/fusion tests | general CI pytest |
| cross-document identity | `tests/lacey_engine/test_cross_document_line_identity.py` and fail-closed companion | general CI pytest |
| Product Intelligence parser/domain | `tests/product_intelligence/` | general CI pytest + existing Assurance parser regressions |
| Product Intelligence snapshot/read path | `tests/test_us_lacey_product_intelligence_snapshot.py` + UI tests | general CI + U.S. Lacey PostgreSQL Gate |
| Product Intelligence worker ordering/idempotency | `tests/test_us_lacey_product_intelligence_worker.py` | general CI + U.S. Lacey PostgreSQL Gate |
| Product Intelligence RLS/supersession | `tests/test_us_lacey_product_intelligence_snapshot_postgres.py` | U.S. Lacey PostgreSQL Gate, no skip allowed for targeted Product Intelligence PostgreSQL acceptance |
| Taxonomy Resolver | `tests/test_us_lacey_taxonomy_resolver.py` + `tests/test_us_lacey_product_intelligence_taxonomy.py` | general CI + U.S. Lacey PostgreSQL Gate |
| Regulatory Rules pure domain | `tests/test_us_lacey_regulatory_rules.py` | general CI pytest |
| Regulatory Assessment payload/snapshot/read | `tests/test_us_lacey_regulatory_assessment_payload.py` + `tests/test_us_lacey_regulatory_assessment_snapshot.py` + UI tests | general CI + U.S. Lacey PostgreSQL Gate |
| Regulatory Assessment worker ordering | `tests/test_us_lacey_regulatory_assessment_worker.py` + worker stage timing test | general CI + U.S. Lacey PostgreSQL Gate |
| Regulatory Assessment RLS/supersession | `tests/test_us_lacey_regulatory_assessment_snapshot_postgres.py` | U.S. Lacey PostgreSQL Gate, no skip allowed for targeted acceptance |
| canonical shipment truth | `tests/lacey_engine/test_canonical_shipment_truth.py` | U.S. Lacey PostgreSQL gate + general CI |
| operation/source-set lifecycle | U.S. Lacey operation/source-set/worker tests under `tests/` | `.github/workflows/us-lacey-postgres-gate.yml` |
| worker locking/idempotency | worker/job/lock/source-set tests | U.S. Lacey PostgreSQL gate |
| semantic evidence/snapshots | evidence/text/semantic tests | PostgreSQL migration gate + U.S. Lacey PostgreSQL gate |
| RLS/tenant-owned persistence | negative cross-tenant/RLS tests | PostgreSQL gates |
| review/audit behavior | review/bulk-review/audit tests | general CI + relevant PostgreSQL gate |
| PPQ505/exporters | export snapshot, Excel/XML/PPQ505 tests | general CI; live gate only if runtime integration affected |
| portal/auth/self-service | auth/access/portal tests | general CI + relevant live gate when deployment behavior changes |
| billing | billing/webhook/payment-event tests | general CI |
| web/UI projection | targeted web/UI tests | `lacey-visual-qa.yml` when presentation changes materially |

## Existing CI/release gates
Important workflows currently include:
- `.github/workflows/ci.yml`
- `.github/workflows/assurance-postgres-migration-gate.yml`
- `.github/workflows/us-lacey-postgres-gate.yml`
- `.github/workflows/us-lacey-neon-live-gate.yml`
- `.github/workflows/us-lacey-render-live-gate.yml`
- `.github/workflows/lacey-preview.yml`
- `.github/workflows/lacey-visual-qa.yml`
- `.github/workflows/release-integration-gates.yml`
- `.github/workflows/required-postgres-release-attestation.yml`

## Baseline used by general CI
The current general CI config uses Python 3.11, installs `requirements.txt`, `pytest` and `alembic`, compiles `main.py src tests`, checks a single canonical Alembic head, then runs:

```bash
python -m pytest -q -rs
```

The canonical U.S. Lacey Alembic head after Hito 8 is `050_lacey_regulatory_assessment_snapshots`.

## Product Intelligence / BOM contract
The active BOM capability is protected by:
- `tests/product_intelligence/test_domain.py`
- `tests/product_intelligence/test_units.py`
- `tests/product_intelligence/test_bom_schema.py`
- `tests/product_intelligence/test_bom_ingestion.py`
- `tests/product_intelligence/test_bom_parser_integration.py`
- `tests/product_intelligence/test_bom_provenance_integrity.py`
- `tests/test_us_lacey_product_intelligence_snapshot.py`
- `tests/test_us_lacey_product_intelligence_snapshot_postgres.py`
- `tests/test_us_lacey_product_intelligence_worker.py`
- `tests/test_us_lacey_product_intelligence_ui.py`

Coverage expectations include:
- SKU/component isolation and Decimal unit normalization;
- CSV/XLS/XLSX reuse of the existing Assurance parser;
- file/table/sheet/row provenance;
- partial success for independent invalid rows;
- source-set idempotency and generation/fingerprint fencing;
- `READY / PARTIAL / FAILED / NOT_APPLICABLE / STALE` lifecycle;
- supersession to `STALE` when a newer source set wins;
- current-only reads that do not surface stale snapshots;
- tenant A/B isolation and RLS/FORCE RLS in PostgreSQL;
- worker ordering after canonical publication and before downstream non-canonical/regulatory/multilingual stages;
- terminal workspace hydration showing the Product Intelligence card without a manual full-page reload;
- preservation of the non-canonical boundary: no automatic write to canonical shipment truth, PPQ505 or LAWGS.

Changes to Assurance CSV/XLS/XLSX parsing also require the repository's existing parser regression tests.

## Taxonomy Resolver contract
The active Taxonomy Resolver is protected by:
- `tests/test_us_lacey_taxonomy_resolver.py`
- `tests/test_us_lacey_product_intelligence_taxonomy.py`

Coverage expectations include:
- exact accepted scientific-name resolution;
- synonym, common alias and commercial alias results remaining `REVIEW_REQUIRED`;
- genus-only names such as `Hevea wood` never becoming an automatic species fact;
- deterministic multiple-candidate ambiguity with no winner selection;
- unsupported names and near matches returning `NO_MATCH` rather than fuzzy guesses;
- catalog version, reason, authority source/URL and deterministic confidence in candidate output;
- preservation of BOM file/table/sheet/row/document provenance;
- taxonomy status not changing BOM readiness by itself;
- no direct write to canonical shipment truth, PPQ505 or LAWGS.

Any taxonomy/catalog change must run both focused taxonomy tests and the Product Intelligence snapshot/worker regression tests before the general CI and PostgreSQL gates.

## Deterministic Regulatory Rules contract
The active rule capability is protected by:
- `tests/test_us_lacey_regulatory_rules.py`
- `tests/test_us_lacey_regulatory_assessment_payload.py`
- `tests/test_us_lacey_regulatory_assessment_snapshot.py`
- `tests/test_us_lacey_regulatory_assessment_snapshot_postgres.py`
- `tests/test_us_lacey_regulatory_assessment_worker.py`
- `tests/test_us_lacey_regulatory_assessment_ui.py`
- `tests/test_us_lacey_worker_stage_timing.py`

Coverage expectations include:
- exact threshold values and just-below/above boundaries using `Decimal`;
- protected/unknown status failing closed rather than producing a false-safe PASS;
- same-HTS aggregation inputs remaining explicit rather than inferred from unrelated lines;
- composite/special-use construction facts kept distinct from species/due-care determinability;
- plywood/thin solid plies not being treated as SPECIAL/COMPOSITE merely from a broad material label;
- stable ruleset version, reason codes, evidence references and calculation trace;
- stable input fingerprinting;
- source-set idempotency and generation/fingerprint fencing;
- `CURRENT / STALE` lifecycle and automatic supersession when the source set changes;
- tenant A/B isolation plus RLS/FORCE RLS in PostgreSQL;
- worker ordering after Product Intelligence and before multilingual/source-set finalization;
- current-only/current-ruleset customer reads;
- UI wording that makes every result rule-scoped and explicitly non-final;
- no automatic write to canonical shipment truth, PPQ505, LAWGS or ACE.

Missing required inputs must produce `INDETERMINATE`/review-required rather than a guessed `PASS`. A `PASS` on one rule is never an overall shipment compliance status.

## Rules for future capabilities

### Source-linked review
Add end-to-end tests that a displayed decision can resolve to an existing evidence/document anchor and that reviewer overrides create new audited actions rather than erasing source observations.

## Agent rule
Do not guess which tests matter from filenames alone. Read `CAPABILITIES.toml`, search imports/callers, run focused tests first, then broaden according to the table above.
