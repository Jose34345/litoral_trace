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

The canonical U.S. Lacey Alembic head after Hito 7 remains `049_lacey_product_intelligence_snapshots`; Hito 7 adds no migration.

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
- worker ordering after canonical publication and before multilingual shadow/source-set finalization;
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

## Rules for future capabilities

### Regulatory rules
Boundary tests are mandatory. For de minimis-style calculations, test exact threshold values, just below/above thresholds, unit conversion, missing weights, multi-line aggregation and ruleset versioning. Missing inputs must produce INDETERMINATE/REVIEW_REQUIRED rather than a guessed PASS.

### Source-linked review
Add end-to-end tests that a displayed decision can resolve to an existing evidence/document anchor and that reviewer overrides create new audited actions rather than erasing source observations.

## Agent rule
Do not guess which tests matter from filenames alone. Read `CAPABILITIES.toml`, search imports/callers, run focused tests first, then broaden according to the table above.
