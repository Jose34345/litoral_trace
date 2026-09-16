# U.S. Lacey Test Matrix

Use this as a routing guide. Exact test names evolve; search existing tests before adding duplicates.

| Capability changed | Minimum focused validation | Broader validation |
|---|---|---|
| `lacey_engine` classification/parsing | relevant `tests/lacey_engine/` tests | general CI pytest |
| AI routing/provider/shadow | AI architecture/routing/shadow/resilience tests under `tests/lacey_engine/` | general CI pytest |
| specialist routing/fusion | specialist + line-binding/fusion tests | general CI pytest |
| cross-document identity | `tests/lacey_engine/test_cross_document_line_identity.py` and fail-closed companion | general CI pytest |
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

## Rules for future capabilities

### Product Composition/BOM
Add golden structured BOM cases covering SKU/component isolation, unit normalization and source provenance. Include negative cases preventing cross-SKU component leakage.

### Taxonomy Resolver
Add golden exact-name, synonym, commercial/common alias, ambiguity and no-match cases. A candidate ambiguity test must prove the resolver does not auto-promote an uncertain species.

### Regulatory rules
Boundary tests are mandatory. For de minimis-style calculations, test exact threshold values, just below/above thresholds, unit conversion, missing weights, multi-line aggregation and ruleset versioning. Missing inputs must produce INDETERMINATE/REVIEW_REQUIRED rather than a guessed PASS.

### Source-linked review
Add end-to-end tests that a displayed decision can resolve to an existing evidence/document anchor and that reviewer overrides create new audited actions rather than erasing source observations.

## Agent rule
Do not guess which tests matter from filenames alone. Read `CAPABILITIES.toml`, search imports/callers, run focused tests first, then broaden according to the table above.