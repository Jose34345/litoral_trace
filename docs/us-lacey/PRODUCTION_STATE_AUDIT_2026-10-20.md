# Litoral Trace — Production state and Northflank audit
**Audit date:** 2026-10-20 (Argentina)  
**Scope:** current U.S. Lacey production, commercial readiness and corporate-domain routing.  
**Authority order:** active production HTTP/DNS → GitHub deployment SHA and CI → PostgreSQL read-only checks → code/migrations → historical notes.

## 1. Canonical product and infrastructure
- Product: U.S. Lacey Act **shipment-document preparation and human review**, for U.S. importers and customs brokers. No automatic ACE/LAWGS submission, legal advice, or compliance certification. Don't market the synthetic sample as real customers.
- Source of truth branch: `feature/us-lacey-pilot-platform` in `Jose34345/litoral_trace`. The `main` branch is a divergent legacy/backup line; **do not merge it into Lacey or deploy it as the current product**.
- Current confirmed deployed SHA at audit: `3988b94ca3dd801ac8790c492040cb56d2cceb4c` (PR #464), following `9825906053ec2a9ce951c8810f6fe7d6b6971827` (PR #463).
- Public product origin: `https://lacey.litoraltrace.com`. DNS resolves through Northflank-managed hostnames. GitHub recorded **success** deployment statuses for web and worker on the current SHA.
- Application: FastAPI modular monolith; `src/litoral_trace/web/us_lacey_unified_app.py` composes public marketing, portal, sandbox, payment, operations and admin routes. `us_lacey_free_app.py` provides portal primitives. Internal worker operates separately.
- Database: Supabase PostgreSQL project `Litoral Trace US Lacey` in `us-east-1`, reported `ACTIVE_HEALTHY` on 2026-10-20. Strict organization boundaries and RLS are essential.
- Documentation authority: `docs/us-lacey/README.md`, `ARCHITECTURE.md`, `CAPABILITIES.toml`, `INVARIANTS.md`, `PIPELINE.md`, `TEST_MATRIX.md`.

## 2. Public routes observed on 2026-10-20
| URL | Result | Meaning |
| --- | --- | --- |
| `lacey.litoraltrace.com/health` | HTTP 200: `healthy/us-lacey-pilot` | Public liveness, not a full dependency audit |
| `lacey.litoraltrace.com/` | HTTP 200 | Product landing |
| `lacey.litoraltrace.com/company` | HTTP 200 | Public Argentina-founded company identity |
| `lacey.litoraltrace.com/security` | HTTP 200 | Public security / service boundaries |
| `lacey.litoraltrace.com/demo` | HTTP 200 | Synthetic demonstration |
| `lacey.litoraltrace.com/sandbox/start` | HTTP 200 | Evaluation intake |
| `litoraltrace.com/` | HTTP 200 with old forestry headline | **Not yet the desired corporate identity** |
| `litoraltrace.com/company`, `/trust` | HTTP 404 | The apex site is on a different hosting destination |

These are anonymous HTTP checks only; they do **not** validate real checkout, human activation, correct document extraction, or end-to-end regulatory readiness.

## 3. Northflank access — blocked but product serving traffic
- Installed desktop CLI: `@northflank/cli 0.13.0`.
- Two saved CLI contexts: `litoraltrace` and an auto-login team context. **Both returned HTTP 401 Unauthorized** when requesting the project list on 2026-10-20.
- The contexts were restored to the initial active selection after testing. No token values were read out or published.
- A fresh interactive `northflank login` is required before auditing **actual project/service settings**, ingress custom-domain ownership, environment, build branch, scaling, release rollout, and worker metrics.
- Do not infer service-level health or certificate readiness solely from GitHub deployment status.
- **Do not change DNS** or attach the corporate apex until the relevant Northflank project and ingress are authenticated and checked.

## 4. Commercial and database read-only snapshot
- Postgres project is `ACTIVE_HEALTHY`; table listing reported RLS enabled on operational/commercial tables (RLS FORCE and correct policies require gate/DB contract checks).
- `us_lacey_operations`: 33 records: 24 `REVIEW_REQUIRED`, 7 `COMPLETED`, 2 `NEW`. Mixed QA/internal data; **not** a customer success rate.
- `us_lacey_subscriptions`: one `PENDING` row.
- `us_lacey_payments`: one `PENDING` row. **No confirmed received payment** based on the queried state.
- Outreach: 22 links, 33 sessions and 38 events in corresponding tables; visits/clicks do not establish human prospect interest. Earlier funnel audit found no confirmed external sandbox activation; re-audit before claiming new traction.
- Supplier evidence, audit events and pilot quality structures exist; do not mistake test/demo records for paid usage.
- Price currently shown on Lacey: USD 149/month, up to 100 operations; successful real Paddle settlement/activation has not been demonstrated with an external paying user.

## 5. Product authority and non-negotiable controls
- Ingestion/extraction: `src/litoral_trace/lacey_engine/`. Extracted candidates are **not** authoritative facts.
- Product Intelligence: versioned, source-linked SKU/BOM snapshots. Does not publish canonical filing truth.
- Regulatory checks: versioned deterministic evaluations; missing facts remain review-required/indeterminate.
- Canonical reconciliation and human review: `src/litoral_trace/us_lacey/` and its portal routes.
- Supplier identity/evidence reuse: only with deterministic binding, verified evidence, valid authorization and audit. No cross-supplier reuse without matching identity.
- Export products: preparation work products, not an automated government filing.
- Maintain tenant isolation, source-set generation/fencing, append-only audit logs, explicit human confirmation, 4-hour original-document retention rules and no raw document logging.
- Pack 2 remains an acceptance scenario. Re-run golden tests for any change touching source precedence, botanical claims, amounts or review authority.

## 6. What this sprint has actually delivered
- PR #463: synthetic samples clearly identified; corporate Company and Trust templates; Lacey marketing identity alignment. Merged and deployed.
- PR #464: public `/company` and `/security` on Lacey rather than non-existent external links. Merged and **HTTP 200 verified**.
- Current Northflank-only candidate: public `/corporate` preview and apex-aware corporate landing. Until a verified custom-domain binding exists, it **does not** mean `litoraltrace.com` is hosted by Northflank.
- Legacy PR #465 targets the separate `p2-enterprise-production` history; do not merge it as a shortcut for the Northflank domain.

## 7. Release and migration gates
1. Branch from the current canonical production head, never from divergent legacy `main` or `p2-enterprise-production`.
2. Read `AGENTS.md` and canonical docs; change minimum viable templates/routes/tests.
3. Run focused pytest + full selected frontend regression + reproducible CSS build and `git diff --check`.
4. GitHub PR checks: Python, CSS drift, Lacey smoke and PostgreSQL gates when invoked/required. No merge if critical gates failed/cancelled.
5. Merge after green gates; verify GitHub deployment exact SHA for Northflank web and worker.
6. Verify live HTTPS routes, response headers, preview, sign-in boundary and user-facing content. Never claim production merely because CI passed.

## 8. Highest priority follow-up — business before features
**P0**: Refresh Northflank CLI authorization and inspect the actual project/service ingress/domain settings; verify one stable source of production and avoid duplicate hosting.
**P0**: Host the international corporate pages on Northflank and test root behavior on both allowed Host values. Only then plan apex DNS/SSL migration, with explicit rollback and no disruption of Lacey.
**P0**: Run one real, safe checkout-to-entitlement test before sending invoices/promising immediate Paddle funds.
**P1**: Use the already prepared English/Spanish company one-pagers and direct, personal Argentina↔U.S. introductions; prioritize FAIMA and verified importers/brokers, not bulk unsolicited email.
**P1**: Record human sandbox activations and pilot payments separately from automated link events and internal QA.
**Deferred**: new OCR/AI architecture, invented trust seals, paid memberships, LLC formation, generic dashboards, obsolete forestry marketing.

## 9. Integrity / unresolved evidence
- Administrative Northflank read access: **NOT VERIFIED** (CLI HTTP 401).
- `litoraltrace.com` routing to Northflank: **NOT ACTIVE** as of this audit.
- Validated paid customer: **NONE CONFIRMED**.
- Verified buyer adoption: **NONE CONFIRMED**.
- Product liveness, public Lacey pages and PostgreSQL project: **OBSERVED HEALTHY**.
- This audit does not claim a vulnerability assessment, PCI/SOC 2 status, live payment settlement or legal certification.

Update this file whenever release topology, canonical branch, commercial billing or actual domain ownership changes. Prefer timestamped evidence over inferred completeness.
