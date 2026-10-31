# P0 Commercial Launch Readiness — 2026-10-15

## Scope and decision

**Decision: NO-GO for general U.S. paid pilot outreach pending the blockers below.** Public synthetic sample invitations may be demonstrated by the product owner, subject to truthful synthetic labeling. This report is a live checklist with evidence, not a claim that the platform is ready solely because its pages return HTTP 200.

Commit: feature branch `feat/lacey-market-backed-p0-20261015` (exact merged SHA will be recorded after CI if merged). Owner: Product/QA. Date: 2026-10-15.

| Gate | State | Evidence / version | Risk / owner |
|---|---|---|---|
| Importer public sample opens | PASS (render smoke only) | Edge 1440/390, GET /try/importer HTTP 200, no page errors/overflow | Low; QA. Not a successful full click-through |
| Broker public sample opens | PASS (render smoke only) | Edge 1440/390, GET /try/broker HTTP 200, no page errors/overflow | Low; QA. Not a successful full click-through |
| Sandbox landing | PASS (render smoke only) | Edge 1440/390 GET /sandbox/start HTTP 200, form present, no JS page errors | Low; QA. Not proof first upload succeeds |
| First own shipment end-to-end | PARTIAL | Anonymous consent + synthetic invoice upload accepted on production; terminal review not observed | **BLOCKING** until full terminal verification, Product/QA |
| Reprocess and outputs end-to-end | NOT RUN | Production synthetic Pack2 accepted, final export and reprocess not exercised | **BLOCKING**, QA |
| Pack 2 goldens local | PASS CI / PARTIAL live | Local 11 passed + 2 skipped; PostgreSQL Gate success; six synthetic PDFs uploaded live but review terminal not observed | **BLOCKING** terminal live QA, QA |
| Exception-first UX (template contract) | PASS locally | 4 new tests, dynamic pending count, source caveat retained | QA |
| Review, evaluation, sandbox, outreach unit suite | PASS locally | 74 passed, 1 unrelated deprecation warning | QA |
| PDF visual regression | NOT RUN in authenticated UI for this branch | Existing PR #452 previous smoke tested; ensure no regressions in PR gate | Medium; Frontend QA |
| Tenant isolation / RLS | PASS CI | PostgreSQL Gate success on b326e267, including tenant isolation and RLS checks; live cross-tenant QA not attempted | Security |
| Four-hour original retention | PARTIAL | Sandbox cleanup pytest passed locally; production cleanup schedule not re-audited | **BLOCKING**, SRE |
| Evaluation five-operation policy | PARTIAL | Evaluation pytest green; live full limit scenario not executed | **BLOCKING**, Product |
| No unauthorized experimental lab access | PARTIAL | Template limits lab to configured org IDs; live external tenant check not performed | **BLOCKING**, Security |
| Frontend CSS and Python CI | PASS | GitHub CI and CSS Drift Gate success on source PR #453 | Engineering |
| Worker and web same merged commit | PASS | Northflank web + worker COMPLETED at b326e267, health/ready 200 | SRE |
| Admin signal classification | PARTIAL | Repo tracks raw hits separately from human signals; restricted admin walkthrough not performed | Medium; Commercial Ops |
| Support reply routing | NOT RUN | Verify a real incoming/outgoing support message; don't create misleading auto-confirmations | **BLOCKING**, Commercial Ops |
| Accurate boundaries | PASS static | "No automatic ACE/LAWGS submission" preserved, samples marked synthetic | Product/Compliance |
| Specialized legal review of customs-business scope | NOT RUN | CBP HQ H350722 (Jan 2026) could affect service wording/activity; qualified U.S. trade counsel review | **BLOCKING for broad commercial claims**, Legal |
| Actual buyer comprehension / observed speed | NOT RUN | No independent importers/brokers observed, no measurable time savings | Commercial Research |

## Baseline vs current (do not invent measurements)

- UI baseline (from product-owner screenshots, 2026-10-15): review action located below large shipment header; long scroll and repeated source summaries. Not a time measurement.
- Local new UI: dynamic pending count plus source-backed CTA near top of existing summary. **Browser authenticated visual confirmation remains pending**.
- Public baseline: six Edge page/viewport combinations accessible (2 sizes x 3 routes), 0 observed JavaScript page exceptions, 0 horizontal overflow. Does not include processing or data uploads.
- PDF p95, task time, human decisions saved, activation, payment conversion: **not measured**. No improvement claim is authorized.

## Hard-stop conditions / acceptance

Do not enable public paid-customer rollout with a failed RLS, cross-tenant, evidence authority, source expiry, regulatory readiness, retention or export test. Don't activate Assurance V2 flag. Don't claim CBP filing or customs business authority. Escalate uncertain legal service boundaries to qualified counsel rather than editing legal terminology until understood.

## Next execution after CI

1. Merge scoped UI/docs only after Python/CSS/Postgres gates green and exact HEAD reviewed.
2. Confirm web and worker run merged SHA, public assets and health/ready.
3. QA synthetic Pack 2 on a properly isolated DB and anonymous five-operation evaluation (including failed processing, reprocess, expiration) before broad outreach.
4. Validate first-use UX with external testers, one importer + broker minimum initially. Mark tests as observed, not simulated.
5. Review legal positioning under HQ H350722 and commercial support mailbox.
6. Prepare ~6–8 qualified prospects with individual referral links, **no outreach sends authorized here**.

## Follow-up production QA

See [market-launch-operational-qa-p0.md](./market-launch-operational-qa-p0.md). Six Pack 2 synthetic PDFs processed as sequential child jobs. A premature actionable count was visible during processing. Fix under review: no review count or enabled review CTA before terminal evidence. **The release remains NO-GO for general paid outreach** pending terminal first-use, retention verification, specialist customs-business legal review and buyer research.
