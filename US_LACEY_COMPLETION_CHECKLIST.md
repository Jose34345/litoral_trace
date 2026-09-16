# U.S. Lacey private beta completion checklist

Evidence labels: **REPO VERIFIED**, **TEST VERIFIED**, **EXTERNAL RUNTIME VERIFIED**, **DECLARED ONLY**, or **UNVERIFIED**. A checked item has the stated evidence.

## A. Baseline and test environment

- [x] Repair the local Python 3.11 virtual environment. _(TEST VERIFIED: Python 3.11.9)_
- [x] Confirm the Alembic graph has the expected single head. _(TEST VERIFIED: `038_us_lacey_pilot_activation`)_
- [x] Compile `src/` successfully. _(TEST VERIFIED)_
- [x] Run local U.S. Lacey baseline tests. _(TEST VERIFIED: 32 passed)_
- [x] Run local auth, extraction, reconciliation, and review baseline tests. _(TEST VERIFIED: 53 passed)_
- [ ] Run U.S. Lacey PostgreSQL integration tests against an ephemeral PostGIS instance. _(SKIPPED locally: Docker/PostgreSQL unavailable)_
- [ ] Run the complete `us-lacey-postgres-gate.yml` on a pull request and retain its result. _(UNVERIFIED)_

## B. Safe PILOT activation

- [x] Define the minimum account and subscription state transition for `PAYMENT_PENDING -> PILOT`. _(REPO VERIFIED)_
- [x] Add an organization-scoped, server-side control-plane operation for PILOT activation. _(REPO VERIFIED)_
- [x] Require an authenticated platform-superadmin actor for that operation. _(REPO VERIFIED)_
- [x] Persist actor, organization, timestamp, reason, and transition outcome in the audit log. _(REPO VERIFIED)_
- [x] Make repeated activation idempotent without commercial state changes. _(REPO VERIFIED)_
- [x] Keep payment and subscription state unchanged during PILOT activation. _(REPO VERIFIED)_
- [x] Ensure PILOT activation never calls the payment-verification operation. _(REPO VERIFIED)_
- [ ] Run PostgreSQL assertions for authorization, isolation, idempotency, payment invariants, audit persistence, and entitlement. _(SKIPPED locally: no ephemeral PostgreSQL)_

## C. Canonical Litoral Trace UI

- [ ] Replace the U.S. Lacey HTML shell with the canonical shared layout where compatible.
- [ ] Reuse canonical global CSS and design tokens instead of portal-inline CSS.
- [ ] Reuse canonical navigation, logo, typography, buttons, form controls, cards, tables, badges, and alerts.
- [ ] Refactor signup, check-email, verification, login, billing, and operations list without changing URLs.
- [ ] Refactor operation detail, upload, review, and export surfaces without changing server behavior.
- [ ] Remove replaced U.S. Lacey inline CSS safely.
- [ ] Add focused rendering or smoke coverage for the shared portal shell.

## D. Operations and entitlement

- [x] Gate operational access by server-side entitlement. _(REPO VERIFIED; TEST VERIFIED in local U.S. Lacey baseline)_
- [x] Enforce operation quota when creating a new operation. _(REPO VERIFIED; TEST VERIFIED)_
- [ ] Validate create-operation behavior with a PILOT account in ephemeral PostgreSQL. _(UNVERIFIED)_
- [ ] Validate operations cannot cross organization boundaries in ephemeral PostgreSQL. _(UNVERIFIED)_

## E. S3 evidence storage

- [x] Declare private U.S. bucket, prefix, encryption, versioning, public-access block, and restricted IAM workload access. _(EXTERNAL RUNTIME VERIFIED)_
- [x] Validate content type, upload size, filename handling, and SHA-256 idempotency in code/tests. _(REPO VERIFIED; TEST VERIFIED)_
- [ ] Upload an authorized document from the live portal and confirm object placement under the tenant-safe prefix. _(UNVERIFIED)_
- [ ] Confirm no public object access and no cross-tenant object access with runtime identities. _(UNVERIFIED)_

## F. Worker and queue

- [x] Implement atomic claim with `FOR UPDATE SKIP LOCKED`, heartbeat, retry, and stale-job recovery. _(REPO VERIFIED)_
- [ ] Prove two worker instances cannot process the same U.S. Lacey job in ephemeral PostgreSQL. _(UNVERIFIED)_
- [ ] Prove worker crash recovery and retry exhaustion in ephemeral PostgreSQL. _(UNVERIFIED)_
- [ ] Process a portal-uploaded production-beta document with the real worker. _(UNVERIFIED)_
- [ ] Replace free-tier inline-worker topology before claiming continuous processing availability. _(DECLARED ONLY)_

## G. Document intelligence

- [x] Implement deterministic parsing, OCR-capable processing, classification, field extraction, and provenance projection. _(REPO VERIFIED; TEST VERIFIED for local extraction suite)_
- [ ] Validate representative customer-authorized PDFs/XLSX/CSV through the real worker. _(UNVERIFIED)_
- [ ] Record extraction confidence and provenance for each field in a production-beta operation. _(UNVERIFIED)_

## H. Reconciliation and exceptions

- [x] Implement reconciliation, missing/conflicting-field projection, and exception-first review state. _(REPO VERIFIED; TEST VERIFIED for local reconciliation suite)_
- [ ] Validate a real multi-document operation produces expected missing and conflicting fields. _(UNVERIFIED)_
- [ ] Validate reconciliation and exception isolation across two tenants in PostgreSQL. _(UNVERIFIED)_

## I. Human review

- [x] Implement accept/edit/not-required actions with CSRF validation and audit events. _(REPO VERIFIED; TEST VERIFIED for local review suite)_
- [ ] Prove review completion blocks while jobs, failures, conflicts, or required fields remain. _(UNVERIFIED)_
- [ ] Run an authorized human-review flow end-to-end in production beta. _(UNVERIFIED)_

## J. Export package

- [x] Implement CSV/XLSX preparation exports with evidence-oriented metadata and non-legal-filing language. _(REPO VERIFIED)_
- [ ] Validate generated CSV/XLSX from a completed real operation. _(UNVERIFIED)_
- [ ] Verify exported package preserves source/evidence references and cannot be produced before completion. _(UNVERIFIED)_

## K. Audit and security

- [x] Verify externally that Neon production is on migration `037_us_lacey_portal_auth` with U.S. Lacey RLS/FORCE RLS and expected role restrictions. _(EXTERNAL RUNTIME VERIFIED; production upgrade intentionally pending)_
- [x] Verify externally that runtime and worker use separate pooled Neon identities with no superuser/BYPASSRLS. _(EXTERNAL RUNTIME VERIFIED)_
- [x] Complete repository secret triage with no real literal secret found. _(REPO VERIFIED)_
- [ ] Execute tenant A/B RLS isolation gates against ephemeral PostgreSQL. _(SKIPPED locally)_
- [ ] Verify portal login rate limiting or document its compensating control. _(UNVERIFIED)_

## L. Production-beta E2E

- [x] Verify Render web `/health` and `/ready` in production. _(EXTERNAL RUNTIME VERIFIED)_
- [x] Verify real signup, Brevo delivery, email verification, login, valid session, billing, and `PAYMENT_PENDING` state. _(EXTERNAL RUNTIME VERIFIED)_
- [ ] Activate an authorized test organization as PILOT using the audited control-plane transition. _(UNVERIFIED; production action intentionally pending)_
- [ ] Run operation → upload → S3 → queue → worker → extraction → reconciliation → review → export on the production beta. _(UNVERIFIED)_
- [ ] Test tenant isolation and recovery/failure paths in a non-production environment before production beta sign-off. _(UNVERIFIED)_
