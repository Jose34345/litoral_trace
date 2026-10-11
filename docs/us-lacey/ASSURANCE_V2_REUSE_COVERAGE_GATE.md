# Assurance V2: current-source coverage before historical reuse

**Status (2026-10-11):** implementation safety gate; not production authorization.

The read-only Assurance V2 case projection may show a **historical reuse proposal**
only after a current, finalized source-set revision has a complete ingestion-verified
`assurance.claim.snapshot.v1` record for **every current operation document**.
A legacy operation field with `MISSING` does **not** prove the new PDF bundle
contains no botanical claim: source-linked candidates may not have reached the
canonical field projection.

## Conservative policy

- The candidate snapshot must match the current revision and fingerprint, every
  current document ID/version/original SHA-256, tenant and operation, and exact
  per-document and aggregate claim counts. Duplicate, missing, malformed,
  unverified, failed or partially processed snapshots cannot permit reuse.
- Every candidate remains `CANDIDATE` / `PROPOSED`. A literal supported
  value is source evidence, **not** a human-approved value.
- Any current non-placeholder botanical candidate blocks historical reuse for
  that botanical field. Even an unbound candidate blocks: it cannot safely be
  attributed to a different supplier/SKU/line without human verification.
- A literal placeholder is ignored **only** if it is marked unsupported,
  normalized to `None`, and independently matches the ingestion placeholder
  vocabulary. A forged `NONFACTUAL_PLACEHOLDER` reason is not enough.
- Missing claim coverage causes **abstention**, not a claim that history is
  invalid. The UI must not describe historical evidence as current-PDF evidence.
- `evaluate_reuse` continues to enforce tenant, exact supplier/SKU/line,
  verified origin, expiry, context, source-set freshness, conflicting current
  fields and contradictory historical evidence. The case reader now supplies
  all current candidate values explicitly instead of silently passing the
  default empty tuple.

## Verification

`tests/test_assurance_v2_reuse_coverage.py` covers complete/absent/partial/
tampered snapshots, an unbound supported current botanical, a real placeholder
and a forged placeholder marker. The 13 original synthetic PDFs are stored
unchanged under `tests/fixtures/assurance_v2_pdf_e2e/` with SHA-256 manifest;
`tests/test_assurance_v2_real_pdf_packs.py` checks raw-byte extraction, but
this is **not** the complete two-shipment PostgreSQL/worker/human-review/export
acceptance.

## Remaining release blockers

The feature remains default OFF. Transactional review commands, canonical
publication, verified memory promotion, explicit historical reuse acceptance,
source-set-fenced export authorization, staging/shadow comparisons and complete
two-original-PDF-pack PostgreSQL E2E are still required. No change in this
increment authorizes filing-ready export or production ENABLED mode.
