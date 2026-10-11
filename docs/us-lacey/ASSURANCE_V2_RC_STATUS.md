# Assurance V2 integration candidate — NO-GO

Date: 2026-10-11. Target: `feature/us-lacey-pilot-platform` at
`7c99b04edcb8c762a75170c8cd5eb86a3a72dd56`. Integration branch:
`feat/assurance-v2-integration-rc`. The target HEAD was rechecked before
publishing the branch and had not advanced. PR #402 was the runtime base;
#396–399 were not merged a second time. No production data or deployment was
changed. `US_LACEY_ASSURANCE_V2_MODE=ENABLED` remains prohibited.

## Integrated behavior

- The strict `assurance.claim.v1 → assurance.v2/1.0.0 → AssuranceCase` bridge
  retains tenant, operation, revision/generation/fingerprint, document version
  and hash, source locator, raw and normalized value, entity identity,
  support/authority/conflict, human decision and historical provenance.
  Invalid or incomplete contracts abstain. Source support never grants human
  authorization.
- Worker replay stores an idempotent current source claim sidecar without
  rewriting historical Engine runs. A failed sidecar cannot fall back to
  obsolete claims.
- Authenticated, CSRF protected V2 review, exact supplier–SKU–line
  confirmation, evidence application, export authorization and locked
  issuance gateways are mounted in SHADOW. Commands remain OFF by default.
- Historical evidence follows `ELIGIBLE → audited actor application →
  VERIFIED_REUSE`, retains its original document reference, and disappears
  from the current case on revocation. The second shipment need not repeat
  botanical facts to propose reuse.
- V2 adopted operations cannot use legacy CSV/XLSX exports to bypass the V2
  gate. Historical V1 operations retain their route behavior. Federal
  submission is never automatic.
- The UI now displays source linked observations and their original PDF links
  separately from authoritative fields. A supported `article_component`
  observation without a safe line association creates an explicit blocking
  `SOURCE_CLAIM_UNBOUND` exception. The source-claim stream still reports
  `UNBOUND` for all 164 observations across the four shipments; the canonical
  field projection is a separate path. This unresolved bridge is a P0 gate.
  The reader now recognizes the contract's three valid bound states
  (`EXPLICIT_SKU`, `SOURCE_LOCAL_LINE`, and `SOURCE_ROW`) instead of comparing
  against a nonexistent `BOUND` state. That correction does not bind this
  corpus: all of its source observations remain `UNBOUND`.

## PostgreSQL golden result

The test `tests/test_assurance_v2_golden_postgres.py` uploads the original
7 Pack 1 PDFs and 6 Pack 2 PDFs through the customer portal, verifies exact
SHA-256 bytes in Vault, runs the real worker, reads the tenant case, and uses
authenticated V2 routes for review, identity and reuse. Its two additional
adversarial packs are reproducible, versioned PDF files derived from Pack 2;
the original 13 files are untouched.

| Shipment | Candidates / anchored | Workspace values | Result |
| --- | ---: | ---: | --- |
| 1, original supplier | 47 / 40 | 18 | Genus `Phyllostachys`, species `edulis`, harvest country `CHINA` source linked; 3 human decisions and 3 promoted memories. |
| 2, same supplier and SKU | 39 / 30 | 12 current-source values | No botanical restatement; 3 eligible memories, exact identity confirmation, then 3 audited `VERIFIED_REUSE` applications. |
| 3, same SKU, different supplier | 39 / 30 | 9 | New supplier identity; zero eligible reuse. |
| 4, contradictory species PDF | 39 / 31 | 12 | Literal `aurea` is source supported; species reuse blocked. |

After the positive application assertions, the test revokes one source
evidence through the runtime role. The second shipment loses verified reuse
and readiness. This is intentionally the final persisted test state. The
negative tests also prove tenant isolation, CSRF rejection, final issuance
denial, legacy export denial and an incomplete marked draft. All 25 worker
jobs completed with zero stage errors in the latest disposable run.

The botanical subset defined by the fixture has 3/3 correct supported facts
in Pack 1 and 3/3 expected abstentions in Pack 2. On this **narrow** subset,
precision and recall are 100%; this is not a system wide estimate. The corpus
does not include independently reviewed labels for all fields or true/false
exception categories, so full precision, recall, abstention and false
exception rates are **not established**. Detailed counters are in the local
`assurance_v2_metrics.json` release artifact.

## Gates executed

| Gate | Result |
| --- | --- |
| Main Python suite (CI configuration) | 2,454 passed; 317 optional or external integration skips. |
| Directed Assurance V2 PostgreSQL/RLS, UI, memory and output suite | 69 passed, zero skipped. |
| PostgreSQL golden after Alembic rollback/re-upgrade | 1 passed; four shipment scenarios. |
| Alembic | One head `079_us_lacey_assurance_v2_claim_snapshots`; local disposable PostgreSQL downgrade to 078 and upgrade to 079 passed. |
| Frontend | `npm ci`, build, CSS reproducibility and `npm audit --audit-level=high` passed; zero advisories. |
| Remote CI | Pending PR checks; local results do not replace GitHub CI. |

## Release blockers

**P0 remains open.** The 13 original synthetic PDFs do not produce a
filing-ready second shipment. Required fields such as filing entry reference,
merchandise description, entered value, plant quantity and metric unit are
still missing or lack source-linked V2 authority in the operational field
ledger. One supported `article_component` candidate is explicitly unbound
to a line, and no V2 source observation has an exact line association in this
run. The gate correctly returns 409 for issuance and legacy exports;
there is no final dossier or authorized final export from this Golden path.
It would be incorrect to fabricate a customs reference, invent human
authorization or claim a complete E2E release.

Further release acceptance still needs a complete source-backed packet and
human review for every required field, an actual final dossier/export from
that packet, PostgreSQL concurrent mutation tests around issuance, and a
reviewed full-field ground truth for false/real exceptions and precision/
recall. Document expiration and supersession have isolated checks but still
need the same full HTTP/worker acceptance path. Remote CI must pass before
the draft PR can be considered for release review.

## Rollback and production

Keep `US_LACEY_ASSURANCE_V2_MODE` at `OFF` or `SHADOW`; keep V2 commands and
the review journal OFF in production. The additive 079 sidecar migration can
be downgraded to 078 and upgraded back on a disposable database; downgrade
drops sidecar data, so retain backups before any future production rollback.
Do not deploy, enable final mode, issue federal filings or merge for release
until the P0 is closed and the complete Golden is green. Production requires
separate explicit approval after all gates and remote CI pass.

## Follow-up: exact source-document identity bridge

**Scope:** separate branch `fix/assurance-v2-source-identity-bridge` based on RC
`bc320837`. This is a candidate-only bridge, not a release authorization.

- Reuses existing independent PDF identity corroboration to establish the
  exact single invoice line, manufacturer SKU and supplier name/address.
- Attaches `DOCUMENT_EXACT_SINGLE_SKU` only to source-supported,
  source-anchored product/plant claims in documents carrying that SKU; general
  shipment claims remain UNBOUND. Multiple product lines, mixed logical
  documents or insufficient witnesses abstain.
- Persists the tenant/revision/fingerprint, invoice/SKU/supplier witness
  document IDs + hashes, exact line/SKU and supplier identity digest without
  copying source text or granting human authority.
- The case reader validates every document witness against the *current*
  tenant source set, including revisions, before accepting candidate context.
- Existing original synthetic four-shipment PostgreSQL Golden passed after
  changing the obsolete expectation that `article_component` must be UNBOUND.
  The new expectation is exact documentary binding while the unpublished
  source claim remains blocking until actual human/regulatory authority exists.

**Measured effect on the original fixture packets:** Pack 1 binds 12/47
candidate observations (3 genus, 3 species, 3 country of harvest, HTS,
article component, description); Pack 2 binds 2/39 (HTS and article
component). Other shipment-level and unsupported observations are intentionally
not assigned to a product line. The authenticated four-shipment PostgreSQL
Golden passed, 36 focused local tests passed, the dedicated Pg/RLS/runtime
group passed **15 tests / 0 skips**, and GitHub CI and PostgreSQL Gate for
PR #404's initial head passed. No production quality metrics are inferred
from this small synthetic corpus.

**Release status remains NO-GO:** this bridge does not create missing filing
entry reference, merchandise description, entered value, plant quantity or
metric unit; it does not authorize final issuance. Full required-field review,
authorized final dossier/export, concurrency, full-field ground truth and
remote checks remain distinct release gates. No production deployment or
`ENABLED` activation is part of this patch.

## RC follow-up: source-backed human field review and issue idempotency

**Change set:** PR #405 builds on the integrated #404, and the target remains
`feat/assurance-v2-integration-rc`. This is a development-only improvement;
it **does not change the NO-GO release decision**.

### Implemented and exercised

- The reviewer can select an exact, supported, **current** `SourceLinkedClaim`
  from a required field's exception without copying opaque IDs. The backend
  verifies tenant, source-set revision/generation/fingerprint, original document
  version/SHA-256 and its text-span locator; product values require an
  independently confirmed exact supplier/SKU/line.
- Authentication, CSRF, operation locks, source field update, V2 human review
  journal, and audit event share a transaction. A journal rejection rolls the
  selection back rather than granting implicit AI/source authority.
- The case UI now presents a **read-only required-evidence checklist**:
  verified, reviewable current source, unconfirmed current value, or missing
  evidence. It neither changes a regulatory state nor authorizes a package.
- The original four-shipment HTTP/Vault/worker PostgreSQL Golden now verifies
  one **human-confirmed `article_component`** source selection in Shipment 2,
  in addition to the three verified historical botanical reuses and the two
  adversarial anti-reuse cases.
- The PostgreSQL issuer operation-lock test submits two concurrent requests
  for an identical *synthetic, already-authorized* case and asserts **one
  immutable package/event**. The issuer reloads that package after rechecking
  the current fenced case, instead of issuing timestamp-dependent duplicates.
  This isolated fixture does not prove concurrent source-document mutation
  safety or the final original-PDF filing path.

### Still blocking the production release

The current original-PDF Shipment 2 still has no authenticated, source-backed,
approved **filing entry reference** and **merchandise description**. The
entered value (USD 11,250.00) and plant-material quantity (288.0 KG) are
visible literally in the original synthetic invoice/entry worksheet PDFs,
but are **not correctly projected into the current authoritative field ledger**;
neither are they approved for final output. A literal text occurrence alone
is not a reviewed PPQ field. Remaining required fields and exceptions must be
resolved with exact current evidence, not invented/misassigned values.

Before release: demonstrate the complete HTTP/worker source-backed packet,
review all required fields, authorize and retrieve *actual* final dossier,
Excel and LAWGS XML; test source replacement, revocation, expiration and
concurrent mutation; establish full-field reviewed ground truth; require
all latest remote checks green. Keep `US_LACEY_ASSURANCE_V2_MODE` OFF/SHADOW
and federal submission disabled. No merge to production or deploy while a
P0 remains.
