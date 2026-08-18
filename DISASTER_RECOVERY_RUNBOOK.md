Litoral Trace - Disaster Recovery Runbook

This runbook defines the formal disaster recovery contract for Litoral Trace production data and recovery operations. It distinguishes recovery objectives, provider history capabilities, independent backup requirements, and separate recovery domains for PostgreSQL metadata versus Vault object bytes.

Never commit restore credentials, backup URLs, Neon administrative links, object-storage secrets, service-account keys, or incident-specific access tokens to Git.

1. Recovery objectives

PostgreSQL operational RPO

<= 15 minutes

Meaning: the maximum acceptable committed PostgreSQL data loss for a covered production recovery incident.

This is an enterprise target. The current environment must not claim that <= 15 minutes has already been proven in every failure scenario.

PostgreSQL operational RTO

<= 4 hours

Meaning: measured from declaration of a recoverable production incident until the application is restored to an accepted operational state.

This is an enterprise target. The successful 2026-08-17 restore drill proves restore mechanics for one recovery point, not <= 4 hours under all production conditions.

2. Provider history versus RPO

History or PITR lookback is not the same thing as RPO.

Current Neon state:

6-hour history window

Go-live PITR/history target:

>= 7 days

Contract status:

CURRENT GAP - NOT YET GO-LIVE COMPLIANT

The current Neon Free plan does not satisfy the >= 7 days go-live history target.

3. Independent logical backup retention

Independent PostgreSQL logical backup target RPO:

<= 24 hours

Planned implementation:

P2.7A3

This independent pg_dump / pg_restore layer is required in addition to Neon PITR, provider history, and provider snapshots.

P2.7A3A status:

tooling implemented and real isolated logical recovery drill passed on 2026-08-17

Application schema/data logical recovery is proven for the recorded recovery point.

Provider IAM / ownership / ACL reconciliation is NOT claimed by this logical restore.

pg_restore portability/atomicity is required:

- direct/unpooled target only
- isolated target must be empty before restore
- credentials only through libpq environment
- portable restore flags must exclude source ownership/ACL replay
- restore must run in a single transaction

Production was not overwritten or swapped during the drill.

P2.7A3 status:

CLOSED on 2026-08-17 after scheduled off-platform backup acceptance.

P2.7A3C operational evidence:

- GitHub Actions workflow run 32086976028: PASS
- PostgreSQL client: 17.11
- logical backup created from production
- AWS authentication: OIDC assumed role
- durable off-platform publication: PASS
- server-side encryption: SSE-S3
- Object Lock: Governance
- default retention: 35 days
- dump, manifest, and complete marker remotely verified by size and SHA-256

The configured twice-daily schedule operationally supports the <= 24 hours independent logical backup RPO target.

P2.7A3 requires direct/unpooled connections.

PostgreSQL client/server major versions must match.

Backup artifacts must never be committed.

Backup integrity is SHA-256 verified before restore.

4. Recovery layers and domains

Recovery priority:

Layer 1: Neon PITR / provider snapshot recovery

Layer 2: independent pg_dump / pg_restore fallback

Layer 3: Vault object-storage recovery

PostgreSQL database recovery and Vault / S3 object recovery are separate recovery domains.

Restoring PostgreSQL metadata does not restore S3 / Vault object bytes.

Vault object recovery remains a separate gate and is not proven complete by the PostgreSQL restore drill.

5. Default restore procedure

The default production recovery procedure is:

1. declare incident
2. stop or constrain writes when needed
3. identify recovery point
4. restore into an isolated branch/environment first
5. verify schema and critical data
6. verify PostGIS
7. verify Alembic revision
8. verify tenant/security invariants where applicable
9. only then perform controlled cutover/finalization
10. verify application readiness
11. preserve old production state until recovery is accepted
12. record evidence and incident outcome

Production must not be overwritten first when an isolated restore path is available.

Blind production database overwrite is prohibited unless there is an approved emergency reason and that reason is explicitly documented.

Blind Alembic downgrade is prohibited as a disaster recovery shortcut.

6. Migration safety contract

Every production schema-changing deployment must have a recovery point created or verified before Alembic upgrade head.

The recovery point record must identify:

- release commit
- current Alembic revision
- recovery timestamp/snapshot
- target database/environment
- operator
- verification status

Controlled/manual deployment remains valid. This contract does not require automatic production deployment.

7. Credential safety contract

DATABASE_URL:

runtime principal

must not be used as backup or restore administrative identity.

WORKER_DATABASE_URL:

worker capability principal

must not be used for backup or restore administration.

MIGRATION_DATABASE_URL:

migration/DDL credential

is not automatically equivalent to the backup or restore credential.

Backup and restore credentials must follow least privilege and be supplied ephemerally through the deployment secret mechanism.

Never commit credentials or backup URLs.

8. Fail-closed rules

Deployment or recovery is invalid if any of the following is true:

- no pre-migration recovery point exists for a schema-changing release
- recovery point cannot be identified
- restore credentials are persisted in Git
- operator cannot identify the target environment
- restore verification fails
- critical schema/data checks fail
- required PostGIS capability is absent
- recovery would blindly overwrite production without an approved emergency reason
- backup is assumed valid without a successful restore test

9. Restore drills

Required restore-drill cadence:

- one successful restore drill before first production go-live
- at least quarterly thereafter
- after material changes to backup/restore architecture when warranted

Restore evidence must record at minimum:

- date/time
- source environment/branch
- recovery mechanism
- recovery point
- isolated restore target
- database reachability
- PostgreSQL version
- PostGIS availability/version
- Alembic revision
- critical schema/table verification
- selected critical row-count/data verification
- elapsed restore/verification time
- final result
- whether production was modified

10. Historical evidence: P2.7A1 provider restore drill

Date:

2026-08-17

Mechanism:

Neon manual snapshot + multi-step isolated restore

Source:

production

Result:

PASS

Verified:

- PostgreSQL 17.10
- PostGIS 3.5
- Alembic 008_add_platform_control_plane_functions
- critical row-count parity: organizations 4, users 4, lotes 1, audit_logs 6
- critical table inventory parity: PASS
- production replaced: NO

This evidence proves restore mechanics for that recovery point.

It does not prove:

- 7-day history retention
- <= 15 minutes RPO in every incident
- <= 4 hours RTO under full production conditions
- independent pg_dump recovery
- Vault object recovery

11. Historical evidence: P2.7A3B real logical backup/restore drill

Date:

2026-08-17

Mechanism:

portable atomic pg_dump / pg_restore isolated logical recovery drill

Source:

production

source release 894f5d3

source database neondb

Backup artifacts:

- dump: 20260817T181632Z_production.dump
- manifest: 20260817T181632Z_production.manifest.json
- SHA-256: df4b805a64f3bd8e0b88430a54cbf71e06dfd0a250df344d2dd24817327ca122
- size: 91895 bytes
- pg_dump: PostgreSQL 17.11

Verified source metadata:

- PostgreSQL 17.10
- PostGIS 3.5.0
- Alembic 008_add_platform_control_plane_functions
- critical row-count parity: organizations 4, users 4, lotes 1, audit_logs 6
- application table inventory:
  api_keys, audit_logs, licenses, lotes, organizations, satellite_ndvi_observations, user_sessions, users

Isolated restore target:

- enterprise-integration
- database p27a3_restore
- PostgreSQL 17.10

Final restore report:

- format_version: p27a3.restore.v1
- result: PASS
- started_at_utc: 2026-08-17T18:41:43Z
- completed_at_utc: 2026-08-17T18:42:32Z
- elapsed_seconds: 48.862
- PostgreSQL 17.10
- PostGIS 3.5.0
- source Alembic 008_add_platform_control_plane_functions
- table_inventory_match: true
- critical_row_counts_match: true

Security/portability semantics:

- manifest contained no database URL
- manifest contained no password token
- manifest contained no pooler hostname
- restore report contained no database URL
- restore report contained no password token
- restore report contained no pooler hostname
- application schema/data logical recovery is proven
- provider IAM / ownership / ACL reconciliation is NOT claimed by this restore
- pg_restore is portable and atomic
- restore target must be isolated and empty
- production overwritten/swapped: NO

Hardening evidence before final PASS:

1. SQLAlchemy-style postgresql+psycopg URLs were normalized to libpq-compatible psycopg URLs and CLI errors were made secret-safe.
2. Empty-target preflight was separated from post-restore Alembic/PostGIS/application verification.
3. pg_restore was fixed to pass --dbname while keeping credentials exclusively in libpq environment.
4. pg_restore was hardened with --no-owner, --no-privileges, and --single-transaction so provider-managed ACL metadata does not break portability and failed restores do not leave partial state.

Historical pre-pass status now superseded by this evidence:

- tooling implemented/tested locally
- REAL pg_dump / pg_restore DRILL STILL REQUIRED

P2.7A3C operational gap was closed by the successful scheduled-workflow acceptance run and durable off-platform S3 publication.

This closes P2.7A3 overall.

It does not replace P2.7A4 Vault object-storage recovery.

12. P2.7A4 Vault object-storage recovery contract

P2.7A4 status:

CLOSED - recovery contract defined and bound to existing code-level recovery primitives.

This status defines the recovery contract. It does NOT claim that a production Vault provider-loss drill or an independent Vault replica has already been proven. Final operational disaster-recovery acceptance remains P2.7A6.

Recovery domain

Vault recovery is a coordinated recovery of two distinct data domains:

- PostgreSQL Vault metadata
- private object-storage bytes

A PostgreSQL restore alone is insufficient when Vault documents exist.

The active primary object store is not an independent backup merely because versioning is enabled. Versioning inside the same provider/failure domain is a recovery primitive, not by itself a provider-independent backup.

Authoritative recovery tuple

For an available Vault document, recovery evidence is identified by the coordinated tuple:

- organization_id
- public_id
- status
- storage_backend
- storage_bucket
- object_key
- storage_version_id when present
- size_bytes
- content_type
- sha256

ETag is not accepted as recovery integrity proof.

SHA-256 stored in PostgreSQL is the canonical content-integrity value.

Exact-version rule

When storage_version_id is present, verification and recovery MUST address that exact object version.

A recovery operation that silently falls back from a recorded storage_version_id to an unversioned/current object is invalid.

When storage_version_id is absent, the operator MUST NOT claim exact-version recoverability. The candidate current object or an independently identified recovery copy must instead be verified against the PostgreSQL size, content type, and SHA-256 before it is accepted.

Tenant boundary rule

Vault object keys are tenant-scoped.

The expected active key namespace is:

<key_prefix>/tenants/<organization_id>/objects/<opaque_object_id>

Recovery MUST reject a candidate whose object binding is inconsistent with the document organization.

Recovery tooling must never use an object from one organization to repair metadata belonging to another organization.

Integrity verification

Before recovered bytes are accepted:

1. verify expected storage bucket or explicitly approved recovery bucket
2. verify exact storage version when storage_version_id exists
3. verify size_bytes
4. verify content_type when the provider returns it
5. stream the complete object
6. recompute SHA-256 over the complete byte stream
7. compare the computed SHA-256 with PostgreSQL metadata

A matching ETag alone is never sufficient.

A size-only match is never sufficient.

Restore and cutover procedure

If the active Vault object is missing or corrupt:

1. declare the affected document and tenant
2. preserve the existing PostgreSQL metadata
3. locate an independently justified recovery candidate
4. recover bytes into an isolated recovery bucket/key or other non-destructive target first
5. verify the candidate using the full integrity contract
6. verify tenant binding
7. only after successful verification perform a controlled metadata repoint/cutover
8. validate the application download path after cutover
9. preserve the previous binding until recovery acceptance when technically possible
10. record the recovery evidence

Blind in-place overwrite of the current Vault object is prohibited when an isolated recovery target is available.

Database metadata must not be repointed before recovered bytes have passed integrity verification.

Deletion semantics

Documents whose authoritative PostgreSQL status is deleted MUST NOT be automatically resurrected by a database or object-storage recovery.

Historical object versions or recovery copies belonging to a logically deleted document are not, by themselves, authorization to make the document available again.

delete_pending and delete_failed states require incident-specific reconciliation; recovery must not silently convert them to available.

Fail-closed conditions

Vault recovery fails closed when any of the following is true:

- PostgreSQL metadata for the document cannot be identified
- tenant binding is inconsistent
- storage bucket/key binding is unexplained
- a recorded storage_version_id cannot be retrieved
- object bytes are missing
- size verification fails
- content-type verification fails when available
- SHA-256 verification fails
- a deleted document would be resurrected implicitly
- recovery requires a blind production overwrite without an approved emergency reason
- a same-provider version is incorrectly represented as an independent backup
- recovery evidence is incomplete

Existing code-level recovery primitives

The current Vault implementation already provides the primitives required by this contract:

- storage_version_id is persisted with Vault document metadata when supplied by storage
- S3-compatible head/get operations accept a version_id
- verified Vault download requests the persisted storage_version_id
- verified download checks size and content type
- verified download recomputes SHA-256 over the complete object before exposing bytes
- Vault object keys are tenant-scoped

P2.7A4 therefore does not change normal Vault upload/delete semantics.

Operational acceptance boundary

P2.7A4 proves and documents recovery semantics.

P2.7A6 remains responsible for final production disaster-recovery acceptance, including any provider-loss scenario, independent Vault recovery copy/replica requirement, measured recovery evidence, and go-live sign-off.

13. P2.7A5 executable pre-migration recovery gate

P2.7A5 status:

CLOSED on 2026-08-18 after real production pre-migration recovery-gate acceptance.

Acceptance evidence:

- stale recovery point 20260818T021501Z_production.manifest.json was rejected because it exceeded the 120-minute migration freshness limit
- stale-evidence result: NO MIGRATION
- fresh production logical-backup workflow run 32155184817: PASS
- fresh manifest: 20260818T153402Z_production.manifest.json
- source release: 0c94022ad7ffdae781b85b8dac38f18e64de0e05
- source Alembic revision: 008_add_platform_control_plane_functions
- gate format: p27a5.gate.v1
- manifest SHA-256: 109026eaf5c421b5051f26c882f60cc35a18650e0f47f99d30a4be0804bf9190
- source identity SHA-256: d0f40f7dbfdb29812fe6aafd8f7e7c8bec7393a91342775fffde5eba6931a383
- backup age at verification: 915 seconds
- verification timestamp: 2026-08-18T15:49:17Z
- verification status: PASS
- production migration executed during acceptance: NO

The operational test therefore proves both fail-closed stale-evidence rejection and successful fresh-evidence validation against the real production database.

Purpose

Every production migration must have a recovery point that is demonstrably bound to the production state immediately before the migration.

A backup file existing somewhere is insufficient.

The gate runs before Alembic.

If validation fails, the required result is:

NO MIGRATION

Recovery evidence

The gate consumes two P2.7A3 artifacts obtained from the protected off-platform backup domain:

- the exact production manifest
- its complete.json publication marker

The manifest must not be renamed or modified after retrieval.

The complete marker must not be regenerated locally.

The operator does not need to download the full PostgreSQL dump for this gate.

Publication-chain verification

The gate verifies:

- manifest format
- complete-marker format
- manifest_sha256
- manifest filename binding
- dump filename binding
- dump SHA-256 binding
- dump size binding
- source label
- release commit
- backup timestamp
- publication timestamp

The P2.7A3 complete marker is meaningful because it is published only after the remote dump and manifest have been verified.

Database binding

The recovery point is compared to the database referenced by the ephemeral MIGRATION_DATABASE_URL.

The gate verifies:

- database name
- source_identity_sha256
- current Alembic revision
- PostgreSQL major version
- PostGIS major.minor version

source_identity_sha256 binds the recovery point to the host/port/database identity without writing the database URL or hostname into the gate result.

Release binding

PRE_MIGRATION_SOURCE_RELEASE_COMMIT must identify the currently deployed production release before migration.

It must match both the recovery manifest and complete marker.

It is deliberately not the candidate release commit.

Freshness

The default maximum recovery-point age for a schema-changing production migration is:

120 minutes

The backup creation timestamp is authoritative for freshness.

A future timestamp beyond the allowed clock-skew tolerance is invalid.

A normal <= 24 hour logical-backup RPO is not, by itself, sufficient for the stricter pre-migration gate.

Operator evidence

The deployment supplies:

- PRE_MIGRATION_SOURCE_RELEASE_COMMIT
- PRE_MIGRATION_OPERATOR
- PRE_MIGRATION_TARGET_ENV=production
- PRE_MIGRATION_MAX_AGE_MINUTES
- PRE_MIGRATION_RECOVERY_MANIFEST
- PRE_MIGRATION_RECOVERY_COMPLETE

A successful gate emits a sanitized PASS record containing:

- verification status
- verification timestamp
- operator
- source production release
- backup timestamp
- backup age
- Alembic revision
- source_identity_sha256
- manifest_sha256

The record does not contain a database URL, password, storage credential, or provider endpoint.

Fail-closed rules

NO MIGRATION is required when any of the following is true:

- recovery evidence is missing or invalid
- manifest_sha256 does not match
- manifest and complete marker disagree
- dump SHA-256 or size bindings disagree
- source label is not production
- recovery release does not match the currently deployed production release
- recovery point is older than the configured pre-migration limit
- evidence timestamps are invalid
- database name differs
- source_identity_sha256 differs
- Alembic revision differs
- PostgreSQL major version differs
- PostGIS major.minor version differs
- target environment is not production

Security boundary

The evidence files must be retrieved from the protected off-platform recovery domain using an authorized operator/read-only identity.

The deployment mounts them read-only.

MIGRATION_DATABASE_URL remains ephemeral and is not written into the evidence.

The executable gate does not weaken the separate Vault recovery contract.

Acceptance boundary

P2.7A5 operational acceptance is complete.

The gate has demonstrated both required production behaviors:

- stale recovery evidence fails closed with NO MIGRATION
- fresh, correctly bound production recovery evidence returns PASS

No Alembic migration was executed during either acceptance test.

P2.7A6 remains responsible for final DR acceptance and go-live sign-off.
