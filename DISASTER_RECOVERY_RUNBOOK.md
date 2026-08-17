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

P2.7A3 is NOT CLOSED yet.

P2.7A3C - scheduled independent logical backup + durable off-platform retention remains required to operationally support the <= 24 hours logical backup RPO target.

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

Open gap retained:

P2.7A3C - scheduled independent logical backup + durable off-platform retention is still required to operationally support the <= 24 hours logical backup RPO target.

This does not close P2.7A3 overall and does not replace P2.7A4 Vault object-storage recovery.
