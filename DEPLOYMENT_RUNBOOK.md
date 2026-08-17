Litoral Trace — Production Deployment Runbook

This runbook covers the reviewed production topology for Litoral Trace. It is intentionally strict about PostgreSQL role separation, private Vault object storage, worker readiness, migration credentials, and fail-closed deployment gates.

Never paste real database passwords, JWT secrets, object-storage credentials, Google service-account JSON, lease tokens, private keys, or presigned URLs into Git-tracked files, terminal transcripts, issue comments, or application logs.

1. Production topology

The shared application image supports two long-lived services:

app: FastAPI API, internal port 8000, health/readiness endpoint /ready.

worker: durable satellite worker, internal Prometheus port 9108.

db: optional self-hosted PostgreSQL/PostGIS service.

proxy: Nginx public ingress for ports 80/443.

Nginx is the canonical TLS termination point. Port 80 only redirects to HTTPS. Port 443 serves the application with:

/etc/nginx/certs/fullchain.pem

/etc/nginx/certs/privkey.pem

Certificates and private keys must not be committed. They must be provisioned before deployment, private-key permissions must remain restricted, and certificate renewal/rotation is an operational responsibility.

Vault object storage: external/private S3-compatible bucket. It is not created by the application.

The worker metrics port is internal only. Do not publish 9108:9108.

The API and worker use the same image but different commands, credentials, and healthchecks.

2. PostgreSQL credential separation

Runtime API principal — DATABASE_URL

The API runtime role must be:

LOGIN

NOSUPERUSER

NOBYPASSRLS

non-owner of application tables

limited to runtime grants

Worker capability principal — WORKER_DATABASE_URL

The satellite worker capability role is separate from the API runtime role. It is used for queue-wide worker functions such as claim, stale recovery, and aggregate queue metrics.

It must not be a schema owner or migration principal.

The worker also receives DATABASE_URL because tenant-scoped result persistence deliberately travels through the ordinary runtime/RLS path.

Migration owner — MIGRATION_DATABASE_URL

The migration owner is for Alembic/DDL/controlled maintenance only.

Do not place MIGRATION_DATABASE_URL in the persistent app or worker environment. The deployment script injects it only into an ephemeral migration container and then removes it from the deployment shell.

3. Vault object-storage contract

Production Vault storage is mandatory for the production Compose topology.

Required deployment values:

STORAGE_BACKEND=s3

STORAGE_BUCKET_NAME=<private dedicated bucket>

STORAGE_REGION=<provider region>

STORAGE_USE_TLS=1

STORAGE_VERIFY_TLS=1

Optional/provider-specific values include:

STORAGE_ENDPOINT_URL

STORAGE_ACCESS_KEY_ID

STORAGE_SECRET_ACCESS_KEY

STORAGE_SESSION_TOKEN

STORAGE_FORCE_PATH_STYLE

STORAGE_CA_BUNDLE_PATH

timeout/retry settings

STORAGE_KEY_PREFIX

upload ceiling and MIME allowlist

Prefer provider-native IAM/workload identity. Static credentials must come from the deployment secret store and must never be committed.

For custom CA deployments, STORAGE_CA_BUNDLE_PATH must reference a read-only file that actually exists inside the API runtime/container.

The application never creates production buckets. Provision the bucket before deployment.

4. Vault bucket security

The production bucket must be dedicated/private and configured with:

public access blocked;

encryption at rest enabled by provider/bucket default;

TLS for transport;

certificate verification enabled;

lifecycle/retention rules reviewed separately from application soft-delete;

access logs/provider audit logs enabled where available.

The current storage readiness probe uses S3 HeadBucket. On AWS-compatible IAM this requires bucket-level s3:ListBucket.

Minimum application object access for a dedicated Vault bucket is conceptually:

{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "VaultBucketReadiness",
      "Effect": "Allow",
      "Action": ["s3:ListBucket"],
      "Resource": ["arn:aws:s3:::YOUR_PRIVATE_VAULT_BUCKET"]
    },
    {
      "Sid": "VaultObjectLifecycle",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:GetObjectVersion",
        "s3:DeleteObjectVersion"
      ],
      "Resource": [
        "arn:aws:s3:::YOUR_PRIVATE_VAULT_BUCKET/vault/*"
      ]
    }
  ]
}

Adapt resource syntax to the actual provider. If bucket versioning is disabled, version-specific actions may be omitted. Do not grant bucket creation, ACL mutation, public-policy mutation, or administrative IAM actions to the application runtime.

Because this is a dedicated Vault bucket and object keys are opaque/server-generated, the bucket-level readiness permission does not expose business filenames.

5. Secret provisioning

Use the deployment platform's secret manager whenever possible.

For self-hosted Compose, an untracked .env can supply runtime variables. Start from .env.example and replace placeholders securely.

At minimum, production must provide:

DATABASE_URL

WORKER_DATABASE_URL

JWT_SECRET_KEY

required GEE/Google Cloud runtime credentials

STORAGE_BACKEND

STORAGE_BUCKET_NAME

provider-specific storage identity/configuration

PostgreSQL bootstrap values if the bundled db service is used

MIGRATION_DATABASE_URL should preferably be injected into the deployment shell immediately before deployment and not persisted as a long-lived service variable.

Never reuse the database bootstrap/owner password as runtime or worker credentials.

6. Pre-deployment gates

From the reviewed release checkout:

git status --short
git rev-parse HEAD
python -m alembic heads

Confirm:

the expected release commit is checked out;

the working tree is clean;

Alembic has exactly one expected head;

the target database is the intended production database;

a recent backup/recovery point exists before schema-changing deployments;

the Vault bucket already exists and is private.

Validate Compose before touching services:

docker compose -f docker-compose.prod.yml config --quiet

Build:

docker compose -f docker-compose.prod.yml build app worker

Do not deploy if either command fails.

6A. CI release gate

Before a release candidate is eligible for production deployment:

GitHub CI must be green.

Python tests must be green.

The Alembic single canonical head check must pass.

The frontend build must be reproducible.

npm high-severity audit must pass.

The production Docker image must build.

Production Compose configuration validation must pass.

P2.6A does not deploy automatically.

Production deployment remains a controlled/manual action through the reviewed deployment process until a later protected CD gate exists.

6B. Disaster recovery pre-migration gate

See DISASTER_RECOVERY_RUNBOOK.md.

Schema-changing production deployments require a verified recovery point before migration.

Go-live PITR/history target is >= 7 days, and current provider settings must be checked against that target before release approval.

Successful isolated restore testing is required.

Independent pg_dump fallback and Vault recovery are covered by the disaster recovery program.

Blind production database downgrade/restore is prohibited.

7. Vault storage preflight

Before any database migration, validate the exact runtime storage contract through the built application image:

docker compose -f docker-compose.prod.yml run --rm --no-deps \
  app python -m litoral_trace.storage.readiness

Expected safe output:

Vault storage readiness: ready.

The command must return non-zero when:

production storage is not configured;

storage configuration is invalid;

the endpoint is unreachable;

TLS/certificate validation fails;

credentials/IAM are insufficient;

HeadBucket fails.

The check intentionally does not print bucket credentials, provider exception bodies, object keys, or secrets.

8. Controlled migration

Export the owner URL only for the migration operation:

export MIGRATION_DATABASE_URL='<load-from-secret-manager>'

Run:

chmod +x deploy_production.sh
./deploy_production.sh

The deployment script:

validates Docker Compose;

builds the API/worker image;

checks private Vault object storage before migration;

runs python -m alembic upgrade head in an ephemeral container with the migration owner credential;

unsets the owner credential from the deployment shell;

starts db, app, worker, and proxy;

waits for API and worker health;

rechecks /ready inside the API container;

rechecks Vault storage from inside the live API container;

executes worker --check.

Never run Alembic by manually pointing the runtime role at DDL.

Production disables FastAPI schema/documentation endpoints:

/docs

/redoc

/openapi.json

9. Readiness and liveness

API liveness

GET /health

Liveness means the FastAPI process can answer.

API readiness

GET /ready

Readiness fails closed when:

runtime PostgreSQL is unavailable; or

production Vault storage is missing/unavailable/unauthorized.

In non-production environments an intentionally unconfigured Vault does not make /ready fail. Once storage is configured, an unhealthy storage dependency fails readiness.

The response remains intentionally minimal and does not disclose dependency names, endpoints, buckets, credentials, or provider error details.

Satellite worker

Container readiness:

python -m litoral_trace.workers.satellite_worker --check

The worker check is non-destructive and must not claim/execute jobs or invoke GEE work.

10. Operational verification

Inspect services:

docker compose -f docker-compose.prod.yml ps

API logs:

docker compose -f docker-compose.prod.yml logs --tail=100 app

Worker logs:

docker compose -f docker-compose.prod.yml logs --tail=100 worker

API readiness:

docker compose -f docker-compose.prod.yml exec -T app \
  curl -fsS http://127.0.0.1:8000/ready

Vault storage readiness:

docker compose -f docker-compose.prod.yml exec -T app \
  python -m litoral_trace.storage.readiness

Worker readiness:

docker compose -f docker-compose.prod.yml exec -T worker \
  python -m litoral_trace.workers.satellite_worker --check

Do not expose the worker metrics port or object-storage administrative consoles publicly.

11. Vault incident interpretation

If /health is healthy but /ready is unavailable, inspect database and Vault storage dependencies.

For Vault failures:

confirm provider service health;

confirm TLS/certificate chain;

confirm runtime identity/credential validity;

confirm s3:ListBucket on the dedicated bucket;

confirm object actions on the configured prefix;

do not rotate credentials blindly without understanding active sessions/deployments;

do not delete PostgreSQL metadata to hide an object-storage failure;

do not hard-delete Vault rows through SQL.

Vault upload/delete lifecycle already persists failure states and compensation outcomes. Operational repair must preserve those records.

12. Graceful worker shutdown

Use Compose:

docker compose -f docker-compose.prod.yml stop worker

The worker handles SIGINT/SIGTERM and stops new claims around its active execution boundary. Do not use --once as the long-lived production command.

13. Rollback principles

Application rollback and database rollback are separate decisions.

For application code:

identify the previously accepted release commit;

check out that release;

rebuild;

restart;

verify API, Vault storage, and worker readiness.

Do not blindly run alembic downgrade in production.

Object-storage rollback is also separate from database rollback. Never delete bucket contents merely because application code is rolled back.

14. Security invariants

Deployment is invalid if any of the following is true:

API receives WORKER_DATABASE_URL;

API or worker receives persistent MIGRATION_DATABASE_URL;

worker receives Vault object-storage credentials without a documented need;

runtime/worker DB roles are owners or have BYPASSRLS;

real credentials are committed to Git;

production Vault storage is unconfigured;

Vault bucket is public;

TLS or certificate verification is disabled for production storage;

application runtime can create buckets or modify public bucket policy;

worker metrics are published to a public host port;

healthchecks claim jobs or invoke GEE;

production falls back to SQLite;

init_db.py mutates production schema.

Alembic is the only supported production schema migration mechanism.
