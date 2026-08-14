Litoral Trace - Production Deployment Runbook

This runbook covers the self-hosted Docker Compose production topology forLitoral Trace. It is intentionally strict about PostgreSQL role separation,worker readiness, and migration credentials.

Production deployment must use reviewed artifacts only. Never paste realdatabase passwords, JWT secrets, service-account JSON, lease tokens, orprivate keys into Git-tracked files, terminal transcripts, issue comments,or application logs.

1. Production topology

The shared Docker image supports two long-lived application services:

app: FastAPI API, internal port 8000, readiness endpoint /ready.

worker: durable satellite worker, internal Prometheus port 9108.

db: optional self-hosted PostgreSQL/PostGIS service.

proxy: Nginx public ingress for ports 80/443.

The worker metrics port is internal only. Do not add a host mapping such as9108:9108.

The API and worker use the same image, but they use different commands anddifferent healthchecks.

2. PostgreSQL credential separation

Three database principals have different responsibilities:

Runtime API principal — DATABASE_URL

The API runtime principal must be a non-owner PostgreSQL role with:

NOSUPERUSER;

NOBYPASSRLS;

no ownership of application tables;

only the grants required by the normal application runtime.

Worker capability principal — WORKER_DATABASE_URL

The worker capability principal is separate from the API runtime role. It isused for queue-wide worker functions such as atomic claim/recovery/aggregatequeue metrics. It must not be a schema owner or migration principal.

The worker also receives DATABASE_URL because tenant-scoped resultpersistence is deliberately performed through the ordinary runtime/RLS path.

Migration owner — MIGRATION_DATABASE_URL

The migration owner is for Alembic / DDL / controlled maintenance only.

Do not place MIGRATION_DATABASE_URL in the persistent app or workerservice environment.

The deployment script injects it only into an ephemeral migration container andthen unsets it from the deployment shell.

3. Secret provisioning

Use the deployment platform's secret manager whenever possible.

For a self-hosted Compose deployment, an untracked .env may provide runtimeCompose variables, but it must never be committed. Start from .env.exampleand replace every placeholder securely.

At minimum, the deployment environment must provide the values required bydocker-compose.prod.yml, including:

DATABASE_URL;

WORKER_DATABASE_URL;

JWT_SECRET_KEY;

POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB when using the bundledself-hosted db service;

Google Earth Engine / Google Cloud credentials required by the productionworker.

MIGRATION_DATABASE_URL should preferably be injected into the deploymentshell from a secret manager immediately before the deployment and not kept asa long-lived runtime variable.

Never reuse the database bootstrap/owner password as the runtime or workercredential.

4. Pre-deployment gates

From the reviewed release checkout:

git status --short
git rev-parse HEAD
python -m alembic heads

Confirm:

the expected release commit is checked out;

the working tree is clean;

Alembic has exactly one expected head;

the target database is the intended production database;

a recent backup/recovery point exists before schema-changing deployments.

Validate the Compose file before touching services:

docker compose -f docker-compose.prod.yml config --quiet

Build the shared image:

docker compose -f docker-compose.prod.yml build app worker

Do not deploy if either command fails.

5. Controlled migration

Export the migration owner URL only for the migration operation:

export MIGRATION_DATABASE_URL='<load-from-secret-manager>'

Run the deployment script:

chmod +x deploy_production.sh
./deploy_production.sh

The script performs the following sequence:

validates Docker Compose;

builds the shared API/worker image;

runs python -m alembic upgrade head in an ephemeral container with themigration owner credential;

removes the owner credential from the deployment shell;

starts db, app, worker, and proxy;

waits for API and worker healthchecks;

rechecks API /ready inside the API container;

executes worker --check inside the worker container.

The worker --check path is non-destructive: it checks worker queue capabilityand the ordinary runtime database path without claiming or executing a job.

6. Readiness and liveness

API

Liveness:

GET /health

Readiness:

GET /ready

The production container healthcheck uses /ready, because a process that isalive but cannot reach its runtime database must not be considered ready.

Satellite worker

Container readiness executes:

python -m litoral_trace.workers.satellite_worker --check

A successful check exits 0; a failed dependency check exits non-zero.

Normal worker execution is:

python -m litoral_trace.workers.satellite_worker

Do not use --once as the long-lived production command.

7. Operational verification

Inspect service state:

docker compose -f docker-compose.prod.yml ps

Inspect recent API logs:

docker compose -f docker-compose.prod.yml logs --tail=100 app

Inspect recent worker logs:

docker compose -f docker-compose.prod.yml logs --tail=100 worker

Run an explicit API readiness check from inside the container:

docker compose -f docker-compose.prod.yml exec -T app \
  curl -fsS http://127.0.0.1:8000/ready

Run an explicit worker readiness check:

docker compose -f docker-compose.prod.yml exec -T worker \
  python -m litoral_trace.workers.satellite_worker --check

Do not expose the worker metrics port publicly. Prometheus or another internalscraper should reach worker:9108 from the private Docker network.

8. Graceful worker shutdown

The durable worker handles SIGINT and SIGTERM by requesting shutdown andstopping new claims after the active execution boundary.

For a normal deployment/restart use Compose rather than killing the processdirectly:

docker compose -f docker-compose.prod.yml stop worker

The Compose service includes a grace period so an active execution has time tofinish its shutdown path.

Crash/restart semantics, stale recovery under abrupt termination, and zombielease rejection belong to the dedicated failure/recovery acceptance gate andmust be validated before declaring the complete asynchronous worker subsystemproduction-ready.

9. Rollback principles

Application rollback and database rollback are separate decisions.

For application code:

identify the previously accepted release commit;

check out that release;

rebuild the image;

restart the services;

verify API and worker readiness.

Do not blindly run alembic downgrade in production. Database rollback mustbe reviewed per migration and must account for data written after the upgrade.

If a migration introduces an unsafe production state, stop the affectedapplication services and follow the database recovery/restore procedure for thespecific incident.

10. Security invariants

The deployment is invalid if any of the following is true:

API receives WORKER_DATABASE_URL;

API or worker receives persistent MIGRATION_DATABASE_URL;

runtime/worker DB roles are schema owners or have BYPASSRLS;

real database credentials are committed to Git;

worker metrics are published directly to a public host port;

healthchecks claim jobs or invoke Google Earth Engine;

production falls back to SQLite;

init_db.py is used to create/mutate the production schema.

Alembic remains the only supported production schema migration mechanism.