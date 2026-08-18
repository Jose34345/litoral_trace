#!/usr/bin/env bash
# Litoral Trace - controlled production deployment
set -Eeuo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.prod.yml}"
APP_DIR="${APP_DIR:-/opt/litoral_trace}"

log() {
    printf '%s\n' "$*"
}

fail() {
    printf 'ERROR: %s\n' "$*" >&2
    exit 1
}

require_command() {
    command -v "$1" >/dev/null 2>&1 || fail "Required command not found: $1"
}

require_nonempty_env() {
    local name="$1"
    if [ -z "${!name:-}" ]; then
        fail "Required environment variable is not set: $name"
    fi
}

wait_for_service_health() {
    local service="$1"
    local timeout_seconds="${2:-120}"
    local elapsed=0
    local container_id=""
    local health=""

    while [ "$elapsed" -lt "$timeout_seconds" ]; do
        container_id="$(docker compose -f "$COMPOSE_FILE" ps -q "$service" 2>/dev/null || true)"

        if [ -n "$container_id" ]; then
            health="$(
                docker inspect \
                    --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' \
                    "$container_id" 2>/dev/null || true
            )"

            case "$health" in
                healthy)
                    log "OK: service '$service' is healthy."
                    return 0
                    ;;
                unhealthy|exited|dead)
                    docker compose -f "$COMPOSE_FILE" logs --tail=100 "$service" || true
                    fail "Service '$service' entered state: $health"
                    ;;
            esac
        fi

        sleep 2
        elapsed=$((elapsed + 2))
    done

    docker compose -f "$COMPOSE_FILE" ps || true
    docker compose -f "$COMPOSE_FILE" logs --tail=100 "$service" || true
    fail "Timed out waiting for service '$service' to become healthy."
}

log "=========================================================="
log "Litoral Trace - production deployment"
log "=========================================================="

require_command docker
require_command realpath

docker compose version >/dev/null 2>&1 \
    || fail "Docker Compose v2 ('docker compose') is required."

[ -d "$APP_DIR" ] || fail "Application directory does not exist: $APP_DIR"
cd "$APP_DIR"

[ -f "$COMPOSE_FILE" ] || fail "Compose file not found: $COMPOSE_FILE"

# Docker Compose may read runtime values from an untracked .env or from the
# deployment environment. MIGRATION_DATABASE_URL is deliberately not part of
# either long-lived service environment.
require_nonempty_env MIGRATION_DATABASE_URL

# P2.7A5 requires recovery evidence materialized from the immutable
# off-platform backup domain before any migration attempt.
require_nonempty_env PRE_MIGRATION_RECOVERY_MANIFEST
require_nonempty_env PRE_MIGRATION_RECOVERY_COMPLETE
require_nonempty_env PRE_MIGRATION_SOURCE_RELEASE_COMMIT
require_nonempty_env PRE_MIGRATION_OPERATOR

PRE_MIGRATION_TARGET_ENV="${PRE_MIGRATION_TARGET_ENV:-production}"
PRE_MIGRATION_MAX_AGE_MINUTES="${PRE_MIGRATION_MAX_AGE_MINUTES:-120}"

[ "$PRE_MIGRATION_TARGET_ENV" = "production" ] \
    || fail "PRE_MIGRATION_TARGET_ENV must be production."

[ -f "$PRE_MIGRATION_RECOVERY_MANIFEST" ] \
    || fail "Pre-migration recovery manifest does not exist."

[ -f "$PRE_MIGRATION_RECOVERY_COMPLETE" ] \
    || fail "Pre-migration recovery complete marker does not exist."

RECOVERY_MANIFEST_PATH="$(
    realpath "$PRE_MIGRATION_RECOVERY_MANIFEST"
)"
RECOVERY_COMPLETE_PATH="$(
    realpath "$PRE_MIGRATION_RECOVERY_COMPLETE"
)"

log "Validating Compose configuration..."
docker compose -f "$COMPOSE_FILE" config --quiet

log "Building shared API/worker image..."
docker compose -f "$COMPOSE_FILE" build app worker

# Fail before touching the database when the production Vault storage contract
# is missing, invalid, unreachable, or unauthorized.
log "Verifying private Vault object storage before migration..."
docker compose -f "$COMPOSE_FILE" run \
    --rm \
    --no-deps \
    app \
    python -m litoral_trace.storage.readiness

# P2.7A5 fail-closed gate. Recovery evidence is mounted read-only and the
# database owner credential is passed through the environment without being
# persisted in the service definition.
log "Verifying pre-migration recovery point..."
docker compose -f "$COMPOSE_FILE" run \
    --rm \
    --no-deps \
    -e MIGRATION_DATABASE_URL \
    -e PRE_MIGRATION_SOURCE_RELEASE_COMMIT \
    -e PRE_MIGRATION_OPERATOR \
    -e PRE_MIGRATION_TARGET_ENV \
    -e PRE_MIGRATION_MAX_AGE_MINUTES \
    -v "${RECOVERY_MANIFEST_PATH}:/run/litoral-recovery/manifest.json:ro" \
    -v "${RECOVERY_COMPLETE_PATH}:/run/litoral-recovery/complete.json:ro" \
    app \
    python -m scripts.pre_migration_recovery_gate \
        --manifest /run/litoral-recovery/manifest.json \
        --complete-marker /run/litoral-recovery/complete.json \
        --source-label production

log "Applying Alembic migrations with an ephemeral owner credential..."
docker compose -f "$COMPOSE_FILE" run \
    --rm \
    --no-deps \
    -e MIGRATION_DATABASE_URL \
    app \
    python -m alembic upgrade head

# Reduce accidental owner-credential persistence in the deployment shell after
# the only operation that requires it.
unset MIGRATION_DATABASE_URL

log "Starting production services..."
docker compose -f "$COMPOSE_FILE" up -d db app worker proxy

log "Waiting for API readiness..."
wait_for_service_health app 180

log "Waiting for satellite worker readiness..."
wait_for_service_health worker 180

log "Verifying API readiness from inside the API container..."
docker compose -f "$COMPOSE_FILE" exec -T app \
    curl -fsS http://127.0.0.1:8000/ready >/dev/null

log "Verifying Vault storage readiness from inside the API container..."
docker compose -f "$COMPOSE_FILE" exec -T app \
    python -m litoral_trace.storage.readiness

log "Verifying worker readiness without claiming a job..."
docker compose -f "$COMPOSE_FILE" exec -T worker \
    python -m litoral_trace.workers.satellite_worker --check

log "Current service state:"
docker compose -f "$COMPOSE_FILE" ps

log "=========================================================="
log "Deployment completed successfully."
log "API, Vault storage, and satellite worker are healthy."
log "=========================================================="