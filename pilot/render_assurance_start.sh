#!/bin/sh
set -eu

CONFIG_PATH="${LT_ASSURANCE_PILOT_CONFIG_PATH:-/app/pilot/assurance-pilot-staging.bootstrap.json}"
PORT_VALUE="${PORT:-10000}"

if [ "${ENVIRONMENT:-}" != "staging" ]; then
  echo "Assurance staging start aborted: ENVIRONMENT must be staging." >&2
  exit 1
fi

if [ "${LT_ASSURANCE_PILOT_MODE:-0}" != "1" ]; then
  echo "Assurance staging start aborted: pilot mode is disabled." >&2
  exit 1
fi

if [ ! -r "$CONFIG_PATH" ]; then
  echo "Assurance staging start aborted: pilot config is not readable." >&2
  exit 1
fi

exec uvicorn main:app \
  --host 0.0.0.0 \
  --port "$PORT_VALUE" \
  --workers 1 \
  --no-access-log \
  --log-config /app/pilot/logging.json
