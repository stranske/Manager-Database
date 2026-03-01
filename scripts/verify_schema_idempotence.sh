#!/usr/bin/env bash
set -euo pipefail

# Verifies schema.sql can be applied to a fresh Postgres database twice.
# If no reachable Postgres instance is configured, this script starts a
# temporary local Postgres 16 cluster under /tmp.
#
# Docker shortcut (no local Postgres needed):
#   docker run --rm -e POSTGRES_HOST_AUTH_METHOD=trust pgvector/pgvector:pg16 &
#   sleep 3 && PGHOST=localhost PGPORT=5432 bash scripts/verify_schema_idempotence.sh

SCHEMA_PATH="${1:-schema.sql}"
MAINTENANCE_DB="${PGMAINTENANCE_DB:-postgres}"
DB_NAME="schema_verify_${USER:-runner}_$(date +%s)_$$"
PG16_BIN_DIR="${PG16_BIN_DIR:-/usr/lib/postgresql/16/bin}"

TEMP_PG_ROOT=""
TEMP_PG_DATA=""
TEMP_PG_LOG=""
TEMP_PG_PORT=""

if [[ ! -f "${SCHEMA_PATH}" ]]; then
  echo "Schema file not found: ${SCHEMA_PATH}" >&2
  exit 1
fi

resolve_bin() {
  local tool="$1"
  if command -v "${tool}" >/dev/null 2>&1; then
    command -v "${tool}"
    return 0
  fi
  if [[ -x "${PG16_BIN_DIR}/${tool}" ]]; then
    echo "${PG16_BIN_DIR}/${tool}"
    return 0
  fi
  echo "Required command not found: ${tool}" >&2
  return 1
}

PSQL_BIN="$(resolve_bin psql)"
CREATEDB_BIN="$(resolve_bin createdb)"
DROPDB_BIN="$(resolve_bin dropdb)"

can_connect() {
  "${PSQL_BIN}" -v ON_ERROR_STOP=1 --dbname="${MAINTENANCE_DB}" -c "SELECT 1;" >/dev/null 2>&1
}

start_temp_pg() {
  local initdb_bin
  local pg_ctl_bin
  initdb_bin="$(resolve_bin initdb)"
  pg_ctl_bin="$(resolve_bin pg_ctl)"

  TEMP_PG_ROOT="$(mktemp -d /tmp/schema-verify-pg.XXXXXX)"
  TEMP_PG_DATA="${TEMP_PG_ROOT}/data"
  TEMP_PG_LOG="${TEMP_PG_ROOT}/postgres.log"
  TEMP_PG_PORT="${PGPORT:-5432}"

  echo "No reachable Postgres detected; starting temporary local Postgres 16..."
  "${initdb_bin}" --auth=trust --no-instructions -D "${TEMP_PG_DATA}" >/dev/null
  if ! "${pg_ctl_bin}" \
    -D "${TEMP_PG_DATA}" \
    -l "${TEMP_PG_LOG}" \
    -o "-p ${TEMP_PG_PORT} -k ${TEMP_PG_ROOT} -c listen_addresses=''" \
    start >/dev/null; then
    echo "Failed to start temporary Postgres. Startup log:" >&2
    sed -n '1,200p' "${TEMP_PG_LOG}" >&2 || true
    if grep -qiE "could not create any (tcp/ip|unix-domain) sockets|operation not permitted" "${TEMP_PG_LOG}"; then
      cat >&2 <<'EOF'
This environment appears to block database socket creation.
Run this script in an environment with socket permissions (e.g. local shell or CI runner with Docker/Postgres).
EOF
    fi
    exit 1
  fi

  export PGHOST="${TEMP_PG_ROOT}"
  export PGPORT="${TEMP_PG_PORT}"
  PG_CTL_BIN="${pg_ctl_bin}"
}

cleanup() {
  "${DROPDB_BIN}" --if-exists --maintenance-db="${MAINTENANCE_DB}" "${DB_NAME}" >/dev/null 2>&1 || true
  if [[ -n "${TEMP_PG_DATA}" ]]; then
    "${PG_CTL_BIN:-pg_ctl}" -D "${TEMP_PG_DATA}" -m fast stop >/dev/null 2>&1 || true
  fi
  if [[ -n "${TEMP_PG_ROOT}" ]]; then
    rm -rf "${TEMP_PG_ROOT}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

if ! can_connect; then
  start_temp_pg
fi

echo "Creating fresh database: ${DB_NAME}"
"${CREATEDB_BIN}" --maintenance-db="${MAINTENANCE_DB}" "${DB_NAME}"

echo "Applying ${SCHEMA_PATH} (pass 1)"
"${PSQL_BIN}" --dbname="${DB_NAME}" -v ON_ERROR_STOP=1 -f "${SCHEMA_PATH}" >/dev/null

echo "Applying ${SCHEMA_PATH} (pass 2)"
"${PSQL_BIN}" --dbname="${DB_NAME}" -v ON_ERROR_STOP=1 -f "${SCHEMA_PATH}" >/dev/null

echo "Schema apply verification succeeded (fresh + idempotent)."
