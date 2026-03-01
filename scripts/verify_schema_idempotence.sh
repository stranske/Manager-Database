#!/usr/bin/env bash
set -euo pipefail

# Verifies schema.sql can be applied to a fresh Postgres database twice.
# Connection parameters are read from standard PG* environment variables.

SCHEMA_PATH="${1:-schema.sql}"
MAINTENANCE_DB="${PGMAINTENANCE_DB:-postgres}"
DB_NAME="schema_verify_${USER:-runner}_$(date +%s)_$$"

if [[ ! -f "${SCHEMA_PATH}" ]]; then
  echo "Schema file not found: ${SCHEMA_PATH}" >&2
  exit 1
fi

cleanup() {
  dropdb --if-exists --maintenance-db="${MAINTENANCE_DB}" "${DB_NAME}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "Creating fresh database: ${DB_NAME}"
createdb --maintenance-db="${MAINTENANCE_DB}" "${DB_NAME}"

echo "Applying ${SCHEMA_PATH} (pass 1)"
psql --dbname="${DB_NAME}" -v ON_ERROR_STOP=1 -f "${SCHEMA_PATH}" >/dev/null

echo "Applying ${SCHEMA_PATH} (pass 2)"
psql --dbname="${DB_NAME}" -v ON_ERROR_STOP=1 -f "${SCHEMA_PATH}" >/dev/null

echo "Schema apply verification succeeded (fresh + idempotent)."
