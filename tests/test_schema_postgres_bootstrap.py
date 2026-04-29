"""Smoke gate: a fresh Postgres database can apply ``schema.sql`` end-to-end.

The existing ``test_schema.py`` exercises Alembic migrations against SQLite. That
covers the ORM-side schema but not the canonical ``schema.sql`` that
``docker-compose`` mounts into ``/docker-entrypoint-initdb.d/`` for a real
Postgres bring-up. A bootstrap-order bug in ``schema.sql`` (e.g. creating an
index on a materialized view before the view exists) would fail at compose-up
time but pass every existing test. This module fills that gap.

The smoke is gated on ``MGRDB_PG_TEST_URL``: when unset, the test skips so that
local and SQLite-only CI paths stay green. To run it:

    docker compose up -d db
    export MGRDB_PG_TEST_URL=postgresql://postgres:$POSTGRES_PASSWORD@localhost:5432/postgres
    pytest tests/test_schema_postgres_bootstrap.py
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCHEMA_SQL = ROOT / "schema.sql"

# API/ETL-critical tables that downstream services (api/, etl/, alerts/, dashboard/,
# chains/) all assume exist after bootstrap. Grouped by category:
#   core:     managers, filings, holdings, daily_diffs, api_usage, documents
#   alerts:   alert_rules, alert_history
#   signals:  conviction_scores, crowded_trades, contrarian_signals
#   activism: activism_filings, activism_events
EXPECTED_TABLES = {
    # core
    "managers",
    "filings",
    "holdings",
    "daily_diffs",
    "api_usage",
    "documents",
    # alerts
    "alert_rules",
    "alert_history",
    # signals / computed
    "conviction_scores",
    "crowded_trades",
    "contrarian_signals",
    # activism
    "activism_filings",
    "activism_events",
}
EXPECTED_MATVIEWS = {"monthly_usage", "mv_daily_report"}
EXPECTED_INDEXES = {"mv_daily_report_idx"}


def test_mv_daily_report_idx_defined_after_matview_in_sql():
    """Text-level ordering guard: mv_daily_report_idx must appear after the matview DDL.

    Runs without a live Postgres instance so it catches regressions in every CI path,
    not just when MGRDB_PG_TEST_URL is set.
    """
    sql = SCHEMA_SQL.read_text()
    matview_pos = sql.find("CREATE MATERIALIZED VIEW mv_daily_report")
    index_pos = sql.find("CREATE UNIQUE INDEX IF NOT EXISTS mv_daily_report_idx")

    assert matview_pos != -1, (
        "schema.sql does not contain 'CREATE MATERIALIZED VIEW mv_daily_report' — "
        "matview definition is missing or was renamed"
    )
    assert index_pos != -1, (
        "schema.sql does not contain 'CREATE UNIQUE INDEX IF NOT EXISTS mv_daily_report_idx' — "
        "index definition is missing or was renamed"
    )
    assert matview_pos < index_pos, (
        f"schema.sql ordering error: mv_daily_report_idx (offset {index_pos}) appears before "
        f"CREATE MATERIALIZED VIEW mv_daily_report (offset {matview_pos}). "
        "Move the index creation to after the matview DDL."
    )


@pytest.fixture(scope="module")
def pg_url() -> str:
    url = os.environ.get("MGRDB_PG_TEST_URL")
    if not url:
        pytest.skip("MGRDB_PG_TEST_URL not set; skipping Postgres bootstrap smoke")
    return url


@pytest.fixture(scope="module")
def psycopg_module():
    psycopg = pytest.importorskip("psycopg")
    return psycopg


def _reset_public_schema(conn) -> None:
    """Drop and recreate the public schema so schema.sql runs against a clean slate."""
    with conn.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS public CASCADE")
        cur.execute("CREATE SCHEMA public")
        cur.execute("GRANT ALL ON SCHEMA public TO public")
    conn.commit()


def _apply_schema_sql(conn) -> None:
    """Apply the canonical schema.sql, surfacing the exact failing statement on error."""
    sql = SCHEMA_SQL.read_text()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
    except Exception as exc:
        conn.rollback()
        pytest.fail(
            f"schema.sql bootstrap failed: {type(exc).__name__}: {exc}\n"
            f"(this almost always indicates an object-ordering problem in schema.sql)"
        )


def test_schema_sql_bootstraps_clean_postgres(pg_url, psycopg_module):
    """schema.sql must apply end-to-end to a freshly created public schema."""
    with psycopg_module.connect(pg_url, autocommit=False) as conn:
        _reset_public_schema(conn)
        _apply_schema_sql(conn)

        with conn.cursor() as cur:
            cur.execute("SELECT tablename FROM pg_tables WHERE schemaname = current_schema()")
            tables = {row[0] for row in cur.fetchall()}
        missing_tables = EXPECTED_TABLES - tables
        assert not missing_tables, f"schema.sql did not create expected tables: {missing_tables}"


def test_schema_sql_creates_matviews_before_their_indexes(pg_url, psycopg_module):
    """Regression guard for the mv_daily_report ordering bug (issue #906).

    The matview must exist (and the unique index on it must also exist) after
    bootstrap. If the index were created before the matview, schema.sql would
    have failed in the previous test — but we also assert the index exists here
    so a future re-introduction of the bug surfaces with a clear message.
    """
    with psycopg_module.connect(pg_url, autocommit=False) as conn:
        _reset_public_schema(conn)
        _apply_schema_sql(conn)

        with conn.cursor() as cur:
            cur.execute("SELECT matviewname FROM pg_matviews WHERE schemaname = current_schema()")
            matviews = {row[0] for row in cur.fetchall()}
            cur.execute("SELECT indexname FROM pg_indexes WHERE schemaname = current_schema()")
            indexes = {row[0] for row in cur.fetchall()}

        missing_matviews = EXPECTED_MATVIEWS - matviews
        assert not missing_matviews, f"missing matviews after bootstrap: {missing_matviews}"

        missing_indexes = EXPECTED_INDEXES - indexes
        assert not missing_indexes, (
            f"missing indexes after bootstrap: {missing_indexes} "
            f"(check schema.sql ordering: matview creation must precede its index)"
        )
