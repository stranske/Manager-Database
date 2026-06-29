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
from urllib.parse import quote

import pytest
from alembic.config import Config

from alembic import command

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
EXPECTED_INDEXES = {"idx_news_items_topics_gin", "mv_daily_report_idx"}
IGNORED_PARITY_TABLES = {"alembic_version"}


def test_split_sql_statements_handles_dollar_quotes():
    """_split_sql_statements must keep a DO $$...$$; block as a single statement."""
    sql = (
        "CREATE TABLE t (id int);\n"
        "DO $$\n"
        "BEGIN\n"
        "  EXECUTE $mv$CREATE VIEW v AS SELECT 1$mv$;\n"
        "END\n"
        "$$;\n"
        "CREATE INDEX i ON t (id);\n"
    )
    stmts = _split_sql_statements(sql)
    assert len(stmts) == 3, f"expected 3 statements, got {len(stmts)}: {stmts}"
    assert any("DO" in s for s in stmts), "DO block missing from split result"
    assert any("CREATE TABLE" in s for s in stmts)
    assert any("CREATE INDEX" in s for s in stmts)


def test_split_sql_statements_parses_schema_sql():
    """schema.sql should split into non-empty statements; the DO blocks must stay whole."""
    sql = SCHEMA_SQL.read_text()
    stmts = _split_sql_statements(sql)
    assert len(stmts) >= 30, f"expected ≥30 statements in schema.sql, got {len(stmts)}"
    for i, stmt in enumerate(stmts, 1):
        assert stmt.strip(), f"statement {i} is empty after split"
    do_stmts = [s for s in stmts if s.lstrip().upper().startswith("DO")]
    assert (
        len(do_stmts) == 2
    ), f"expected exactly 2 DO blocks (monthly_usage + mv_daily_report), got {len(do_stmts)}"


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


def _split_sql_statements(sql: str) -> list[str]:
    """Split SQL text into individual statements, correctly handling dollar-quoted blocks.

    PostgreSQL dollar-quoting ($$...$$, $tag$...$tag$) can contain semicolons that must
    not be treated as statement terminators.  This parser tracks the outermost dollar-quote
    tag so DO-blocks and EXECUTE strings are kept as a single statement.
    """
    stmts: list[str] = []
    buf: list[str] = []
    in_dollar_quote = False
    dollar_tag = ""
    i = 0
    n = len(sql)

    while i < n:
        ch = sql[i]
        if not in_dollar_quote:
            if ch == "-" and i + 1 < n and sql[i + 1] == "-":
                # Line comment — skip to end of line (not added to buf)
                while i < n and sql[i] != "\n":
                    i += 1
                continue
            if ch == "$":
                # Scan forward to find the closing $, forming a tag like $$ or $mv$
                j = i + 1
                while j < n and sql[j] != "$":
                    j += 1
                if j < n:
                    tag = sql[i : j + 1]
                    in_dollar_quote = True
                    dollar_tag = tag
                    buf.append(tag)
                    i = j + 1
                    continue
            if ch == ";":
                buf.append(";")
                stmt = "".join(buf).strip()
                if stmt:
                    stmts.append(stmt)
                buf = []
                i += 1
                continue
        else:
            # Inside dollar-quote: only the matching closing tag ends it
            if sql[i : i + len(dollar_tag)] == dollar_tag:
                buf.append(dollar_tag)
                i += len(dollar_tag)
                in_dollar_quote = False
                dollar_tag = ""
                continue

        buf.append(ch)
        i += 1

    remaining = "".join(buf).strip()
    if remaining:
        stmts.append(remaining)

    return stmts


def _reset_public_schema(conn) -> None:
    """Drop and recreate the public schema so schema.sql runs against a clean slate."""
    with conn.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS public CASCADE")
        cur.execute("CREATE SCHEMA public")
        cur.execute("GRANT ALL ON SCHEMA public TO public")
    conn.commit()


def _apply_schema_sql(conn) -> None:
    """Apply schema.sql one statement at a time; report the exact failing statement."""
    sql = SCHEMA_SQL.read_text()
    stmts = _split_sql_statements(sql)
    for idx, stmt in enumerate(stmts, 1):
        try:
            with conn.cursor() as cur:
                cur.execute(stmt)
        except Exception as exc:
            conn.rollback()
            preview = stmt if len(stmt) <= 400 else stmt[:400] + "..."
            pytest.fail(
                f"schema.sql failed at statement {idx}/{len(stmts)}:\n"
                f"  {type(exc).__name__}: {exc}\n"
                f"Statement:\n{preview}"
            )
    conn.commit()


def _alembic_config(db_url: str) -> Config:
    config = Config(str(ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(ROOT / "alembic"))
    config.set_main_option("sqlalchemy.url", db_url)
    return config


def _url_with_search_path(db_url: str, schema_name: str) -> str:
    separator = "&" if "?" in db_url else "?"
    return f"{db_url}{separator}options={quote(f'-csearch_path={schema_name}', safe='')}"


def _reset_schema(conn, schema_name: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE')
        cur.execute(f'CREATE SCHEMA "{schema_name}"')
        cur.execute(f'SET search_path TO "{schema_name}"')
    conn.commit()


def _catalog_snapshot(conn) -> dict[str, set[tuple[str, ...]]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT tablename
              FROM pg_tables
             WHERE schemaname = current_schema()
               AND tablename <> ALL(%s)
            """,
            (list(IGNORED_PARITY_TABLES),),
        )
        tables = {(row[0],) for row in cur.fetchall()}

        cur.execute(
            """
            SELECT table_name, column_name, is_nullable
              FROM information_schema.columns
             WHERE table_schema = current_schema()
               AND table_name <> ALL(%s)
             ORDER BY table_name, ordinal_position
            """,
            (list(IGNORED_PARITY_TABLES),),
        )
        columns = {(row[0], row[1], row[2]) for row in cur.fetchall()}

        cur.execute(
            """
            SELECT tablename, indexname
              FROM pg_indexes
             WHERE schemaname = current_schema()
               AND tablename <> ALL(%s)
             ORDER BY tablename, indexname
            """,
            (list(IGNORED_PARITY_TABLES),),
        )
        indexes = {(row[0], row[1]) for row in cur.fetchall()}

    return {"tables": tables, "columns": columns, "indexes": indexes}


def _assert_snapshots_match(
    schema_sql_snapshot: dict[str, set[tuple[str, ...]]],
    alembic_snapshot: dict[str, set[tuple[str, ...]]],
) -> None:
    messages: list[str] = []
    for key in ("tables", "columns", "indexes"):
        only_schema = schema_sql_snapshot[key] - alembic_snapshot[key]
        only_alembic = alembic_snapshot[key] - schema_sql_snapshot[key]
        if only_schema:
            messages.append(f"{key} only in schema.sql bootstrap: {sorted(only_schema)}")
        if only_alembic:
            messages.append(f"{key} only in Alembic bootstrap: {sorted(only_alembic)}")
    assert not messages, "\n".join(messages)


def test_schema_sql_bootstraps_clean_postgres(pg_url, psycopg_module):
    """schema.sql must apply end-to-end and create all API/ETL-critical tables and matviews."""
    with psycopg_module.connect(pg_url, autocommit=False) as conn:
        _reset_public_schema(conn)
        _apply_schema_sql(conn)

        with conn.cursor() as cur:
            cur.execute("SELECT tablename FROM pg_tables WHERE schemaname = current_schema()")
            tables = {row[0] for row in cur.fetchall()}
            cur.execute("SELECT matviewname FROM pg_matviews WHERE schemaname = current_schema()")
            matviews = {row[0] for row in cur.fetchall()}

        missing_tables = EXPECTED_TABLES - tables
        assert not missing_tables, (
            f"schema.sql did not create expected tables: {missing_tables!r}\n"
            f"tables present: {sorted(tables)}"
        )
        missing_matviews = EXPECTED_MATVIEWS - matviews
        assert not missing_matviews, (
            f"schema.sql did not create expected matviews: {missing_matviews!r}\n"
            f"matviews present: {sorted(matviews)}"
        )


def test_schema_sql_and_alembic_postgres_parity(pg_url, psycopg_module, monkeypatch):
    """schema.sql and Alembic must produce the same Postgres structural contract."""
    monkeypatch.delenv("DB_URL", raising=False)
    schema_sql_schema = "mgr_schema_sql_parity"
    alembic_schema = "mgr_alembic_parity"

    with psycopg_module.connect(pg_url, autocommit=False) as conn:
        _reset_schema(conn, schema_sql_schema)
        _apply_schema_sql(conn)
        schema_sql_snapshot = _catalog_snapshot(conn)

        _reset_schema(conn, alembic_schema)

    command.upgrade(_alembic_config(_url_with_search_path(pg_url, alembic_schema)), "head")

    with psycopg_module.connect(pg_url, autocommit=False) as conn:
        with conn.cursor() as cur:
            cur.execute(f'SET search_path TO "{alembic_schema}"')
        alembic_snapshot = _catalog_snapshot(conn)
        _assert_snapshots_match(schema_sql_snapshot, alembic_snapshot)


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
