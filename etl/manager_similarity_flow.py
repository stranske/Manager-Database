"""Compute deterministic manager similarity from latest disclosed holdings."""

from __future__ import annotations

import argparse
import json
import sqlite3
from collections.abc import Sequence
from itertools import combinations
from math import isfinite, sqrt
from typing import Any

from adapters.base import connect_db, get_placeholder, get_table_columns, table_exists
from embeddings import embed_text


def cosine_similarity(left: list[float], right: list[float]) -> float | None:
    """Return cosine similarity for two finite, equally-sized vectors."""
    if len(left) != len(right) or not left or not all(isfinite(value) for value in (*left, *right)):
        return None
    denominator = sqrt(sum(value * value for value in left)) * sqrt(
        sum(value * value for value in right)
    )
    if denominator == 0 or not isfinite(denominator):
        return None
    score = sum(a * b for a, b in zip(left, right, strict=True)) / denominator
    return score if isfinite(score) else None


def ensure_manager_similarity_table(conn: Any) -> None:
    """Ensure dialect-specific similarity storage without committing caller work."""
    if isinstance(conn, sqlite3.Connection):
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("""CREATE TABLE IF NOT EXISTS manager_similarity (
            manager_id_a INTEGER NOT NULL REFERENCES managers(id),
            manager_id_b INTEGER NOT NULL REFERENCES managers(id),
            jaccard REAL NOT NULL, cosine REAL, overlap_count INTEGER NOT NULL, union_count INTEGER NOT NULL,
            computed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (manager_id_a, manager_id_b),
            CHECK (manager_id_a < manager_id_b)
        )""")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_manager_similarity_a ON manager_similarity(manager_id_a)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_manager_similarity_b ON manager_similarity(manager_id_b)"
        )
        columns = get_table_columns(conn, "manager_similarity")
        if "cosine" not in columns:
            conn.execute("ALTER TABLE manager_similarity ADD COLUMN cosine REAL")
        return

    # This helper also runs on API reads. Even guarded ALTER TABLE takes a
    # schema lock, so inspect the resolved relation before issuing any DDL.
    table_exists, has_cosine, has_index_a, has_index_b = conn.execute("""
        SELECT pg_catalog.to_regclass('manager_similarity') IS NOT NULL,
            EXISTS (
                SELECT 1 FROM pg_catalog.pg_attribute
                WHERE attrelid = pg_catalog.to_regclass('manager_similarity')
                  AND attname = 'cosine' AND attnum > 0 AND NOT attisdropped
            ),
            EXISTS (
                SELECT 1 FROM pg_catalog.pg_index i
                JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid
                WHERE i.indrelid = pg_catalog.to_regclass('manager_similarity')
                  AND c.relname = 'idx_manager_similarity_a'
            ),
            EXISTS (
                SELECT 1 FROM pg_catalog.pg_index i
                JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid
                WHERE i.indrelid = pg_catalog.to_regclass('manager_similarity')
                  AND c.relname = 'idx_manager_similarity_b'
            )
    """).fetchone()
    # PostgreSQL's canonical manager key differs from SQLite's local schema.
    # FLOAT(24) is equivalent to the REAL columns in schema.sql and migrations.
    if not table_exists:
        conn.execute("""CREATE TABLE IF NOT EXISTS manager_similarity (
            manager_id_a BIGINT NOT NULL REFERENCES managers(manager_id),
            manager_id_b BIGINT NOT NULL REFERENCES managers(manager_id),
            jaccard FLOAT(24) NOT NULL, cosine FLOAT(24),
            overlap_count INTEGER NOT NULL, union_count INTEGER NOT NULL,
            computed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (manager_id_a, manager_id_b),
            CHECK (manager_id_a < manager_id_b)
        )""")
    if not has_index_a:
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_manager_similarity_a ON manager_similarity(manager_id_a)"
        )
    if not has_index_b:
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_manager_similarity_b ON manager_similarity(manager_id_b)"
        )
    if not has_cosine:
        conn.execute("ALTER TABLE manager_similarity ADD COLUMN IF NOT EXISTS cosine FLOAT(24)")


def refresh_manager_similarity(conn: Any) -> dict[str, Any]:
    """Replace pairwise similarities using each manager's latest filing and return a run summary."""
    ensure_manager_similarity_table(conn)
    if isinstance(conn, sqlite3.Connection) and not (
        table_exists(conn, "filings") and table_exists(conn, "holdings")
    ):
        rows: list[tuple[Any, Any]] = []
    else:
        current_holding_filter = (
            " AND h.superseded_at IS NULL"
            if "superseded_at" in get_table_columns(conn, "holdings")
            else ""
        )
        rows = conn.execute(
            """WITH ranked AS (
        SELECT f.manager_id, f.filing_id,
               ROW_NUMBER() OVER (PARTITION BY f.manager_id ORDER BY f.period_end DESC, f.filing_id DESC) AS rn
        FROM filings f
    )
    SELECT r.manager_id, COALESCE(NULLIF(h.resolved_ticker, ''), NULLIF(h.cusip, ''))
    FROM ranked r LEFT JOIN holdings h ON h.filing_id = r.filing_id"""
            + current_holding_filter
            + """
    WHERE r.rn = 1"""
        ).fetchall()
    holdings: dict[int, set[str]] = {}
    missing_security = 0
    for manager_id, security in rows:
        securities = holdings.setdefault(int(manager_id), set())
        if security is not None:
            securities.add(str(security))
        else:
            missing_security += 1
    vectors = {
        manager_id: embed_text(" ".join(sorted(securities)))
        for manager_id, securities in holdings.items()
    }

    def replace_rows() -> tuple[int, int, int]:
        if (
            not isinstance(conn, sqlite3.Connection)
            or table_exists(conn, "managers")
            or conn.execute("SELECT 1 FROM manager_similarity LIMIT 1").fetchone() is not None
        ):
            conn.execute("DELETE FROM manager_similarity")
        ph = get_placeholder(conn)
        pairs_computed = 0
        rows_written = 0
        empty_union = 0
        for left, right in combinations(sorted(holdings), 2):
            union = holdings[left] | holdings[right]
            overlap = holdings[left] & holdings[right]
            if not union:
                empty_union += 1
                continue
            pairs_computed += 1
            conn.execute(
                "INSERT INTO manager_similarity "
                "(manager_id_a, manager_id_b, jaccard, cosine, overlap_count, union_count) "
                f"VALUES ({ph}, {ph}, {ph}, {ph}, {ph}, {ph})",
                (
                    left,
                    right,
                    len(overlap) / len(union),
                    cosine_similarity(vectors[left], vectors[right]),
                    len(overlap),
                    len(union),
                ),
            )
            rows_written += 1
        return pairs_computed, rows_written, empty_union

    if isinstance(conn, sqlite3.Connection):
        with conn:
            pairs_computed, rows_written, empty_union = replace_rows()
    else:
        with conn.transaction():
            pairs_computed, rows_written, empty_union = replace_rows()

    skipped_by_reason: dict[str, int] = {}
    if missing_security:
        skipped_by_reason["missing_security_identifier"] = missing_security
    if empty_union:
        skipped_by_reason["empty_union"] = empty_union
    return {
        "rows_scanned": len(rows),
        "managers_scanned": len(holdings),
        "pairs_computed": pairs_computed,
        "rows_written": rows_written,
        "skipped_by_reason": skipped_by_reason,
        "status": "updated" if rows_written > 0 else "empty",
    }


def compute_manager_similarity(conn: Any) -> int:
    """Replace pairwise similarities using each manager's latest filing."""
    return int(refresh_manager_similarity(conn)["rows_written"])


def main(argv: Sequence[str] | None = None) -> int:
    """Run manager similarity refresh from the command line and print JSON summary."""
    parser = argparse.ArgumentParser(
        description="Refresh pairwise manager similarity from latest disclosed holdings."
    )
    parser.add_argument(
        "--db-path",
        default=None,
        help="Optional SQLite database path override when DB_URL is unset.",
    )
    args = parser.parse_args(list(argv) if argv is not None else None)
    conn = connect_db(args.db_path) if args.db_path is not None else connect_db()
    try:
        summary = refresh_manager_similarity(conn)
    finally:
        conn.close()
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
