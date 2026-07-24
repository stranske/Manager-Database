"""Compute deterministic manager similarity from latest disclosed holdings."""

from __future__ import annotations

import sqlite3
from itertools import combinations
from typing import Any

from adapters.base import get_placeholder, get_table_columns


def ensure_manager_similarity_table(conn: Any) -> None:
    if isinstance(conn, sqlite3.Connection):
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("""CREATE TABLE IF NOT EXISTS manager_similarity (
            manager_id_a INTEGER NOT NULL REFERENCES managers(id),
            manager_id_b INTEGER NOT NULL REFERENCES managers(id),
            jaccard REAL NOT NULL, overlap_count INTEGER NOT NULL, union_count INTEGER NOT NULL,
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


def compute_manager_similarity(conn: Any) -> int:
    """Replace pairwise similarities using each manager's latest filing."""
    ensure_manager_similarity_table(conn)
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
    for manager_id, security in rows:
        securities = holdings.setdefault(int(manager_id), set())
        if security is not None:
            securities.add(str(security))

    def replace_rows() -> int:
        conn.execute("DELETE FROM manager_similarity")
        ph = get_placeholder(conn)
        count = 0
        for left, right in combinations(sorted(holdings), 2):
            union = holdings[left] | holdings[right]
            overlap = holdings[left] & holdings[right]
            if not union:
                continue
            conn.execute(
                "INSERT INTO manager_similarity (manager_id_a, manager_id_b, jaccard, overlap_count, union_count) "
                f"VALUES ({ph}, {ph}, {ph}, {ph}, {ph})",
                (left, right, len(overlap) / len(union), len(overlap), len(union)),
            )
            count += 1
        return count

    if isinstance(conn, sqlite3.Connection):
        with conn:
            return replace_rows()
    with conn.transaction():
        return replace_rows()
