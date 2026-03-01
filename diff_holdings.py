"""Diff the latest two filings for a given CIK."""

from __future__ import annotations

import sqlite3
import sys

from adapters.base import connect_db


def _fetch_latest_sets(manager_id: int, conn):
    """Fetch holdings data keyed by CUSIP for the latest two filing dates."""
    placeholder = "?" if isinstance(conn, sqlite3.Connection) else "%s"
    cursor = conn.execute(
        f"""
        SELECT f.filed_date, h.cusip, h.shares, h.value_usd
        FROM holdings h
        JOIN filings f ON f.filing_id = h.filing_id
        JOIN managers m ON m.manager_id = f.manager_id
        WHERE m.manager_id = {placeholder}
        ORDER BY f.filed_date DESC
    """,
        (manager_id,),
    )

    grouped: dict[object, dict[str, dict[str, int | float | None]]] = {}
    ordered_dates: list[object] = []

    for filed_date, cusip, shares, value_usd in cursor:
        if filed_date not in grouped:
            if len(ordered_dates) == 2:
                break
            ordered_dates.append(filed_date)
            grouped[filed_date] = {}
        grouped[filed_date][cusip] = {"shares": shares, "value_usd": value_usd}

    if not ordered_dates:
        raise SystemExit("Manager not found")
    if len(ordered_dates) < 2:
        raise SystemExit("Need at least two filings")
    return grouped[ordered_dates[0]], grouped[ordered_dates[1]]


def _fetch_legacy_sets(cik: str, db_path: str):
    conn = connect_db(db_path)
    cur = conn.execute("SELECT filed, cusip FROM holdings WHERE cik=? ORDER BY filed DESC", (cik,))
    grouped: dict[str, set[str]] = {}
    row_count = 0
    for filed, cusip in cur:
        grouped.setdefault(filed, set()).add(cusip)
        row_count += 1
    conn.close()
    if row_count == 0:
        raise SystemExit("CIK not found")
    dates = sorted(grouped.keys(), reverse=True)[:2]
    if len(dates) < 2:
        raise SystemExit("Need at least two filings")
    return grouped[dates[0]], grouped[dates[1]]


def diff_holdings(cik: str, db_path: str = "dev.db"):
    current, prior = _fetch_legacy_sets(cik, db_path)
    additions = current - prior
    exits = prior - current
    return additions, exits


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: diff_holdings.py <CIK>")
        sys.exit(1)
    adds, exits = diff_holdings(sys.argv[1])
    print("Additions:", ", ".join(sorted(adds)))
    print("Exits:", ", ".join(sorted(exits)))
