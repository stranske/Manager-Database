"""Diff the latest two filings for a given CIK."""

from __future__ import annotations

import sys
from adapters.base import connect_db


def _fetch_latest_sets(cik: str, db_path: str):
    conn = connect_db(db_path)
    cur = conn.execute(
        "SELECT filed, cusip FROM holdings WHERE cik=? ORDER BY filed DESC",
        (cik,),
    )
    rows = cur.fetchall()
    conn.close()
    if not rows:
        raise SystemExit("CIK not found")
    grouped = {}
    for filed, cusip in rows:
        grouped.setdefault(filed, set()).add(cusip)
    dates = sorted(grouped.keys(), reverse=True)[:2]
    if len(dates) < 2:
        raise SystemExit("Need at least two filings")
    return grouped[dates[0]], grouped[dates[1]]


def diff_holdings(cik: str, db_path: str = "dev.db"):
    current, prior = _fetch_latest_sets(cik, db_path)
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
