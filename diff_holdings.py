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
    # Process rows one at a time to avoid loading entire dataset into memory
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
