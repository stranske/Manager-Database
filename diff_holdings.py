"""Diff the latest two filings for a manager (or CIK via lookup)."""

from __future__ import annotations

import sqlite3
import sys
from typing import Any

from adapters.base import connect_db


def _placeholder(conn: Any) -> str:
    """Return the parameterised-query placeholder for the connection dialect."""
    return "?" if isinstance(conn, sqlite3.Connection) else "%s"


def _fetch_latest_sets(
    manager_id: int, conn: Any
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    """Fetch holdings keyed by CUSIP for the two most-recent filing dates.

    Returns ``(current, prior)`` where each is
    ``{cusip: {"shares": int|None, "value_usd": float|None, "name_of_issuer": str|None}}``.
    """
    ph = _placeholder(conn)
    cursor = conn.execute(
        f"""
        SELECT f.filed_date, h.cusip, h.shares, h.value_usd, h.name_of_issuer
        FROM holdings h
        JOIN filings f ON f.filing_id = h.filing_id
        WHERE f.manager_id = {ph}
        ORDER BY f.filed_date DESC
        """,
        (manager_id,),
    )

    grouped: dict[object, dict[str, dict[str, Any]]] = {}
    ordered_dates: list[object] = []

    for filed_date, cusip, shares, value_usd, name_of_issuer in cursor:
        if filed_date not in grouped:
            if len(ordered_dates) == 2:
                break
            ordered_dates.append(filed_date)
            grouped[filed_date] = {}
        grouped[filed_date][cusip] = {
            "shares": shares,
            "value_usd": value_usd,
            "name_of_issuer": name_of_issuer,
        }

    if not ordered_dates:
        raise SystemExit("Manager not found or has no filings")
    if len(ordered_dates) < 2:
        raise SystemExit("Need at least two filings to compute a diff")
    return grouped[ordered_dates[0]], grouped[ordered_dates[1]]


def _resolve_manager_id(identifier: int | str, conn: Any) -> int:
    """Resolve a manager_id (int) or CIK string to a manager_id."""
    if isinstance(identifier, int):
        return identifier

    cik = identifier.strip()
    ph = _placeholder(conn)
    row = conn.execute(
        f"SELECT manager_id FROM managers WHERE cik = {ph} LIMIT 1",
        (cik,),
    ).fetchone()
    if row is not None:
        return int(row[0])
    # If the string is purely digits, treat it as a numeric manager_id.
    if cik.isdigit():
        return int(cik)
    raise SystemExit(f"Manager not found for identifier: {cik}")


def _compare_optional(curr: int | float | None, prev: int | float | None) -> int | None:
    """Compare two nullable numbers: 1 if curr > prev, -1 if <, 0 if equal, None if null."""
    if curr is None or prev is None:
        return None
    if curr > prev:
        return 1
    if curr < prev:
        return -1
    return 0


def diff_holdings(manager_id: int | str, conn: Any = None) -> list[dict[str, Any]]:
    """Compute structured diffs between the two most-recent filings.

    Parameters
    ----------
    manager_id:
        An integer ``manager_id`` or a CIK string (looked up in ``managers``).
    conn:
        A database connection.  When ``None``, ``connect_db()`` is called.
        A ``str`` is accepted for backward compatibility (treated as a db path).

    Returns
    -------
    list of dicts, each with keys:
        cusip, name_of_issuer, delta_type, shares_prev, shares_curr,
        value_prev, value_curr
    """
    owns_connection = False
    if conn is None:
        conn = connect_db()
        owns_connection = True
    elif isinstance(conn, str):
        conn = connect_db(conn)
        owns_connection = True

    try:
        resolved = _resolve_manager_id(manager_id, conn)
        current, prior = _fetch_latest_sets(resolved, conn)
    finally:
        if owns_connection:
            conn.close()

    results: list[dict[str, Any]] = []
    for cusip in sorted(set(current) | set(prior)):
        prev = prior.get(cusip)
        curr = current.get(cusip)

        if prev is None and curr is not None:
            results.append(
                {
                    "cusip": cusip,
                    "name_of_issuer": curr.get("name_of_issuer"),
                    "delta_type": "ADD",
                    "shares_prev": None,
                    "shares_curr": curr["shares"],
                    "value_prev": None,
                    "value_curr": curr["value_usd"],
                }
            )
            continue

        if curr is None and prev is not None:
            results.append(
                {
                    "cusip": cusip,
                    "name_of_issuer": prev.get("name_of_issuer"),
                    "delta_type": "EXIT",
                    "shares_prev": prev["shares"],
                    "shares_curr": None,
                    "value_prev": prev["value_usd"],
                    "value_curr": None,
                }
            )
            continue

        if prev is None or curr is None:
            continue

        direction = _compare_optional(curr["shares"], prev["shares"])
        if direction in (None, 0):
            direction = _compare_optional(curr["value_usd"], prev["value_usd"])
        if direction is None or direction == 0:
            continue

        results.append(
            {
                "cusip": cusip,
                "name_of_issuer": curr.get("name_of_issuer") or prev.get("name_of_issuer"),
                "delta_type": "INCREASE" if direction > 0 else "DECREASE",
                "shares_prev": prev["shares"],
                "shares_curr": curr["shares"],
                "value_prev": prev["value_usd"],
                "value_curr": curr["value_usd"],
            }
        )

    return results


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: diff_holdings.py <CIK_or_manager_id>")
        sys.exit(1)
    # Always pass as string — _resolve_manager_id handles CIK lookup
    # and numeric fallback without losing leading zeros.
    for row in diff_holdings(sys.argv[1]):
        print(
            f"{row['cusip']}: {row['delta_type']} "
            f"(shares {row['shares_prev']} -> {row['shares_curr']}, "
            f"value {row['value_prev']} -> {row['value_curr']})"
        )
