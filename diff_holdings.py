"""Diff the latest two filings for a manager."""

from __future__ import annotations

import sqlite3
import sys
import argparse
from typing import Any

from adapters.base import connect_db


def _placeholder(conn: Any) -> str:
    return "?" if isinstance(conn, sqlite3.Connection) else "%s"


def _fetch_latest_sets(manager_id: int, conn):
    """Fetch holdings data keyed by CUSIP for the latest two filing dates."""
    placeholder = _placeholder(conn)
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


def _fetch_latest_sets_legacy(cik: str, conn):
    """Fallback for legacy flat SQLite holdings table."""
    placeholder = _placeholder(conn)
    cursor = conn.execute(
        f"""
        SELECT filed, cusip, sshPrnamt, value
        FROM holdings
        WHERE cik = {placeholder}
        ORDER BY filed DESC
    """,
        (cik,),
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


def _resolve_manager_id(manager_id_or_cik: int | str, conn) -> int:
    if isinstance(manager_id_or_cik, int):
        return manager_id_or_cik

    identifier = manager_id_or_cik.strip()
    placeholder = _placeholder(conn)
    row = conn.execute(
        f"SELECT manager_id FROM managers WHERE cik = {placeholder} LIMIT 1",
        (identifier,),
    ).fetchone()
    if row is not None:
        return int(row[0])
    if identifier.isdigit():
        return int(identifier)
    raise SystemExit("Manager not found")


def _compare_optional(curr: int | float | None, prev: int | float | None) -> int | None:
    if curr is None or prev is None:
        return None
    if curr > prev:
        return 1
    if curr < prev:
        return -1
    return 0


def diff_holdings(manager_id: int | str, conn=None) -> list[dict[str, int | float | str | None]]:
    owns_connection = False
    if conn is None:
        conn = connect_db("dev.db")
        owns_connection = True
    elif isinstance(conn, str):
        # Backward compatibility for older callers still providing a SQLite db_path.
        conn = connect_db(conn)
        owns_connection = True

    try:
        try:
            resolved_manager_id = _resolve_manager_id(manager_id, conn)
            current, prior = _fetch_latest_sets(resolved_manager_id, conn)
        except sqlite3.OperationalError as exc:
            # Legacy tests still use the original flat SQLite holdings schema.
            if "no such table: managers" not in str(exc):
                raise
            if not isinstance(manager_id, str):
                raise
            current, prior = _fetch_latest_sets_legacy(manager_id.strip(), conn)
    finally:
        if owns_connection:
            conn.close()

    results: list[dict[str, int | float | str | None]] = []
    for cusip in sorted(set(current) | set(prior)):
        prev = prior.get(cusip)
        curr = current.get(cusip)

        if prev is None and curr is not None:
            results.append(
                {
                    "cusip": cusip,
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
                "delta_type": "INCREASE" if direction > 0 else "DECREASE",
                "shares_prev": prev["shares"],
                "shares_curr": curr["shares"],
                "value_prev": prev["value_usd"],
                "value_curr": curr["value_usd"],
            }
        )
    return results


def _parse_cli_identifier(argv: list[str]) -> int | str:
    parser = argparse.ArgumentParser(description="Diff holdings by CIK or manager_id.")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--cik", dest="cik", help="Manager CIK value.")
    group.add_argument("--manager-id", dest="manager_id", type=int, help="Manager numeric ID.")
    parser.add_argument("identifier", nargs="?", help="CIK or manager_id.")
    args = parser.parse_args(argv)

    if args.manager_id is not None:
        return args.manager_id
    if args.cik is not None:
        return args.cik.strip()
    if args.identifier is None:
        parser.error("Provide either --cik, --manager-id, or positional identifier.")
    return args.identifier.strip()


if __name__ == "__main__":
    identifier = _parse_cli_identifier(sys.argv[1:])
    for row in diff_holdings(identifier):
        print(
            f"{row['cusip']}: {row['delta_type']} "
            f"(shares {row['shares_prev']} -> {row['shares_curr']}, "
            f"value {row['value_prev']} -> {row['value_curr']})"
        )
