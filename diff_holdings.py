"""Diff the latest two filings for a manager (or CIK via lookup)."""

from __future__ import annotations

import sqlite3
import sys
import time
from typing import Any

from adapters.base import connect_db, get_table_columns, resolve_manager_id_column
from tools.registry import run_contract_fields
from tools.run_contract import RunResult


def _placeholder(conn: Any) -> str:
    """Return the parameterised-query placeholder for the connection dialect."""
    return "?" if isinstance(conn, sqlite3.Connection) else "%s"


def _is_amendment(filing_type: Any) -> bool:
    return str(filing_type or "").strip().upper().endswith("/A")


def _sort_key(value: Any) -> str:
    return "" if value is None else str(value)


def _select_authoritative_filings(manager_id: int, conn: Any) -> list[tuple[Any, int]]:
    """Return authoritative filing IDs for the latest two reporting periods.

    Raw EDGAR rows remain queryable in ``filings``/``holdings``. This selector is
    the reconciliation boundary for downstream diffs: per manager-period, use the
    latest amendment when present, otherwise the latest original filing.
    """
    ph = _placeholder(conn)
    filing_columns = get_table_columns(conn, "filings")
    period_column = "period_end" if "period_end" in filing_columns else "filed_date"
    cursor = conn.execute(
        f"""
        SELECT f.filing_id, f.{period_column}, f.filed_date, f.type
        FROM filings f
        WHERE f.manager_id = {ph}
        """,
        (manager_id,),
    )

    by_period: dict[Any, tuple[int, Any, Any]] = {}
    for filing_id, period_key, filed_date, filing_type in cursor:
        period = period_key or filed_date
        if period is None:
            continue
        candidate = (int(filing_id), filed_date, filing_type)
        existing = by_period.get(period)
        if existing is None:
            by_period[period] = candidate
            continue
        existing_rank = (
            _is_amendment(existing[2]),
            _sort_key(existing[1]),
            existing[0],
        )
        candidate_rank = (
            _is_amendment(candidate[2]),
            _sort_key(candidate[1]),
            candidate[0],
        )
        if candidate_rank > existing_rank:
            by_period[period] = candidate

    return [
        (period, filing[0])
        for period, filing in sorted(
            by_period.items(),
            key=lambda item: (_sort_key(item[0]), _sort_key(item[1][1]), item[1][0]),
            reverse=True,
        )[:2]
    ]


def _fetch_holdings_for_filing(conn: Any, filing_id: int) -> dict[str, dict[str, Any]]:
    ph = _placeholder(conn)
    cursor = conn.execute(
        f"""
        SELECT h.cusip, h.shares, h.value_usd, h.name_of_issuer
        FROM holdings h
        WHERE h.filing_id = {ph}
        ORDER BY h.cusip
        """,
        (filing_id,),
    )
    return {
        cusip: {
            "shares": shares,
            "value_usd": value_usd,
            "name_of_issuer": name_of_issuer,
        }
        for cusip, shares, value_usd, name_of_issuer in cursor
    }


def _fetch_latest_sets(
    manager_id: int, conn: Any
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    """Fetch holdings keyed by CUSIP for the latest two authoritative periods.

    Returns ``(current, prior)`` where each is
    ``{cusip: {"shares": int|None, "value_usd": float|None, "name_of_issuer": str|None}}``.
    """
    authoritative = _select_authoritative_filings(manager_id, conn)
    if not authoritative:
        raise SystemExit("Manager not found or has no filings")
    if len(authoritative) < 2:
        raise SystemExit("Need at least two filings to compute a diff")
    current_filing_id = authoritative[0][1]
    prior_filing_id = authoritative[1][1]
    return _fetch_holdings_for_filing(conn, current_filing_id), _fetch_holdings_for_filing(
        conn, prior_filing_id
    )


def _resolve_manager_id(identifier: int | str, conn: Any) -> int:
    """Resolve a manager_id (int) or CIK string to a manager_id."""
    if isinstance(identifier, int):
        return identifier

    cik = identifier.strip()
    ph = _placeholder(conn)
    id_column = resolve_manager_id_column(conn)
    row = conn.execute(
        f"SELECT {id_column} FROM managers WHERE cik = {ph} LIMIT 1",
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


def diff_holdings(manager_id: int | str, conn: Any = None) -> RunResult:
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
    RunResult
        A replayable envelope whose ``outputs`` (also exposed as ``.deltas``) is
        the list of delta dicts, each with keys:
        cusip, name_of_issuer, delta_type, shares_prev, shares_curr,
        value_prev, value_curr.  ``inputs`` echoes the resolved ``manager_id``.
    """
    start = time.perf_counter()
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

    return RunResult(
        tool="diff_holdings",
        inputs={"manager_id": resolved},
        outputs=results,
        **run_contract_fields("diff_holdings"),
        provenance={"manager_id": resolved},
        latency_ms=int((time.perf_counter() - start) * 1000),
        status="success",
    )


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: diff_holdings.py <CIK_or_manager_id>")
        sys.exit(1)
    # Always pass as string — _resolve_manager_id handles CIK lookup
    # and numeric fallback without losing leading zeros.
    for row in diff_holdings(sys.argv[1]).deltas:
        print(
            f"{row['cusip']}: {row['delta_type']} "
            f"(shares {row['shares_prev']} -> {row['shares_curr']}, "
            f"value {row['value_prev']} -> {row['value_curr']})"
        )
