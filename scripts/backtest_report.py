#!/usr/bin/env python3
"""Print stored backtest run metrics (#1464).

Internal reporting surface. Per the #1464 owner decision the underlying free-source
prices are internal-use only, so this prints derived statistics and is deliberately
not wired into the public API.

Usage::

    python -m scripts.backtest_report --manager-id 1
    python -m scripts.backtest_report --strategy high_conviction_new_buys --json
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from adapters.base import connect_db  # noqa: E402
from etl.backtest_flow import query_backtest_runs  # noqa: E402


def _format_pct(value: Any) -> str:
    if value is None:
        return "n/a"
    return f"{float(value) * 100:.2f}%"


def _format_ratio(value: Any) -> str:
    if value is None:
        return "n/a"
    return f"{float(value):.3f}"


def render_rows(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "No backtest runs recorded."
    header = (
        f"{'run':>5}  {'strategy':<26} {'mgr':>5} {'periods':>7} {'pos':>5} "
        f"{'skip':>5} {'total':>9} {'annual':>9} {'sharpe':>7} {'hit':>7} {'excess':>9}"
    )
    lines = [header, "-" * len(header)]
    for row in rows:
        lines.append(
            f"{row['run_id']:>5}  {str(row['strategy'])[:26]:<26} "
            f"{'' if row['manager_id'] is None else row['manager_id']:>5} "
            f"{row['periods']:>7} {row['positions']:>5} {row['positions_skipped']:>5} "
            f"{_format_pct(row['total_return']):>9} "
            f"{_format_pct(row['annualized_return']):>9} "
            f"{_format_ratio(row['sharpe']):>7} "
            f"{_format_pct(row['hit_rate']):>7} "
            f"{_format_pct(row['excess_return']):>9}"
        )
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--strategy", default=None, help="filter by strategy name")
    parser.add_argument("--manager-id", type=int, default=None, help="filter by manager")
    parser.add_argument("--limit", type=int, default=20, help="maximum runs to show")
    parser.add_argument("--json", action="store_true", help="emit JSON instead of a table")
    args = parser.parse_args(argv)

    conn = connect_db()
    try:
        rows = query_backtest_runs(
            conn,
            strategy=args.strategy,
            manager_id=args.manager_id,
            limit=args.limit,
        )
    finally:
        conn.close()

    if args.json:
        print(json.dumps(rows, indent=2, default=str))
    else:
        print(render_rows(rows))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
