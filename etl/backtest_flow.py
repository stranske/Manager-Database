"""Signal-alpha backtest harness over point-in-time holdings (#1464 / design #1401).

Replays a strategy against ``holdings_as_of`` snapshots, enters each selected
position after a disclosure lag, and reports return / annualized / Sharpe /
hit-rate against a benchmark. Results land in ``backtest_runs`` and
``backtest_results``.

INTERNAL USE ONLY: prices come from a free provider under the #1464 owner
decision. Only derived statistics may leave this module.
"""

from __future__ import annotations

import json
import logging
import math
import sqlite3
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import Any

from adapters.base import get_placeholder, get_table_columns
from adapters.prices import PriceAdapter
from etl.point_in_time import holdings_as_of
from utils.numeric import finite_float_or_none

logger = logging.getLogger(__name__)

DEFAULT_STRATEGY = "high_conviction_new_buys"
DEFAULT_ENTRY_LAG_DAYS = 1
DEFAULT_HOLDING_PERIOD_DAYS = 91
DEFAULT_TOP_N = 10
DEFAULT_BENCHMARK = "SPY"
PERIODS_PER_YEAR = 4.0


def _is_missing_postgres_table_error(exc: Exception) -> bool:
    message = str(exc)
    return (
        "does not exist" in message
        or getattr(exc, "pgcode", None) == "42P01"
        or "UndefinedTable" in exc.__class__.__name__
    )


def ensure_backtest_tables(conn: Any) -> None:
    """Create backtest tables on SQLite; fail fast when Postgres has no schema."""
    if isinstance(conn, sqlite3.Connection):
        conn.execute("""CREATE TABLE IF NOT EXISTS backtest_runs (
                run_id INTEGER PRIMARY KEY,
                strategy TEXT NOT NULL,
                manager_id INTEGER,
                start_date DATE NOT NULL,
                end_date DATE NOT NULL,
                entry_lag_days INTEGER NOT NULL DEFAULT 0,
                holding_period_days INTEGER NOT NULL DEFAULT 91,
                benchmark_ticker TEXT,
                price_source TEXT,
                periods INTEGER NOT NULL DEFAULT 0,
                positions INTEGER NOT NULL DEFAULT 0,
                positions_skipped INTEGER NOT NULL DEFAULT 0,
                total_return REAL,
                annualized_return REAL,
                sharpe REAL,
                hit_rate REAL,
                benchmark_total_return REAL,
                excess_return REAL,
                params_json TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )""")
        conn.execute("""CREATE TABLE IF NOT EXISTS backtest_results (
                result_id INTEGER PRIMARY KEY,
                run_id INTEGER NOT NULL REFERENCES backtest_runs(run_id) ON DELETE CASCADE,
                decision_date DATE NOT NULL,
                entry_date DATE NOT NULL,
                exit_date DATE NOT NULL,
                ticker TEXT,
                cusip TEXT,
                entry_price REAL,
                exit_price REAL,
                position_return REAL,
                benchmark_return REAL,
                excess_return REAL,
                weight REAL,
                status TEXT NOT NULL DEFAULT 'filled',
                skip_reason TEXT
            )""")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_backtest_results_run ON backtest_results(run_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_backtest_runs_strategy "
            "ON backtest_runs(strategy, created_at)"
        )
        return

    for table in ("backtest_runs", "backtest_results"):
        try:
            conn.execute(f"SELECT 1 FROM {table} LIMIT 1")
        except Exception as exc:
            if _is_missing_postgres_table_error(exc):
                raise RuntimeError(
                    f"{table} table is missing on Postgres; apply schema migrations first"
                ) from exc
            raise


@dataclass
class Position:
    """One security selected by the strategy at a decision date."""

    decision_date: date
    entry_date: date
    exit_date: date
    ticker: str | None
    cusip: str | None
    weight: float | None = None
    entry_price: float | None = None
    exit_price: float | None = None
    position_return: float | None = None
    benchmark_return: float | None = None
    excess_return: float | None = None
    status: str = "filled"
    skip_reason: str | None = None


@dataclass
class BacktestReport:
    """Aggregate outcome of a backtest run."""

    strategy: str
    manager_id: int | None
    start_date: date
    end_date: date
    periods: int = 0
    period_returns: list[float] = field(default_factory=list)
    positions: list[Position] = field(default_factory=list)
    total_return: float | None = None
    annualized_return: float | None = None
    sharpe: float | None = None
    hit_rate: float | None = None
    benchmark_total_return: float | None = None
    excess_return: float | None = None
    run_id: int | None = None

    @property
    def filled(self) -> list[Position]:
        return [p for p in self.positions if p.status == "filled"]

    @property
    def skipped(self) -> list[Position]:
        return [p for p in self.positions if p.status != "filled"]


def _coerce_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if not value:
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _knowledge_time(row: dict[str, Any]) -> datetime | None:
    raw = row.get("knowledge_time")
    if not raw:
        return None
    if isinstance(raw, datetime):
        return raw if raw.tzinfo else raw.replace(tzinfo=UTC)
    try:
        parsed = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def decision_cutoff(decision_date: date) -> datetime:
    """End of the decision day in UTC.

    A decision dated ``D`` may act on everything disclosed during ``D``. Both the
    as-of query and the guard below use this single cutoff, so the harness cannot
    query one boundary and validate against another.
    """
    return datetime(
        decision_date.year, decision_date.month, decision_date.day, 23, 59, 59, tzinfo=UTC
    )


def enforce_no_lookahead(
    rows: Sequence[dict[str, Any]],
    decision_date: date,
) -> list[dict[str, Any]]:
    """Drop any holding that was not yet knowable on ``decision_date``.

    ``holdings_as_of`` already applies this bound in SQL. Re-checking it here keeps
    the guarantee local to the harness, so a future change that swaps in a
    current-holdings query cannot silently introduce look-ahead bias.
    """
    cutoff = decision_cutoff(decision_date)
    visible: list[dict[str, Any]] = []
    excluded = 0
    for row in rows:
        known_at = _knowledge_time(row)
        if known_at is None or known_at > cutoff:
            excluded += 1
            logger.warning(
                "Dropping look-ahead holding from decision set",
                extra={
                    "decision_date": decision_date.isoformat(),
                    "knowledge_time": known_at.isoformat() if known_at is not None else None,
                    "cusip": row.get("cusip"),
                    "reason": (
                        "unknown_knowledge_time" if known_at is None else "future_knowledge_time"
                    ),
                },
            )
            continue
        visible.append(row)
    if excluded:
        logger.warning(
            "Excluded holdings without decision-time knowledge",
            extra={"decision_date": decision_date.isoformat(), "excluded_count": excluded},
        )
    return visible


def _security_key(row: dict[str, Any]) -> str | None:
    ticker = row.get("resolved_ticker")
    if ticker:
        return str(ticker).strip().upper()
    cusip = row.get("cusip")
    return str(cusip).strip() if cusip else None


def _ticker_for(row: dict[str, Any]) -> str | None:
    ticker = row.get("resolved_ticker")
    return str(ticker).strip().upper() if ticker else None


def visible_holdings(conn: Any, manager_id: int, as_of: date) -> list[dict[str, Any]]:
    """Holdings knowable by the end of ``as_of``, with the look-ahead guard reapplied."""
    rows = holdings_as_of(conn, manager_id, decision_cutoff(as_of))
    return enforce_no_lookahead(rows, as_of)


def select_new_buys(
    conn: Any,
    manager_id: int,
    decision_date: date,
    prior_date: date | None,
    *,
    top_n: int = DEFAULT_TOP_N,
) -> list[dict[str, Any]]:
    """Securities newly disclosed at ``decision_date``, ranked by position size.

    "High-conviction new buys" = securities present in the as-of snapshot that were
    absent from the prior snapshot, largest reported value first.
    """
    current = visible_holdings(conn, manager_id, decision_date)
    if not current:
        return []
    prior_keys: set[str] = set()
    if prior_date is not None:
        for row in visible_holdings(conn, manager_id, prior_date):
            key = _security_key(row)
            if key:
                prior_keys.add(key)

    # An as-of snapshot can carry the same security from several reporting
    # periods; keep the largest disclosed position so one buy is one position.
    best_by_key: dict[str, dict[str, Any]] = {}
    for row in current:
        key = _security_key(row)
        if key is None or key in prior_keys:
            continue
        incumbent = best_by_key.get(key)
        if incumbent is None or (
            finite_float_or_none(row.get("value_usd"), min_value=0.0) or 0.0
        ) > (finite_float_or_none(incumbent.get("value_usd"), min_value=0.0) or 0.0):
            best_by_key[key] = row

    new_rows = list(best_by_key.values())
    new_rows.sort(
        key=lambda r: (
            -(finite_float_or_none(r.get("value_usd"), min_value=0.0) or 0.0),
            str(_security_key(r) or ""),
        )
    )
    return new_rows[: max(0, int(top_n))]


def derive_decision_dates(
    conn: Any,
    manager_id: int,
    start_date: date,
    end_date: date,
) -> list[date]:
    """Distinct filing disclosure dates in range, which drive the decision cadence."""
    if "filed_date" not in get_table_columns(conn, "filings"):
        return []
    ph = get_placeholder(conn)
    rows = conn.execute(
        "SELECT DISTINCT filed_date FROM filings "
        f"WHERE manager_id = {ph} AND filed_date IS NOT NULL "
        f"AND filed_date >= {ph} AND filed_date <= {ph} "
        "ORDER BY filed_date",
        (manager_id, start_date.isoformat(), end_date.isoformat()),
    ).fetchall()
    dates = [_coerce_date(row[0]) for row in rows]
    return [d for d in dates if d is not None]


def _sharpe(returns: Sequence[float], *, risk_free_period: float = 0.0) -> float | None:
    """Annualized Sharpe from per-period returns using the sample standard deviation."""
    if len(returns) < 2:
        return None
    excess = [r - risk_free_period for r in returns]
    mean = sum(excess) / len(excess)
    variance = sum((value - mean) ** 2 for value in excess) / (len(excess) - 1)
    stdev = math.sqrt(variance)
    if stdev == 0:
        return None
    return (mean / stdev) * math.sqrt(PERIODS_PER_YEAR)


def _compound(returns: Sequence[float]) -> float | None:
    if not returns:
        return None
    total = 1.0
    for value in returns:
        total *= 1.0 + value
    return total - 1.0


def _annualize(total_return: float | None, periods: int) -> float | None:
    if total_return is None or periods <= 0:
        return None
    growth = 1.0 + total_return
    if growth <= 0:
        return -1.0
    return growth ** (PERIODS_PER_YEAR / periods) - 1.0


def run_backtest(
    conn: Any,
    manager_id: int,
    start_date: date,
    end_date: date,
    *,
    price_adapter: PriceAdapter,
    strategy: str = DEFAULT_STRATEGY,
    decision_dates: Sequence[date] | None = None,
    entry_lag_days: int = DEFAULT_ENTRY_LAG_DAYS,
    holding_period_days: int = DEFAULT_HOLDING_PERIOD_DAYS,
    top_n: int = DEFAULT_TOP_N,
    benchmark_ticker: str | None = DEFAULT_BENCHMARK,
    persist: bool = True,
) -> BacktestReport:
    """Replay ``strategy`` over point-in-time holdings and report performance.

    Only holdings knowable at each decision date are eligible. A position whose
    entry or exit price is unavailable is recorded as skipped and excluded from the
    metrics rather than aborting the run.
    """
    report = BacktestReport(
        strategy=strategy,
        manager_id=manager_id,
        start_date=start_date,
        end_date=end_date,
    )

    dates = list(decision_dates) if decision_dates is not None else None
    if dates is None:
        dates = derive_decision_dates(conn, manager_id, start_date, end_date)
    dates = sorted({d for d in dates if start_date <= d <= end_date})

    period_returns: list[float] = []
    benchmark_returns: list[float] = []
    prior_date: date | None = None

    for decision_date in dates:
        selected = select_new_buys(conn, manager_id, decision_date, prior_date, top_n=top_n)
        prior_date = decision_date
        if not selected:
            continue

        entry_date = decision_date + timedelta(days=max(0, int(entry_lag_days)))
        exit_date = entry_date + timedelta(days=max(1, int(holding_period_days)))
        benchmark_return = _benchmark_return(price_adapter, benchmark_ticker, entry_date, exit_date)

        realized: list[float] = []
        for row in selected:
            position = _price_position(
                row,
                price_adapter,
                decision_date=decision_date,
                entry_date=entry_date,
                exit_date=exit_date,
                benchmark_return=benchmark_return,
            )
            report.positions.append(position)
            if position.position_return is not None:
                realized.append(position.position_return)

        # Excess return is meaningful only when the portfolio and benchmark
        # compound over the identical decision periods. Keep the individual
        # positions for auditability, but exclude an unbenchmarked period from
        # aggregate comparison metrics.
        if not realized or benchmark_return is None:
            continue
        period_returns.append(sum(realized) / len(realized))
        benchmark_returns.append(benchmark_return)

    report.periods = len(period_returns)
    report.period_returns = period_returns
    report.total_return = _compound(period_returns)
    report.annualized_return = _annualize(report.total_return, report.periods)
    report.sharpe = _sharpe(period_returns)
    report.benchmark_total_return = _compound(benchmark_returns) if benchmark_returns else None
    if report.total_return is not None and report.benchmark_total_return is not None:
        report.excess_return = report.total_return - report.benchmark_total_return

    filled = report.filled
    if filled:
        wins = sum(1 for p in filled if (p.position_return or 0.0) > 0)
        report.hit_rate = wins / len(filled)

    if persist:
        report.run_id = persist_backtest(
            conn,
            report,
            entry_lag_days=entry_lag_days,
            holding_period_days=holding_period_days,
            benchmark_ticker=benchmark_ticker,
            price_source=price_adapter.source,
            top_n=top_n,
        )
    return report


def _benchmark_return(
    price_adapter: PriceAdapter,
    benchmark_ticker: str | None,
    entry_date: date,
    exit_date: date,
) -> float | None:
    if not benchmark_ticker:
        return None
    entry = price_adapter.close_on_or_before(benchmark_ticker, entry_date)
    exit_price = price_adapter.close_on_or_before(benchmark_ticker, exit_date)
    if entry is None or exit_price is None or entry <= 0:
        logger.warning(
            "Benchmark price unavailable; period recorded without benchmark",
            extra={"benchmark": benchmark_ticker, "entry_date": entry_date.isoformat()},
        )
        return None
    return (exit_price - entry) / entry


def _price_position(
    row: dict[str, Any],
    price_adapter: PriceAdapter,
    *,
    decision_date: date,
    entry_date: date,
    exit_date: date,
    benchmark_return: float | None,
) -> Position:
    ticker = _ticker_for(row)
    cusip = str(row["cusip"]) if row.get("cusip") else None
    position = Position(
        decision_date=decision_date,
        entry_date=entry_date,
        exit_date=exit_date,
        ticker=ticker,
        cusip=cusip,
        weight=finite_float_or_none(row.get("value_usd"), min_value=0.0),
        benchmark_return=benchmark_return,
    )

    if not ticker:
        position.status = "skipped"
        position.skip_reason = "unresolved_ticker"
        logger.warning("Holding has no resolved ticker; skipping", extra={"cusip": cusip})
        return position

    entry_price = price_adapter.close_on_or_before(ticker, entry_date)
    exit_price = price_adapter.close_on_or_before(ticker, exit_date)
    if entry_price is None or exit_price is None or entry_price <= 0:
        position.status = "skipped"
        position.skip_reason = "missing_price"
        logger.warning(
            "Missing price for position; excluded from metrics",
            extra={
                "ticker": ticker,
                "entry_date": entry_date.isoformat(),
                "exit_date": exit_date.isoformat(),
            },
        )
        return position

    position.entry_price = entry_price
    position.exit_price = exit_price
    position.position_return = (exit_price - entry_price) / entry_price
    if benchmark_return is not None:
        position.excess_return = position.position_return - benchmark_return
    return position


def persist_backtest(
    conn: Any,
    report: BacktestReport,
    *,
    entry_lag_days: int,
    holding_period_days: int,
    benchmark_ticker: str | None,
    price_source: str,
    top_n: int,
) -> int | None:
    """Write the run header and per-position rows; returns the new ``run_id``."""
    ensure_backtest_tables(conn)
    ph = get_placeholder(conn)
    params_json = json.dumps(
        {"top_n": top_n, "periods_per_year": PERIODS_PER_YEAR},
        sort_keys=True,
    )
    columns = (
        "strategy, manager_id, start_date, end_date, entry_lag_days, holding_period_days, "
        "benchmark_ticker, price_source, periods, positions, positions_skipped, total_return, "
        "annualized_return, sharpe, hit_rate, benchmark_total_return, excess_return, params_json"
    )
    values = (
        report.strategy,
        report.manager_id,
        report.start_date.isoformat(),
        report.end_date.isoformat(),
        int(entry_lag_days),
        int(holding_period_days),
        benchmark_ticker,
        price_source,
        report.periods,
        len(report.filled),
        len(report.skipped),
        report.total_return,
        report.annualized_return,
        report.sharpe,
        report.hit_rate,
        report.benchmark_total_return,
        report.excess_return,
        params_json,
    )
    placeholders = ", ".join([ph] * len(values))
    insert_sql = f"INSERT INTO backtest_runs({columns}) VALUES ({placeholders})"

    def insert_rows() -> int | None:
        if isinstance(conn, sqlite3.Connection):
            cursor = conn.execute(insert_sql, values)
            run_id = int(cursor.lastrowid) if cursor.lastrowid is not None else None
        else:
            row = conn.execute(f"{insert_sql} RETURNING run_id", values).fetchone()
            run_id = int(row[0]) if row else None

        if run_id is not None:
            for position in report.positions:
                conn.execute(
                    "INSERT INTO backtest_results("
                    "run_id, decision_date, entry_date, exit_date, ticker, cusip, entry_price, "
                    "exit_price, position_return, benchmark_return, excess_return, weight, "
                    "status, skip_reason) "
                    f"VALUES ({', '.join([ph] * 14)})",
                    (
                        run_id,
                        position.decision_date.isoformat(),
                        position.entry_date.isoformat(),
                        position.exit_date.isoformat(),
                        position.ticker,
                        position.cusip,
                        position.entry_price,
                        position.exit_price,
                        position.position_return,
                        position.benchmark_return,
                        position.excess_return,
                        position.weight,
                        position.status,
                        position.skip_reason,
                    ),
                )
        return run_id

    if isinstance(conn, sqlite3.Connection):
        with conn:
            run_id = insert_rows()
    else:
        # connect_db() returns psycopg connections in autocommit mode. A
        # transaction context keeps a failed detail insert from orphaning its run.
        with conn.transaction():
            run_id = insert_rows()
    return run_id


def query_backtest_runs(
    conn: Any,
    *,
    strategy: str | None = None,
    manager_id: int | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    """Read-only accessor for stored run metrics (internal reporting surface)."""
    if not get_table_columns(conn, "backtest_runs"):
        return []
    ph = get_placeholder(conn)
    where: list[str] = []
    params: list[Any] = []
    if strategy:
        where.append(f"strategy = {ph}")
        params.append(strategy)
    if manager_id is not None:
        where.append(f"manager_id = {ph}")
        params.append(manager_id)
    where_sql = (" WHERE " + " AND ".join(where)) if where else ""
    params.append(max(1, int(limit)))
    rows = conn.execute(
        "SELECT run_id, strategy, manager_id, start_date, end_date, periods, positions, "
        "positions_skipped, total_return, annualized_return, sharpe, hit_rate, "
        "benchmark_total_return, excess_return, created_at "
        f"FROM backtest_runs{where_sql} ORDER BY run_id DESC LIMIT {ph}",
        tuple(params),
    ).fetchall()
    keys = (
        "run_id",
        "strategy",
        "manager_id",
        "start_date",
        "end_date",
        "periods",
        "positions",
        "positions_skipped",
        "total_return",
        "annualized_return",
        "sharpe",
        "hit_rate",
        "benchmark_total_return",
        "excess_return",
        "created_at",
    )
    return [dict(zip(keys, row, strict=False)) for row in rows]
