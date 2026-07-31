"""Position-level performance attribution over disclosed holdings (#1465 / design #1402).

For each disclosed position, compute the buy-and-hold return from the disclosure
date forward using ``holdings_as_of`` plumbing + the free-source price adapter.
Manager-level realized return and hit-rate are aggregates over filled positions.

INTERNAL USE ONLY for raw prices: the API surfaces derived returns, not closes.
"""

from __future__ import annotations

import logging
import sqlite3
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any

from adapters.base import get_placeholder, get_table_columns, is_sqlite
from adapters.prices import PriceAdapter
from etl.backtest_flow import decision_cutoff, enforce_no_lookahead, visible_holdings
from utils.numeric import finite_float_or_none

logger = logging.getLogger(__name__)


def _is_missing_postgres_table_error(exc: Exception) -> bool:
    message = str(exc)
    return (
        "does not exist" in message
        or getattr(exc, "pgcode", None) == "42P01"
        or "UndefinedTable" in exc.__class__.__name__
    )


def ensure_manager_attribution_table(conn: Any) -> None:
    """Create ``manager_attribution`` on SQLite; fail fast when Postgres has no schema."""
    if isinstance(conn, sqlite3.Connection):
        # Declared types mirror migration 019's SQLite rendering exactly; the schema
        # parity test compares PRAGMA table_info types, not just column names.
        conn.execute("""CREATE TABLE IF NOT EXISTS manager_attribution (
                attribution_id INTEGER PRIMARY KEY,
                manager_id BIGINT NOT NULL,
                filing_id BIGINT,
                disclosure_date DATE NOT NULL,
                as_of_date DATE NOT NULL,
                security_key TEXT NOT NULL,
                ticker TEXT,
                cusip TEXT,
                name_of_issuer TEXT,
                disclosure_price FLOAT,
                as_of_price FLOAT,
                position_return FLOAT,
                value_usd FLOAT,
                status TEXT NOT NULL DEFAULT 'filled',
                skip_reason TEXT,
                computed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (manager_id, filing_id, security_key, as_of_date)
            )""")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_manager_attribution_manager "
            "ON manager_attribution(manager_id, as_of_date)"
        )
        # NULLs are distinct under UNIQUE, so filing-less rows need a partial index to
        # stay dedupable. Keeps the runtime contract identical to migration 019.
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_manager_attribution_no_filing "
            "ON manager_attribution(manager_id, security_key, as_of_date) "
            "WHERE filing_id IS NULL"
        )
        return

    try:
        conn.execute("SELECT 1 FROM manager_attribution LIMIT 1")
    except Exception as exc:
        if _is_missing_postgres_table_error(exc):
            raise RuntimeError(
                "manager_attribution table is missing on Postgres; apply schema migrations first"
            ) from exc
        raise


@dataclass
class PositionAttribution:
    """One disclosed position's forward return since its disclosure date."""

    manager_id: int
    filing_id: int | None
    disclosure_date: date
    as_of_date: date
    security_key: str
    ticker: str | None
    cusip: str | None
    name_of_issuer: str | None
    value_usd: float | None = None
    disclosure_price: float | None = None
    as_of_price: float | None = None
    position_return: float | None = None
    status: str = "filled"
    skip_reason: str | None = None


@dataclass
class AttributionReport:
    """Position rows plus manager-level aggregates for one as-of window."""

    manager_id: int
    as_of_date: date
    positions: list[PositionAttribution] = field(default_factory=list)
    realized_return: float | None = None
    hit_rate: float | None = None

    @property
    def filled(self) -> list[PositionAttribution]:
        return [p for p in self.positions if p.status == "filled"]

    @property
    def skipped(self) -> list[PositionAttribution]:
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


def _security_key(row: dict[str, Any]) -> str | None:
    ticker = row.get("resolved_ticker")
    if ticker:
        return str(ticker).strip().upper()
    cusip = row.get("cusip")
    return str(cusip).strip() if cusip else None


def _ticker_for(row: dict[str, Any]) -> str | None:
    ticker = row.get("resolved_ticker")
    return str(ticker).strip().upper() if ticker else None


def derive_disclosure_dates(
    conn: Any,
    manager_id: int,
    *,
    start_date: date | None,
    end_date: date,
) -> list[date]:
    """Distinct filing disclosure dates that define attribution periods."""
    if "filed_date" not in get_table_columns(conn, "filings"):
        return []
    ph = get_placeholder(conn)
    where = [f"manager_id = {ph}", "filed_date IS NOT NULL", f"filed_date <= {ph}"]
    params: list[Any] = [manager_id, end_date.isoformat()]
    if start_date is not None:
        where.append(f"filed_date >= {ph}")
        params.append(start_date.isoformat())
    rows = conn.execute(
        f"SELECT DISTINCT filed_date FROM filings WHERE {' AND '.join(where)} ORDER BY filed_date",
        tuple(params),
    ).fetchall()
    dates = [_coerce_date(row[0]) for row in rows]
    return [d for d in dates if d is not None]


def _filing_id_for_disclosure(conn: Any, manager_id: int, disclosure_date: date) -> int | None:
    """Pick the manager's latest filing filed on ``disclosure_date``."""
    if "filed_date" not in get_table_columns(conn, "filings"):
        return None
    ph = get_placeholder(conn)
    row = conn.execute(
        "SELECT filing_id FROM filings "
        f"WHERE manager_id = {ph} AND filed_date = {ph} "
        "ORDER BY filing_id DESC LIMIT 1",
        (manager_id, disclosure_date.isoformat()),
    ).fetchone()
    if not row:
        return None
    return int(row[0])


def _holdings_for_disclosure(
    conn: Any,
    manager_id: int,
    disclosure_date: date,
) -> tuple[int | None, list[dict[str, Any]]]:
    """Return (filing_id, holdings) knowable at disclosure for that period.

    Uses ``visible_holdings`` (``holdings_as_of`` + the local look-ahead guard) so
    a future knowledge_time cannot leak into the disclosure-period decision set.
    Among the as-of snapshot, keep rows belonging to the disclosure filing when
    that filing is identifiable; otherwise keep the full visible set.

    An identified filing with no visible rows yields an empty list rather than the
    full snapshot: attributing other filings' holdings to this ``filing_id`` would
    persist — and surface through the API — a filing reference they do not belong to.
    """
    filing_id = _filing_id_for_disclosure(conn, manager_id, disclosure_date)
    visible = visible_holdings(conn, manager_id, disclosure_date)
    if filing_id is None:
        return None, visible
    return filing_id, [row for row in visible if int(row.get("filing_id") or -1) == filing_id]


def attribute_position(
    row: dict[str, Any],
    *,
    manager_id: int,
    filing_id: int | None,
    disclosure_date: date,
    as_of_date: date,
    price_adapter: PriceAdapter,
    entry_date: date | None = None,
) -> PositionAttribution:
    """Compute one position's forward return.

    ``entry_date`` defaults to ``disclosure_date``. The deliberate-break path
    passes an earlier entry to prove look-behind would corrupt the return.
    """
    start = entry_date if entry_date is not None else disclosure_date
    security_key = _security_key(row) or "UNKNOWN"
    ticker = _ticker_for(row)
    cusip = str(row["cusip"]) if row.get("cusip") else None
    position = PositionAttribution(
        manager_id=manager_id,
        filing_id=filing_id,
        disclosure_date=disclosure_date,
        as_of_date=as_of_date,
        security_key=security_key,
        ticker=ticker,
        cusip=cusip,
        name_of_issuer=str(row["name_of_issuer"]) if row.get("name_of_issuer") else None,
        value_usd=finite_float_or_none(row.get("value_usd"), min_value=0.0),
    )

    if start > as_of_date:
        position.status = "skipped"
        position.skip_reason = "entry_after_as_of"
        return position

    if not ticker:
        position.status = "skipped"
        position.skip_reason = "unresolved_ticker"
        logger.warning("Holding has no resolved ticker; skipping", extra={"cusip": cusip})
        return position

    # finite_float_or_none also rejects NaN/inf: NaN survives a `<= 0` guard and would
    # poison position_return, the aggregates, and finally JSON serialization.
    disclosure_price = finite_float_or_none(price_adapter.close_on_or_before(ticker, start))
    as_of_price = finite_float_or_none(price_adapter.close_on_or_before(ticker, as_of_date))
    if disclosure_price is None or as_of_price is None or disclosure_price <= 0:
        position.status = "skipped"
        position.skip_reason = "missing_price"
        logger.warning(
            "Missing price for attribution; excluded from metrics",
            extra={
                "ticker": ticker,
                "disclosure_date": disclosure_date.isoformat(),
                "as_of_date": as_of_date.isoformat(),
            },
        )
        return position

    position.disclosure_price = disclosure_price
    position.as_of_price = as_of_price
    position.position_return = (as_of_price - disclosure_price) / disclosure_price
    return position


def _aggregate(report: AttributionReport) -> None:
    """Fill the report's manager-level metrics from the single aggregation definition."""
    summary = summarize_manager_attribution(report.positions)
    report.realized_return = summary["realized_return"]
    report.hit_rate = summary["hit_rate"]


def run_attribution(
    conn: Any,
    manager_id: int,
    as_of_date: date,
    *,
    price_adapter: PriceAdapter,
    start_date: date | None = None,
    disclosure_dates: Sequence[date] | None = None,
    entry_date_offset_days: int = 0,
    persist: bool = True,
) -> AttributionReport:
    """Attribute disclosed positions from each disclosure date forward to ``as_of_date``.

    ``entry_date_offset_days`` is for the deliberate-break demonstration: a negative
    offset starts the return window before disclosure and must fail the hand-computed
    acceptance assertion. Production callers leave it at 0.
    """
    report = AttributionReport(manager_id=manager_id, as_of_date=as_of_date)
    dates = list(disclosure_dates) if disclosure_dates is not None else None
    if dates is None:
        dates = derive_disclosure_dates(
            conn, manager_id, start_date=start_date, end_date=as_of_date
        )
    dates = sorted(
        {d for d in dates if d <= as_of_date and (start_date is None or d >= start_date)}
    )

    # Deduplicate by security alone so a security is attributed once for this as-of
    # window. ``dates`` is ascending, so the earliest disclosure wins; keying by
    # (filing, security) would attribute the same security once per filing that
    # re-discloses it and double-count it in the manager aggregates.
    seen: set[str] = set()
    for disclosure_date in dates:
        filing_id, holdings = _holdings_for_disclosure(conn, manager_id, disclosure_date)
        entry_date = disclosure_date + timedelta(days=int(entry_date_offset_days))

        for row in holdings:
            key = _security_key(row)
            if key is None:
                key = "UNKNOWN"
            if key in seen:
                continue
            seen.add(key)
            report.positions.append(
                attribute_position(
                    row,
                    manager_id=manager_id,
                    filing_id=filing_id,
                    disclosure_date=disclosure_date,
                    as_of_date=as_of_date,
                    price_adapter=price_adapter,
                    entry_date=entry_date,
                )
            )

    _aggregate(report)
    if persist:
        persist_attribution(conn, report)
    return report


def persist_attribution(conn: Any, report: AttributionReport) -> int:
    """Upsert position rows for this manager/as-of window; returns rows written."""
    ensure_manager_attribution_table(conn)
    ph = get_placeholder(conn)
    written = 0

    def write_rows() -> int:
        count = 0
        excluded = "excluded" if is_sqlite(conn) else "EXCLUDED"
        update_clause = (
            "DO UPDATE SET "
            f"ticker={excluded}.ticker, cusip={excluded}.cusip, "
            f"name_of_issuer={excluded}.name_of_issuer, "
            f"disclosure_price={excluded}.disclosure_price, "
            f"as_of_price={excluded}.as_of_price, "
            f"position_return={excluded}.position_return, "
            f"value_usd={excluded}.value_usd, "
            f"status={excluded}.status, skip_reason={excluded}.skip_reason"
        )
        insert_clause = (
            "INSERT INTO manager_attribution("
            "manager_id, filing_id, disclosure_date, as_of_date, security_key, "
            "ticker, cusip, name_of_issuer, disclosure_price, as_of_price, "
            "position_return, value_usd, status, skip_reason) "
            f"VALUES ({', '.join([ph] * 14)}) "
        )
        # A NULL filing_id never matches the four-column unique constraint, so those
        # rows must target the partial index instead or every run re-inserts them.
        sql_with_filing = (
            f"{insert_clause}"
            f"ON CONFLICT(manager_id, filing_id, security_key, as_of_date) {update_clause}"
        )
        sql_without_filing = (
            f"{insert_clause}"
            "ON CONFLICT(manager_id, security_key, as_of_date) WHERE filing_id IS NULL "
            f"{update_clause}"
        )
        for position in report.positions:
            conn.execute(
                sql_with_filing if position.filing_id is not None else sql_without_filing,
                (
                    position.manager_id,
                    position.filing_id,
                    position.disclosure_date.isoformat(),
                    position.as_of_date.isoformat(),
                    position.security_key,
                    position.ticker,
                    position.cusip,
                    position.name_of_issuer,
                    position.disclosure_price,
                    position.as_of_price,
                    position.position_return,
                    position.value_usd,
                    position.status,
                    position.skip_reason,
                ),
            )
            count += 1
        return count

    if isinstance(conn, sqlite3.Connection):
        with conn:
            written = write_rows()
    else:
        with conn.transaction():
            written = write_rows()
    return written


def query_manager_attribution(
    conn: Any,
    manager_id: int,
    *,
    as_of_date: date | None = None,
    limit: int = 200,
) -> list[dict[str, Any]]:
    """Read-only position attribution rows (derived returns; no price redistribution)."""
    if not get_table_columns(conn, "manager_attribution"):
        return []
    ph = get_placeholder(conn)
    where = [f"manager_id = {ph}"]
    params: list[Any] = [manager_id]
    if as_of_date is not None:
        where.append(f"as_of_date = {ph}")
        params.append(as_of_date.isoformat())
    params.append(max(1, int(limit)))
    rows = conn.execute(
        "SELECT attribution_id, manager_id, filing_id, disclosure_date, as_of_date, "
        "security_key, ticker, cusip, name_of_issuer, position_return, value_usd, "
        "status, skip_reason, computed_at "
        f"FROM manager_attribution WHERE {' AND '.join(where)} "
        f"ORDER BY disclosure_date, security_key LIMIT {ph}",
        tuple(params),
    ).fetchall()
    keys = (
        "attribution_id",
        "manager_id",
        "filing_id",
        "disclosure_date",
        "as_of_date",
        "security_key",
        "ticker",
        "cusip",
        "name_of_issuer",
        "position_return",
        "value_usd",
        "status",
        "skip_reason",
        "computed_at",
    )
    return [dict(zip(keys, row, strict=False)) for row in rows]


def summarize_manager_attribution(positions: Sequence[PositionAttribution]) -> dict[str, Any]:
    """Manager-level realized return + hit-rate over filled positions."""
    filled = [p for p in positions if p.status == "filled" and p.position_return is not None]
    if not filled:
        return {
            "positions": 0,
            "positions_skipped": sum(1 for p in positions if p.status != "filled"),
            "realized_return": None,
            "hit_rate": None,
        }
    returns = [p.position_return for p in filled if p.position_return is not None]
    return {
        "positions": len(filled),
        "positions_skipped": sum(1 for p in positions if p.status != "filled"),
        "realized_return": sum(returns) / len(returns),
        "hit_rate": sum(1 for value in returns if value > 0) / len(returns),
    }


# Re-export the look-ahead helpers so tests can demonstrate the deliberate break
# against the same decision cutoff the production path uses.
__all__ = [
    "AttributionReport",
    "PositionAttribution",
    "attribute_position",
    "decision_cutoff",
    "enforce_no_lookahead",
    "ensure_manager_attribution_table",
    "persist_attribution",
    "query_manager_attribution",
    "run_attribution",
    "summarize_manager_attribution",
]
