"""Prefect flow for conviction score computation and market signal detection."""

from __future__ import annotations

import datetime as dt
import json
import logging
import os
import sqlite3
from typing import Any, TypedDict

from prefect import flow, task
from prefect.schedules import Cron

from adapters.base import connect_db
from etl.logging_setup import configure_logging

configure_logging("conviction_flow")
logger = logging.getLogger(__name__)


class _DailyDiffEntry(TypedDict):
    manager_id: int
    cusip: str
    name_of_issuer: str | None
    direction: str
    shares_prev: int | None
    shares_curr: int | None
    value_prev: float | None
    value_curr: float | None


def _placeholder(conn: Any) -> str:
    return "?" if isinstance(conn, sqlite3.Connection) else "%s"


def _ensure_conviction_scores_table(conn: Any) -> None:
    """Create conviction_scores on SQLite; fail fast on missing Postgres schema."""
    if isinstance(conn, sqlite3.Connection):
        conn.execute("""CREATE TABLE IF NOT EXISTS conviction_scores (
                score_id INTEGER PRIMARY KEY AUTOINCREMENT,
                manager_id INTEGER NOT NULL,
                filing_id INTEGER NOT NULL,
                cusip TEXT NOT NULL,
                name_of_issuer TEXT,
                shares INTEGER,
                value_usd REAL,
                conviction_pct REAL,
                portfolio_weight REAL,
                computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (filing_id, cusip)
            )""")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_conviction_manager ON conviction_scores(manager_id)"
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_conviction_cusip ON conviction_scores(cusip)")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_conviction_pct ON conviction_scores(conviction_pct DESC)"
        )
        return

    try:
        conn.execute("SELECT 1 FROM conviction_scores LIMIT 1")
    except Exception as exc:
        message = str(exc)
        exc_name = exc.__class__.__name__
        pgcode = getattr(exc, "pgcode", None)
        missing_table = (
            "does not exist" in message or pgcode == "42P01" or "UndefinedTable" in exc_name
        )
        if missing_table:
            raise RuntimeError(
                "conviction_scores table is missing on Postgres; apply schema migrations first"
            ) from exc
        raise


def _ensure_api_usage_table(conn: Any) -> None:
    if isinstance(conn, sqlite3.Connection):
        conn.execute("""CREATE TABLE IF NOT EXISTS api_usage (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                source TEXT,
                endpoint TEXT,
                status INT,
                bytes INT,
                latency_ms INT,
                cost_usd REAL
            )""")
        return
    conn.execute("SELECT 1 FROM api_usage LIMIT 1")


def _record_flow_usage(
    conn: Any,
    *,
    status: int,
    scores_computed: int,
    latency_ms: int,
) -> None:
    ph = _placeholder(conn)
    conn.execute(
        "INSERT INTO api_usage(source, endpoint, status, bytes, latency_ms, cost_usd) "
        f"VALUES ({ph}, {ph}, {ph}, {ph}, {ph}, {ph})",
        ("prefect", "flow:conviction-scoring", status, scores_computed, latency_ms, 0.0),
    )


@task
def compute_conviction_scores(filing_id: int, conn: Any) -> int:
    """Compute and upsert conviction scores for one filing."""
    ph = _placeholder(conn)

    manager_row = conn.execute(
        f"SELECT manager_id FROM filings WHERE filing_id = {ph}",
        (filing_id,),
    ).fetchone()
    if not manager_row:
        logger.warning(
            "Filing not found; skipping score computation", extra={"filing_id": filing_id}
        )
        return 0

    manager_id = int(manager_row[0])
    rows = conn.execute(
        f"SELECT cusip, name_of_issuer, shares, value_usd FROM holdings WHERE filing_id = {ph}",
        (filing_id,),
    ).fetchall()

    if not rows:
        logger.info("No holdings found for filing", extra={"filing_id": filing_id})
        return 0

    total_value = sum(float(row[3] or 0.0) for row in rows)

    upsert_sql = (
        "INSERT INTO conviction_scores "
        "(manager_id, filing_id, cusip, name_of_issuer, shares, value_usd, "
        "conviction_pct, portfolio_weight) "
        f"VALUES ({ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}) "
        "ON CONFLICT(filing_id, cusip) DO UPDATE SET "
        "manager_id = excluded.manager_id, "
        "name_of_issuer = excluded.name_of_issuer, "
        "shares = excluded.shares, "
        "value_usd = excluded.value_usd, "
        "conviction_pct = excluded.conviction_pct, "
        "portfolio_weight = excluded.portfolio_weight, "
        "computed_at = CURRENT_TIMESTAMP"
    )

    for cusip, issuer, shares, value_usd in rows:
        numeric_value = float(value_usd or 0.0)
        if total_value > 0:
            portfolio_weight = numeric_value / total_value
            conviction_pct = portfolio_weight * 100.0
        else:
            portfolio_weight = 0.0
            conviction_pct = 0.0

        conn.execute(
            upsert_sql,
            (
                manager_id,
                filing_id,
                cusip,
                issuer,
                shares,
                numeric_value,
                conviction_pct,
                portfolio_weight,
            ),
        )

    logger.info(
        "Computed conviction scores",
        extra={
            "filing_id": filing_id,
            "manager_id": manager_id,
            "holdings": len(rows),
            "total_value_usd": total_value,
        },
    )
    return len(rows)


@task
def score_all_latest_filings(conn: Any) -> dict[str, int]:
    """Score latest filing per manager."""
    rows = conn.execute("""SELECT filing_id
        FROM (
            SELECT
                f.manager_id,
                f.filing_id,
                ROW_NUMBER() OVER (
                    PARTITION BY f.manager_id
                    ORDER BY (f.filed_date IS NULL), f.filed_date DESC, f.filing_id DESC
                ) AS rank_in_manager
            FROM filings f
            JOIN managers m ON m.manager_id = f.manager_id
        ) ranked
        WHERE rank_in_manager = 1
        ORDER BY filing_id""").fetchall()

    if not rows:
        logger.warning("No latest filings found for conviction scoring")
        return {"filings_scored": 0, "scores_computed": 0}

    total_scores = 0
    for (filing_id,) in rows:
        total_scores += compute_conviction_scores.fn(int(filing_id), conn)

    return {"filings_scored": len(rows), "scores_computed": total_scores}


def _resolve_crowded_trade_min_managers(default: int = 3) -> int:
    raw = os.getenv("CROWDED_TRADE_MIN_MANAGERS")
    if raw is None or raw.strip() == "":
        return default
    try:
        parsed = int(raw)
    except ValueError:
        logger.warning(
            "Invalid CROWDED_TRADE_MIN_MANAGERS value; using default",
            extra={"raw_value": raw, "default": default},
        )
        return default
    return max(1, parsed)


def _ensure_crowded_trades_table(conn: Any) -> None:
    if not isinstance(conn, sqlite3.Connection):
        return
    conn.execute("""CREATE TABLE IF NOT EXISTS crowded_trades (
            crowd_id INTEGER PRIMARY KEY AUTOINCREMENT,
            cusip TEXT NOT NULL,
            name_of_issuer TEXT,
            manager_count INTEGER NOT NULL,
            manager_ids TEXT NOT NULL,
            total_value_usd REAL,
            avg_conviction_pct REAL,
            max_conviction_pct REAL,
            report_date TEXT NOT NULL,
            computed_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (cusip, report_date)
        )""")


def _ensure_contrarian_signals_table(conn: Any) -> None:
    if not isinstance(conn, sqlite3.Connection):
        return
    conn.execute("""CREATE TABLE IF NOT EXISTS contrarian_signals (
            signal_id INTEGER PRIMARY KEY AUTOINCREMENT,
            manager_id INTEGER NOT NULL,
            cusip TEXT NOT NULL,
            name_of_issuer TEXT,
            direction TEXT NOT NULL CHECK (direction IN ('BUY', 'SELL', 'INCREASE', 'DECREASE')),
            consensus_direction TEXT NOT NULL CHECK (
                consensus_direction IN ('BUY', 'SELL', 'INCREASE', 'DECREASE', 'HOLD')
            ),
            manager_delta_shares INTEGER,
            manager_delta_value REAL,
            consensus_count INTEGER,
            report_date TEXT NOT NULL,
            detected_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (manager_id, cusip, report_date)
        )""")


def _fetch_latest_conviction_rows(
    conn: Any,
    report_date: str,
) -> list[tuple[int, str, str | None, float, float | None]]:
    ph = _placeholder(conn)
    latest_sql = f"""
        WITH ranked_filings AS (
            SELECT
                filing_id,
                manager_id,
                ROW_NUMBER() OVER (
                    PARTITION BY manager_id
                    ORDER BY COALESCE(filed_date, period_end) DESC, filing_id DESC
                ) AS rn
            FROM filings
            WHERE COALESCE(filed_date, period_end) <= {ph}
        ),
        manager_positions AS (
            SELECT
                rf.manager_id,
                h.cusip,
                MAX(h.name_of_issuer) AS name_of_issuer,
                SUM(COALESCE(h.value_usd, 0)) AS value_usd
            FROM ranked_filings rf
            JOIN holdings h ON h.filing_id = rf.filing_id
            WHERE rf.rn = 1
              AND h.cusip IS NOT NULL
            GROUP BY rf.manager_id, h.cusip
        ),
        manager_totals AS (
            SELECT manager_id, SUM(value_usd) AS total_value_usd
            FROM manager_positions
            GROUP BY manager_id
        )
        SELECT
            p.manager_id,
            p.cusip,
            p.name_of_issuer,
            p.value_usd,
            CASE
                WHEN t.total_value_usd > 0 THEN (p.value_usd / t.total_value_usd) * 100.0
                ELSE NULL
            END AS conviction_pct
        FROM manager_positions p
        JOIN manager_totals t ON t.manager_id = p.manager_id
        """
    fallback_sql = f"""
        WITH ranked_filings AS (
            SELECT
                filing_id,
                manager_id,
                ROW_NUMBER() OVER (
                    PARTITION BY manager_id
                    ORDER BY filed_date DESC, filing_id DESC
                ) AS rn
            FROM filings
            WHERE filed_date <= {ph}
        ),
        manager_positions AS (
            SELECT
                rf.manager_id,
                h.cusip,
                MAX(h.name_of_issuer) AS name_of_issuer,
                SUM(COALESCE(h.value_usd, 0)) AS value_usd
            FROM ranked_filings rf
            JOIN holdings h ON h.filing_id = rf.filing_id
            WHERE rf.rn = 1
              AND h.cusip IS NOT NULL
            GROUP BY rf.manager_id, h.cusip
        ),
        manager_totals AS (
            SELECT manager_id, SUM(value_usd) AS total_value_usd
            FROM manager_positions
            GROUP BY manager_id
        )
        SELECT
            p.manager_id,
            p.cusip,
            p.name_of_issuer,
            p.value_usd,
            CASE
                WHEN t.total_value_usd > 0 THEN (p.value_usd / t.total_value_usd) * 100.0
                ELSE NULL
            END AS conviction_pct
        FROM manager_positions p
        JOIN manager_totals t ON t.manager_id = p.manager_id
        """
    try:
        rows = conn.execute(latest_sql, (report_date,)).fetchall()
    except sqlite3.OperationalError as exc:
        if "no such column: period_end" not in str(exc):
            raise
        rows = conn.execute(fallback_sql, (report_date,)).fetchall()
    return [
        (
            int(row[0]),
            str(row[1]),
            str(row[2]) if row[2] is not None else None,
            float(row[3]),
            float(row[4]) if row[4] is not None else None,
        )
        for row in rows
    ]


def _map_delta_direction(delta_type: str) -> str | None:
    mapping = {
        "ADD": "BUY",
        "EXIT": "SELL",
        "INCREASE": "INCREASE",
        "DECREASE": "DECREASE",
        "BUY": "BUY",
        "SELL": "SELL",
    }
    return mapping.get(delta_type.upper())


def _compute_delta_value(
    prev_value: float | None,
    curr_value: float | None,
) -> float | None:
    if curr_value is not None and prev_value is not None:
        return curr_value - prev_value
    if curr_value is not None:
        return curr_value
    if prev_value is not None:
        return -prev_value
    return None


def _compute_delta_shares(
    prev_shares: int | None,
    curr_shares: int | None,
) -> int | None:
    if curr_shares is not None and prev_shares is not None:
        return curr_shares - prev_shares
    if curr_shares is not None:
        return curr_shares
    if prev_shares is not None:
        return -prev_shares
    return None


@task
def detect_crowded_trades(
    report_date: str,
    min_managers: int | None = None,
    conn: Any | None = None,
) -> int:
    """Detect and upsert crowded trades for a report date.

    A crowded trade is a CUSIP held in latest filings by at least ``min_managers`` managers.
    """
    threshold = (
        _resolve_crowded_trade_min_managers() if min_managers is None else max(1, min_managers)
    )
    owned_conn = conn is None
    db = conn or connect_db()

    try:
        _ensure_crowded_trades_table(db)
        latest_rows = _fetch_latest_conviction_rows(db, report_date)

        grouped: dict[str, dict[str, Any]] = {}
        for manager_id, cusip, issuer, value_usd, conviction_pct in latest_rows:
            bucket = grouped.setdefault(
                cusip,
                {
                    "cusip": cusip,
                    "name_of_issuer": issuer,
                    "manager_ids": set(),
                    "total_value_usd": 0.0,
                    "convictions": [],
                },
            )
            bucket["manager_ids"].add(manager_id)
            if bucket.get("name_of_issuer") is None and issuer is not None:
                bucket["name_of_issuer"] = issuer
            bucket["total_value_usd"] += value_usd
            if conviction_pct is not None:
                bucket["convictions"].append(conviction_pct)

        crowded_rows = []
        for entry in grouped.values():
            manager_ids = sorted(entry["manager_ids"])
            manager_count = len(manager_ids)
            if manager_count < threshold:
                continue
            convictions = entry["convictions"]
            avg_conviction = sum(convictions) / len(convictions) if convictions else None
            max_conviction = max(convictions) if convictions else None
            crowded_rows.append(
                {
                    "cusip": entry["cusip"],
                    "name_of_issuer": entry["name_of_issuer"],
                    "manager_count": manager_count,
                    "manager_ids": manager_ids,
                    "total_value_usd": entry["total_value_usd"],
                    "avg_conviction_pct": avg_conviction,
                    "max_conviction_pct": max_conviction,
                }
            )

        ph = _placeholder(db)
        db.execute(f"DELETE FROM crowded_trades WHERE report_date = {ph}", (report_date,))

        upsert_sql = (
            "INSERT INTO crowded_trades "
            "(cusip, name_of_issuer, manager_count, manager_ids, total_value_usd, "
            "avg_conviction_pct, max_conviction_pct, report_date) "
            f"VALUES ({ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}) "
            "ON CONFLICT(cusip, report_date) DO UPDATE SET "
            "name_of_issuer = excluded.name_of_issuer, "
            "manager_count = excluded.manager_count, "
            "manager_ids = excluded.manager_ids, "
            "total_value_usd = excluded.total_value_usd, "
            "avg_conviction_pct = excluded.avg_conviction_pct, "
            "max_conviction_pct = excluded.max_conviction_pct, "
            "computed_at = CURRENT_TIMESTAMP"
        )

        for row in crowded_rows:
            manager_ids_value: Any
            if isinstance(db, sqlite3.Connection):
                manager_ids_value = json.dumps(row["manager_ids"])
            else:
                manager_ids_value = row["manager_ids"]
            db.execute(
                upsert_sql,
                (
                    row["cusip"],
                    row["name_of_issuer"],
                    row["manager_count"],
                    manager_ids_value,
                    row["total_value_usd"],
                    row["avg_conviction_pct"],
                    row["max_conviction_pct"],
                    report_date,
                ),
            )

        if isinstance(db, sqlite3.Connection):
            db.commit()

        logger.info(
            "Crowded trade detection finished",
            extra={
                "report_date": report_date,
                "threshold": threshold,
                "detected": len(crowded_rows),
            },
        )
        return len(crowded_rows)
    finally:
        if owned_conn:
            db.close()


@flow
def score_conviction_positions(
    report_date: str,
    conn: Any | None = None,
) -> int:
    """Compute conviction inputs for a report date."""
    owned_conn = conn is None
    db = conn or connect_db()
    try:
        latest_rows = _fetch_latest_conviction_rows(db, report_date)
        return len(latest_rows)
    finally:
        if owned_conn:
            db.close()


@task
def detect_contrarian_signals(
    report_date: str,
    conn: Any | None = None,
) -> int:
    owned_conn = conn is None
    db = conn or connect_db()

    try:
        _ensure_contrarian_signals_table(db)
        ph = _placeholder(db)
        daily_rows = db.execute(
            f"""
            SELECT manager_id, cusip, name_of_issuer, delta_type, shares_prev, shares_curr,
                   value_prev, value_curr
            FROM daily_diffs
            WHERE report_date = {ph}
              AND cusip IS NOT NULL
            """,
            (report_date,),
        ).fetchall()

        grouped: dict[str, list[_DailyDiffEntry]] = {}
        for row in daily_rows:
            direction = _map_delta_direction(str(row[3]))
            if direction is None:
                continue
            entry: _DailyDiffEntry = {
                "manager_id": int(row[0]),
                "cusip": str(row[1]),
                "name_of_issuer": str(row[2]) if row[2] is not None else None,
                "direction": direction,
                "shares_prev": int(row[4]) if row[4] is not None else None,
                "shares_curr": int(row[5]) if row[5] is not None else None,
                "value_prev": float(row[6]) if row[6] is not None else None,
                "value_curr": float(row[7]) if row[7] is not None else None,
            }
            grouped.setdefault(entry["cusip"], []).append(entry)

        db.execute(f"DELETE FROM contrarian_signals WHERE report_date = {ph}", (report_date,))

        upsert_sql = (
            "INSERT INTO contrarian_signals "
            "(manager_id, cusip, name_of_issuer, direction, consensus_direction, "
            "manager_delta_shares, manager_delta_value, consensus_count, report_date) "
            f"VALUES ({ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}) "
            "ON CONFLICT(manager_id, cusip, report_date) DO UPDATE SET "
            "name_of_issuer = excluded.name_of_issuer, "
            "direction = excluded.direction, "
            "consensus_direction = excluded.consensus_direction, "
            "manager_delta_shares = excluded.manager_delta_shares, "
            "manager_delta_value = excluded.manager_delta_value, "
            "consensus_count = excluded.consensus_count, "
            "detected_at = CURRENT_TIMESTAMP"
        )

        inserted = 0
        for cusip_entries in grouped.values():
            positive = [e for e in cusip_entries if e["direction"] in ("BUY", "INCREASE")]
            negative = [e for e in cusip_entries if e["direction"] in ("SELL", "DECREASE")]
            total_directional = len(positive) + len(negative)
            if total_directional == 0:
                continue

            positive_ratio = len(positive) / total_directional
            negative_ratio = len(negative) / total_directional
            if positive_ratio >= 0.6:
                consensus_side = "positive"
                consensus_count = len(positive)
            elif negative_ratio >= 0.6:
                consensus_side = "negative"
                consensus_count = len(negative)
            else:
                continue

            if consensus_count < 3:
                continue

            if consensus_side == "positive":
                consensus_direction = (
                    "BUY"
                    if sum(1 for e in positive if e["direction"] == "BUY")
                    >= sum(1 for e in positive if e["direction"] == "INCREASE")
                    else "INCREASE"
                )
                contrarians = negative
            else:
                consensus_direction = (
                    "SELL"
                    if sum(1 for e in negative if e["direction"] == "SELL")
                    >= sum(1 for e in negative if e["direction"] == "DECREASE")
                    else "DECREASE"
                )
                contrarians = positive

            for entry in contrarians:
                delta_shares = _compute_delta_shares(entry["shares_prev"], entry["shares_curr"])
                delta_value = _compute_delta_value(entry["value_prev"], entry["value_curr"])
                db.execute(
                    upsert_sql,
                    (
                        entry["manager_id"],
                        entry["cusip"],
                        entry["name_of_issuer"],
                        entry["direction"],
                        consensus_direction,
                        delta_shares,
                        delta_value,
                        consensus_count,
                        report_date,
                    ),
                )
                inserted += 1

        if isinstance(db, sqlite3.Connection):
            db.commit()

        logger.info(
            "Contrarian signal detection finished",
            extra={"report_date": report_date, "detected": inserted},
        )
        return inserted
    finally:
        if owned_conn:
            db.close()


@task
def dispatch_conviction_alerts(
    report_date: str,
    crowded_trades: int,
    contrarian_signals: int,
) -> int:
    """Dispatch conviction alerts after signal generation."""
    total_alerts = crowded_trades + contrarian_signals
    logger.info(
        "Conviction alerts dispatched",
        extra={
            "report_date": report_date,
            "crowded_trades": crowded_trades,
            "contrarian_signals": contrarian_signals,
            "total_alerts": total_alerts,
        },
    )
    return total_alerts


@flow
def conviction_flow(
    report_date: str | None = None,
    min_managers: int | None = None,
) -> dict[str, int]:
    """Run nightly conviction pipeline: scoring, signals, then alerts."""
    if report_date is None and min_managers is None:
        started_at = dt.datetime.now(dt.UTC)
        db = connect_db()
        status = 200
        scores_computed = 0
        try:
            _ensure_conviction_scores_table(db)
            _ensure_api_usage_table(db)
            summary = score_all_latest_filings.fn(db)
            scores_computed = summary["scores_computed"]
            return summary
        except Exception:
            status = 500
            raise
        finally:
            elapsed_ms = int((dt.datetime.now(dt.UTC) - started_at).total_seconds() * 1000)
            try:
                _record_flow_usage(
                    db,
                    status=status,
                    scores_computed=scores_computed,
                    latency_ms=max(elapsed_ms, 0),
                )
                if isinstance(db, sqlite3.Connection):
                    db.commit()
            finally:
                db.close()

    resolved_date = report_date or str(dt.date.today() - dt.timedelta(days=1))
    scored = score_conviction_positions.fn(resolved_date)
    crowded = detect_crowded_trades.fn(resolved_date, min_managers=min_managers)
    contrarian = detect_contrarian_signals.fn(resolved_date)
    alerts = dispatch_conviction_alerts.fn(resolved_date, crowded, contrarian)
    return {
        "scored_positions": scored,
        "crowded_trades": crowded,
        "contrarian_signals": contrarian,
        "alerts_dispatched": alerts,
    }


if __name__ == "__main__":
    conviction_flow()


CONVICTION_FLOW_NIGHTLY_CRON = os.getenv("CONVICTION_FLOW_CRON", "0 2 * * *")
CONVICTION_FLOW_TIMEZONE = os.getenv("CONVICTION_FLOW_TIMEZONE", os.getenv("TZ", "UTC"))
conviction_flow_deployment = conviction_flow.to_deployment(
    "conviction-nightly",
    schedule=Cron(CONVICTION_FLOW_NIGHTLY_CRON, timezone=CONVICTION_FLOW_TIMEZONE),
)
conviction_deployment = conviction_flow_deployment
