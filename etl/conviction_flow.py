from __future__ import annotations

import datetime as dt
import json
import logging
import os
import sqlite3
from typing import Any

from prefect import flow, task

from adapters.base import connect_db
from etl.logging_setup import configure_logging

configure_logging("conviction_flow")
logger = logging.getLogger(__name__)


def _placeholder(conn: Any) -> str:
    return "?" if isinstance(conn, sqlite3.Connection) else "%s"


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


def _fetch_latest_conviction_rows(
    conn: Any,
    report_date: str,
) -> list[tuple[int, str, str | None, float, float | None]]:
    ph = _placeholder(conn)
    rows = conn.execute(
        f"""
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
        """,
        (report_date,),
    ).fetchall()
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
def conviction_flow(report_date: str | None = None, min_managers: int | None = None) -> int:
    """Run conviction crowded-trade detection for a date."""
    resolved_date = report_date or str(dt.date.today() - dt.timedelta(days=1))
    return detect_crowded_trades.fn(resolved_date, min_managers=min_managers)


if __name__ == "__main__":
    conviction_flow()
