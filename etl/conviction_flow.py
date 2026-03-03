"""Prefect flow for per-filing conviction score computation."""

from __future__ import annotations

import logging
import sqlite3
import time
from typing import Any

from prefect import flow, task
from prefect.schedules import Cron

from adapters.base import connect_db
from etl.logging_setup import configure_logging, log_outcome

configure_logging("conviction_flow")
logger = logging.getLogger(__name__)


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


@flow(name="conviction-scoring")
def conviction_flow() -> dict[str, int]:
    """Main conviction scoring flow: scores latest filing per manager."""
    conn = connect_db()
    start = time.perf_counter()

    try:
        _ensure_conviction_scores_table(conn)
        _ensure_api_usage_table(conn)

        result = score_all_latest_filings.fn(conn)
        latency_ms = int((time.perf_counter() - start) * 1000)
        _record_flow_usage(
            conn,
            status=200,
            scores_computed=result["scores_computed"],
            latency_ms=latency_ms,
        )

        if isinstance(conn, sqlite3.Connection):
            conn.commit()

        log_outcome(
            logger,
            "Conviction scoring flow completed",
            has_data=result["scores_computed"] > 0,
            extra={"filings_scored": result["filings_scored"], **result},
        )
        return result
    except Exception:
        latency_ms = int((time.perf_counter() - start) * 1000)
        try:
            _record_flow_usage(conn, status=500, scores_computed=0, latency_ms=latency_ms)
            if isinstance(conn, sqlite3.Connection):
                conn.commit()
        except Exception:
            logger.debug("Failed to record api_usage row for failed conviction flow", exc_info=True)
        raise
    finally:
        conn.close()


conviction_deployment = conviction_flow.to_deployment(
    name="conviction-nightly",
    schedule=Cron("0 2 * * *", timezone="UTC"),
)


if __name__ == "__main__":
    conviction_flow()
