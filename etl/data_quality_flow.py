"""Daily standing checks over persisted ingestion data, with replayable delivery."""

from __future__ import annotations

import json
import logging
from dataclasses import asdict
from datetime import UTC, datetime
from typing import Any

from prefect import flow
from prefect.schedules import Cron

from adapters.base import connect_db, get_placeholder
from alerts.data_quality import DataQualityCheckResult, evaluate_daily_quality
from alerts.integration import fire_data_quality_alerts
from etl.daily_diff_flow import LOCAL_TZ

logger = logging.getLogger(__name__)
TABLES = ("filings", "holdings", "news_items")


def _timestamp(value: Any) -> datetime | None:
    if value is None:
        return None
    try:
        parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
    except ValueError:
        logger.warning("Unparseable timestamp in quality input")
        return None
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)


def _daily_snapshot(conn: Any, now: datetime) -> dict[str, Any]:
    """Commit one immutable snapshot per UTC date before attempting delivery.

    Landing evidence is the latest persisted created_at, not a claim that a
    scheduler ran. Row-count baselines are the latest earlier daily snapshot.
    """
    ph = get_placeholder(conn)
    check_date = now.date().isoformat()
    conn.execute("""CREATE TABLE IF NOT EXISTS data_quality_runs (
        check_date TEXT PRIMARY KEY,
        snapshot_json TEXT NOT NULL
    )""")
    row = conn.execute(
        f"SELECT snapshot_json FROM data_quality_runs WHERE check_date = {ph}", (check_date,)
    ).fetchone()
    if row is not None:
        return json.loads(row[0])
    previous = conn.execute(
        f"SELECT snapshot_json FROM data_quality_runs WHERE check_date < {ph} "
        "ORDER BY check_date DESC LIMIT 1",
        (check_date,),
    ).fetchone()
    baseline = json.loads(previous[0])["counts"] if previous else {}
    counts = {
        table: int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]) for table in TABLES
    }
    edgar = conn.execute(
        "SELECT MAX(created_at), COUNT(*) FROM filings WHERE source IN ('edgar', 'sec')"
    ).fetchone()
    news = conn.execute(
        "SELECT MAX(created_at), MAX(published_at), COUNT(*) FROM news_items"
    ).fetchone()
    unmapped, total = conn.execute("""SELECT
        COUNT(DISTINCT CASE WHEN
            COALESCE(NULLIF(h.resolved_ticker, ''), NULLIF(h.resolved_figi, ''),
                     NULLIF(c.ticker, ''), NULLIF(c.figi, '')) IS NULL THEN h.cusip END),
        COUNT(DISTINCT h.cusip)
        FROM holdings h LEFT JOIN identifier_resolution_cache c ON c.cusip = h.cusip
        WHERE h.superseded_at IS NULL AND TRIM(COALESCE(h.cusip, '')) <> ''
    """).fetchone()
    results = evaluate_daily_quality(
        now=now,
        flow_landed_at={"edgar-nightly": _timestamp(edgar[0]), "news-hourly": _timestamp(news[0])},
        flow_row_counts={"edgar-nightly": int(edgar[1]), "news-hourly": int(news[2])},
        table_counts={
            table: (count, baseline.get(table, count)) for table, count in counts.items()
        },
        news_max_published_at=_timestamp(news[1]),
        unmapped_cusip_rate_pct=100.0 * unmapped / total if total else 0.0,
    )
    snapshot = {
        "check_date": check_date,
        "evaluated_at": now.isoformat(),
        "counts": counts,
        "results": [asdict(result) for result in results],
    }
    conn.execute(
        f"INSERT INTO data_quality_runs(check_date, snapshot_json) VALUES ({ph}, {ph}) "
        "ON CONFLICT(check_date) DO NOTHING",
        (check_date, json.dumps(snapshot, sort_keys=True)),
    )
    conn.commit()
    # Concurrent check calls use the same winning snapshot and delivery key.
    row = conn.execute(
        f"SELECT snapshot_json FROM data_quality_runs WHERE check_date = {ph}", (check_date,)
    ).fetchone()
    return json.loads(row[0])


@flow
def data_quality_flow(now: datetime | None = None) -> dict[str, Any]:
    """Check persisted harvests and deliver failures without failing their owning flows."""
    current = _timestamp(now) or datetime.now(UTC)
    conn = connect_db()
    try:
        snapshot = _daily_snapshot(conn, current)
        results = [DataQualityCheckResult(**result) for result in snapshot["results"]]
        warnings: list[str] = []
        try:
            alert_ids = fire_data_quality_alerts(conn, results, occurred_at=current)
        except Exception:
            conn.rollback()
            warnings.append(
                "Alert delivery incomplete; reconcile durable outbox claims before retry"
            )
            logger.exception("Data-quality alert delivery incomplete")
            alert_ids = []
        failures = [result.name for result in results if result.failed]
        if failures:
            logger.warning("Data-quality checks failed: %s", ", ".join(failures))
        return {**snapshot, "failures": failures, "alert_ids": alert_ids, "warnings": warnings}
    finally:
        conn.close()


data_quality_deployment = data_quality_flow.to_deployment(
    "data-quality", schedule=Cron("30 8 * * *", timezone=LOCAL_TZ)
)


if __name__ == "__main__":
    data_quality_flow()
