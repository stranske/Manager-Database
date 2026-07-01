from __future__ import annotations

import csv
import datetime as dt
import io
import logging
import os
import time
from typing import Any

from prefect import flow, task
from prefect.schedules import Cron

from adapters.base import connect_db, get_placeholder, is_postgres, is_sqlite
from alerts.integration import evaluate_and_record_alerts
from alerts.models import AlertEvent
from diff_holdings import diff_holdings
from etl.logging_setup import configure_logging, log_outcome
from tools.registry import run_contract_fields
from tools.run_contract import RunResult, new_run_id, write_artifact_bundle

configure_logging("daily_diff_flow")
logger = logging.getLogger(__name__)

DAILY_DIFF_ARTIFACT_TOOL = "daily" "_diff"

_placeholder = get_placeholder


def _ensure_daily_diffs_table(conn: Any) -> None:
    """Ensure daily_diffs exists for the active backend.

    SQLite test/dev runs create the table on demand. Postgres relies on
    canonical schema migrations and should fail fast if the table is missing.
    """
    if is_sqlite(conn):
        conn.execute("""CREATE TABLE IF NOT EXISTS daily_diffs (
                diff_id INTEGER PRIMARY KEY AUTOINCREMENT,
                manager_id INTEGER NOT NULL,
                report_date TEXT NOT NULL,
                cusip TEXT NOT NULL,
                name_of_issuer TEXT,
                delta_type TEXT NOT NULL,
                shares_prev INTEGER,
                shares_curr INTEGER,
                value_prev REAL,
                value_curr REAL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )""")
        return

    try:
        conn.execute("SELECT 1 FROM daily_diffs LIMIT 1")
    except Exception as exc:
        # Only treat clearly-identified "missing table" errors as a schema issue.
        message = str(exc)
        exc_type_name = exc.__class__.__name__
        pgcode = getattr(exc, "pgcode", None)
        # psycopg UndefinedTable typically has SQLSTATE 42P01 and/or a recognizable name/message.
        is_missing_table = (
            "does not exist" in message or pgcode == "42P01" or "UndefinedTable" in exc_type_name
        )
        if is_missing_table:
            raise RuntimeError(
                "daily_diffs table is missing on Postgres; apply schema migrations first"
            ) from exc
        # For all other errors (permissions, connectivity, syntax, etc.), re-raise the original.
        raise


def _delete_existing_diffs(conn: Any, manager_id: int, report_date: str) -> None:
    """Delete any existing diffs for this manager/date before reinserting (idempotency)."""
    ph = get_placeholder(conn)
    conn.execute(
        f"DELETE FROM daily_diffs WHERE manager_id = {ph} AND report_date = {ph}",
        (manager_id, report_date),
    )


def _insert_diffs(
    conn: Any,
    manager_id: int,
    report_date: str,
    diffs: list[dict[str, Any]],
) -> None:
    """Insert diff rows into the daily_diffs table."""
    ph = get_placeholder(conn)
    sql = (
        "INSERT INTO daily_diffs "
        "(manager_id, report_date, cusip, name_of_issuer, delta_type, "
        "shares_prev, shares_curr, value_prev, value_curr) "
        f"VALUES ({ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph})"
    )
    for row in diffs:
        conn.execute(
            sql,
            (
                manager_id,
                report_date,
                row["cusip"],
                row.get("name_of_issuer"),
                row["delta_type"],
                row.get("shares_prev"),
                row.get("shares_curr"),
                row.get("value_prev"),
                row.get("value_curr"),
            ),
        )


def _refresh_matview(conn: Any) -> None:
    """Refresh the mv_daily_report materialized view (Postgres only)."""
    if is_postgres(conn):
        try:
            conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS mv_daily_report_idx "
                "ON mv_daily_report (report_date, manager_id, cusip, delta_type)"
            )
            conn.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY mv_daily_report")
        except Exception as exc:
            # Only suppress "does not exist" errors (fresh environments
            # without the Alembic migration applied); re-raise real failures.
            if "does not exist" in str(exc).lower():
                logger.debug("mv_daily_report refresh skipped (view does not exist)")
            else:
                raise


def _fetch_all_manager_ids(conn: Any) -> list[int]:
    """Return all manager_ids from the managers table."""
    rows = conn.execute("SELECT manager_id FROM managers ORDER BY manager_id").fetchall()
    return [r[0] for r in rows]


def _daily_diff_csv(conn: Any, report_date: str) -> str:
    """Return the current report date's persisted deltas as CSV."""
    ph = get_placeholder(conn)
    rows = conn.execute(
        "SELECT cusip, name_of_issuer, delta_type, shares_prev, shares_curr, "
        "value_prev, value_curr FROM daily_diffs WHERE report_date = "
        f"{ph} ORDER BY cusip, delta_type",
        (report_date,),
    ).fetchall()
    headers = [
        "cusip",
        "name_of_issuer",
        "delta_type",
        "shares_prev",
        "shares_curr",
        "value_prev",
        "value_curr",
    ]
    output = io.StringIO()
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(headers)
    writer.writerows(rows)
    return output.getvalue()


def _daily_diff_data_quality(conn: Any, report_date: str) -> dict[str, Any]:
    ph = get_placeholder(conn)
    rows = conn.execute(
        "SELECT manager_id, cusip, shares_prev, shares_curr, value_prev, value_curr "
        "FROM daily_diffs WHERE report_date = "
        f"{ph} ORDER BY manager_id, cusip, delta_type",
        (report_date,),
    ).fetchall()
    missing_fields: list[dict[str, Any]] = []
    for row in rows:
        if len(row) < 6:
            continue
        manager_id, cusip, shares_prev, shares_curr, value_prev, value_curr = row
        for field, value in (
            ("shares_prev", shares_prev),
            ("shares_curr", shares_curr),
            ("value_prev", value_prev),
            ("value_curr", value_curr),
        ):
            if value is None:
                missing_fields.append(
                    {
                        "manager_id": manager_id,
                        "cusip": cusip,
                        "field": field,
                    }
                )
    confidence = "low" if missing_fields else "high"
    return {
        "missing_fields": missing_fields,
        "conflicts": [],
        "confidence": confidence,
    }


def _fetch_inserted_diffs(conn: Any, manager_id: int, report_date: str) -> list[dict[str, Any]]:
    ph = get_placeholder(conn)
    rows = conn.execute(
        "SELECT manager_id, report_date, cusip, name_of_issuer, delta_type, "
        "shares_prev, shares_curr, value_prev, value_curr "
        "FROM daily_diffs WHERE manager_id = "
        f"{ph} AND report_date = {ph} ORDER BY cusip, delta_type",
        (manager_id, report_date),
    ).fetchall()
    return [
        {
            "manager_id": row[0],
            "report_date": row[1],
            "cusip": row[2],
            "name_of_issuer": row[3],
            "delta_type": row[4],
            "shares_prev": row[5],
            "shares_curr": row[6],
            "value_prev": row[7],
            "value_curr": row[8],
        }
        for row in rows
    ]


def _large_delta_event_from_diff(row: dict[str, Any]) -> AlertEvent:
    raw_delta_type = str(row["delta_type"]).upper()
    normalized_delta_type = {
        "ADD": "buy",
        "INCREASE": "buy",
        "EXIT": "sell",
        "DECREASE": "sell",
    }.get(raw_delta_type, raw_delta_type.lower())
    value_prev = row.get("value_prev")
    value_curr = row.get("value_curr")
    value_usd = None
    if value_prev is not None and value_curr is not None:
        value_usd = abs(float(value_curr) - float(value_prev))
    elif value_curr is not None:
        value_usd = abs(float(value_curr))
    elif value_prev is not None:
        value_usd = abs(float(value_prev))

    payload = {
        "manager_id": row["manager_id"],
        "report_date": row["report_date"],
        "cusip": row["cusip"],
        "name_of_issuer": row.get("name_of_issuer"),
        "delta_type": normalized_delta_type,
        "raw_delta_type": raw_delta_type,
        "shares_prev": row.get("shares_prev"),
        "shares_curr": row.get("shares_curr"),
        "value_prev": value_prev,
        "value_curr": value_curr,
        "value_usd": value_usd,
    }
    return AlertEvent(
        event_type="large_delta",
        manager_id=int(row["manager_id"]),
        payload=payload,
    )


def _record_large_delta_alerts(conn: Any, manager_id: int, report_date: str) -> list[int]:
    alert_ids: list[int] = []
    for row in _fetch_inserted_diffs(conn, manager_id, report_date):
        alert_ids.extend(evaluate_and_record_alerts(conn, _large_delta_event_from_diff(row)))
    return alert_ids


@task
def compute_manager_diffs(manager_id: int, report_date: str, conn: Any) -> int:
    """Compute and store diffs for a single manager. Returns change count."""
    diffs = diff_holdings(manager_id, conn).deltas
    _delete_existing_diffs(conn, manager_id, report_date)
    _insert_diffs(conn, manager_id, report_date, diffs)
    return len(diffs)


@flow
def daily_diff_flow(date: str | None = None) -> RunResult:
    """Compute holdings diffs for all managers and store in daily_diffs.

    Returns a replayable :class:`RunResult` whose ``outputs`` summarizes the run
    (``managers_processed``, ``managers_skipped``, ``total_changes``) and whose
    ``warnings`` records any managers skipped for having fewer than two filings.
    """
    start = time.perf_counter()
    conn = connect_db()
    report_date = date or str(dt.date.today() - dt.timedelta(days=1))
    run_id = new_run_id()

    try:
        _ensure_daily_diffs_table(conn)
        manager_ids = _fetch_all_manager_ids(conn)

        if not manager_ids:
            logger.warning("No managers found in database")
            artifacts = write_artifact_bundle(
                run_id,
                DAILY_DIFF_ARTIFACT_TOOL,
                {"deltas.csv": _daily_diff_csv(conn, report_date)},
                inputs={"date": report_date},
            )
            return RunResult(
                run_id=run_id,
                tool="daily_diff_flow",
                inputs={"date": report_date},
                outputs={
                    "managers_processed": 0,
                    "managers_skipped": 0,
                    "total_changes": 0,
                    "data_quality": {
                        "missing_fields": [],
                        "conflicts": [],
                        "confidence": "unknown",
                    },
                },
                artifacts=artifacts,
                **run_contract_fields("daily_diff_flow"),
                warnings=["No managers found in database"],
                latency_ms=int((time.perf_counter() - start) * 1000),
                status="success",
            )

        # Postgres runs with autocommit=True, so an explicit transaction is
        # required to make the DELETE-then-INSERT cycle atomic per batch.
        if is_postgres(conn):
            conn.execute("BEGIN")

        total_changes = 0
        managers_processed = 0
        managers_skipped = 0
        warnings: list[str] = []

        for mid in manager_ids:
            try:
                count = compute_manager_diffs.fn(mid, report_date, conn)
                if count:
                    _record_large_delta_alerts(conn, mid, report_date)
                total_changes += count
                managers_processed += 1
                log_outcome(
                    logger,
                    "Manager diff computed",
                    has_data=count > 0,
                    extra={
                        "manager_id": mid,
                        "date": report_date,
                        "changes": count,
                    },
                )
            except SystemExit:
                # diff_holdings raises SystemExit when < 2 filings exist.
                managers_skipped += 1
                warnings.append(f"manager {mid} skipped (< 2 filings)")
                logger.debug(
                    "Skipped manager %d (< 2 filings)",
                    mid,
                    extra={"manager_id": mid},
                )
            except Exception:
                logger.exception(
                    "Daily diff failed for manager %d",
                    mid,
                    extra={"manager_id": mid, "date": report_date},
                )
                raise

        if not is_sqlite(conn):
            conn.execute("COMMIT")
        else:
            conn.commit()

        _refresh_matview(conn)

        logger.info(
            "Daily diff flow finished",
            extra={
                "date": report_date,
                "managers_processed": managers_processed,
                "managers_skipped": managers_skipped,
                "total_changes": total_changes,
            },
        )

        artifacts = write_artifact_bundle(
            run_id,
            DAILY_DIFF_ARTIFACT_TOOL,
            {"deltas.csv": _daily_diff_csv(conn, report_date)},
            inputs={"date": report_date},
        )

        return RunResult(
            run_id=run_id,
            tool="daily_diff_flow",
            inputs={"date": report_date},
            outputs={
                "managers_processed": managers_processed,
                "managers_skipped": managers_skipped,
                "total_changes": total_changes,
                "data_quality": _daily_diff_data_quality(conn, report_date),
            },
            artifacts=artifacts,
            **run_contract_fields("daily_diff_flow"),
            warnings=warnings,
            latency_ms=int((time.perf_counter() - start) * 1000),
            status="success",
        )
    finally:
        conn.close()


if __name__ == "__main__":
    daily_diff_flow()


# Prefect deployment with daily schedule at 08:00 local time
def _resolve_local_timezone() -> str:
    env = os.getenv("TZ")
    if env:
        return env

    tzinfo = dt.datetime.now().astimezone().tzinfo
    tz_key = getattr(tzinfo, "key", None) if tzinfo else None
    if isinstance(tz_key, str) and tz_key:
        return tz_key  # Prefer canonical IANA identifier.

    try:
        localtime_path = os.path.realpath("/etc/localtime")
    except FileNotFoundError:
        localtime_path = None

    if localtime_path:
        for zone_root in (
            "/usr/share/zoneinfo/",
            "/usr/lib/zoneinfo/",
            "/var/db/timezone/zoneinfo/",
        ):
            if localtime_path.startswith(zone_root):
                return localtime_path[len(zone_root) :]
    return "UTC"


LOCAL_TZ = _resolve_local_timezone()
daily_diff_deployment = daily_diff_flow.to_deployment(
    "daily-diff",
    schedule=Cron("0 8 * * *", timezone=LOCAL_TZ),
)
