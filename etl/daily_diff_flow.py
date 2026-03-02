from __future__ import annotations

import datetime as dt
import logging
import os
import sqlite3
from typing import Any

from prefect import flow, task
from prefect.schedules import Cron

from adapters.base import connect_db
from diff_holdings import diff_holdings
from etl.logging_setup import configure_logging, log_outcome

configure_logging("daily_diff_flow")
logger = logging.getLogger(__name__)


def _placeholder(conn: Any) -> str:
    return "?" if isinstance(conn, sqlite3.Connection) else "%s"


def _is_postgres(conn: Any) -> bool:
    return not isinstance(conn, sqlite3.Connection)


def _ensure_daily_diffs_table(conn: Any) -> None:
    """Ensure daily_diffs exists for the active backend.

    SQLite test/dev runs create the table on demand. Postgres relies on
    canonical schema migrations and should fail fast if the table is missing.
    """
    if isinstance(conn, sqlite3.Connection):
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
            "does not exist" in message
            or pgcode == "42P01"
            or "UndefinedTable" in exc_type_name
        )
        if is_missing_table:
            raise RuntimeError(
                "daily_diffs table is missing on Postgres; apply schema migrations first"
            ) from exc
        # For all other errors (permissions, connectivity, syntax, etc.), re-raise the original.
        raise


def _delete_existing_diffs(conn: Any, manager_id: int, report_date: str) -> None:
    """Delete any existing diffs for this manager/date before reinserting (idempotency)."""
    ph = _placeholder(conn)
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
    ph = _placeholder(conn)
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
    if _is_postgres(conn):
        try:
            conn.execute("REFRESH MATERIALIZED VIEW mv_daily_report")
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


@task
def compute_manager_diffs(manager_id: int, report_date: str, conn: Any) -> int:
    """Compute and store diffs for a single manager. Returns change count."""
    diffs = diff_holdings(manager_id, conn)
    _delete_existing_diffs(conn, manager_id, report_date)
    _insert_diffs(conn, manager_id, report_date, diffs)
    return len(diffs)


@flow
def daily_diff_flow(date: str | None = None) -> None:
    """Compute holdings diffs for all managers and store in daily_diffs."""
    conn = connect_db()
    report_date = date or str(dt.date.today() - dt.timedelta(days=1))

    try:
        _ensure_daily_diffs_table(conn)
        manager_ids = _fetch_all_manager_ids(conn)

        if not manager_ids:
            logger.warning("No managers found in database")
            return

        # Postgres runs with autocommit=True, so an explicit transaction is
        # required to make the DELETE-then-INSERT cycle atomic per batch.
        if _is_postgres(conn):
            conn.execute("BEGIN")

        total_changes = 0
        managers_processed = 0
        managers_skipped = 0

        for mid in manager_ids:
            try:
                count = compute_manager_diffs.fn(mid, report_date, conn)
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

        if not isinstance(conn, sqlite3.Connection):
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
