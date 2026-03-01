from __future__ import annotations

import datetime as dt
import logging
import os
import sqlite3

from prefect import flow, task
from prefect.schedules import Cron

from adapters.base import connect_db
from diff_holdings import diff_holdings
from etl.logging_setup import configure_logging, log_outcome

configure_logging("daily_diff_flow")
logger = logging.getLogger(__name__)


def _placeholder(conn) -> str:
    return "?" if isinstance(conn, sqlite3.Connection) else "%s"


def _fetch_managers(conn, cik_list: list[str] | None = None) -> list[tuple[int, str | None]]:
    placeholder = _placeholder(conn)
    cleaned = [cik.strip() for cik in (cik_list or []) if cik and cik.strip()]
    if cleaned:
        values = ",".join([placeholder] * len(cleaned))
        cursor = conn.execute(
            f"SELECT manager_id, cik FROM managers WHERE cik IN ({values}) ORDER BY manager_id",
            tuple(cleaned),
        )
        return [(int(manager_id), cik) for manager_id, cik in cursor.fetchall()]
    cursor = conn.execute("SELECT manager_id, cik FROM managers ORDER BY manager_id")
    return [(int(manager_id), cik) for manager_id, cik in cursor.fetchall()]


@task
def compute(date: str, db_path: str, cik_list: list[str] | None = None) -> None:
    try:
        conn = connect_db(db_path)
        placeholder = _placeholder(conn)
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
                value_curr REAL
            )""")

        managers = _fetch_managers(conn, cik_list)
        total_changes = 0

        for manager_id, _cik in managers:
            try:
                raw_diffs = diff_holdings(manager_id, conn)
            except SystemExit:
                # Some managers may not yet have enough filings to diff.
                continue

            conn.execute(
                f"DELETE FROM daily_diffs WHERE report_date = {placeholder} AND manager_id = {placeholder}",
                (date, manager_id),
            )
            for row in raw_diffs:
                conn.execute(
                    "INSERT INTO daily_diffs ("
                    "manager_id, report_date, cusip, name_of_issuer, delta_type, "
                    "shares_prev, shares_curr, value_prev, value_curr"
                    f") VALUES ({','.join([placeholder] * 9)})",
                    (
                        manager_id,
                        date,
                        row.get("cusip"),
                        row.get("name_of_issuer"),
                        row.get("delta_type"),
                        row.get("shares_prev"),
                        row.get("shares_curr"),
                        row.get("value_prev"),
                        row.get("value_curr"),
                    ),
                )
            total_changes += len(raw_diffs)

        conn.commit()
        conn.close()
        log_outcome(
            logger,
            "Daily diff computed",
            has_data=total_changes > 0,
            extra={
                "date": date,
                "managers": len(managers),
                "changes": total_changes,
            },
        )
    except Exception:
        logger.exception("Daily diff failed", extra={"date": date})
        raise


@flow
def daily_diff_flow(cik_list: list[str] | None = None, date: str | None = None):
    if cik_list is None:
        env = os.getenv("CIK_LIST", "0001791786,0001434997")
        cik_list = [c.strip() for c in env.split(",")]
    db_path = os.getenv("DB_PATH", "dev.db")
    date = date or str(dt.date.today() - dt.timedelta(days=1))
    compute(date, db_path, cik_list)
    conn = connect_db(db_path)
    try:
        if isinstance(conn, sqlite3.Connection):
            logger.info("Skipping mv_daily_report refresh for SQLite backend")
        else:
            conn.execute("REFRESH MATERIALIZED VIEW mv_daily_report")
    finally:
        conn.close()
    logger.info("Daily diff flow finished", extra={"date": date, "ciks": len(cik_list)})


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
DAILY_DIFF_CRON = "0 8 * * *"
daily_diff_deployment = daily_diff_flow.to_deployment(
    "daily-diff",
    schedule=Cron(DAILY_DIFF_CRON, timezone=LOCAL_TZ),
)
