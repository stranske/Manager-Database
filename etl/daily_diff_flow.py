from __future__ import annotations

import datetime as dt
import logging
import os

from prefect import flow, task
from prefect.schedules import Cron

from adapters.base import connect_db
from diff_holdings import diff_holdings
from etl.logging_setup import configure_logging, log_outcome

configure_logging("daily_diff_flow")
logger = logging.getLogger(__name__)


@task
def compute(cik: str, date: str, db_path: str) -> None:
    try:
        additions, exits = diff_holdings(cik, db_path)
        conn = connect_db(db_path)
        conn.execute(
            """CREATE TABLE IF NOT EXISTS daily_diff (
                date TEXT,
                cik TEXT,
                cusip TEXT,
                change TEXT
            )"""
        )
        for cusip in additions:
            conn.execute(
                "INSERT INTO daily_diff VALUES (?,?,?,?)",
                (date, cik, cusip, "ADD"),
            )
        for cusip in exits:
            conn.execute(
                "INSERT INTO daily_diff VALUES (?,?,?,?)",
                (date, cik, cusip, "EXIT"),
            )
        conn.commit()
        conn.close()
        total_changes = len(additions) + len(exits)
        log_outcome(
            logger,
            "Daily diff computed",
            has_data=total_changes > 0,
            extra={
                "cik": cik,
                "date": date,
                "additions": len(additions),
                "exits": len(exits),
                "changes": total_changes,
            },
        )
    except Exception:
        logger.exception("Daily diff failed", extra={"cik": cik, "date": date})
        raise


@flow
def daily_diff_flow(cik_list: list[str] | None = None, date: str | None = None):
    if cik_list is None:
        env = os.getenv("CIK_LIST", "0001791786,0001434997")
        cik_list = [c.strip() for c in env.split(",")]
    db_path = os.getenv("DB_PATH", "dev.db")
    date = date or str(dt.date.today() - dt.timedelta(days=1))
    for cik in cik_list:
        compute(cik, date, db_path)
    logger.info("Daily diff flow finished", extra={"date": date, "ciks": len(cik_list)})


if __name__ == "__main__":
    daily_diff_flow()


# Prefect deployment with daily schedule at 08:00 local time
def _resolve_local_timezone() -> str:
    env = os.getenv("TZ")
    if env:
        return env

    tzinfo = dt.datetime.now().astimezone().tzinfo
    if tzinfo and getattr(tzinfo, "key", None):
        return tzinfo.key  # Prefer canonical IANA identifier.

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
