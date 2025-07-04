from __future__ import annotations

import datetime as dt
import os

from prefect import flow, task

from adapters.base import connect_db
from diff_holdings import diff_holdings


@task
def compute(cik: str, date: str, db_path: str) -> None:
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


@flow
def daily_diff_flow(cik_list: list[str] | None = None, date: str | None = None):
    if cik_list is None:
        env = os.getenv("CIK_LIST", "0001791786,0001434997")
        cik_list = [c.strip() for c in env.split(",")]
    db_path = os.getenv("DB_PATH", "dev.db")
    date = date or str(dt.date.today() - dt.timedelta(days=1))
    for cik in cik_list:
        compute(cik, date, db_path)


if __name__ == "__main__":
    daily_diff_flow()
