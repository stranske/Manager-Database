"""Prefect flow that posts a daily summary to Slack."""

from __future__ import annotations

import datetime as dt
import os
from typing import Optional

import pandas as pd
import requests  # type: ignore
from prefect import flow, task

from adapters.base import connect_db


@task
def summarise(date: str) -> str:
    conn = connect_db()
    df = pd.read_sql_query(
        "SELECT cik, cusip, change FROM daily_diff WHERE date = ?",
        conn,
        params=(date,),
    )
    conn.close()
    summary = f"{len(df)} changes on {date}"
    webhook = os.getenv("SLACK_WEBHOOK_URL")
    if webhook:
        requests.post(webhook, json={"text": summary})
    return summary


@flow
def summariser_flow(date: Optional[str] = None) -> str:
    if date is None:
        date = str(dt.date.today() - dt.timedelta(days=1))
    return summarise(date)


if __name__ == "__main__":
    print(summariser_flow())
