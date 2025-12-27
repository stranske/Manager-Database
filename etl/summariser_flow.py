"""Prefect flow that posts a daily summary to Slack."""

from __future__ import annotations

import datetime as dt
import os

import pandas as pd
import requests  # type: ignore
from prefect import flow, task

from adapters.base import connect_db, tracked_call


@task
async def summarise(date: str) -> str:
    """Return and optionally post the daily change summary."""
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
        # log Slack webhook usage to api_usage table
        async with tracked_call("slack", webhook) as log:
            resp = requests.post(webhook, json={"text": summary})
            log(resp)
    return summary


@flow
async def summariser_flow(date: str | None = None) -> str:
    """Run the summary task for the given date."""
    if date is None:
        date = str(dt.date.today() - dt.timedelta(days=1))
    return await summarise(date)


if __name__ == "__main__":
    import asyncio

    print(asyncio.run(summariser_flow()))
