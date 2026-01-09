"""Prefect flow that posts a daily summary to Slack."""

from __future__ import annotations

import datetime as dt
import logging
import os

import pandas as pd
import requests  # type: ignore
from prefect import flow, task

from adapters.base import connect_db, tracked_call
from etl.logging_setup import configure_logging, log_outcome

configure_logging("summariser_flow")
logger = logging.getLogger(__name__)


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
            try:
                resp = requests.post(webhook, json={"text": summary})
                log(resp)
                level = logging.INFO if resp.status_code < 400 else logging.ERROR
                logger.log(
                    level,
                    "Posted Slack summary",
                    extra={"date": date, "status": resp.status_code},
                )
            except Exception:
                logger.exception("Slack webhook failed", extra={"date": date})
                raise
    else:
        logger.warning("Slack webhook unset; skipping post", extra={"date": date})
    log_outcome(
        logger,
        "Summary generated",
        has_data=not df.empty,
        extra={"date": date, "rows": len(df)},
    )
    return summary


@flow
async def summariser_flow(date: str | None = None) -> str:
    """Run the summary task for the given date."""
    if date is None:
        date = str(dt.date.today() - dt.timedelta(days=1))
    summary = await summarise(date)
    logger.info("Summariser flow finished", extra={"date": date})
    return summary


if __name__ == "__main__":
    import asyncio

    print(asyncio.run(summariser_flow()))
