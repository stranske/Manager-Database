"""UK filings wrapper flow for ingest_flow."""

from __future__ import annotations

import os

from prefect import flow
from prefect.schedules import Cron

from etl.ingest_flow import ingest_flow


@flow
async def uk_flow(company_numbers: list[str] | None = None, since: str | None = None):
    return await ingest_flow(jurisdiction="uk", identifiers=company_numbers, since=since)


UK_FLOW_NIGHTLY_CRON = os.getenv("UK_FLOW_CRON", "0 1 * * *")
UK_FLOW_TIMEZONE = os.getenv("UK_FLOW_TIMEZONE", os.getenv("TZ", "UTC"))
uk_flow_deployment = uk_flow.to_deployment(
    "uk-nightly",
    schedule=Cron(UK_FLOW_NIGHTLY_CRON, timezone=UK_FLOW_TIMEZONE),
)


if __name__ == "__main__":
    import asyncio

    asyncio.run(uk_flow())
