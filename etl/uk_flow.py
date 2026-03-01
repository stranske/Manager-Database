"""UK filings wrapper flow for ingest_flow."""

from __future__ import annotations

from prefect import flow

from etl.ingest_flow import ingest_flow


@flow
async def uk_flow(company_numbers: list[str] | None = None, since: str | None = None):
    return await ingest_flow(jurisdiction="uk", identifiers=company_numbers, since=since)


if __name__ == "__main__":
    import asyncio

    asyncio.run(uk_flow())
