"""US EDGAR wrapper flow for backward compatibility."""

from __future__ import annotations

import logging
import os

from prefect import flow, task

import etl.ingest_flow as ingest_module
from adapters.base import get_adapter
from etl.logging_setup import configure_logging, log_outcome

RAW_DIR = ingest_module.RAW_DIR
S3 = ingest_module.S3
BUCKET = ingest_module.BUCKET
DB_PATH = os.getenv("DB_PATH", "dev.db")
ADAPTER = get_adapter("edgar")

configure_logging("edgar_flow")
logger = logging.getLogger(__name__)


@task
async def fetch_and_store(cik: str, since: str):
    # Keep legacy monkeypatch points in this module synchronized with ingest_flow.
    ingest_module.RAW_DIR = RAW_DIR
    ingest_module.S3 = S3
    ingest_module.BUCKET = BUCKET
    ingest_module.DB_PATH = DB_PATH
    return await ingest_module.fetch_and_store.fn(
        cik,
        since,
        jurisdiction="us",
        adapter=ADAPTER,
        db_path=DB_PATH,
    )


@flow
async def edgar_flow(cik_list: list[str] | None = None, since: str | None = None):
    ingest_module.RAW_DIR = RAW_DIR
    ingest_module.S3 = S3
    ingest_module.BUCKET = BUCKET
    ingest_module.DB_PATH = DB_PATH
    all_rows = await ingest_module.ingest_flow(
        jurisdiction="us",
        identifiers=cik_list,
        since=since,
        fetcher=fetch_and_store,
    )
    log_outcome(
        logger,
        "EDGAR flow finished",
        has_data=bool(all_rows),
        extra={"total_rows": len(all_rows)},
    )
    return all_rows


if __name__ == "__main__":
    import asyncio

    asyncio.run(edgar_flow())
