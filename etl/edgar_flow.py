"""Prefect flow orchestrating EDGAR pulls."""

from __future__ import annotations

import os
import json
from pathlib import Path

from prefect import flow, task

from adapters import edgar

RAW_DIR = Path(os.getenv("RAW_DIR", "./data/raw"))
RAW_DIR.mkdir(parents=True, exist_ok=True)


@task
async def fetch_and_store(cik: str, since: str):
    filings = await edgar.list_new_filings(cik, since)
    results = []
    for filing in filings:
        raw = await edgar.download(filing)
        (RAW_DIR / f"{filing['accession']}.xml").write_text(raw)
        results.extend(await edgar.parse(raw))
    return results


@flow
async def edgar_flow(cik_list: list[str] | None = None, since: str | None = None):
    if cik_list is None:
        env = os.getenv("CIK_LIST", "0001791786,0001434997")
        cik_list = [c.strip() for c in env.split(',')]
    since = since or ("1970-01-01")
    all_rows = []
    for cik in cik_list:
        try:
            rows = await fetch_and_store(cik, since)
            all_rows.extend(rows)
        except UserWarning:
            pass
    # placeholder: write to SQLite in later stages
    (RAW_DIR / "parsed.json").write_text(json.dumps(all_rows))
    return all_rows


if __name__ == "__main__":
    import asyncio
    asyncio.run(edgar_flow())
