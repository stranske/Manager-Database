"""Prefect flow orchestrating EDGAR pulls."""

from __future__ import annotations

import os
import json
from pathlib import Path

import boto3
from prefect import flow, task

from adapters.base import connect_db, get_adapter
from embeddings import store_document

RAW_DIR = Path(os.getenv("RAW_DIR", "./data/raw"))
RAW_DIR.mkdir(parents=True, exist_ok=True)

S3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
    aws_access_key_id=os.getenv("MINIO_ROOT_USER", "minio"),
    aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD", "minio123"),
    region_name="us-east-1",
)
BUCKET = os.getenv("MINIO_BUCKET", "filings")
DB_PATH = os.getenv("DB_PATH", "dev.db")
JURISDICTION = os.getenv("JURISDICTION", "us")
_MAP = {"us": "edgar", "uk": "uk", "ca": "canada"}
ADAPTER = get_adapter(_MAP.get(JURISDICTION, "edgar"))


@task
async def fetch_and_store(cik: str, since: str):
    filings = await ADAPTER.list_new_filings(cik, since)
    conn = connect_db(DB_PATH)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS holdings (
            cik TEXT,
            accession TEXT,
            filed DATE,
            nameOfIssuer TEXT,
            cusip TEXT,
            value INTEGER,
            sshPrnamt INTEGER
        )
        """
    )
    results = []
    for filing in filings:
        raw = await ADAPTER.download(filing)
        S3.put_object(
            Bucket=BUCKET,
            Key=f"raw/{filing['accession']}.xml",
            Body=raw,
            ServerSideEncryption="AES256",
        )
        parsed = await ADAPTER.parse(raw)
        store_document(raw)
        for row in parsed:
            conn.execute(
                "INSERT INTO holdings VALUES (?,?,?,?,?,?,?)",
                (
                    cik,
                    filing["accession"],
                    filing["filed"],
                    row["nameOfIssuer"],
                    row["cusip"],
                    row["value"],
                    row["sshPrnamt"],
                ),
            )
        results.extend(parsed)
    conn.commit()
    conn.close()
    return results


@flow
async def edgar_flow(cik_list: list[str] | None = None, since: str | None = None):
    if cik_list is None:
        env = os.getenv("CIK_LIST", "0001791786,0001434997")
        cik_list = [c.strip() for c in env.split(",")]
    since = since or ("1970-01-01")
    all_rows = []
    for cik in cik_list:
        try:
            rows = await fetch_and_store(cik, since)
            all_rows.extend(rows)
        except UserWarning:
            pass
    (RAW_DIR / "parsed.json").write_text(json.dumps(all_rows))
    return all_rows


if __name__ == "__main__":
    import asyncio

    asyncio.run(edgar_flow())
