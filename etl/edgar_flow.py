"""Prefect flow orchestrating EDGAR pulls."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import sqlite3
from pathlib import Path

import boto3
from prefect import flow, task

from adapters.base import connect_db, get_adapter
from embeddings import store_document
from etl.logging_setup import configure_logging, log_outcome

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

configure_logging("edgar_flow")
logger = logging.getLogger(__name__)


@task
async def fetch_and_store(cik: str, since: str):
    filings = await ADAPTER.list_new_filings(cik, since)
    conn = connect_db(DB_PATH)
    is_sqlite = isinstance(conn, sqlite3.Connection)
    placeholder = "?" if is_sqlite else "%s"
    manager_cursor = conn.execute(
        f"SELECT manager_id FROM managers WHERE cik = {placeholder}",
        (cik,),
    )
    manager_row = manager_cursor.fetchone()
    if not manager_row:
        logger.warning("Manager not found; skipping filings", extra={"cik": cik})
        conn.close()
        return []
    manager_id = manager_row[0]
    results = []
    row_count = 0
    # Memory optimization: limit results accumulation for large datasets
    max_results = int(os.getenv("MAX_RESULTS_IN_MEMORY", "100000"))
    for filing in filings:
        raw = await ADAPTER.download(filing)
        raw_bytes = raw.encode("utf-8") if isinstance(raw, str) else raw
        raw_key = (
            f"raw/edgar/{hashlib.sha256(raw_bytes).hexdigest()[:16]}_{filing['accession']}.xml"
        )
        filing_url = (
            "https://www.sec.gov/Archives/edgar/data/"
            f"{int(str(cik).zfill(10))}/{filing['accession'].replace('-', '')}/primary_doc.xml"
        )
        S3.put_object(
            Bucket=BUCKET,
            Key=raw_key,
            Body=raw,
            ServerSideEncryption="AES256",
        )
        if is_sqlite:
            filing_cursor = conn.execute(
                """INSERT INTO filings (manager_id, type, filed_date, source, url, raw_key, schema_version)
                   VALUES (?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT DO NOTHING
                   RETURNING filing_id""",
                (manager_id, "13F-HR", filing["filed"], "edgar", filing_url, raw_key, 1),
            )
        else:
            filing_cursor = conn.execute(
                """INSERT INTO filings (manager_id, type, filed_date, source, url, raw_key, schema_version)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)
                   ON CONFLICT DO NOTHING
                   RETURNING filing_id""",
                (manager_id, "13F-HR", filing["filed"], "edgar", filing_url, raw_key, 1),
            )
        filing_row = filing_cursor.fetchone()
        if filing_row:
            filing_id = filing_row[0]
        else:
            filing_lookup_cursor = conn.execute(
                f"""SELECT filing_id FROM filings
                    WHERE manager_id = {placeholder} AND raw_key = {placeholder}""",
                (manager_id, raw_key),
            )
            existing = filing_lookup_cursor.fetchone()
            if not existing:
                logger.warning(
                    "Unable to resolve filing id after insert",
                    extra={"cik": cik, "manager_id": manager_id, "raw_key": raw_key},
                )
                continue
            filing_id = existing[0]
        parsed = await ADAPTER.parse(raw)
        # TODO(S6-01): pass manager_id and kind='filing_text' once store_document supports metadata.
        store_document(raw)
        # Check threshold before processing to decide if we keep results
        should_keep_results = row_count < max_results
        for row in parsed:
            if is_sqlite:
                conn.execute(
                    """INSERT INTO holdings (filing_id, cusip, name_of_issuer, shares, value_usd)
                       VALUES (?, ?, ?, ?, ?)""",
                    (
                        filing_id,
                        row["cusip"],
                        row["nameOfIssuer"],
                        row["sshPrnamt"],
                        row["value"],
                    ),
                )
            else:
                conn.execute(
                    """INSERT INTO holdings (filing_id, cusip, name_of_issuer, shares, value_usd)
                       VALUES (%s, %s, %s, %s, %s)""",
                    (
                        filing_id,
                        row["cusip"],
                        row["nameOfIssuer"],
                        row["sshPrnamt"],
                        row["value"],
                    ),
                )
            row_count += 1
        # Commit after each filing to free transaction memory
        conn.commit()
        logger.info(
            "Stored filing and holdings",
            extra={
                "cik": cik,
                "manager_id": manager_id,
                "filing_id": filing_id,
                "rows_in_filing": len(parsed),
            },
        )
        # Only accumulate results if we checked the threshold before processing
        # This ensures consistent filing-level behavior
        if should_keep_results:
            results.extend(parsed)
    logger.info(
        "Stored filings",
        extra={
            "cik": cik,
            "manager_id": manager_id,
            "filings": len(filings),
            "rows": row_count,
        },
    )
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
            log_outcome(
                logger,
                "EDGAR flow completed",
                has_data=bool(rows),
                extra={"cik": cik, "rows": len(rows)},
            )
        except UserWarning:
            logger.warning("No filings found", extra={"cik": cik, "since": since})
        except Exception:
            logger.exception("EDGAR flow failed", extra={"cik": cik, "since": since})
    (RAW_DIR / "parsed.json").write_text(json.dumps(all_rows))
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
