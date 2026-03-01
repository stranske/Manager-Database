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
    placeholder = "?" if isinstance(conn, sqlite3.Connection) else "%s"
    filings_existed = bool(
        conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='filings'"
        ).fetchone()[0]
    )
    conn.execute("""CREATE TABLE IF NOT EXISTS filings (
            filing_id INTEGER PRIMARY KEY AUTOINCREMENT,
            manager_id INTEGER NOT NULL,
            type TEXT NOT NULL,
            filed_date TEXT,
            source TEXT,
            url TEXT,
            raw_key TEXT UNIQUE,
            schema_version INTEGER
        )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS holdings (
            holding_id INTEGER PRIMARY KEY AUTOINCREMENT,
            filing_id INTEGER,
            cik TEXT,
            accession TEXT,
            filed DATE,
            nameOfIssuer TEXT,
            cusip TEXT,
            value INTEGER,
            sshPrnamt INTEGER,
            name_of_issuer TEXT,
            shares INTEGER,
            value_usd INTEGER
        )""")
    managers_table_exists = bool(
        conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='managers'"
        ).fetchone()
    )
    manager_id: int | None = None
    if managers_table_exists:
        manager_columns = {row[1] for row in conn.execute("PRAGMA table_info(managers)").fetchall()}
        manager_id_col = "manager_id" if "manager_id" in manager_columns else "id"
        manager_row = conn.execute(
            f"SELECT {manager_id_col} FROM managers WHERE cik = {placeholder}",
            (cik,),
        ).fetchone()
        if manager_row is None:
            logger.warning("Manager not found; skipping filings", extra={"cik": cik})
            conn.close()
            return []
        manager_id = int(manager_row[0])

    filings_columns = {row[1] for row in conn.execute("PRAGMA table_info(filings)").fetchall()}
    holdings_columns = {row[1] for row in conn.execute("PRAGMA table_info(holdings)").fetchall()}
    results = []
    row_count = 0
    # Memory optimization: limit results accumulation for large datasets
    max_results = int(os.getenv("MAX_RESULTS_IN_MEMORY", "100000"))
    for filing in filings:
        raw = await ADAPTER.download(filing)
        accession = str(filing["accession"])
        digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]
        raw_key = f"raw/edgar/{digest}_{accession}.xml"
        S3.put_object(
            Bucket=BUCKET,
            Key=raw_key,
            Body=raw,
            ServerSideEncryption="AES256",
        )
        filing_values: dict[str, object] = {
            "manager_id": manager_id,
            "type": "13F-HR",
            "filed_date": filing.get("filed"),
            "source": "edgar",
            "url": None,
            "raw_key": raw_key,
            "schema_version": 1,
        }
        filing_insert_cols = [c for c in filing_values if c in filings_columns]
        filing_insert_params = [filing_values[c] for c in filing_insert_cols]
        if isinstance(conn, sqlite3.Connection):
            filing_id_row = None
            if manager_id is not None:
                conn.execute(
                    (
                        f"INSERT OR IGNORE INTO filings({', '.join(filing_insert_cols)}) "
                        f"VALUES ({', '.join('?' for _ in filing_insert_cols)})"
                    ),
                    filing_insert_params,
                )
                filing_id_row = conn.execute(
                    "SELECT filing_id FROM filings WHERE raw_key = ?", (raw_key,)
                ).fetchone()
        else:
            filing_id_row = None
            if manager_id is not None:
                conn.execute(
                    (
                        f"INSERT INTO filings({', '.join(filing_insert_cols)}) "
                        f"VALUES ({', '.join('%s' for _ in filing_insert_cols)}) "
                        "ON CONFLICT (raw_key) DO NOTHING"
                    ),
                    filing_insert_params,
                )
                filing_id_row = conn.execute(
                    "SELECT filing_id FROM filings WHERE raw_key = %s", (raw_key,)
                ).fetchone()
        filing_id = int(filing_id_row[0]) if filing_id_row else 0

        parsed = await ADAPTER.parse(raw)
        conn.commit()
        # TODO(S3-01): always pass resolved manager_id through once lineage rollout is complete.
        try:
            store_document(
                raw,
                db_path=DB_PATH,
                manager_id=None if filings_existed else manager_id,
                kind="filing_text",
                filename=f"{accession}.xml",
            )
        except TypeError:
            store_document(raw)
        # Check threshold before processing to decide if we keep results
        should_keep_results = row_count < max_results
        for row in parsed:
            holding_values: dict[str, object] = {
                "filing_id": filing_id,
                "cik": cik,
                "accession": accession,
                "filed": filing.get("filed"),
                "nameOfIssuer": row["nameOfIssuer"],
                "cusip": row["cusip"],
                "value": row["value"],
                "sshPrnamt": row["sshPrnamt"],
                "name_of_issuer": row["nameOfIssuer"],
                "shares": row["sshPrnamt"],
                "value_usd": row["value"],
            }
            holding_insert_cols = [c for c in holding_values if c in holdings_columns]
            if holding_insert_cols:
                holding_insert_params = [holding_values[c] for c in holding_insert_cols]
                if isinstance(conn, sqlite3.Connection):
                    conn.execute(
                        (
                            f"INSERT INTO holdings({', '.join(holding_insert_cols)}) "
                            f"VALUES ({', '.join('?' for _ in holding_insert_cols)})"
                        ),
                        holding_insert_params,
                    )
                else:
                    conn.execute(
                        (
                            f"INSERT INTO holdings({', '.join(holding_insert_cols)}) "
                            f"VALUES ({', '.join('%s' for _ in holding_insert_cols)})"
                        ),
                        holding_insert_params,
                    )
            row_count += 1
        # Commit after each filing to free transaction memory
        conn.commit()
        # Only accumulate results if we checked the threshold before processing
        # This ensures consistent filing-level behavior
        if should_keep_results:
            results.extend(parsed)
    logger.info(
        "Stored filings",
        extra={"cik": cik, "filings": len(filings), "rows": row_count},
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
