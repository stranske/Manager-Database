import hashlib
import json
import logging
import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

import etl.edgar_flow as flow


class DummyAdapter:
    async def list_new_filings(self, cik, since):
        return [{"accession": "1", "cik": cik, "filed": "2024-05-01"}]

    async def download(self, filing):
        return "<xml></xml>"

    async def parse(self, raw):
        return [{"nameOfIssuer": "Corp", "cusip": "AAA", "value": 1, "sshPrnamt": 1}]


class MultiFilingAdapter:
    async def list_new_filings(self, cik, since):
        return [
            {"accession": "1", "cik": cik, "filed": "2024-05-01"},
            {"accession": "2", "cik": cik, "filed": "2024-05-02"},
        ]

    async def download(self, filing):
        return f"<xml accession='{filing['accession']}'></xml>"

    async def parse(self, raw):
        return [{"nameOfIssuer": "Corp", "cusip": "AAA", "value": 1, "sshPrnamt": 1}]


class EmptyFilingAdapter:
    async def list_new_filings(self, cik, since):
        return []

    async def download(self, filing):
        raise AssertionError("download should not be called for empty filings")

    async def parse(self, raw):
        raise AssertionError("parse should not be called for empty filings")


class MultiRowAdapter:
    async def list_new_filings(self, cik, since):
        return [{"accession": "1", "cik": cik, "filed": "2024-05-01"}]

    async def download(self, filing):
        return "<xml></xml>"

    async def parse(self, raw):
        return [
            {"nameOfIssuer": "CorpA", "cusip": "AAA", "value": 1, "sshPrnamt": 1},
            {"nameOfIssuer": "CorpB", "cusip": "BBB", "value": 2, "sshPrnamt": 2},
        ]


def _setup_relational_schema(db_path: Path, cik: str = "0", manager_id: int = 100) -> None:
    conn = sqlite3.connect(db_path)
    conn.execute("""CREATE TABLE IF NOT EXISTS managers (
            manager_id INTEGER PRIMARY KEY,
            name TEXT,
            cik TEXT UNIQUE
        )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS filings (
            filing_id INTEGER PRIMARY KEY AUTOINCREMENT,
            manager_id INTEGER NOT NULL,
            type TEXT NOT NULL,
            filed_date TEXT,
            source TEXT,
            url TEXT,
            raw_key TEXT UNIQUE,
            schema_version INTEGER,
            FOREIGN KEY(manager_id) REFERENCES managers(manager_id)
        )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS holdings (
            holding_id INTEGER PRIMARY KEY AUTOINCREMENT,
            filing_id INTEGER NOT NULL,
            cusip TEXT,
            name_of_issuer TEXT,
            shares INTEGER,
            value_usd INTEGER,
            FOREIGN KEY(filing_id) REFERENCES filings(filing_id)
        )""")
    conn.execute(
        "INSERT INTO managers(manager_id, name, cik) VALUES (?, ?, ?)",
        (manager_id, "Manager", cik),
    )
    conn.commit()
    conn.close()


@pytest.mark.nightly
@pytest.mark.asyncio
async def test_fetch_and_store_encryption(monkeypatch, tmp_path):
    db_path = tmp_path / "dev.db"
    _setup_relational_schema(db_path)
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.setattr(flow, "DB_PATH", str(db_path))
    monkeypatch.setattr(flow, "ADAPTER", DummyAdapter())

    calls = {}

    def put_object(**kwargs):
        calls.update(kwargs)

    monkeypatch.setattr(flow.S3, "put_object", put_object)

    await flow.fetch_and_store.fn("0", "2024-01-01")

    assert calls.get("ServerSideEncryption") == "AES256"
    expected_prefix = hashlib.sha256(b"<xml></xml>").hexdigest()[:16]
    assert calls.get("Key") == f"raw/edgar/{expected_prefix}_1.xml"


@pytest.mark.asyncio
async def test_fetch_and_store_inserts_filings_and_holdings(monkeypatch, tmp_path):
    db_path = tmp_path / "dev.db"
    _setup_relational_schema(db_path)
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.setattr(flow, "DB_PATH", str(db_path))
    monkeypatch.setattr(flow, "ADAPTER", MultiFilingAdapter())

    put_calls = []

    def put_object(**kwargs):
        put_calls.append(kwargs)

    stored = []

    def record_document(raw):
        stored.append(raw)

    monkeypatch.setattr(flow.S3, "put_object", put_object)
    monkeypatch.setattr(flow, "store_document", record_document)

    results = await flow.fetch_and_store.fn("0", "2024-01-01")

    assert len(results) == 2
    assert len(put_calls) == 2
    assert stored == ["<xml accession='1'></xml>", "<xml accession='2'></xml>"]

    conn = sqlite3.connect(db_path)
    filings = conn.execute(
        "SELECT filing_id, manager_id, source, raw_key FROM filings ORDER BY filing_id"
    ).fetchall()
    holdings = conn.execute(
        "SELECT filing_id, cusip, name_of_issuer, shares, value_usd FROM holdings ORDER BY holding_id"
    ).fetchall()
    conn.close()

    assert len(filings) == 2
    assert all(row[1] == 100 for row in filings)
    assert all(row[2] == "edgar" for row in filings)
    assert filings[0][3].endswith("_1.xml")
    assert filings[1][3].endswith("_2.xml")
    assert [row[0] for row in holdings] == [filings[0][0], filings[1][0]]


@pytest.mark.asyncio
async def test_fetch_and_store_handles_empty_filings(monkeypatch, tmp_path):
    db_path = tmp_path / "dev.db"
    _setup_relational_schema(db_path)
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.setattr(flow, "DB_PATH", str(db_path))
    monkeypatch.setattr(flow, "ADAPTER", EmptyFilingAdapter())

    put_calls = []
    stored = []

    def put_object(**kwargs):
        put_calls.append(kwargs)

    def record_document(raw):
        stored.append(raw)

    monkeypatch.setattr(flow.S3, "put_object", put_object)
    monkeypatch.setattr(flow, "store_document", record_document)

    results = await flow.fetch_and_store.fn("0", "2024-01-01")

    assert results == []
    assert put_calls == []
    assert stored == []
    conn = sqlite3.connect(db_path)
    filing_count = conn.execute("SELECT COUNT(*) FROM filings").fetchone()[0]
    holding_count = conn.execute("SELECT COUNT(*) FROM holdings").fetchone()[0]
    conn.close()
    assert filing_count == 0
    assert holding_count == 0


@pytest.mark.asyncio
async def test_fetch_and_store_inserts_multiple_rows(monkeypatch, tmp_path):
    db_path = tmp_path / "dev.db"
    _setup_relational_schema(db_path)
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.setattr(flow, "DB_PATH", str(db_path))
    monkeypatch.setattr(flow, "ADAPTER", MultiRowAdapter())

    stored = []

    def record_document(raw):
        stored.append(raw)

    def put_object(**kwargs):
        expected_prefix = hashlib.sha256(b"<xml></xml>").hexdigest()[:16]
        assert kwargs["Key"] == f"raw/edgar/{expected_prefix}_1.xml"

    monkeypatch.setattr(flow.S3, "put_object", put_object)
    monkeypatch.setattr(flow, "store_document", record_document)

    results = await flow.fetch_and_store.fn("0", "2024-01-01")

    assert len(results) == 2
    assert stored == ["<xml></xml>"]
    conn = sqlite3.connect(db_path)
    filing_id = conn.execute("SELECT filing_id FROM filings").fetchone()[0]
    rows = conn.execute(
        "SELECT filing_id, cusip, value_usd, shares FROM holdings ORDER BY cusip"
    ).fetchall()
    conn.close()
    assert rows == [(filing_id, "AAA", 1, 1), (filing_id, "BBB", 2, 2)]


@pytest.mark.asyncio
async def test_fetch_and_store_skips_when_manager_missing(monkeypatch, tmp_path, caplog):
    db_path = tmp_path / "dev.db"
    _setup_relational_schema(db_path, cik="not-used")
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.setattr(flow, "DB_PATH", str(db_path))
    monkeypatch.setattr(flow, "ADAPTER", DummyAdapter())

    caplog.set_level(logging.WARNING, logger="etl.edgar_flow")
    rows = await flow.fetch_and_store.fn("0", "2024-01-01")

    assert rows == []
    assert any("Manager not found; skipping filings" in msg for msg in caplog.messages)


@pytest.mark.asyncio
async def test_edgar_flow_skips_userwarning_and_writes_json(monkeypatch, tmp_path):
    captured = {"since": None}

    async def fake_fetch_and_store(cik, since):
        if cik == "bad":
            raise UserWarning("not a filer")
        captured["since"] = since
        return [{"nameOfIssuer": "Corp", "cusip": "AAA", "value": 1, "sshPrnamt": 1}]

    monkeypatch.setattr(flow, "fetch_and_store", fake_fetch_and_store)
    monkeypatch.setattr(flow, "RAW_DIR", tmp_path)

    rows = await flow.edgar_flow.fn(cik_list=["ok", "bad"])

    assert rows == [{"nameOfIssuer": "Corp", "cusip": "AAA", "value": 1, "sshPrnamt": 1}]
    assert captured["since"] == "1970-01-01"
    parsed_path = tmp_path / "parsed.json"
    assert parsed_path.exists()
    assert json.loads(parsed_path.read_text()) == rows


@pytest.mark.asyncio
async def test_edgar_flow_default_ciks(monkeypatch, tmp_path):
    seen = []

    async def fake_fetch_and_store(cik, since):
        seen.append(cik)
        return []

    monkeypatch.setenv("CIK_LIST", "0001,0002")
    monkeypatch.setattr(flow, "fetch_and_store", fake_fetch_and_store)
    monkeypatch.setattr(flow, "RAW_DIR", tmp_path)

    await flow.edgar_flow.fn(cik_list=None, since="2024-01-01")

    assert seen == ["0001", "0002"]


@pytest.mark.asyncio
async def test_edgar_flow_logs_missing_filings(monkeypatch, tmp_path, caplog):
    async def fake_fetch_and_store(cik, since):
        raise UserWarning("not a filer")

    monkeypatch.setattr(flow, "fetch_and_store", fake_fetch_and_store)
    monkeypatch.setattr(flow, "RAW_DIR", tmp_path)

    caplog.set_level(logging.WARNING, logger="etl.edgar_flow")
    await flow.edgar_flow.fn(cik_list=["bad"])

    assert any("No filings found" in msg for msg in caplog.messages)
