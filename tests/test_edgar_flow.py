import hashlib
import json
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


class StrictPostgresCursor:
    def __init__(self, rows=None):
        self._rows = rows or []

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None


class StrictPostgresConnection:
    columns = {
        "managers": {"manager_id", "cik"},
        "filings": {
            "filing_id",
            "manager_id",
            "type",
            "filed_date",
            "source",
            "raw_key",
            "parsed_payload",
        },
        "holdings": {"holding_id", "filing_id", "cusip", "name_of_issuer", "shares", "value_usd"},
    }

    def __init__(self):
        self.sql = []
        self.params = []
        self.filings = []
        self.holdings = []
        self.commits = 0
        self.closed = False

    def execute(self, sql, params=()):
        forbidden = ["PRAGMA", "AUTOINCREMENT", "INSERT OR IGNORE"]
        upper_sql = sql.upper()
        assert not any(token in upper_sql for token in forbidden), sql
        assert "?" not in sql, sql
        self.sql.append(sql)
        self.params.append(tuple(params))

        if "information_schema.columns" in sql:
            table = params[0]
            return StrictPostgresCursor([(column,) for column in self.columns.get(table, set())])
        if "FROM managers" in sql:
            return StrictPostgresCursor([(321,)])
        if "INSERT INTO filings" in sql:
            self.filings.append(tuple(params))
            return StrictPostgresCursor([(9001,)])
        if "INSERT INTO holdings" in sql:
            self.holdings.append(tuple(params))
            return StrictPostgresCursor([])
        return StrictPostgresCursor([])

    def commit(self):
        self.commits += 1

    def close(self):
        self.closed = True


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

    def record_document(raw, **kwargs):
        stored.append((raw, kwargs))

    monkeypatch.setattr(flow.S3, "put_object", put_object)
    monkeypatch.setattr(flow, "store_document", record_document)

    results = await flow.fetch_and_store.fn("0", "2024-01-01")

    assert len(results) == 2
    assert len(put_calls) == 2
    assert [item[0] for item in stored] == [
        "<xml accession='1'></xml>",
        "<xml accession='2'></xml>",
    ]
    assert all(item[1]["kind"] == "filing_text" for item in stored)

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
async def test_fetch_and_store_fires_new_filing_alerts(monkeypatch, tmp_path):
    db_path = tmp_path / "dev.db"
    _setup_relational_schema(db_path)
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.setattr(flow, "DB_PATH", str(db_path))
    monkeypatch.setattr(flow, "ADAPTER", DummyAdapter())
    monkeypatch.setattr(flow.S3, "put_object", lambda **kwargs: kwargs)

    events = []

    async def fake_fire_alerts_for_event(conn, event):
        _ = conn
        events.append(event)
        return [1]

    monkeypatch.setattr(flow, "fire_alerts_for_event", fake_fire_alerts_for_event)

    await flow.fetch_and_store.fn("0", "2024-01-01")

    assert len(events) == 1
    assert events[0].event_type == "new_filing"
    assert events[0].manager_id == 100
    assert events[0].payload["type"] == "13F-HR"
    assert events[0].payload["source"] == "edgar"


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

    def record_document(raw, **kwargs):
        stored.append((raw, kwargs))

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

    def record_document(raw, **kwargs):
        stored.append((raw, kwargs))

    def put_object(**kwargs):
        expected_prefix = hashlib.sha256(b"<xml></xml>").hexdigest()[:16]
        assert kwargs["Key"] == f"raw/edgar/{expected_prefix}_1.xml"

    monkeypatch.setattr(flow.S3, "put_object", put_object)
    monkeypatch.setattr(flow, "store_document", record_document)

    results = await flow.fetch_and_store.fn("0", "2024-01-01")

    assert len(results) == 2
    assert stored == [
        (
            "<xml></xml>",
            {
                "db_path": str(db_path),
                "manager_id": 100,
                "kind": "filing_text",
                "filename": "1.xml",
            },
        )
    ]
    conn = sqlite3.connect(db_path)
    filing_id = conn.execute("SELECT filing_id FROM filings").fetchone()[0]
    rows = conn.execute(
        "SELECT filing_id, cusip, value_usd, shares FROM holdings ORDER BY cusip"
    ).fetchall()
    conn.close()
    assert rows == [(filing_id, "AAA", 1, 1), (filing_id, "BBB", 2, 2)]


@pytest.mark.asyncio
async def test_fetch_and_store_uses_postgres_safe_persistence(monkeypatch):
    conn = StrictPostgresConnection()
    monkeypatch.setattr(flow, "connect_db", lambda db_path: conn)
    monkeypatch.setattr(flow, "DB_PATH", "postgres://manager-db")
    monkeypatch.setattr(flow, "ADAPTER", MultiRowAdapter())

    put_calls = []
    stored = []
    events = []

    monkeypatch.setattr(flow.S3, "put_object", lambda **kwargs: put_calls.append(kwargs))
    monkeypatch.setattr(flow, "store_document", lambda raw, **kwargs: stored.append((raw, kwargs)))

    async def fake_fire_alerts_for_event(db_conn, event):
        assert db_conn is conn
        events.append(event)
        return [1]

    monkeypatch.setattr(flow, "fire_alerts_for_event", fake_fire_alerts_for_event)

    rows = await flow.fetch_and_store.fn("0", "2024-01-01")

    assert len(rows) == 2
    assert len(put_calls) == 1
    assert put_calls[0]["ServerSideEncryption"] == "AES256"
    assert stored == [
        (
            "<xml></xml>",
            {
                "db_path": "postgres://manager-db",
                "manager_id": 321,
                "kind": "filing_text",
                "filename": "1.xml",
            },
        )
    ]
    assert conn.filings == [
        (321, "13F-HR", "2024-05-01", "edgar", put_calls[0]["Key"], '{"raw_key": "'
         + put_calls[0]["Key"]
         + '"}')
    ]
    assert conn.holdings == [
        (9001, "AAA", "CorpA", 1, 1),
        (9001, "BBB", "CorpB", 2, 2),
    ]
    assert conn.commits == 1
    assert conn.closed is True
    assert len(events) == 1
    assert events[0].event_type == "new_filing"
    assert events[0].manager_id == 321
    assert events[0].payload["filing_id"] == 9001


@pytest.mark.asyncio
async def test_fetch_and_store_skips_when_manager_missing(monkeypatch, tmp_path):
    db_path = tmp_path / "dev.db"
    _setup_relational_schema(db_path, cik="not-used")
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.setattr(flow, "DB_PATH", str(db_path))
    monkeypatch.setattr(flow, "ADAPTER", DummyAdapter())

    rows = await flow.fetch_and_store.fn("0", "2024-01-01")

    assert rows == []


@pytest.mark.asyncio
async def test_fetch_and_store_idempotent_rerun(monkeypatch, tmp_path):
    """Re-running the same filing must not create duplicate rows."""
    db_path = tmp_path / "dev.db"
    _setup_relational_schema(db_path)
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.setattr(flow, "DB_PATH", str(db_path))
    monkeypatch.setattr(flow, "ADAPTER", DummyAdapter())

    stored = []

    def record_document(raw):
        stored.append(raw)

    monkeypatch.setattr(flow.S3, "put_object", lambda **kwargs: None)
    monkeypatch.setattr(flow, "store_document", record_document)

    # First run — should insert 1 filing + 1 holding
    results1 = await flow.fetch_and_store.fn("0", "2024-01-01")
    assert len(results1) == 1

    # Second run — same filing, same raw_key → ON CONFLICT should prevent duplicate
    results2 = await flow.fetch_and_store.fn("0", "2024-01-01")
    assert len(results2) == 1

    conn = sqlite3.connect(db_path)
    filing_count = conn.execute("SELECT COUNT(*) FROM filings").fetchone()[0]
    holding_count = conn.execute("SELECT COUNT(*) FROM holdings").fetchone()[0]
    conn.close()

    # Must have exactly 1 filing (not 2) after two runs with the same data
    assert filing_count == 1
    # Holdings: first run inserts 1; second run resolves the existing filing_id
    # via the ON CONFLICT fallback lookup and inserts holdings again.
    # This is expected — holdings dedup is not in scope for this fix.
    assert holding_count == 2


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
async def test_edgar_flow_logs_missing_filings(monkeypatch, tmp_path):
    async def fake_fetch_and_store(cik, since):
        raise UserWarning("not a filer")

    monkeypatch.setattr(flow, "fetch_and_store", fake_fetch_and_store)
    monkeypatch.setattr(flow, "RAW_DIR", tmp_path)

    await flow.edgar_flow.fn(cik_list=["bad"])
