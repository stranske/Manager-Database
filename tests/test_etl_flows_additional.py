import hashlib
import json
import sqlite3
from contextlib import asynccontextmanager
from datetime import date

import pytest

import etl.daily_diff_flow as daily_flow
import etl.edgar_flow as edgar_flow
import etl.summariser_flow as summariser_flow


def seed_manager(db_path, cik, manager_id=1):
    conn = sqlite3.connect(db_path)
    conn.execute("""CREATE TABLE IF NOT EXISTS managers (
            manager_id INTEGER PRIMARY KEY,
            name TEXT,
            cik TEXT UNIQUE
        )""")
    conn.execute(
        "INSERT INTO managers(manager_id, name, cik) VALUES (?, ?, ?)",
        (manager_id, "Manager", cik),
    )
    conn.commit()
    conn.close()


# Fixed date for deterministic flow defaults.
class FixedDate(date):
    @classmethod
    def today(cls):
        return cls(2024, 5, 2)


@pytest.mark.asyncio
async def test_edgar_flow_writes_parsed_json_and_skips_warnings(tmp_path, monkeypatch):
    monkeypatch.setenv("CIK_LIST", "skip,ok")
    monkeypatch.setattr(edgar_flow, "RAW_DIR", tmp_path)
    calls = []

    async def fake_fetch_and_store(cik, since):
        calls.append((cik, since))
        if cik == "skip":
            raise UserWarning("skip")
        return [
            {
                "nameOfIssuer": "Corp",
                "cusip": f"{cik}CUS",
                "value": 1,
                "sshPrnamt": 1,
            }
        ]

    # Mock task to avoid external storage and adapters.
    monkeypatch.setattr(edgar_flow, "fetch_and_store", fake_fetch_and_store)

    rows = await edgar_flow.edgar_flow.fn()

    assert calls == [("skip", "1970-01-01"), ("ok", "1970-01-01")]
    assert rows == [{"nameOfIssuer": "Corp", "cusip": "okCUS", "value": 1, "sshPrnamt": 1}]
    parsed = json.loads((tmp_path / "parsed.json").read_text())
    assert parsed == rows


@pytest.mark.asyncio
async def test_fetch_and_store_returns_empty_for_no_filings(tmp_path, monkeypatch):
    class EmptyAdapter:
        async def list_new_filings(self, cik, since):
            return []

        async def download(self, filing):
            raise AssertionError("download should not be called")

        async def parse(self, raw):
            raise AssertionError("parse should not be called")

    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.setattr(edgar_flow, "DB_PATH", str(db_path))
    monkeypatch.setattr(edgar_flow, "ADAPTER", EmptyAdapter())

    rows = await edgar_flow.fetch_and_store.fn("0", "2024-01-01")

    assert rows == []
    conn = sqlite3.connect(db_path)
    # Ensure the holdings table exists even with no filings.
    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='holdings'"
    ).fetchall()
    conn.close()
    assert tables == [("holdings",)]


@pytest.mark.asyncio
async def test_fetch_and_store_uploads_raw_and_persists_rows(tmp_path, monkeypatch):
    filing = {"accession": "0001", "filed": "2024-01-01"}

    class Adapter:
        async def list_new_filings(self, cik, since):
            return [filing]

        async def download(self, filing_info):
            assert filing_info == filing
            return "<xml>raw</xml>"

        async def parse(self, raw):
            assert raw == "<xml>raw</xml>"
            return [
                {"nameOfIssuer": "CorpA", "cusip": "AAA", "value": 10, "sshPrnamt": 1},
                {"nameOfIssuer": "CorpB", "cusip": "BBB", "value": 20, "sshPrnamt": 2},
            ]

    # Capture side effects for assertions without external services.
    recorded = {}

    def fake_store_document(raw, **kwargs):
        recorded["stored"] = raw
        recorded["store_kwargs"] = kwargs

    class DummyS3:
        def put_object(self, **kwargs):
            recorded["s3"] = kwargs

    db_path = tmp_path / "dev.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.setattr(edgar_flow, "DB_PATH", str(db_path))
    monkeypatch.setattr(edgar_flow, "ADAPTER", Adapter())
    monkeypatch.setattr(edgar_flow, "S3", DummyS3())
    monkeypatch.setattr(edgar_flow, "BUCKET", "filings-test")
    monkeypatch.setattr(edgar_flow, "store_document", fake_store_document)
    seed_manager(db_path, "0001")

    rows = await edgar_flow.fetch_and_store.fn("0001", "2024-01-01")

    assert rows == [
        {"nameOfIssuer": "CorpA", "cusip": "AAA", "value": 10, "sshPrnamt": 1},
        {"nameOfIssuer": "CorpB", "cusip": "BBB", "value": 20, "sshPrnamt": 2},
    ]
    assert recorded["stored"] == "<xml>raw</xml>"
    expected_prefix = hashlib.sha256(b"<xml>raw</xml>").hexdigest()[:16]
    assert recorded["store_kwargs"]["kind"] == "filing_text"
    assert recorded["store_kwargs"]["manager_id"] == 1
    assert recorded["store_kwargs"]["filename"] == "0001.xml"
    assert recorded["s3"] == {
        "Bucket": "filings-test",
        "Key": f"raw/edgar/{expected_prefix}_0001.xml",
        "Body": "<xml>raw</xml>",
        "ServerSideEncryption": "AES256",
    }
    conn = sqlite3.connect(db_path)
    filing = conn.execute("SELECT filing_id, manager_id, source, raw_key FROM filings").fetchone()
    rows = conn.execute(
        "SELECT filing_id, name_of_issuer, cusip, value_usd, shares FROM holdings ORDER BY cusip"
    ).fetchall()
    conn.close()
    assert filing == (1, 1, "edgar", f"raw/edgar/{expected_prefix}_0001.xml")
    assert rows == [
        (1, "CorpA", "AAA", 10, 1),
        (1, "CorpB", "BBB", 20, 2),
    ]


def _seed_canonical_schema(db_path):
    """Seed a SQLite DB with the canonical schema and sample managers/filings/holdings."""
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE managers (manager_id INTEGER PRIMARY KEY, name TEXT, cik TEXT UNIQUE)"
    )
    conn.execute(
        "CREATE TABLE filings ("
        "filing_id INTEGER PRIMARY KEY, manager_id INTEGER, "
        "type TEXT, filed_date TEXT, source TEXT, raw_key TEXT)"
    )
    conn.execute(
        "CREATE TABLE holdings ("
        "holding_id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "filing_id INTEGER, cusip TEXT, name_of_issuer TEXT, "
        "shares INTEGER, value_usd REAL)"
    )
    conn.execute(
        "CREATE TABLE daily_diffs ("
        "diff_id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "manager_id INTEGER NOT NULL, report_date TEXT NOT NULL, "
        "cusip TEXT NOT NULL, name_of_issuer TEXT, delta_type TEXT NOT NULL, "
        "shares_prev INTEGER, shares_curr INTEGER, "
        "value_prev REAL, value_curr REAL, created_at TEXT DEFAULT CURRENT_TIMESTAMP)"
    )
    # Two managers
    conn.executemany(
        "INSERT INTO managers(manager_id, name, cik) VALUES (?,?,?)",
        [(1, "FundA", "0001"), (2, "FundB", "0002")],
    )
    # Manager 1: two filings — AAA increased, BBB exited, CCC added
    conn.executemany(
        "INSERT INTO filings(filing_id, manager_id, type, filed_date, source) VALUES (?,?,?,?,?)",
        [
            (101, 1, "13F-HR", "2024-04-01", "edgar"),
            (102, 1, "13F-HR", "2024-01-01", "edgar"),
        ],
    )
    conn.executemany(
        "INSERT INTO holdings(filing_id, cusip, name_of_issuer, shares, value_usd) VALUES (?,?,?,?,?)",
        [
            (101, "AAA", "CorpA", 120, 1200),
            (101, "CCC", "CorpC", 40, 400),
            (102, "AAA", "CorpA", 100, 1000),
            (102, "BBB", "CorpB", 30, 300),
        ],
    )
    # Manager 2: only one filing — should be skipped (needs 2).
    conn.execute(
        "INSERT INTO filings(filing_id, manager_id, type, filed_date, source) "
        "VALUES (201, 2, '13F-HR', '2024-04-01', 'edgar')"
    )
    conn.execute(
        "INSERT INTO holdings(filing_id, cusip, name_of_issuer, shares, value_usd) "
        "VALUES (201, 'ZZZ', 'CorpZ', 50, 500)"
    )
    conn.commit()
    conn.close()


def test_daily_diff_flow_processes_all_managers(tmp_path, monkeypatch):
    """Flow iterates all managers, writes to daily_diffs, skips those with < 2 filings."""
    monkeypatch.setattr(daily_flow.dt, "date", FixedDate)
    db_path = str(tmp_path / "dev.db")
    _seed_canonical_schema(db_path)
    monkeypatch.setenv("DB_PATH", db_path)
    monkeypatch.delenv("DB_URL", raising=False)

    daily_flow.daily_diff_flow.fn(date=None)

    conn = sqlite3.connect(db_path)
    rows = conn.execute(
        "SELECT manager_id, report_date, cusip, delta_type FROM daily_diffs ORDER BY cusip"
    ).fetchall()
    conn.close()

    # Manager 1 should produce 3 diffs: AAA INCREASE, BBB EXIT, CCC ADD
    assert len(rows) == 3
    assert rows == [
        (1, "2024-05-01", "AAA", "INCREASE"),
        (1, "2024-05-01", "BBB", "EXIT"),
        (1, "2024-05-01", "CCC", "ADD"),
    ]


def test_daily_diff_flow_writes_all_four_delta_types(tmp_path, monkeypatch):
    """All 4 delta types (ADD, EXIT, INCREASE, DECREASE) are written to daily_diffs."""
    db_path = str(tmp_path / "dev.db")
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE managers (manager_id INTEGER PRIMARY KEY, name TEXT, cik TEXT UNIQUE)"
    )
    conn.execute(
        "CREATE TABLE filings ("
        "filing_id INTEGER PRIMARY KEY, manager_id INTEGER, "
        "type TEXT, filed_date TEXT, source TEXT)"
    )
    conn.execute(
        "CREATE TABLE holdings ("
        "holding_id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "filing_id INTEGER, cusip TEXT, name_of_issuer TEXT, "
        "shares INTEGER, value_usd REAL)"
    )
    conn.execute("INSERT INTO managers VALUES (1, 'TestFund', '0000')")
    conn.executemany(
        "INSERT INTO filings VALUES (?,?,?,?,?)",
        [(101, 1, "13F-HR", "2024-04-01", "edgar"), (102, 1, "13F-HR", "2024-01-01", "edgar")],
    )
    conn.executemany(
        "INSERT INTO holdings(filing_id, cusip, name_of_issuer, shares, value_usd) VALUES (?,?,?,?,?)",
        [
            (101, "AAA", "CorpA", 120, 1200),  # INCREASE
            (101, "CCC", "CorpC", 40, 400),  # ADD
            (101, "EEE", "CorpE", 8, 80),  # DECREASE
            (102, "AAA", "CorpA", 100, 1000),
            (102, "BBB", "CorpB", 30, 300),  # EXIT
            (102, "EEE", "CorpE", 10, 100),
        ],
    )
    conn.commit()
    conn.close()

    monkeypatch.setenv("DB_PATH", db_path)
    monkeypatch.delenv("DB_URL", raising=False)

    daily_flow.daily_diff_flow.fn(date="2024-05-01")

    conn = sqlite3.connect(db_path)
    rows = conn.execute(
        "SELECT cusip, delta_type, shares_prev, shares_curr FROM daily_diffs ORDER BY cusip"
    ).fetchall()
    conn.close()

    assert rows == [
        ("AAA", "INCREASE", 100, 120),
        ("BBB", "EXIT", 30, None),
        ("CCC", "ADD", None, 40),
        ("EEE", "DECREASE", 10, 8),
    ]


def test_daily_diff_flow_idempotent_rerun(tmp_path, monkeypatch):
    """Running the flow twice for the same date must not create duplicates."""
    db_path = str(tmp_path / "dev.db")
    _seed_canonical_schema(db_path)
    monkeypatch.setenv("DB_PATH", db_path)
    monkeypatch.delenv("DB_URL", raising=False)

    daily_flow.daily_diff_flow.fn(date="2024-05-01")
    daily_flow.daily_diff_flow.fn(date="2024-05-01")

    conn = sqlite3.connect(db_path)
    count = conn.execute("SELECT COUNT(*) FROM daily_diffs").fetchone()[0]
    conn.close()

    # Manager 1 produces 3 diffs; re-run should NOT double them.
    assert count == 3


@pytest.mark.asyncio
async def test_summarise_posts_to_slack_when_webhook_set(tmp_path, monkeypatch):
    db_file = tmp_path / "dev.db"
    conn = sqlite3.connect(db_file)
    conn.execute("CREATE TABLE daily_diff (date TEXT, cik TEXT, cusip TEXT, change TEXT)")
    conn.execute(
        "INSERT INTO daily_diff VALUES (?,?,?,?)",
        ("2024-01-02", "1", "AAA", "ADD"),
    )
    conn.commit()
    conn.close()
    monkeypatch.setenv("DB_PATH", str(db_file))
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://example.test/webhook")
    calls = {}

    @asynccontextmanager
    async def fake_tracked_call(source, endpoint):
        calls["source"] = source
        calls["endpoint"] = endpoint

        def log(resp):
            calls["status"] = resp.status_code

        yield log

    class DummyResp:
        status_code = 200
        content = b"ok"

    # Stub outbound HTTP while still logging usage.
    monkeypatch.setattr(summariser_flow, "tracked_call", fake_tracked_call)
    monkeypatch.setattr(summariser_flow.requests, "post", lambda *_args, **_kw: DummyResp())

    result = await summariser_flow.summarise.fn("2024-01-02")

    assert result == "1 changes on 2024-01-02"
    assert calls == {
        "source": "slack",
        "endpoint": "https://example.test/webhook",
        "status": 200,
    }


@pytest.mark.asyncio
async def test_summarise_skips_webhook_when_unset(tmp_path, monkeypatch):
    db_file = tmp_path / "dev.db"
    conn = sqlite3.connect(db_file)
    conn.execute("CREATE TABLE daily_diff (date TEXT, cik TEXT, cusip TEXT, change TEXT)")
    conn.executemany(
        "INSERT INTO daily_diff VALUES (?,?,?,?)",
        [
            ("2024-01-02", "1", "AAA", "ADD"),
            ("2024-01-02", "2", "BBB", "EXIT"),
        ],
    )
    conn.commit()
    conn.close()
    monkeypatch.setenv("DB_PATH", str(db_file))
    monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)

    # Guard against accidental outbound HTTP when webhook is unset.
    def fail_post(*_args, **_kwargs):
        raise AssertionError("requests.post should not be called")

    monkeypatch.setattr(summariser_flow.requests, "post", fail_post)

    result = await summariser_flow.summarise.fn("2024-01-02")

    assert result == "2 changes on 2024-01-02"


@pytest.mark.asyncio
async def test_summariser_flow_defaults_to_yesterday(monkeypatch):
    monkeypatch.setattr(summariser_flow.dt, "date", FixedDate)
    seen = {}

    async def fake_summarise(date_value):
        seen["date"] = date_value
        return "ok"

    # Avoid database access while validating the default date logic.
    monkeypatch.setattr(summariser_flow, "summarise", fake_summarise)

    result = await summariser_flow.summariser_flow.fn()

    assert result == "ok"
    assert seen["date"] == "2024-05-01"
